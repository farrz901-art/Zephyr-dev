from __future__ import annotations

import hashlib
import importlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, cast
from uuid import uuid4

from uns_stream.backends.base import PartitionBackend
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import (
    DocumentMetadata,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)

S3_DOCUMENT_SOURCE_KIND = "s3_document_v1"
_CONTENT_TYPE_EXTENSION_MAP: dict[str, str] = {
    "application/json": ".json",
    "application/pdf": ".pdf",
    "application/xml": ".xml",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/plain": ".txt",
    "text/xml": ".xml",
}


class S3ObjectBodyProtocol(Protocol):
    def read(self) -> bytes: ...


class S3DocumentSourceClientProtocol(Protocol):
    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
        VersionId: str | None = None,
    ) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class S3DocumentSourceConfigV1:
    bucket: str
    key: str
    region: str
    access_key: str
    secret_key: str
    endpoint_url: str | None
    session_token: str | None
    version_id: str | None


@dataclass(frozen=True, slots=True)
class S3DocumentSourceFetchV1:
    bucket: str
    key: str
    version_id: str | None
    etag: str | None
    filename: str
    mime_type: str | None
    content: bytes


def is_s3_document_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == S3_DOCUMENT_SOURCE_KIND


def load_s3_document_source_config(raw: dict[str, object]) -> S3DocumentSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("uns-stream S3 source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    def _required_str(field_name: str) -> str:
        value = typed_source.get(field_name)
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"uns-stream S3 source field 'source.{field_name}' must be a non-empty string"
            )
        return value

    def _optional_str(field_name: str) -> str | None:
        value = typed_source.get(field_name)
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"uns-stream S3 source field 'source.{field_name}' must be a non-empty string"
            )
        return value

    return S3DocumentSourceConfigV1(
        bucket=_required_str("bucket"),
        key=_required_str("key"),
        region=_required_str("region"),
        access_key=_required_str("access_key"),
        secret_key=_required_str("secret_key"),
        endpoint_url=_optional_str("endpoint_url"),
        session_token=_optional_str("session_token"),
        version_id=_optional_str("version_id"),
    )


def normalize_s3_document_source_identity_sha(*, config: S3DocumentSourceConfigV1) -> str:
    canonical = json.dumps(
        {
            "kind": S3_DOCUMENT_SOURCE_KIND,
            "bucket": config.bucket,
            "key": config.key,
            "region": config.region,
            "endpoint_url": config.endpoint_url,
            "version_id": config.version_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _source_error(
    *,
    message: str,
    retryable: bool,
    details: dict[str, object] | None = None,
) -> ZephyrError:
    merged_details: dict[str, object] = {
        "retryable": retryable,
        "source_kind": S3_DOCUMENT_SOURCE_KIND,
    }
    if details is not None:
        merged_details.update(details)
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=message,
        details=merged_details,
    )


def _status_retryable(*, status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _exception_retryable(*, exc: Exception) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return "timeout" in text or "connection" in text or "endpoint" in text


def _normalize_mime_type(value: str | None) -> str | None:
    if value is None:
        return None
    mime_type = value.split(";", 1)[0].strip().lower()
    if not mime_type:
        return None
    return mime_type


def _infer_filename(*, key: str, mime_type: str | None) -> str:
    basename = Path(key).name
    if basename:
        if Path(basename).suffix:
            return basename
        inferred_ext = _CONTENT_TYPE_EXTENSION_MAP.get(mime_type or "")
        if inferred_ext is not None:
            return f"{basename}{inferred_ext}"
        return basename

    fallback_ext = _CONTENT_TYPE_EXTENSION_MAP.get(mime_type or "")
    if fallback_ext is not None:
        return f"downloaded{fallback_ext}"
    return "downloaded"


def _build_s3_document_source_client(
    *,
    config: S3DocumentSourceConfigV1,
) -> S3DocumentSourceClientProtocol:
    boto3 = importlib.import_module("boto3")
    session_module = getattr(boto3, "session")
    session_cls = getattr(session_module, "Session")
    session = session_cls(
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        aws_session_token=config.session_token,
        region_name=config.region,
    )
    client_kwargs: dict[str, object] = {
        "endpoint_url": config.endpoint_url,
        "region_name": config.region,
    }
    try:
        botocore_config_module = importlib.import_module("botocore.config")
    except ModuleNotFoundError:
        config_cls = None
    else:
        config_cls = getattr(botocore_config_module, "Config", None)
    if callable(config_cls):
        client_kwargs["config"] = config_cls(
            connect_timeout=5,
            read_timeout=5,
            proxies={},
        )
    client = session.client("s3", **client_kwargs)
    return cast(S3DocumentSourceClientProtocol, client)


def fetch_s3_document_source(
    *,
    config: S3DocumentSourceConfigV1,
    client: S3DocumentSourceClientProtocol | None = None,
) -> S3DocumentSourceFetchV1:
    resolved_client = client
    if resolved_client is None:
        try:
            resolved_client = _build_s3_document_source_client(config=config)
        except Exception as exc:
            raise _source_error(
                message="uns-stream S3 source client initialization failed",
                retryable=False,
                details={
                    "bucket": config.bucket,
                    "key": config.key,
                    "version_id": config.version_id,
                    "exc_type": type(exc).__name__,
                    "exc": str(exc),
                },
            ) from exc

    try:
        if config.version_id is None:
            response = resolved_client.get_object(
                Bucket=config.bucket,
                Key=config.key,
            )
        else:
            response = resolved_client.get_object(
                Bucket=config.bucket,
                Key=config.key,
                VersionId=config.version_id,
            )
    except Exception as exc:
        details: dict[str, object] = {
            "bucket": config.bucket,
            "key": config.key,
            "version_id": config.version_id,
            "exc_type": type(exc).__name__,
            "exc": str(exc),
        }
        retryable = _exception_retryable(exc=exc)
        response_obj = getattr(exc, "response", None)
        if isinstance(response_obj, dict):
            response = cast("dict[str, object]", response_obj)
            metadata_obj = response.get("ResponseMetadata")
            if isinstance(metadata_obj, dict):
                metadata = cast("dict[str, object]", metadata_obj)
                status_code = metadata.get("HTTPStatusCode")
                if isinstance(status_code, int):
                    details["status_code"] = status_code
                    retryable = _status_retryable(status_code=status_code)
            error_obj = response.get("Error")
            if isinstance(error_obj, dict):
                error = cast("dict[str, object]", error_obj)
                code = error.get("Code")
                message = error.get("Message")
                if isinstance(code, str):
                    details["backend_error_code"] = code
                if isinstance(message, str):
                    details["backend_error_message"] = message
        raise _source_error(
            message="uns-stream S3 source request failed",
            retryable=retryable,
            details=details,
        ) from exc

    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise _source_error(
            message="uns-stream S3 source response body is missing",
            retryable=False,
            details={
                "bucket": config.bucket,
                "key": config.key,
                "version_id": config.version_id,
            },
        )
    content = cast(S3ObjectBodyProtocol, body).read()
    mime_type = _normalize_mime_type(cast("str | None", response.get("ContentType")))
    version_id = cast("str | None", response.get("VersionId"))
    etag = cast("str | None", response.get("ETag"))
    filename = _infer_filename(key=config.key, mime_type=mime_type)

    return S3DocumentSourceFetchV1(
        bucket=config.bucket,
        key=config.key,
        version_id=version_id or config.version_id,
        etag=etag,
        filename=filename,
        mime_type=mime_type,
        content=content,
    )


def _load_s3_document_source_config_from_path(path: Path) -> S3DocumentSourceConfigV1 | None:
    if path.suffix.lower() != ".json":
        return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if not isinstance(raw, dict):
        return None
    typed_raw = cast("dict[str, object]", raw)
    if not is_s3_document_source_spec(typed_raw):
        return None

    try:
        return load_s3_document_source_config(typed_raw)
    except ValueError as err:
        source_raw = typed_raw.get("source")
        bucket = None
        key = None
        if isinstance(source_raw, dict):
            typed_source = cast("dict[str, object]", source_raw)
            bucket_candidate = typed_source.get("bucket")
            key_candidate = typed_source.get("key")
            if isinstance(bucket_candidate, str):
                bucket = bucket_candidate
            if isinstance(key_candidate, str):
                key = key_candidate
        raise _source_error(
            message=str(err),
            retryable=False,
            details={"bucket": bucket, "key": key},
        ) from err


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    config = _load_s3_document_source_config_from_path(Path(filename))
    if config is None:
        return default_sha
    return normalize_s3_document_source_identity_sha(config=config)


def _with_fetch_provenance(
    *,
    result: PartitionResult,
    fetched: S3DocumentSourceFetchV1,
    sha256: str,
) -> PartitionResult:
    elements: list[ZephyrElement] = []
    for element in result.elements:
        metadata = dict(element.metadata)
        metadata["source_kind"] = S3_DOCUMENT_SOURCE_KIND
        metadata["source_bucket"] = fetched.bucket
        metadata["source_key"] = fetched.key
        metadata["fetched_filename"] = fetched.filename
        if fetched.version_id is not None:
            metadata["source_version_id"] = fetched.version_id
        if fetched.etag is not None:
            metadata["source_etag"] = fetched.etag
        if fetched.mime_type is not None:
            metadata["fetched_mime_type"] = fetched.mime_type
        elements.append(
            ZephyrElement(
                element_id=element.element_id,
                type=element.type,
                text=element.text,
                metadata=metadata,
            )
        )

    return PartitionResult(
        document=DocumentMetadata(
            filename=fetched.filename,
            mime_type=fetched.mime_type,
            sha256=sha256,
            size_bytes=len(fetched.content),
            created_at_utc=result.document.created_at_utc,
        ),
        engine=result.engine,
        elements=elements,
        normalized_text=result.normalized_text,
        warnings=list(result.warnings),
    )


def process_file(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> PartitionResult:
    path = Path(filename)
    config = _load_s3_document_source_config_from_path(path)
    if config is None:
        return auto_partition(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )

    fetched = fetch_s3_document_source(config=config)
    tmp_dir = path.parent / f".zephyr-uns-s3-{uuid4().hex}"
    tmp_dir.mkdir(parents=False, exist_ok=False)
    try:
        fetched_path = tmp_dir / fetched.filename
        fetched_path.write_bytes(fetched.content)
        result = auto_partition(
            filename=str(fetched_path),
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=len(fetched.content),
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    resolved_sha = sha256 if sha256 is not None else result.document.sha256
    return _with_fetch_provenance(result=result, fetched=fetched, sha256=resolved_sha)
