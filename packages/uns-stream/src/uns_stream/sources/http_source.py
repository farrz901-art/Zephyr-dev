from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, cast
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import httpx

from uns_stream._internal.enhanced_partition import build_explicit_partition_kwargs
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

HTTP_DOCUMENT_SOURCE_KIND = "http_document_v1"
_DEFAULT_TIMEOUT_S = 10.0
_CONTENT_TYPE_EXTENSION_MAP: dict[str, str] = {
    "application/json": ".json",
    "application/pdf": ".pdf",
    "application/xml": ".xml",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/plain": ".txt",
    "text/xml": ".xml",
}


@dataclass(frozen=True, slots=True)
class HttpDocumentSourceConfigV1:
    url: str
    accept: str | None
    timeout_s: float


@dataclass(frozen=True, slots=True)
class HttpDocumentSourceFetchV1:
    source_url: str
    filename: str
    mime_type: str | None
    content: bytes


def is_http_document_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == HTTP_DOCUMENT_SOURCE_KIND


def load_http_document_source_config(raw: dict[str, object]) -> HttpDocumentSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("uns-stream HTTP source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    url = typed_source.get("url")
    if not isinstance(url, str) or not url:
        raise ValueError("uns-stream HTTP source field 'source.url' must be a non-empty string")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("uns-stream HTTP source field 'source.url' must use http:// or https://")

    accept = typed_source.get("accept")
    if accept is not None and (not isinstance(accept, str) or not accept):
        raise ValueError("uns-stream HTTP source field 'source.accept' must be a non-empty string")

    timeout_raw = typed_source.get("timeout_s", _DEFAULT_TIMEOUT_S)
    if isinstance(timeout_raw, (int, float)):
        timeout_s = float(timeout_raw)
    else:
        raise ValueError(
            "uns-stream HTTP source field 'source.timeout_s' must be a positive number"
        )
    if timeout_s <= 0:
        raise ValueError(
            "uns-stream HTTP source field 'source.timeout_s' must be a positive number"
        )

    return HttpDocumentSourceConfigV1(
        url=url,
        accept=accept,
        timeout_s=timeout_s,
    )


def normalize_http_document_source_identity_sha(*, config: HttpDocumentSourceConfigV1) -> str:
    canonical = json.dumps(
        {
            "kind": HTTP_DOCUMENT_SOURCE_KIND,
            "url": config.url,
            "accept": config.accept,
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
        "source_kind": HTTP_DOCUMENT_SOURCE_KIND,
    }
    if details is not None:
        merged_details.update(details)
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=message,
        details=merged_details,
    )


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _response_header(response: object, name: str) -> str | None:
    headers = getattr(response, "headers", None)
    if headers is None or not hasattr(headers, "get"):
        return None
    value = headers.get(name)
    if not isinstance(value, str) or not value:
        return None
    return value


def _normalize_mime_type(value: str | None) -> str | None:
    if value is None:
        return None
    mime_type = value.split(";", 1)[0].strip().lower()
    if not mime_type:
        return None
    return mime_type


def _filename_from_content_disposition(value: str | None) -> str | None:
    if value is None:
        return None
    for part in value.split(";"):
        segment = part.strip()
        lower_segment = segment.lower()
        if lower_segment.startswith("filename*="):
            raw_value = segment.split("=", 1)[1].strip().strip('"')
            if "''" in raw_value:
                raw_value = raw_value.split("''", 1)[1]
            name = Path(unquote(raw_value)).name
            if name:
                return name
        if lower_segment.startswith("filename="):
            raw_value = segment.split("=", 1)[1].strip().strip('"')
            name = Path(raw_value).name
            if name:
                return name
    return None


def _infer_filename(*, url: str, content_disposition: str | None, mime_type: str | None) -> str:
    from_header = _filename_from_content_disposition(content_disposition)
    if from_header is not None:
        return from_header

    basename = Path(unquote(urlsplit(url).path)).name
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


def fetch_http_document_source(*, config: HttpDocumentSourceConfigV1) -> HttpDocumentSourceFetchV1:
    headers: dict[str, str] = {}
    if config.accept is not None:
        headers["Accept"] = config.accept

    try:
        response = httpx.get(
            config.url,
            headers=headers,
            timeout=config.timeout_s,
            follow_redirects=True,
            trust_env=False,
        )
    except httpx.TimeoutException as err:
        raise _source_error(
            message="uns-stream HTTP source request timed out",
            retryable=True,
            details={"url": config.url},
        ) from err
    except httpx.TransportError as err:
        raise _source_error(
            message="uns-stream HTTP source request failed",
            retryable=True,
            details={"url": config.url, "reason": str(err)},
        ) from err

    if response.status_code >= 400:
        raise _source_error(
            message=f"uns-stream HTTP source request failed with status {response.status_code}",
            retryable=_is_retryable_http_status(response.status_code),
            details={"status_code": response.status_code, "url": config.url},
        )

    content = response.content
    mime_type = _normalize_mime_type(_response_header(response, "Content-Type"))
    filename = _infer_filename(
        url=config.url,
        content_disposition=_response_header(response, "Content-Disposition"),
        mime_type=mime_type,
    )

    return HttpDocumentSourceFetchV1(
        source_url=config.url,
        filename=filename,
        mime_type=mime_type,
        content=content,
    )


def _load_http_document_source_config_from_path(
    path: Path,
) -> HttpDocumentSourceConfigV1 | None:
    if path.suffix.lower() != ".json":
        return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if not isinstance(raw, dict):
        return None
    typed_raw = cast("dict[str, object]", raw)
    if not is_http_document_source_spec(typed_raw):
        return None

    try:
        return load_http_document_source_config(typed_raw)
    except ValueError as err:
        source_raw = typed_raw.get("source")
        url = None
        if isinstance(source_raw, dict):
            url_candidate = cast("dict[str, object]", source_raw).get("url")
            if isinstance(url_candidate, str):
                url = url_candidate
        raise _source_error(
            message=str(err),
            retryable=False,
            details={"url": url},
        ) from err


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    config = _load_http_document_source_config_from_path(Path(filename))
    if config is None:
        return default_sha
    return normalize_http_document_source_identity_sha(config=config)


def _with_fetch_provenance(
    *,
    result: PartitionResult,
    fetched: HttpDocumentSourceFetchV1,
    sha256: str,
) -> PartitionResult:
    elements: list[ZephyrElement] = []
    for element in result.elements:
        metadata = dict(element.metadata)
        metadata["source_kind"] = HTTP_DOCUMENT_SOURCE_KIND
        metadata["source_url"] = fetched.source_url
        metadata["fetched_filename"] = fetched.filename
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
    strategy: PartitionStrategy | None = None,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    profile: str | None = None,
    languages: list[str] | None = None,
    detect_language_per_element: bool | None = None,
    language_fallback: object | None = None,
    skip_infer_table_types: list[str] | None = None,
    infer_table_structure: bool | None = None,
    pdf_infer_table_structure: bool | None = None,
    extract_image_block_types: list[str] | None = None,
    extract_image_block_output_dir: str | None = None,
    extract_image_block_to_payload: bool | None = None,
    data_source_metadata: object | None = None,
    metadata_filename: str | None = None,
    hi_res_model_name: str | None = None,
    model_name: str | None = None,
    starting_page_number: int | None = None,
    ocr_agent: str | None = None,
    table_ocr_agent: str | None = None,
    extra_partition_kwargs: Mapping[str, object] | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> PartitionResult:
    path = Path(filename)
    partition_kwargs = build_explicit_partition_kwargs(
        profile=profile,
        languages=languages,
        detect_language_per_element=detect_language_per_element,
        language_fallback=language_fallback,
        skip_infer_table_types=skip_infer_table_types,
        infer_table_structure=infer_table_structure,
        pdf_infer_table_structure=pdf_infer_table_structure,
        extract_image_block_types=extract_image_block_types,
        extract_image_block_output_dir=extract_image_block_output_dir,
        extract_image_block_to_payload=extract_image_block_to_payload,
        data_source_metadata=data_source_metadata,
        metadata_filename=metadata_filename,
        hi_res_model_name=hi_res_model_name,
        model_name=model_name,
        starting_page_number=starting_page_number,
        ocr_agent=ocr_agent,
        table_ocr_agent=table_ocr_agent,
        extra_partition_kwargs=extra_partition_kwargs,
    )
    config = _load_http_document_source_config_from_path(path)
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
            **partition_kwargs,
        )

    fetched = fetch_http_document_source(config=config)
    tmp_dir = path.parent / f".zephyr-uns-http-{uuid4().hex}"
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
            **partition_kwargs,
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    resolved_sha = sha256 if sha256 is not None else result.document.sha256
    return _with_fetch_provenance(result=result, fetched=fetched, sha256=resolved_sha)
