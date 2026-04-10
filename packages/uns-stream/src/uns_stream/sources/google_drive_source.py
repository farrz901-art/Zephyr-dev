from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
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

GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND = "google_drive_document_v1"
_DEFAULT_TIMEOUT_S = 10.0
_CONTENT_TYPE_EXTENSION_MAP: dict[str, str] = {
    "application/json": ".json",
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.google-apps.document": ".gdoc",
    "application/vnd.google-apps.presentation": ".gslides",
    "application/vnd.google-apps.spreadsheet": ".gsheet",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/markdown": ".md",
    "text/plain": ".txt",
}

GoogleDriveAcquisitionMode = Literal["download", "export"]


@dataclass(frozen=True, slots=True)
class GoogleDriveDocumentSourceConfigV1:
    file_id: str
    access_token: str
    acquisition_mode: GoogleDriveAcquisitionMode
    export_mime_type: str | None
    drive_id: str | None
    timeout_s: float


@dataclass(frozen=True, slots=True)
class GoogleDriveDocumentSourceFetchV1:
    file_id: str
    drive_id: str | None
    acquisition_mode: GoogleDriveAcquisitionMode
    export_mime_type: str | None
    etag: str | None
    filename: str
    mime_type: str | None
    content: bytes


def is_google_drive_document_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND


def load_google_drive_document_source_config(
    raw: dict[str, object],
) -> GoogleDriveDocumentSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("uns-stream Google Drive source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    def _required_str(field_name: str) -> str:
        value = typed_source.get(field_name)
        if not isinstance(value, str) or not value:
            raise ValueError(
                "uns-stream Google Drive source field "
                f"'source.{field_name}' must be a non-empty string"
            )
        return value

    def _optional_str(field_name: str) -> str | None:
        value = typed_source.get(field_name)
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise ValueError(
                "uns-stream Google Drive source field "
                f"'source.{field_name}' must be a non-empty string"
            )
        return value

    acquisition_mode_raw = _required_str("acquisition_mode")
    if acquisition_mode_raw not in {"download", "export"}:
        raise ValueError(
            "uns-stream Google Drive source field 'source.acquisition_mode' must be "
            "'download' or 'export'"
        )
    acquisition_mode = cast("GoogleDriveAcquisitionMode", acquisition_mode_raw)

    export_mime_type = _optional_str("export_mime_type")
    if acquisition_mode == "export" and export_mime_type is None:
        raise ValueError(
            "uns-stream Google Drive source field 'source.export_mime_type' is required "
            "when 'source.acquisition_mode' is 'export'"
        )
    if acquisition_mode == "download" and export_mime_type is not None:
        raise ValueError(
            "uns-stream Google Drive source field 'source.export_mime_type' is only "
            "allowed when 'source.acquisition_mode' is 'export'"
        )

    timeout_raw = typed_source.get("timeout_s", _DEFAULT_TIMEOUT_S)
    if isinstance(timeout_raw, (int, float)):
        timeout_s = float(timeout_raw)
    else:
        raise ValueError(
            "uns-stream Google Drive source field 'source.timeout_s' must be a positive number"
        )
    if timeout_s <= 0:
        raise ValueError(
            "uns-stream Google Drive source field 'source.timeout_s' must be a positive number"
        )

    return GoogleDriveDocumentSourceConfigV1(
        file_id=_required_str("file_id"),
        access_token=_required_str("access_token"),
        acquisition_mode=acquisition_mode,
        export_mime_type=export_mime_type,
        drive_id=_optional_str("drive_id"),
        timeout_s=timeout_s,
    )


def normalize_google_drive_document_source_identity_sha(
    *,
    config: GoogleDriveDocumentSourceConfigV1,
) -> str:
    canonical = json.dumps(
        {
            "kind": GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND,
            "file_id": config.file_id,
            "acquisition_mode": config.acquisition_mode,
            "export_mime_type": config.export_mime_type,
            "drive_id": config.drive_id,
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
        "source_kind": GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND,
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
            name = Path(raw_value).name
            if name:
                return name
        if lower_segment.startswith("filename="):
            raw_value = segment.split("=", 1)[1].strip().strip('"')
            name = Path(raw_value).name
            if name:
                return name
    return None


def _infer_filename(
    *,
    file_id: str,
    acquisition_mode: GoogleDriveAcquisitionMode,
    content_disposition: str | None,
    mime_type: str | None,
) -> str:
    from_header = _filename_from_content_disposition(content_disposition)
    if from_header is not None:
        return from_header

    inferred_ext = _CONTENT_TYPE_EXTENSION_MAP.get(mime_type or "")
    stem = f"drive-{file_id}" if acquisition_mode == "download" else f"drive-export-{file_id}"
    if inferred_ext is not None:
        return f"{stem}{inferred_ext}"
    return stem


def _build_google_drive_request(*, config: GoogleDriveDocumentSourceConfigV1) -> Request:
    quoted_file_id = quote(config.file_id, safe="")
    if config.acquisition_mode == "download":
        url = f"https://www.googleapis.com/drive/v3/files/{quoted_file_id}?alt=media"
    else:
        assert config.export_mime_type is not None
        export_mime = quote(config.export_mime_type, safe="")
        url = (
            "https://www.googleapis.com/drive/v3/files/"
            f"{quoted_file_id}/export?mimeType={export_mime}"
        )
    if config.drive_id is not None:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}supportsAllDrives=true"

    headers = {"Authorization": f"Bearer {config.access_token}"}
    if config.export_mime_type is not None:
        headers["Accept"] = config.export_mime_type
    return Request(url, headers=headers, method="GET")


def fetch_google_drive_document_source(
    *,
    config: GoogleDriveDocumentSourceConfigV1,
) -> GoogleDriveDocumentSourceFetchV1:
    request = _build_google_drive_request(config=config)
    try:
        with urlopen(request, timeout=config.timeout_s) as response:
            content = response.read()
            mime_type = _normalize_mime_type(_response_header(response, "Content-Type"))
            filename = _infer_filename(
                file_id=config.file_id,
                acquisition_mode=config.acquisition_mode,
                content_disposition=_response_header(response, "Content-Disposition"),
                mime_type=mime_type,
            )
            etag = _response_header(response, "ETag")
    except HTTPError as err:
        raise _source_error(
            message=f"uns-stream Google Drive source request failed with status {err.code}",
            retryable=_is_retryable_http_status(err.code),
            details={
                "status_code": err.code,
                "file_id": config.file_id,
                "drive_id": config.drive_id,
                "acquisition_mode": config.acquisition_mode,
                "export_mime_type": config.export_mime_type,
            },
        ) from err
    except URLError as err:
        raise _source_error(
            message="uns-stream Google Drive source request failed",
            retryable=True,
            details={
                "file_id": config.file_id,
                "drive_id": config.drive_id,
                "acquisition_mode": config.acquisition_mode,
                "export_mime_type": config.export_mime_type,
                "reason": str(err.reason),
            },
        ) from err
    except TimeoutError as err:
        raise _source_error(
            message="uns-stream Google Drive source request timed out",
            retryable=True,
            details={
                "file_id": config.file_id,
                "drive_id": config.drive_id,
                "acquisition_mode": config.acquisition_mode,
                "export_mime_type": config.export_mime_type,
            },
        ) from err

    return GoogleDriveDocumentSourceFetchV1(
        file_id=config.file_id,
        drive_id=config.drive_id,
        acquisition_mode=config.acquisition_mode,
        export_mime_type=config.export_mime_type,
        etag=etag,
        filename=filename,
        mime_type=mime_type,
        content=content,
    )


def _load_google_drive_document_source_config_from_path(
    path: Path,
) -> GoogleDriveDocumentSourceConfigV1 | None:
    if path.suffix.lower() != ".json":
        return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if not isinstance(raw, dict):
        return None
    typed_raw = cast("dict[str, object]", raw)
    if not is_google_drive_document_source_spec(typed_raw):
        return None

    try:
        return load_google_drive_document_source_config(typed_raw)
    except ValueError as err:
        source_raw = typed_raw.get("source")
        file_id = None
        drive_id = None
        if isinstance(source_raw, dict):
            typed_source = cast("dict[str, object]", source_raw)
            file_id_candidate = typed_source.get("file_id")
            drive_id_candidate = typed_source.get("drive_id")
            if isinstance(file_id_candidate, str):
                file_id = file_id_candidate
            if isinstance(drive_id_candidate, str):
                drive_id = drive_id_candidate
        raise _source_error(
            message=str(err),
            retryable=False,
            details={"file_id": file_id, "drive_id": drive_id},
        ) from err


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    config = _load_google_drive_document_source_config_from_path(Path(filename))
    if config is None:
        return default_sha
    return normalize_google_drive_document_source_identity_sha(config=config)


def _with_fetch_provenance(
    *,
    result: PartitionResult,
    fetched: GoogleDriveDocumentSourceFetchV1,
    sha256: str,
) -> PartitionResult:
    elements: list[ZephyrElement] = []
    for element in result.elements:
        metadata = dict(element.metadata)
        metadata["source_kind"] = GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND
        metadata["source_file_id"] = fetched.file_id
        metadata["source_acquisition_mode"] = fetched.acquisition_mode
        metadata["fetched_filename"] = fetched.filename
        if fetched.drive_id is not None:
            metadata["source_drive_id"] = fetched.drive_id
        if fetched.export_mime_type is not None:
            metadata["source_export_mime_type"] = fetched.export_mime_type
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
    config = _load_google_drive_document_source_config_from_path(path)
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

    fetched = fetch_google_drive_document_source(config=config)
    tmp_dir = path.parent / f".zephyr-uns-google-drive-{uuid4().hex}"
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
