from __future__ import annotations

import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast
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

CONFLUENCE_DOCUMENT_SOURCE_KIND = "confluence_document_v1"
_DEFAULT_TIMEOUT_S = 10.0
_BODY_FORMAT = "storage"


@dataclass(frozen=True, slots=True)
class ConfluenceDocumentSourceConfigV1:
    site_url: str
    page_id: str
    access_token: str
    space_key: str | None
    page_version: int | None
    timeout_s: float


@dataclass(frozen=True, slots=True)
class ConfluenceDocumentSourceFetchV1:
    site_url: str
    page_id: str
    space_key: str | None
    requested_page_version: int | None
    resolved_page_version: int | None
    page_title: str
    filename: str
    mime_type: str
    body_format: str
    content: bytes


class ConfluenceResponseProtocol(Protocol):
    def read(self) -> bytes: ...


def is_confluence_document_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == CONFLUENCE_DOCUMENT_SOURCE_KIND


def _normalize_site_url(value: str) -> str:
    trimmed = value.strip().rstrip("/")
    if not (trimmed.startswith("http://") or trimmed.startswith("https://")):
        raise ValueError(
            "uns-stream Confluence source field 'source.site_url' must use http:// or https://"
        )
    return trimmed


def load_confluence_document_source_config(
    raw: dict[str, object],
) -> ConfluenceDocumentSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("uns-stream Confluence source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    def _required_str(field_name: str) -> str:
        value = typed_source.get(field_name)
        if not isinstance(value, str) or not value:
            raise ValueError(
                "uns-stream Confluence source field "
                f"'source.{field_name}' must be a non-empty string"
            )
        return value

    def _optional_str(field_name: str) -> str | None:
        value = typed_source.get(field_name)
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise ValueError(
                "uns-stream Confluence source field "
                f"'source.{field_name}' must be a non-empty string"
            )
        return value

    page_version_raw = typed_source.get("page_version")
    page_version: int | None
    if page_version_raw is None:
        page_version = None
    elif isinstance(page_version_raw, int) and page_version_raw > 0:
        page_version = page_version_raw
    else:
        raise ValueError(
            "uns-stream Confluence source field 'source.page_version' must be a positive integer"
        )

    timeout_raw = typed_source.get("timeout_s", _DEFAULT_TIMEOUT_S)
    if isinstance(timeout_raw, (int, float)):
        timeout_s = float(timeout_raw)
    else:
        raise ValueError(
            "uns-stream Confluence source field 'source.timeout_s' must be a positive number"
        )
    if timeout_s <= 0:
        raise ValueError(
            "uns-stream Confluence source field 'source.timeout_s' must be a positive number"
        )

    return ConfluenceDocumentSourceConfigV1(
        site_url=_normalize_site_url(_required_str("site_url")),
        page_id=_required_str("page_id"),
        access_token=_required_str("access_token"),
        space_key=_optional_str("space_key"),
        page_version=page_version,
        timeout_s=timeout_s,
    )


def normalize_confluence_document_source_identity_sha(
    *,
    config: ConfluenceDocumentSourceConfigV1,
) -> str:
    canonical = json.dumps(
        {
            "kind": CONFLUENCE_DOCUMENT_SOURCE_KIND,
            "site_url": config.site_url,
            "page_id": config.page_id,
            "space_key": config.space_key,
            "page_version": config.page_version,
            "body_format": _BODY_FORMAT,
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
        "source_kind": CONFLUENCE_DOCUMENT_SOURCE_KIND,
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


def _api_root(site_url: str) -> str:
    if site_url.endswith("/wiki"):
        return site_url
    return f"{site_url}/wiki"


def _response_json(response: object) -> dict[str, object]:
    payload = json.loads(cast("ConfluenceResponseProtocol", response).read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Confluence source response must be a JSON object")
    return cast("dict[str, object]", payload)


def _storage_body_value(payload: dict[str, object]) -> str:
    body = payload.get("body")
    if not isinstance(body, dict):
        raise ValueError("Confluence source response is missing 'body'")
    storage = cast("dict[str, object]", body).get(_BODY_FORMAT)
    if not isinstance(storage, dict):
        raise ValueError("Confluence source response is missing 'body.storage'")
    value = cast("dict[str, object]", storage).get("value")
    if not isinstance(value, str):
        raise ValueError("Confluence source response is missing 'body.storage.value'")
    return value


def _space_key_from_payload(payload: dict[str, object]) -> str | None:
    space = payload.get("space")
    if not isinstance(space, dict):
        return None
    key = cast("dict[str, object]", space).get("key")
    if not isinstance(key, str) or not key:
        return None
    return key


def _page_version_from_payload(payload: dict[str, object]) -> int | None:
    version = payload.get("version")
    if not isinstance(version, dict):
        return None
    number = cast("dict[str, object]", version).get("number")
    if not isinstance(number, int) or number <= 0:
        return None
    return number


def _sanitize_title_to_filename(*, title: str, page_id: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9]+", "-", title).strip("-").lower()
    if collapsed:
        return f"{collapsed}.html"
    return f"confluence-page-{page_id}.html"


def _build_confluence_request(*, config: ConfluenceDocumentSourceConfigV1) -> Request:
    page_id = quote(config.page_id, safe="")
    url = (
        f"{_api_root(config.site_url)}/rest/api/content/{page_id}?expand=space,version,body.storage"
    )
    return Request(
        url,
        headers={
            "Authorization": f"Bearer {config.access_token}",
            "Accept": "application/json",
        },
        method="GET",
    )


def fetch_confluence_document_source(
    *,
    config: ConfluenceDocumentSourceConfigV1,
) -> ConfluenceDocumentSourceFetchV1:
    request = _build_confluence_request(config=config)
    try:
        with urlopen(request, timeout=config.timeout_s) as response:
            payload = _response_json(response)
    except HTTPError as err:
        raise _source_error(
            message=f"uns-stream Confluence source request failed with status {err.code}",
            retryable=_is_retryable_http_status(err.code),
            details={
                "status_code": err.code,
                "site_url": config.site_url,
                "page_id": config.page_id,
                "space_key": config.space_key,
                "page_version": config.page_version,
            },
        ) from err
    except URLError as err:
        raise _source_error(
            message="uns-stream Confluence source request failed",
            retryable=True,
            details={
                "site_url": config.site_url,
                "page_id": config.page_id,
                "space_key": config.space_key,
                "page_version": config.page_version,
                "reason": str(err.reason),
            },
        ) from err
    except TimeoutError as err:
        raise _source_error(
            message="uns-stream Confluence source request timed out",
            retryable=True,
            details={
                "site_url": config.site_url,
                "page_id": config.page_id,
                "space_key": config.space_key,
                "page_version": config.page_version,
            },
        ) from err
    except ValueError as err:
        raise _source_error(
            message=str(err),
            retryable=False,
            details={
                "site_url": config.site_url,
                "page_id": config.page_id,
                "space_key": config.space_key,
                "page_version": config.page_version,
            },
        ) from err

    title = payload.get("title")
    if not isinstance(title, str) or not title:
        raise _source_error(
            message="Confluence source response is missing 'title'",
            retryable=False,
            details={"site_url": config.site_url, "page_id": config.page_id},
        )

    resolved_space_key = _space_key_from_payload(payload)
    if config.space_key is not None and resolved_space_key != config.space_key:
        raise _source_error(
            message="uns-stream Confluence source space key did not match requested selector",
            retryable=False,
            details={
                "site_url": config.site_url,
                "page_id": config.page_id,
                "space_key": config.space_key,
                "resolved_space_key": resolved_space_key,
            },
        )

    resolved_page_version = _page_version_from_payload(payload)
    if config.page_version is not None and resolved_page_version != config.page_version:
        raise _source_error(
            message="uns-stream Confluence source page version did not match requested version",
            retryable=False,
            details={
                "site_url": config.site_url,
                "page_id": config.page_id,
                "page_version": config.page_version,
                "resolved_page_version": resolved_page_version,
            },
        )

    body_value = _storage_body_value(payload)
    filename = _sanitize_title_to_filename(title=title, page_id=config.page_id)
    return ConfluenceDocumentSourceFetchV1(
        site_url=config.site_url,
        page_id=config.page_id,
        space_key=resolved_space_key,
        requested_page_version=config.page_version,
        resolved_page_version=resolved_page_version,
        page_title=title,
        filename=filename,
        mime_type="text/html",
        body_format=_BODY_FORMAT,
        content=body_value.encode("utf-8"),
    )


def _load_confluence_document_source_config_from_path(
    path: Path,
) -> ConfluenceDocumentSourceConfigV1 | None:
    if path.suffix.lower() != ".json":
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    typed_raw = cast("dict[str, object]", raw)
    if not is_confluence_document_source_spec(typed_raw):
        return None
    try:
        return load_confluence_document_source_config(typed_raw)
    except ValueError as err:
        source_raw = typed_raw.get("source")
        site_url = None
        page_id = None
        if isinstance(source_raw, dict):
            typed_source = cast("dict[str, object]", source_raw)
            site_url_candidate = typed_source.get("site_url")
            page_id_candidate = typed_source.get("page_id")
            if isinstance(site_url_candidate, str):
                site_url = site_url_candidate
            if isinstance(page_id_candidate, str):
                page_id = page_id_candidate
        raise _source_error(
            message=str(err),
            retryable=False,
            details={"site_url": site_url, "page_id": page_id},
        ) from err


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    config = _load_confluence_document_source_config_from_path(Path(filename))
    if config is None:
        return default_sha
    return normalize_confluence_document_source_identity_sha(config=config)


def _with_fetch_provenance(
    *,
    result: PartitionResult,
    fetched: ConfluenceDocumentSourceFetchV1,
    sha256: str,
) -> PartitionResult:
    elements: list[ZephyrElement] = []
    for element in result.elements:
        metadata = dict(element.metadata)
        metadata["source_kind"] = CONFLUENCE_DOCUMENT_SOURCE_KIND
        metadata["source_site_url"] = fetched.site_url
        metadata["source_page_id"] = fetched.page_id
        metadata["source_body_format"] = fetched.body_format
        metadata["source_page_title"] = fetched.page_title
        metadata["fetched_filename"] = fetched.filename
        metadata["fetched_mime_type"] = fetched.mime_type
        if fetched.space_key is not None:
            metadata["source_space_key"] = fetched.space_key
        if fetched.requested_page_version is not None:
            metadata["source_requested_page_version"] = fetched.requested_page_version
        if fetched.resolved_page_version is not None:
            metadata["source_page_version"] = fetched.resolved_page_version
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
    config = _load_confluence_document_source_config_from_path(path)
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

    fetched = fetch_confluence_document_source(config=config)
    tmp_dir = path.parent / f".zephyr-uns-confluence-{uuid4().hex}"
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
