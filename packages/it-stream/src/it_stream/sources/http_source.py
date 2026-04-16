from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from email.message import Message
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request

import httpx

from zephyr_core import ErrorCode, ZephyrError

HTTP_JSON_CURSOR_SOURCE_KIND = "http_json_cursor_v1"
_MAX_HTTP_SOURCE_PAGES = 1000


@dataclass(slots=True)
class _HttpResponseAdapter:
    content: bytes

    def read(self) -> bytes:
        return self.content

    def __enter__(self) -> _HttpResponseAdapter:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


@dataclass(frozen=True, slots=True)
class HttpJsonCursorSourceConfigV1:
    stream: str
    url: str
    cursor_param: str
    query: dict[str, str]


@dataclass(frozen=True, slots=True)
class HttpJsonCursorSourceDocumentV1:
    stream: str
    records: list[dict[str, object]]
    states: list[dict[str, object]]
    logs: list[tuple[str, str]]


def is_http_json_cursor_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == HTTP_JSON_CURSOR_SOURCE_KIND


def load_http_json_cursor_source_config(raw: dict[str, object]) -> HttpJsonCursorSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("it-stream HTTP source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    stream = typed_source.get("stream")
    if not isinstance(stream, str) or not stream:
        raise ValueError("it-stream HTTP source field 'source.stream' must be a non-empty string")

    url = typed_source.get("url")
    if not isinstance(url, str) or not url:
        raise ValueError("it-stream HTTP source field 'source.url' must be a non-empty string")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("it-stream HTTP source field 'source.url' must use http:// or https://")

    cursor_param = typed_source.get("cursor_param")
    if not isinstance(cursor_param, str) or not cursor_param:
        raise ValueError(
            "it-stream HTTP source field 'source.cursor_param' must be a non-empty string"
        )

    query_raw = typed_source.get("query", {})
    if not isinstance(query_raw, dict):
        raise ValueError("it-stream HTTP source field 'source.query' must be an object when set")

    query: dict[str, str] = {}
    for key, value in cast("dict[object, object]", query_raw).items():
        if not isinstance(key, str) or not key:
            raise ValueError("it-stream HTTP source query keys must be non-empty strings")
        if not isinstance(value, str):
            raise ValueError("it-stream HTTP source query values must be strings")
        query[key] = value

    return HttpJsonCursorSourceConfigV1(
        stream=stream,
        url=url,
        cursor_param=cursor_param,
        query=query,
    )


def normalize_http_json_cursor_source_identity_sha(*, config: HttpJsonCursorSourceConfigV1) -> str:
    canonical = json.dumps(
        {
            "kind": HTTP_JSON_CURSOR_SOURCE_KIND,
            "stream": config.stream,
            "url": config.url,
            "cursor_param": config.cursor_param,
            "query": config.query,
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
        "source_kind": HTTP_JSON_CURSOR_SOURCE_KIND,
    }
    if details is not None:
        merged_details.update(details)
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=message,
        details=merged_details,
    )


def _build_page_url(*, config: HttpJsonCursorSourceConfigV1, cursor: str | None) -> str:
    split = urlsplit(config.url)
    query_pairs = dict(parse_qsl(split.query, keep_blank_values=True))
    query_pairs.update(config.query)
    if cursor is not None:
        query_pairs[config.cursor_param] = cursor
    elif config.cursor_param in query_pairs:
        query_pairs.pop(config.cursor_param)

    return urlunsplit(
        (
            split.scheme,
            split.netloc,
            split.path,
            urlencode(sorted(query_pairs.items())),
            split.fragment,
        )
    )


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def urlopen(request: object, timeout: float = 10.0) -> _HttpResponseAdapter:
    full_url = getattr(request, "full_url", None)
    if not isinstance(full_url, str) or not full_url:
        raise URLError("missing request URL")

    try:
        response = httpx.get(full_url, timeout=timeout, trust_env=False)
    except httpx.TimeoutException as err:
        raise TimeoutError("timed out") from err
    except httpx.HTTPError as err:
        raise URLError(str(err)) from err

    if response.is_error:
        raise HTTPError(
            full_url,
            response.status_code,
            response.reason_phrase,
            hdrs=Message(),
            fp=None,
        )
    return _HttpResponseAdapter(content=response.content)


def _read_page_payload(
    *,
    config: HttpJsonCursorSourceConfigV1,
    cursor: str | None,
) -> dict[str, object]:
    page_url = _build_page_url(config=config, cursor=cursor)
    request = Request(page_url, method="GET")

    try:
        with urlopen(request, timeout=10.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as err:
        raise _source_error(
            message=f"it-stream HTTP source request failed with status {err.code}",
            retryable=_is_retryable_http_status(err.code),
            details={"status_code": err.code, "url": page_url},
        ) from err
    except URLError as err:
        raise _source_error(
            message="it-stream HTTP source request failed",
            retryable=True,
            details={"url": page_url, "reason": str(err.reason)},
        ) from err
    except TimeoutError as err:
        raise _source_error(
            message="it-stream HTTP source request timed out",
            retryable=True,
            details={"url": page_url},
        ) from err
    except json.JSONDecodeError as err:
        raise _source_error(
            message="it-stream HTTP source response must be valid JSON",
            retryable=False,
            details={"url": page_url},
        ) from err

    if not isinstance(payload, dict):
        raise _source_error(
            message="it-stream HTTP source response must be a JSON object",
            retryable=False,
            details={"url": page_url, "payload_type": type(payload).__name__},
        )
    return cast("dict[str, object]", payload)


def fetch_http_json_cursor_source(
    *,
    config: HttpJsonCursorSourceConfigV1,
) -> HttpJsonCursorSourceDocumentV1:
    records: list[dict[str, object]] = []
    states: list[dict[str, object]] = []
    logs: list[tuple[str, str]] = []

    cursor: str | None = None
    seen_cursors: set[str] = set()

    for page_number in range(1, _MAX_HTTP_SOURCE_PAGES + 1):
        payload = _read_page_payload(config=config, cursor=cursor)
        records_raw = payload.get("records")
        if not isinstance(records_raw, list):
            raise _source_error(
                message="it-stream HTTP source response field 'records' must be a list",
                retryable=False,
                details={"url": config.url, "page_number": page_number},
            )

        typed_records_raw = cast("list[object]", records_raw)
        page_records: list[dict[str, object]] = []
        for item in typed_records_raw:
            if not isinstance(item, dict):
                raise _source_error(
                    message="it-stream HTTP source records must be JSON objects",
                    retryable=False,
                    details={"url": config.url, "page_number": page_number},
                )
            page_records.append(cast("dict[str, object]", item))
        records.extend(page_records)

        next_cursor = payload.get("next_cursor")
        if next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor):
            raise _source_error(
                message=(
                    "it-stream HTTP source response field 'next_cursor' must be a string or null"
                ),
                retryable=False,
                details={"url": config.url, "page_number": page_number},
            )

        logs.append(
            (
                "INFO",
                f"fetched page={page_number} records={len(page_records)} next_cursor={next_cursor}",
            )
        )

        if next_cursor is None:
            logs.append(("INFO", f"source exhausted after page={page_number}"))
            return HttpJsonCursorSourceDocumentV1(
                stream=config.stream,
                records=records,
                states=states,
                logs=logs,
            )

        if next_cursor in seen_cursors:
            raise _source_error(
                message="it-stream HTTP source cursor progression must advance",
                retryable=False,
                details={"url": config.url, "page_number": page_number, "cursor": next_cursor},
            )
        seen_cursors.add(next_cursor)

        states.append(
            {
                "cursor": next_cursor,
                "page_number": page_number,
                "source_url": config.url,
                "record_count": len(page_records),
            }
        )
        cursor = next_cursor

    raise _source_error(
        message="it-stream HTTP source exceeded the maximum supported page count",
        retryable=False,
        details={"url": config.url, "max_pages": _MAX_HTTP_SOURCE_PAGES},
    )
