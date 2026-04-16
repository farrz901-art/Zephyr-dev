from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Protocol, Sequence, cast
from urllib.parse import urlsplit

from zephyr_core import ErrorCode, ZephyrError

CLICKHOUSE_INCREMENTAL_SOURCE_KIND = "clickhouse_incremental_v1"
_MAX_CLICKHOUSE_SOURCE_BATCHES = 1000
_RETRYABLE_CLICKHOUSE_ERROR_CODES = {159, 160, 202, 209, 210}
_RETRYABLE_TRANSPORT_MESSAGE_FRAGMENTS = (
    "connection refused",
    "failed to establish a new connection",
    "max retries exceeded",
    "timed out",
    "connection timeout",
)


@dataclass(frozen=True, slots=True)
class ClickHouseIncrementalSourceConfigV1:
    stream: str
    connection_name: str
    url: str
    database: str
    table: str
    columns: tuple[str, ...]
    cursor_column: str
    cursor_start: str | None
    batch_size: int
    username: str | None = None
    password: str | None = None


@dataclass(frozen=True, slots=True)
class ClickHouseIncrementalSourceRecordV1:
    data: dict[str, object]
    cursor: str


@dataclass(frozen=True, slots=True)
class ClickHouseIncrementalSourceDocumentV1:
    stream: str
    records: list[ClickHouseIncrementalSourceRecordV1]
    states: list[dict[str, object]]
    logs: list[tuple[str, str]]


class ClickHouseQueryResultProtocol(Protocol):
    @property
    def result_rows(self) -> Sequence[Sequence[object]]: ...


class ClickHouseClientProtocol(Protocol):
    def query(
        self,
        query: str,
        parameters: dict[str, object] | None = None,
    ) -> ClickHouseQueryResultProtocol: ...

    def close(self) -> object: ...


def is_clickhouse_incremental_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == CLICKHOUSE_INCREMENTAL_SOURCE_KIND


def _read_required_non_empty_string(
    *,
    source: dict[str, object],
    key: str,
    error_message: str,
) -> str:
    value = source.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(error_message)
    return value


def _is_simple_identifier(value: str) -> bool:
    if not value:
        return False
    if not (value[0].isalpha() or value[0] == "_"):
        return False
    return all(character.isalnum() or character == "_" for character in value[1:])


def _normalize_endpoint(url: str) -> str:
    split = urlsplit(url)
    if split.scheme not in {"http", "https"} or split.hostname is None:
        raise ValueError(
            "it-stream ClickHouse source field 'source.url' must use http:// or https://"
        )

    port = split.port
    netloc = split.hostname if port is None else f"{split.hostname}:{port}"
    path = split.path or ""
    return f"{split.scheme}://{netloc}{path}"


def load_clickhouse_incremental_source_config(
    raw: dict[str, object],
) -> ClickHouseIncrementalSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("it-stream ClickHouse source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    stream = _read_required_non_empty_string(
        source=typed_source,
        key="stream",
        error_message=(
            "it-stream ClickHouse source field 'source.stream' must be a non-empty string"
        ),
    )
    connection_name = _read_required_non_empty_string(
        source=typed_source,
        key="connection_name",
        error_message=(
            "it-stream ClickHouse source field 'source.connection_name' must be a non-empty string"
        ),
    )
    url = _read_required_non_empty_string(
        source=typed_source,
        key="url",
        error_message="it-stream ClickHouse source field 'source.url' must be a non-empty string",
    )
    _normalize_endpoint(url)
    database = _read_required_non_empty_string(
        source=typed_source,
        key="database",
        error_message=(
            "it-stream ClickHouse source field 'source.database' must be a non-empty string"
        ),
    )
    table = _read_required_non_empty_string(
        source=typed_source,
        key="table",
        error_message="it-stream ClickHouse source field 'source.table' must be a non-empty string",
    )
    cursor_column = _read_required_non_empty_string(
        source=typed_source,
        key="cursor_column",
        error_message=(
            "it-stream ClickHouse source field 'source.cursor_column' must be a non-empty string"
        ),
    )

    for field_name, value in (
        ("database", database),
        ("table", table),
        ("cursor_column", cursor_column),
    ):
        if not _is_simple_identifier(value):
            raise ValueError(
                f"it-stream ClickHouse source field 'source.{field_name}' "
                "must be a simple SQL identifier"
            )

    columns_raw = typed_source.get("columns")
    if not isinstance(columns_raw, list) or not columns_raw:
        raise ValueError(
            "it-stream ClickHouse source field 'source.columns' must be a non-empty list"
        )

    columns: list[str] = []
    seen_columns: set[str] = set()
    for item in cast("list[object]", columns_raw):
        if not isinstance(item, str) or not item:
            raise ValueError("it-stream ClickHouse source columns must be non-empty strings")
        if not _is_simple_identifier(item):
            raise ValueError("it-stream ClickHouse source columns must be simple SQL identifiers")
        if item in seen_columns:
            raise ValueError("it-stream ClickHouse source columns must not repeat values")
        seen_columns.add(item)
        columns.append(item)

    if cursor_column not in seen_columns:
        raise ValueError(
            "it-stream ClickHouse source field 'source.cursor_column' must be listed in "
            "'source.columns'"
        )

    cursor_start_raw = typed_source.get("cursor_start")
    cursor_start: str | None
    if cursor_start_raw is None:
        cursor_start = None
    elif isinstance(cursor_start_raw, str) and cursor_start_raw:
        cursor_start = cursor_start_raw
    else:
        raise ValueError(
            "it-stream ClickHouse source field 'source.cursor_start' must be a non-empty string "
            "or null"
        )

    batch_size_raw = typed_source.get("batch_size")
    if (
        not isinstance(batch_size_raw, int)
        or isinstance(batch_size_raw, bool)
        or batch_size_raw < 1
    ):
        raise ValueError(
            "it-stream ClickHouse source field 'source.batch_size' must be a positive integer"
        )

    username_raw = typed_source.get("username")
    password_raw = typed_source.get("password")
    username: str | None
    password: str | None
    if username_raw is None and password_raw is None:
        username = None
        password = None
    elif (
        isinstance(username_raw, str)
        and username_raw
        and isinstance(password_raw, str)
        and password_raw
    ):
        username = username_raw
        password = password_raw
    else:
        raise ValueError(
            "it-stream ClickHouse source fields 'source.username' and 'source.password' "
            "must be specified together as non-empty strings"
        )

    return ClickHouseIncrementalSourceConfigV1(
        stream=stream,
        connection_name=connection_name,
        url=url,
        database=database,
        table=table,
        columns=tuple(columns),
        cursor_column=cursor_column,
        cursor_start=cursor_start,
        batch_size=batch_size_raw,
        username=username,
        password=password,
    )


def normalize_clickhouse_incremental_source_identity_sha(
    *,
    config: ClickHouseIncrementalSourceConfigV1,
) -> str:
    canonical = json.dumps(
        {
            "kind": CLICKHOUSE_INCREMENTAL_SOURCE_KIND,
            "stream": config.stream,
            "connection_name": config.connection_name,
            "endpoint": _normalize_endpoint(config.url),
            "database": config.database,
            "table": config.table,
            "columns": config.columns,
            "cursor_column": config.cursor_column,
            "cursor_start": config.cursor_start,
            "batch_size": config.batch_size,
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
        "source_kind": CLICKHOUSE_INCREMENTAL_SOURCE_KIND,
    }
    if details is not None:
        merged_details.update(details)
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=message,
        details=merged_details,
    )


def _clickhouse_error_code(err: BaseException) -> int | None:
    code = getattr(err, "code", None)
    if isinstance(code, int) and not isinstance(code, bool):
        return code
    return None


def _iter_exception_chain(err: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    seen_ids: set[int] = set()
    current: BaseException | None = err
    while current is not None and id(current) not in seen_ids:
        chain.append(current)
        seen_ids.add(id(current))
        next_exc = current.__cause__
        if next_exc is None and current.__context__ is not None:
            next_exc = current.__context__
        current = next_exc
    return chain


def _is_retryable_transport_error(err: BaseException) -> bool:
    for candidate in _iter_exception_chain(err):
        if isinstance(candidate, (OSError, TimeoutError)):
            return True
        candidate_message = str(candidate).lower()
        if any(
            fragment in candidate_message for fragment in _RETRYABLE_TRANSPORT_MESSAGE_FRAGMENTS
        ):
            return True
    return False


def _is_retryable_clickhouse_error(err: BaseException) -> bool:
    error_code = _clickhouse_error_code(err)
    return _is_retryable_transport_error(err) or error_code in _RETRYABLE_CLICKHOUSE_ERROR_CODES


def _connect_clickhouse_source(
    *,
    config: ClickHouseIncrementalSourceConfigV1,
) -> ClickHouseClientProtocol:
    split = urlsplit(config.url)
    if split.scheme not in {"http", "https"} or split.hostname is None:
        raise _source_error(
            message="it-stream ClickHouse source URL must use http:// or https://",
            retryable=False,
            details={"connection_name": config.connection_name},
        )

    try:
        clickhouse_module = importlib.import_module("clickhouse_connect")
    except ModuleNotFoundError as err:
        raise _source_error(
            message=(
                "it-stream ClickHouse source requires the optional 'clickhouse-connect' dependency"
            ),
            retryable=False,
            details={"connection_name": config.connection_name},
        ) from err

    get_client = getattr(clickhouse_module, "get_client", None)
    if not callable(get_client):
        raise _source_error(
            message="it-stream ClickHouse source could not resolve clickhouse_connect.get_client",
            retryable=False,
            details={"connection_name": config.connection_name},
        )

    try:
        client = get_client(
            host=split.hostname,
            port=split.port,
            username=config.username,
            password=config.password,
            database=config.database,
            interface=split.scheme,
            secure=split.scheme == "https",
        )
    except Exception as err:
        error_code = _clickhouse_error_code(err)
        raise _source_error(
            message="it-stream ClickHouse source connection failed",
            retryable=_is_retryable_clickhouse_error(err),
            details={
                "connection_name": config.connection_name,
                "database": config.database,
                "table": config.table,
                "error_code": error_code,
            },
        ) from err

    return cast("ClickHouseClientProtocol", client)


def _quote_identifier(value: str) -> str:
    return f"`{value}`"


def _build_query(*, config: ClickHouseIncrementalSourceConfigV1, after_cursor: str | None) -> str:
    select_list = ", ".join(_quote_identifier(column) for column in config.columns)
    where_clause = ""
    if after_cursor is not None:
        where_clause = f" WHERE {_quote_identifier(config.cursor_column)} > %(after_cursor)s"
    return (
        f"SELECT {select_list} "
        f"FROM {_quote_identifier(config.database)}.{_quote_identifier(config.table)}"
        f"{where_clause} "
        f"ORDER BY {_quote_identifier(config.cursor_column)} ASC "
        "LIMIT %(batch_size)s"
    )


def _close_resource(resource: object) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        close()


def _normalize_row_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, list):
        return [_normalize_row_value(item) for item in cast("list[object]", value)]
    if isinstance(value, tuple):
        return [_normalize_row_value(item) for item in cast("tuple[object, ...]", value)]
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for key, item in cast("dict[object, object]", value).items():
            if not isinstance(key, str):
                raise TypeError("ClickHouse JSON object keys must be strings")
            normalized[key] = _normalize_row_value(item)
        return normalized
    raise TypeError(f"Unsupported ClickHouse value type: {type(value).__name__}")


def _fetch_rows(
    *,
    config: ClickHouseIncrementalSourceConfigV1,
    after_cursor: str | None,
) -> list[tuple[object, ...]]:
    client = _connect_clickhouse_source(config=config)
    try:
        parameters: dict[str, object] = {"batch_size": config.batch_size}
        if after_cursor is not None:
            parameters["after_cursor"] = after_cursor
        result = client.query(
            _build_query(config=config, after_cursor=after_cursor),
            parameters=parameters,
        )
        fetched = result.result_rows
    except Exception as err:
        error_code = _clickhouse_error_code(err)
        raise _source_error(
            message="it-stream ClickHouse source query failed",
            retryable=_is_retryable_clickhouse_error(err),
            details={
                "connection_name": config.connection_name,
                "database": config.database,
                "table": config.table,
                "cursor_column": config.cursor_column,
                "after_cursor": after_cursor,
                "error_code": error_code,
            },
        ) from err
    finally:
        _close_resource(client)

    rows: list[tuple[object, ...]] = []
    for item in fetched:
        if not isinstance(item, (list, tuple)):
            raise _source_error(
                message="it-stream ClickHouse source rows must be tuples or lists",
                retryable=False,
                details={
                    "connection_name": config.connection_name,
                    "database": config.database,
                    "table": config.table,
                },
            )
        typed_row = tuple(cast("Sequence[object]", item))
        if len(typed_row) != len(config.columns):
            raise _source_error(
                message="it-stream ClickHouse source row width must match configured columns",
                retryable=False,
                details={
                    "connection_name": config.connection_name,
                    "database": config.database,
                    "table": config.table,
                    "expected_columns": len(config.columns),
                    "actual_columns": len(typed_row),
                },
            )
        rows.append(typed_row)
    return rows


def fetch_clickhouse_incremental_source(
    *,
    config: ClickHouseIncrementalSourceConfigV1,
) -> ClickHouseIncrementalSourceDocumentV1:
    records: list[ClickHouseIncrementalSourceRecordV1] = []
    states: list[dict[str, object]] = []
    logs: list[tuple[str, str]] = []

    after_cursor = config.cursor_start

    for batch_number in range(1, _MAX_CLICKHOUSE_SOURCE_BATCHES + 1):
        rows = _fetch_rows(config=config, after_cursor=after_cursor)
        if not rows:
            logs.append(("INFO", f"source exhausted after batch={batch_number}"))
            return ClickHouseIncrementalSourceDocumentV1(
                stream=config.stream,
                records=records,
                states=states,
                logs=logs,
            )

        batch_records: list[ClickHouseIncrementalSourceRecordV1] = []
        batch_cursor: str | None = None
        for row in rows:
            data: dict[str, object] = {}
            for column, value in zip(config.columns, row, strict=True):
                try:
                    data[column] = _normalize_row_value(value)
                except TypeError as err:
                    raise _source_error(
                        message="it-stream ClickHouse source row values must be JSON-compatible",
                        retryable=False,
                        details={
                            "connection_name": config.connection_name,
                            "database": config.database,
                            "table": config.table,
                            "column": column,
                        },
                    ) from err

            cursor_value = data.get(config.cursor_column)
            if not isinstance(cursor_value, str) or not cursor_value:
                raise _source_error(
                    message=(
                        "it-stream ClickHouse source cursor column must normalize to "
                        "a non-empty string"
                    ),
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "database": config.database,
                        "table": config.table,
                        "cursor_column": config.cursor_column,
                    },
                )
            if after_cursor is not None and cursor_value <= after_cursor:
                raise _source_error(
                    message="it-stream ClickHouse source cursor progression must advance strictly",
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "database": config.database,
                        "table": config.table,
                        "cursor_column": config.cursor_column,
                        "cursor": cursor_value,
                        "after_cursor": after_cursor,
                    },
                )
            if batch_cursor is not None and cursor_value <= batch_cursor:
                raise _source_error(
                    message="it-stream ClickHouse source cursor progression must advance strictly",
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "database": config.database,
                        "table": config.table,
                        "cursor_column": config.cursor_column,
                        "cursor": cursor_value,
                        "previous_cursor": batch_cursor,
                    },
                )

            batch_cursor = cursor_value
            batch_records.append(
                ClickHouseIncrementalSourceRecordV1(data=data, cursor=cursor_value)
            )

        final_batch_cursor = batch_cursor
        if final_batch_cursor is None:
            raise _source_error(
                message="it-stream ClickHouse source batch must contain at least one cursor value",
                retryable=False,
                details={
                    "connection_name": config.connection_name,
                    "database": config.database,
                    "table": config.table,
                },
            )

        records.extend(batch_records)
        states.append(
            {
                "cursor": final_batch_cursor,
                "connection_name": config.connection_name,
                "database": config.database,
                "table": config.table,
                "cursor_column": config.cursor_column,
                "columns": list(config.columns),
                "query_mode": "incremental_table_query",
                "read_direction": "asc",
                "row_count": len(batch_records),
            }
        )
        logs.append(
            (
                "INFO",
                f"fetched batch={batch_number} rows={len(batch_records)} "
                f"cursor={final_batch_cursor}",
            )
        )
        after_cursor = final_batch_cursor

    raise _source_error(
        message="it-stream ClickHouse source exceeded the maximum supported batch count",
        retryable=False,
        details={
            "connection_name": config.connection_name,
            "database": config.database,
            "table": config.table,
            "max_batches": _MAX_CLICKHOUSE_SOURCE_BATCHES,
        },
    )
