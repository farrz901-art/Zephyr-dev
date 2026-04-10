from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Iterable, Protocol, cast

from zephyr_core import ErrorCode, ZephyrError

MONGODB_INCREMENTAL_SOURCE_KIND = "mongodb_incremental_v1"
_MAX_MONGODB_SOURCE_BATCHES = 1000


@dataclass(frozen=True, slots=True)
class MongoDBIncrementalSourceConfigV1:
    stream: str
    connection_name: str
    uri: str
    database: str
    collection: str
    fields: tuple[str, ...]
    cursor_field: str
    cursor_start: str | None
    batch_size: int


@dataclass(frozen=True, slots=True)
class MongoDBIncrementalSourceRecordV1:
    data: dict[str, object]
    cursor: str


@dataclass(frozen=True, slots=True)
class MongoDBIncrementalSourceDocumentV1:
    stream: str
    records: list[MongoDBIncrementalSourceRecordV1]
    states: list[dict[str, object]]
    logs: list[tuple[str, str]]


class MongoDBCollectionHandleProtocol(Protocol):
    def find(
        self,
        filter: dict[str, object],
        projection: dict[str, bool],
        sort: list[tuple[str, int]],
        limit: int,
    ) -> object: ...

    def close(self) -> object: ...


class MongoDBDatabaseProtocol(Protocol):
    def __getitem__(self, name: str) -> object: ...


class MongoDBClientProtocol(Protocol):
    def __getitem__(self, name: str) -> MongoDBDatabaseProtocol: ...

    def close(self) -> object: ...


class _MongoDBCollectionHandle:
    def __init__(self, *, client: object, collection: object) -> None:
        self._client = client
        self._collection = collection

    def find(
        self,
        filter: dict[str, object],
        projection: dict[str, bool],
        sort: list[tuple[str, int]],
        limit: int,
    ) -> object:
        find = getattr(self._collection, "find")
        return find(filter=filter, projection=projection, sort=sort, limit=limit)

    def close(self) -> object:
        close = getattr(self._client, "close", None)
        if callable(close):
            return close()
        return None


def is_mongodb_incremental_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == MONGODB_INCREMENTAL_SOURCE_KIND


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


def _is_simple_mongo_name(value: str) -> bool:
    if not value:
        return False
    if value[0] == "$":
        return False
    return all(character.isalnum() or character == "_" for character in value)


def _normalize_mongodb_uri_for_identity(uri: str) -> str:
    if not (uri.startswith("mongodb://") or uri.startswith("mongodb+srv://")):
        raise ValueError(
            "it-stream MongoDB source field 'source.uri' must use mongodb:// or mongodb+srv://"
        )
    prefix, remainder = uri.split("://", 1)
    if "@" in remainder:
        remainder = remainder.split("@", 1)[1]
    return f"{prefix}://{remainder}"


def load_mongodb_incremental_source_config(
    raw: dict[str, object],
) -> MongoDBIncrementalSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("it-stream MongoDB source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    stream = _read_required_non_empty_string(
        source=typed_source,
        key="stream",
        error_message="it-stream MongoDB source field 'source.stream' must be a non-empty string",
    )
    connection_name = _read_required_non_empty_string(
        source=typed_source,
        key="connection_name",
        error_message=(
            "it-stream MongoDB source field 'source.connection_name' must be a non-empty string"
        ),
    )
    uri = _read_required_non_empty_string(
        source=typed_source,
        key="uri",
        error_message="it-stream MongoDB source field 'source.uri' must be a non-empty string",
    )
    _normalize_mongodb_uri_for_identity(uri)
    database = _read_required_non_empty_string(
        source=typed_source,
        key="database",
        error_message="it-stream MongoDB source field 'source.database' must be a non-empty string",
    )
    collection = _read_required_non_empty_string(
        source=typed_source,
        key="collection",
        error_message=(
            "it-stream MongoDB source field 'source.collection' must be a non-empty string"
        ),
    )
    cursor_field = _read_required_non_empty_string(
        source=typed_source,
        key="cursor_field",
        error_message=(
            "it-stream MongoDB source field 'source.cursor_field' must be a non-empty string"
        ),
    )

    for field_name, value in (
        ("database", database),
        ("collection", collection),
        ("cursor_field", cursor_field),
    ):
        if not _is_simple_mongo_name(value):
            raise ValueError(
                f"it-stream MongoDB source field 'source.{field_name}' "
                "must be a simple document identifier"
            )

    fields_raw = typed_source.get("fields")
    if not isinstance(fields_raw, list) or not fields_raw:
        raise ValueError("it-stream MongoDB source field 'source.fields' must be a non-empty list")

    fields: list[str] = []
    seen_fields: set[str] = set()
    for item in cast("list[object]", fields_raw):
        if not isinstance(item, str) or not item:
            raise ValueError("it-stream MongoDB source fields must be non-empty strings")
        if not _is_simple_mongo_name(item):
            raise ValueError("it-stream MongoDB source fields must be simple document identifiers")
        if item in seen_fields:
            raise ValueError("it-stream MongoDB source fields must not repeat values")
        seen_fields.add(item)
        fields.append(item)

    if cursor_field not in seen_fields:
        raise ValueError(
            "it-stream MongoDB source field 'source.cursor_field' must be listed in 'source.fields'"
        )

    cursor_start_raw = typed_source.get("cursor_start")
    cursor_start: str | None
    if cursor_start_raw is None:
        cursor_start = None
    elif isinstance(cursor_start_raw, str) and cursor_start_raw:
        cursor_start = cursor_start_raw
    else:
        raise ValueError(
            "it-stream MongoDB source field 'source.cursor_start' must be a non-empty string "
            "or null"
        )

    batch_size_raw = typed_source.get("batch_size")
    if (
        not isinstance(batch_size_raw, int)
        or isinstance(batch_size_raw, bool)
        or batch_size_raw < 1
    ):
        raise ValueError(
            "it-stream MongoDB source field 'source.batch_size' must be a positive integer"
        )

    return MongoDBIncrementalSourceConfigV1(
        stream=stream,
        connection_name=connection_name,
        uri=uri,
        database=database,
        collection=collection,
        fields=tuple(fields),
        cursor_field=cursor_field,
        cursor_start=cursor_start,
        batch_size=batch_size_raw,
    )


def normalize_mongodb_incremental_source_identity_sha(
    *,
    config: MongoDBIncrementalSourceConfigV1,
) -> str:
    canonical = json.dumps(
        {
            "kind": MONGODB_INCREMENTAL_SOURCE_KIND,
            "stream": config.stream,
            "connection_name": config.connection_name,
            "uri": _normalize_mongodb_uri_for_identity(config.uri),
            "database": config.database,
            "collection": config.collection,
            "fields": config.fields,
            "cursor_field": config.cursor_field,
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
        "source_kind": MONGODB_INCREMENTAL_SOURCE_KIND,
    }
    if details is not None:
        merged_details.update(details)
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=message,
        details=merged_details,
    )


def _mongodb_error_code(err: object) -> int | None:
    code = getattr(err, "code", None)
    if isinstance(code, int) and not isinstance(code, bool):
        return code
    return None


def _is_retryable_mongodb_error(err: object) -> bool:
    if isinstance(err, (OSError, TimeoutError)):
        return True
    error_labels = getattr(err, "_error_labels", None)
    typed_error_labels = cast("set[object] | None", error_labels)
    if isinstance(typed_error_labels, set) and any(
        isinstance(label, str) and "Retryable" in label for label in typed_error_labels
    ):
        return True
    return type(err).__name__ in {
        "AutoReconnect",
        "ConnectionFailure",
        "NetworkTimeout",
        "ServerSelectionTimeoutError",
    }


def _connect_mongodb_incremental_source(
    *,
    config: MongoDBIncrementalSourceConfigV1,
) -> MongoDBCollectionHandleProtocol:
    try:
        pymongo_module = importlib.import_module("pymongo")
    except ModuleNotFoundError as err:
        raise _source_error(
            message="it-stream MongoDB source requires the optional 'pymongo' dependency",
            retryable=False,
            details={"connection_name": config.connection_name},
        ) from err

    mongo_client_cls = getattr(pymongo_module, "MongoClient", None)
    if not callable(mongo_client_cls):
        raise _source_error(
            message="it-stream MongoDB source could not resolve pymongo.MongoClient",
            retryable=False,
            details={"connection_name": config.connection_name},
        )

    try:
        client = cast("MongoDBClientProtocol", mongo_client_cls(config.uri))
        database = client[config.database]
        collection = database[config.collection]
    except Exception as err:
        raise _source_error(
            message="it-stream MongoDB source connection failed",
            retryable=_is_retryable_mongodb_error(err),
            details={
                "connection_name": config.connection_name,
                "database": config.database,
                "collection": config.collection,
                "error_code": _mongodb_error_code(err),
            },
        ) from err

    return cast(
        "MongoDBCollectionHandleProtocol",
        _MongoDBCollectionHandle(client=client, collection=collection),
    )


def _normalize_document_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, list):
        return [_normalize_document_value(item) for item in cast("list[object]", value)]
    if isinstance(value, tuple):
        return [_normalize_document_value(item) for item in cast("tuple[object, ...]", value)]
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for key, item in cast("dict[object, object]", value).items():
            if not isinstance(key, str):
                raise TypeError("MongoDB document keys must be strings")
            normalized[key] = _normalize_document_value(item)
        return normalized
    raise TypeError(f"Unsupported MongoDB value type: {type(value).__name__}")


def _fetch_documents(
    *,
    config: MongoDBIncrementalSourceConfigV1,
    after_cursor: str | None,
) -> list[dict[str, object]]:
    handle = _connect_mongodb_incremental_source(config=config)
    try:
        filter: dict[str, object] = {}
        if after_cursor is not None:
            filter[config.cursor_field] = {"$gt": after_cursor}
        projection = dict.fromkeys(config.fields, True)
        documents_raw = handle.find(
            filter=filter,
            projection=projection,
            sort=[(config.cursor_field, 1)],
            limit=config.batch_size,
        )
        documents_iterable = cast("Iterable[object]", documents_raw)
        fetched = list(documents_iterable)
    except Exception as err:
        raise _source_error(
            message="it-stream MongoDB source query failed",
            retryable=_is_retryable_mongodb_error(err),
            details={
                "connection_name": config.connection_name,
                "database": config.database,
                "collection": config.collection,
                "cursor_field": config.cursor_field,
                "after_cursor": after_cursor,
                "error_code": _mongodb_error_code(err),
            },
        ) from err
    finally:
        handle.close()

    documents: list[dict[str, object]] = []
    for item in fetched:
        if not isinstance(item, dict):
            raise _source_error(
                message="it-stream MongoDB source documents must be objects",
                retryable=False,
                details={
                    "connection_name": config.connection_name,
                    "database": config.database,
                    "collection": config.collection,
                },
            )
        typed_document = cast("dict[str, object]", item)
        documents.append(typed_document)
    return documents


def fetch_mongodb_incremental_source(
    *,
    config: MongoDBIncrementalSourceConfigV1,
) -> MongoDBIncrementalSourceDocumentV1:
    records: list[MongoDBIncrementalSourceRecordV1] = []
    states: list[dict[str, object]] = []
    logs: list[tuple[str, str]] = []

    after_cursor = config.cursor_start

    for batch_number in range(1, _MAX_MONGODB_SOURCE_BATCHES + 1):
        documents = _fetch_documents(config=config, after_cursor=after_cursor)
        if not documents:
            logs.append(("INFO", f"source exhausted after batch={batch_number}"))
            return MongoDBIncrementalSourceDocumentV1(
                stream=config.stream,
                records=records,
                states=states,
                logs=logs,
            )

        batch_records: list[MongoDBIncrementalSourceRecordV1] = []
        batch_cursor: str | None = None
        for document in documents:
            data: dict[str, object] = {}
            for field in config.fields:
                if field not in document:
                    raise _source_error(
                        message="it-stream MongoDB source documents must include configured fields",
                        retryable=False,
                        details={
                            "connection_name": config.connection_name,
                            "database": config.database,
                            "collection": config.collection,
                            "field": field,
                        },
                    )
                try:
                    data[field] = _normalize_document_value(document[field])
                except TypeError as err:
                    raise _source_error(
                        message="it-stream MongoDB source documents must be JSON-compatible",
                        retryable=False,
                        details={
                            "connection_name": config.connection_name,
                            "database": config.database,
                            "collection": config.collection,
                            "field": field,
                        },
                    ) from err

            cursor_value = data.get(config.cursor_field)
            if not isinstance(cursor_value, str) or not cursor_value:
                raise _source_error(
                    message=(
                        "it-stream MongoDB source cursor field must normalize to a non-empty string"
                    ),
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "database": config.database,
                        "collection": config.collection,
                        "cursor_field": config.cursor_field,
                    },
                )
            if after_cursor is not None and cursor_value <= after_cursor:
                raise _source_error(
                    message="it-stream MongoDB source cursor progression must advance strictly",
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "database": config.database,
                        "collection": config.collection,
                        "cursor_field": config.cursor_field,
                        "cursor": cursor_value,
                        "after_cursor": after_cursor,
                    },
                )
            if batch_cursor is not None and cursor_value <= batch_cursor:
                raise _source_error(
                    message="it-stream MongoDB source cursor progression must advance strictly",
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "database": config.database,
                        "collection": config.collection,
                        "cursor_field": config.cursor_field,
                        "cursor": cursor_value,
                        "previous_cursor": batch_cursor,
                    },
                )

            batch_cursor = cursor_value
            batch_records.append(MongoDBIncrementalSourceRecordV1(data=data, cursor=cursor_value))

        final_batch_cursor = batch_cursor
        if final_batch_cursor is None:
            raise _source_error(
                message="it-stream MongoDB source batch must contain at least one cursor value",
                retryable=False,
                details={
                    "connection_name": config.connection_name,
                    "database": config.database,
                    "collection": config.collection,
                },
            )

        records.extend(batch_records)
        states.append(
            {
                "cursor": final_batch_cursor,
                "connection_name": config.connection_name,
                "database": config.database,
                "collection": config.collection,
                "cursor_field": config.cursor_field,
                "fields": list(config.fields),
                "read_direction": "asc",
                "document_count": len(batch_records),
            }
        )
        logs.append(
            (
                "INFO",
                f"fetched batch={batch_number} documents={len(batch_records)} "
                f"cursor={final_batch_cursor}",
            )
        )
        after_cursor = final_batch_cursor

    raise _source_error(
        message="it-stream MongoDB source exceeded the maximum supported batch count",
        retryable=False,
        details={
            "connection_name": config.connection_name,
            "database": config.database,
            "collection": config.collection,
            "max_batches": _MAX_MONGODB_SOURCE_BATCHES,
        },
    )
