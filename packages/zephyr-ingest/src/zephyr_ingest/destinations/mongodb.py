from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, cast

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import DeliveryPayloadV1
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt


class MongoReplaceResultProtocol(Protocol):
    @property
    def acknowledged(self) -> bool: ...

    @property
    def matched_count(self) -> int: ...

    @property
    def modified_count(self) -> int: ...

    @property
    def upserted_id(self) -> object | None: ...


class MongoCollectionProtocol(Protocol):
    def replace_one(
        self,
        filter: Mapping[str, object],
        replacement: Mapping[str, object],
        *,
        upsert: bool,
    ) -> MongoReplaceResultProtocol: ...


class MongoDatabaseProtocol(Protocol):
    def __getitem__(self, name: str) -> MongoCollectionProtocol: ...


class MongoClientProtocol(Protocol):
    def __getitem__(self, name: str) -> MongoDatabaseProtocol: ...

    def close(self) -> None: ...


class MongoClientFactory(Protocol):
    def __call__(
        self,
        host: str | None = None,
        port: int | None = None,
        document_class: type[dict[str, object]] | None = None,
        tz_aware: bool | None = None,
        connect: bool | None = None,
        *,
        username: str | None = None,
        password: str | None = None,
        serverSelectionTimeoutMS: int | None = None,
    ) -> MongoClientProtocol: ...


_SUPPORTED_WRITE_MODES = {"replace_upsert"}


def make_mongodb_collection(
    *,
    uri: str,
    database: str,
    collection: str,
    timeout_s: float,
    username: str | None = None,
    password: str | None = None,
) -> tuple[MongoClientProtocol, MongoCollectionProtocol]:
    mongo_client_module = importlib.import_module("pymongo.mongo_client")
    mongo_client_cls = cast(MongoClientFactory, getattr(mongo_client_module, "MongoClient"))
    client = mongo_client_cls(
        host=uri,
        username=username,
        password=password,
        serverSelectionTimeoutMS=max(int(timeout_s * 1000), 1),
    )
    database_obj = client[database]
    collection_obj = database_obj[collection]
    return client, collection_obj


def _validate_name(*, value: str, field_name: str) -> None:
    if not value.strip() or "\x00" in value:
        raise ValueError(f"mongodb destination {field_name} must be a non-empty string")


def _validate_write_mode(*, write_mode: str) -> None:
    if write_mode not in _SUPPORTED_WRITE_MODES:
        allowed = ", ".join(sorted(_SUPPORTED_WRITE_MODES))
        raise ValueError(f"mongodb destination write_mode must be one of: {allowed}")


def _exception_failure_kind(*, exc: Exception) -> str:
    exc_type = type(exc).__name__
    text = f"{exc_type} {exc}".lower()

    if exc_type in {"DuplicateKeyError", "DocumentTooLarge"}:
        return "constraint"
    if exc_type in {"NetworkTimeout", "ExecutionTimeout"} or "timeout" in text:
        return "timeout"
    if exc_type in {"AutoReconnect", "ConnectionFailure", "ServerSelectionTimeoutError"}:
        return "connection"
    if "duplicate key" in text or "e11000" in text:
        return "constraint"
    if "validation" in text or "document failed validation" in text:
        return "constraint"
    if "bson" in text and "size" in text:
        return "constraint"
    if "server selection" in text or "connection" in text or "connect" in text:
        return "connection"
    if "authentication" in text or "unauthorized" in text or "not authorized" in text:
        return "client_error"
    return "operational"


def _exception_retryable(*, exc: Exception) -> bool:
    return _exception_failure_kind(exc=exc) in {"timeout", "connection"}


def _build_document(*, payload: DeliveryPayloadV1, idempotency_key: str) -> dict[str, object]:
    return {
        "_id": idempotency_key,
        "delivery_identity": idempotency_key,
        "payload": payload,
    }


def send_delivery_payload_v1_to_mongodb(
    *,
    collection_obj: MongoCollectionProtocol,
    database: str,
    collection: str,
    payload: DeliveryPayloadV1,
    idempotency_key: str,
    write_mode: str,
) -> DeliveryReceipt:
    _validate_name(value=database, field_name="database")
    _validate_name(value=collection, field_name="collection")
    _validate_write_mode(write_mode=write_mode)

    document = _build_document(payload=payload, idempotency_key=idempotency_key)
    details: dict[str, object] = {
        "database": database,
        "collection": collection,
        "document_id": idempotency_key,
        "write_mode": write_mode,
        "attempted_count": 1,
    }

    try:
        result = collection_obj.replace_one({"_id": idempotency_key}, document, upsert=True)
    except Exception as exc:
        details["accepted_count"] = 0
        details["rejected_count"] = 1
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = _exception_retryable(exc=exc)
        details["failure_kind"] = _exception_failure_kind(exc=exc)
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        backend_error_code = getattr(exc, "code", None)
        if isinstance(backend_error_code, int):
            details["backend_error_code"] = backend_error_code
        return DeliveryReceipt(destination="mongodb", ok=False, details=details)

    if not result.acknowledged:
        details["accepted_count"] = 0
        details["rejected_count"] = 1
        details["retryable"] = False
        details["failure_kind"] = "operational"
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        return DeliveryReceipt(destination="mongodb", ok=False, details=details)

    details["accepted_count"] = 1
    details["rejected_count"] = 0
    details["retryable"] = False
    details["matched_count"] = result.matched_count
    details["modified_count"] = result.modified_count
    upserted_id = result.upserted_id
    if upserted_id is not None:
        details["upserted_id"] = str(upserted_id)
        details["operation"] = "upsert_inserted"
    elif result.matched_count > 0:
        details["operation"] = "upsert_replaced"
    else:
        details["operation"] = "upsert_acknowledged"
    return DeliveryReceipt(destination="mongodb", ok=True, details=details)


@dataclass(frozen=True, slots=True)
class MongoDBDestination:
    uri: str
    database: str
    collection: str
    timeout_s: float = 10.0
    write_mode: str = "replace_upsert"
    username: str | None = None
    password: str | None = None
    collection_obj: MongoCollectionProtocol | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @property
    def name(self) -> str:
        return "mongodb"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        del result
        payload = build_delivery_payload_v1(out_root=out_root, sha256=sha256, meta=meta)
        idempotency_key = normalize_delivery_idempotency_key(
            identity=DeliveryIdentityV1(sha256=sha256, run_id=meta.run_id)
        )
        collection_obj = self.collection_obj
        if collection_obj is None:
            client, collection_obj = make_mongodb_collection(
                uri=self.uri,
                database=self.database,
                collection=self.collection,
                timeout_s=self.timeout_s,
                username=self.username,
                password=self.password,
            )
            try:
                return send_delivery_payload_v1_to_mongodb(
                    collection_obj=collection_obj,
                    database=self.database,
                    collection=self.collection,
                    payload=payload,
                    idempotency_key=idempotency_key,
                    write_mode=self.write_mode,
                )
            finally:
                client.close()

        return send_delivery_payload_v1_to_mongodb(
            collection_obj=collection_obj,
            database=self.database,
            collection=self.collection,
            payload=payload,
            idempotency_key=idempotency_key,
            write_mode=self.write_mode,
        )
