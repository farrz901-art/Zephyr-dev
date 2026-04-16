from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_ingest.delivery_idempotency import normalize_weaviate_delivery_object_id
from zephyr_ingest.destinations.base import DeliveryReceipt

logger = logging.getLogger(__name__)


class WeaviateBatchProtocol(Protocol):
    number_errors: int

    def add_object(self, properties: dict[str, Any], uuid: str | None = None) -> str: ...


class WeaviateBatchManagerProtocol(Protocol):
    failed_objects: list[Any]

    def dynamic(self) -> AbstractContextManager[WeaviateBatchProtocol]: ...


class WeaviateCollectionProtocol(Protocol):
    @property
    def batch(self) -> WeaviateBatchManagerProtocol: ...


def _status_failure_kind(*, status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if status_code in {401, 403}:
        return "client_error"
    if 500 <= status_code <= 599:
        return "server_error"
    if status_code in {400, 404, 409, 422}:
        return "constraint"
    return "operational"


def _status_retryable(*, status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def weaviate_failed_object_status(value: object) -> int | None:
    if isinstance(value, dict):
        typed_value = cast(dict[object, object], value)
        raw_status = typed_value.get("status")
        if isinstance(raw_status, int):
            return raw_status
        if isinstance(raw_status, str) and raw_status.isdigit():
            return int(raw_status)
    return None


def classify_weaviate_failed_objects(*, failed_objects: list[Any]) -> tuple[bool, str]:
    statuses = [
        status
        for item in failed_objects
        if (status := weaviate_failed_object_status(item)) is not None
    ]
    if not statuses:
        return True, "operational"
    if any(_status_retryable(status_code=status) for status in statuses):
        retryable_status = next(
            status for status in statuses if _status_retryable(status_code=status)
        )
        return True, _status_failure_kind(status_code=retryable_status)
    return False, _status_failure_kind(status_code=statuses[0])


def weaviate_exception_failure_kind(*, exc: Exception) -> str:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        return _status_failure_kind(status_code=status_code)
    status_code_attr = getattr(exc, "status_code", None)
    if isinstance(status_code_attr, int):
        return _status_failure_kind(status_code=status_code_attr)
    text = str(exc).lower()
    if "timeout" in text:
        return "timeout"
    if "connect" in text or "connection" in text or "network" in text:
        return "connection"
    if "protocol" in text:
        return "protocol"
    if "authentication" in text or "api key" in text or "unauthorized" in text:
        return "client_error"
    return "operational"


def weaviate_exception_retryable(*, exc: Exception) -> bool:
    return weaviate_exception_failure_kind(exc=exc) in {
        "timeout",
        "connection",
        "protocol",
        "rate_limited",
        "server_error",
    }


@dataclass(slots=True, kw_only=True)
class WeaviateDestination:
    """
    Insert 1 Zephyr document as 1 Weaviate object.

    Commit-1 scope:
    - injected collection (no real weaviate-client dependency)
    - unit tests via FakeCollection/FakeBatch
    """

    collection_name: str
    collection: WeaviateCollectionProtocol
    max_batch_errors: int = 0  # 0 => any error fails this delivery
    timeout_s: float | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @property
    def name(self) -> str:
        return "weaviate"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        # No result => nothing to index, but destination executed successfully.
        if result is None:
            return DeliveryReceipt(
                destination=self.name,
                ok=True,
                details={
                    "attempts": 1,
                    "collection": self.collection_name,
                    "skipped": True,
                    "reason": "no_result",
                    "retryable": False,
                },
            )

        obj_uuid = normalize_weaviate_delivery_object_id(sha256=sha256)

        props: dict[str, Any] = {
            "sha256": sha256,
            "uuid": obj_uuid,
            "run_id": meta.run_id,
            "pipeline_version": meta.pipeline_version,
            "timestamp_utc": meta.timestamp_utc,
            "outcome": None if meta.outcome is None else str(meta.outcome),
            "filename": result.document.filename,
            "mime_type": result.document.mime_type,
            "size_bytes": result.document.size_bytes,
            "created_at_utc": result.document.created_at_utc,
            "engine_name": result.engine.name,
            "engine_backend": result.engine.backend,
            "engine_version": result.engine.version,
            "strategy": str(result.engine.strategy),
            "elements_count": len(result.elements),
            "normalized_text": result.normalized_text,
        }
        provenance = meta.provenance
        if provenance is not None:
            if provenance.delivery_origin is not None:
                props["delivery_origin"] = provenance.delivery_origin
            if provenance.execution_mode is not None:
                props["execution_mode"] = provenance.execution_mode

        details: dict[str, Any] = {
            "attempts": 1,
            "collection": self.collection_name,
            "uuid": obj_uuid,
            "object_identity": obj_uuid,
        }

        try:
            with self.collection.batch.dynamic() as batch:
                batch.add_object(properties=props, uuid=obj_uuid)
                batch_errors = batch.number_errors

            failed_objects = self.collection.batch.failed_objects
            failed_count = len(failed_objects)

            details["batch_errors"] = batch_errors
            details["failed_objects"] = failed_count
            details["max_batch_errors"] = self.max_batch_errors
            details["batch_status"] = "ok" if batch_errors == 0 and failed_count == 0 else "partial"

            if failed_count > 0:
                retryable, failure_kind = classify_weaviate_failed_objects(
                    failed_objects=failed_objects
                )
                details["retryable"] = retryable
                details["failure_kind"] = failure_kind
                backend_statuses = [
                    status
                    for item in failed_objects
                    if (status := weaviate_failed_object_status(item)) is not None
                ]
                if backend_statuses:
                    details["backend_statuses"] = backend_statuses
                details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
                return DeliveryReceipt(destination=self.name, ok=False, details=details)

            if batch_errors > self.max_batch_errors:
                details["retryable"] = True
                details["failure_kind"] = "operational"
                details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
                return DeliveryReceipt(destination=self.name, ok=False, details=details)

            details["retryable"] = False
            return DeliveryReceipt(destination=self.name, ok=True, details=details)

        except Exception as exc:
            details["exc_type"] = type(exc).__name__
            details["exc"] = str(exc)
            details["retryable"] = weaviate_exception_retryable(exc=exc)
            details["batch_status"] = "error"
            details["failure_kind"] = weaviate_exception_failure_kind(exc=exc)
            details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
            logger.exception("weaviate_delivery_failed collection=%s", self.collection_name)
            return DeliveryReceipt(destination=self.name, ok=False, details=details)
