from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

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


def _failure_kind_for_batch_outcome(
    *,
    batch_errors: int,
    failed_objects: int,
    max_batch_errors: int,
) -> str | None:
    if batch_errors > max_batch_errors:
        return "error_threshold_exceeded"
    if failed_objects > 0:
        return "partial_failure"
    return None


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

            failure_kind = _failure_kind_for_batch_outcome(
                batch_errors=batch_errors,
                failed_objects=failed_count,
                max_batch_errors=self.max_batch_errors,
            )
            retryable = failure_kind is not None
            details["retryable"] = retryable
            if failure_kind is not None:
                details["failure_kind"] = failure_kind

            if retryable:
                details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
                return DeliveryReceipt(destination=self.name, ok=False, details=details)

            return DeliveryReceipt(destination=self.name, ok=True, details=details)

        except Exception as exc:
            details["exc_type"] = type(exc).__name__
            details["exc"] = str(exc)
            details["retryable"] = True
            details["batch_status"] = "error"
            details["failure_kind"] = "batch_exception"
            details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
            logger.exception("weaviate_delivery_failed collection=%s", self.collection_name)
            return DeliveryReceipt(destination=self.name, ok=False, details=details)
