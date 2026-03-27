from __future__ import annotations

import logging
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from zephyr_core import PartitionResult, RunMetaV1
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


def _uuid_for_sha256(*, sha256: str) -> str:
    # Align with weaviate.util.generate_uuid5(identifier, namespace=""):
    # uuid5(NAMESPACE_DNS, str(namespace)+str(identifier))
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, sha256))


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

        obj_uuid = _uuid_for_sha256(sha256=sha256)

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
        }

        try:
            with self.collection.batch.dynamic() as batch:
                batch.add_object(properties=props, uuid=obj_uuid)
                batch_errors = batch.number_errors

            failed_objects = self.collection.batch.failed_objects
            failed_count = len(failed_objects)

            details["batch_errors"] = batch_errors
            details["failed_objects"] = failed_count

            retryable = (batch_errors > self.max_batch_errors) or (failed_count > 0)
            details["retryable"] = retryable

            if retryable:
                return DeliveryReceipt(destination=self.name, ok=False, details=details)

            return DeliveryReceipt(destination=self.name, ok=True, details=details)

        except Exception as exc:
            details["exc_type"] = type(exc).__name__
            details["exc"] = str(exc)
            details["retryable"] = True
            logger.exception("weaviate_delivery_failed collection=%s", self.collection_name)
            return DeliveryReceipt(destination=self.name, ok=False, details=details)
