"""KafkaDestination: deliver DeliveryPayloadV1 to Kafka topic."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from zephyr_core import RunMetaV1
from zephyr_core.contracts.v1.models import PartitionResult
from zephyr_ingest._internal.delivery_payload import (
    DeliveryPayloadV1,
    build_delivery_payload_v1,
)
from zephyr_ingest.destinations.base import DeliveryReceipt

logger = logging.getLogger(__name__)


class ProducerProtocol(Protocol):
    """Minimal Kafka producer interface (for injection & testing)."""

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None = None,
        value: bytes | None = None,
    ) -> None:
        """Send message to topic; may raise on error."""
        ...

    def flush(self, *, timeout: float | None = None) -> int:
        """Block until all buffered messages are sent; return unflushed count."""
        ...


@dataclass(slots=True, kw_only=True)
class KafkaDestination:
    """
    Deliver partition results to Kafka topic.

    - payload: DeliveryPayloadV1 (schema_version=1)
    - key: "{sha256}:{run_id}" (idempotency / ordering)
    - value: JSON-encoded payload
    - producer: injected (allows FakeProducer for tests)
    """

    topic: str
    producer: ProducerProtocol
    flush_timeout_s: float = 10.0

    @property
    def name(self) -> str:
        """符合 Destination 协议要求的标识名称"""
        return "kafka"

    def __call__(
        self,
        *,
        out_root: str | Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        """Deliver to Kafka; return receipt."""

        actual_out_root = Path(out_root)
        payload: DeliveryPayloadV1 = build_delivery_payload_v1(
            out_root=actual_out_root,
            sha256=sha256,
            meta=meta,
        )

        # Idempotency key (same semantics as webhook)
        key_str = f"{sha256}:{meta.run_id}"
        key_bytes = key_str.encode("utf-8")

        # Value: JSON-encoded payload
        payload_str = json.dumps(payload, ensure_ascii=False)
        value_bytes = payload_str.encode("utf-8")

        details: dict[str, Any] = {
            "topic": self.topic,
            "key_len": len(key_bytes),
            "value_len": len(value_bytes),
            "flush_timeout_s": self.flush_timeout_s,
        }

        try:
            self.producer.produce(topic=self.topic, key=key_bytes, value=value_bytes)
            unflushed = self.producer.flush(timeout=self.flush_timeout_s)

            if unflushed > 0:
                logger.warning(
                    "kafka_flush_incomplete topic=%s unflushed=%d", self.topic, unflushed
                )
                details["unflushed"] = unflushed
                details["retryable"] = True
                return DeliveryReceipt(
                    destination="kafka",
                    ok=False,
                    details=details,
                )

            logger.info(
                "kafka_delivered topic=%s sha256=%s run_id=%s",
                self.topic,
                sha256,
                meta.run_id,
            )
            return DeliveryReceipt(destination="kafka", ok=True, details=details)

        except Exception as exc:
            exc_type = type(exc).__name__
            logger.exception(
                "kafka_delivery_failed topic=%s sha256=%s exc_type=%s",
                self.topic,
                sha256,
                exc_type,
            )
            details["exc_type"] = exc_type
            details["exc"] = str(exc)
            # Conservative: mark retryable unless proven config error
            details["retryable"] = True
            return DeliveryReceipt(destination="kafka", ok=False, details=details)
