"""KafkaDestination: deliver DeliveryPayloadV1 to Kafka topic."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from zephyr_core import ErrorCode, RunMetaV1
from zephyr_core.contracts.v1.models import PartitionResult
from zephyr_ingest._internal.delivery_payload import (
    DeliveryPayloadV1,
    build_delivery_payload_v1,
)
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
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


def _failure_kind_for_unflushed(*, unflushed: int) -> str:
    return "flush_incomplete" if unflushed > 0 else "unknown"


def _failure_kind_for_exception(exc: Exception) -> str:
    if isinstance(exc, TimeoutError):
        return "timeout"
    return "producer_error"


def _is_retryable_kafka_exception(exc: Exception) -> bool:
    return True


def send_delivery_payload_v1_to_kafka(
    *,
    producer: ProducerProtocol,
    topic: str,
    payload: DeliveryPayloadV1,
    key_str: str,
    flush_timeout_s: float,
) -> DeliveryReceipt:
    """
    Shared Kafka send helper used by both KafkaDestination and replay-delivery.
    Keeps serialization + receipt details stable.
    """
    key_bytes = key_str.encode("utf-8")
    payload_str = json.dumps(payload, ensure_ascii=False)
    value_bytes = payload_str.encode("utf-8")

    details: dict[str, Any] = {
        "topic": topic,
        "key_len": len(key_bytes),
        "value_len": len(value_bytes),
        "flush_timeout_s": flush_timeout_s,
        "flush_attempted": False,
    }

    try:
        producer.produce(topic=topic, key=key_bytes, value=value_bytes)
        details["produce_ok"] = True
        details["flush_attempted"] = True
        unflushed = producer.flush(timeout=flush_timeout_s)
        details["flush_completed"] = True

        if unflushed > 0:
            logger.warning("kafka_flush_incomplete topic=%s unflushed=%d", topic, unflushed)
            details["unflushed"] = unflushed
            details["retryable"] = True
            details["failure_kind"] = _failure_kind_for_unflushed(unflushed=unflushed)
            details["error_code"] = str(ErrorCode.DELIVERY_KAFKA_FAILED)
            return DeliveryReceipt(destination="kafka", ok=False, details=details)

        details["retryable"] = False
        return DeliveryReceipt(destination="kafka", ok=True, details=details)
    except Exception as exc:
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = _is_retryable_kafka_exception(exc)
        details["failure_kind"] = _failure_kind_for_exception(exc)
        details["flush_completed"] = False
        details["error_code"] = str(ErrorCode.DELIVERY_KAFKA_FAILED)
        logger.exception("kafka_delivery_failed topic=%s exc_type=%s", topic, details["exc_type"])
        return DeliveryReceipt(destination="kafka", ok=False, details=details)


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
    max_inflight: int | None = None
    rate_limit: float | None = None

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
        key_str = normalize_delivery_idempotency_key(
            identity=DeliveryIdentityV1(sha256=sha256, run_id=meta.run_id)
        )
        receipt = send_delivery_payload_v1_to_kafka(
            producer=self.producer,
            topic=self.topic,
            payload=payload,
            key_str=key_str,
            flush_timeout_s=self.flush_timeout_s,
        )
        if receipt.ok:
            logger.info(
                "kafka_delivered topic=%s sha256=%s run_id=%s", self.topic, sha256, meta.run_id
            )
        return receipt
