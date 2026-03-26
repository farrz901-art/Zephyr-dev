"""Real Kafka producer factory (requires kafka optional extra)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from zephyr_ingest.destinations.kafka import ProducerProtocol

logger = logging.getLogger(__name__)


def make_kafka_producer(
    *,
    brokers: str,
    client_id: str | None = None,
    **extra_config: Any,
) -> ProducerProtocol:
    """
    Create a real Kafka producer (requires confluent-kafka).

    Raises ImportError if kafka extra not installed.
    """
    try:
        from confluent_kafka import Producer  # type: ignore
    except ImportError as e:
        raise ImportError(
            "Kafka client not available. Install with: uv pip install zephyr-ingest[kafka]"
        ) from e

    config: dict[str, Any] = {
        "bootstrap.servers": brokers,
        "client.id": client_id or "zephyr-ingest",
    }
    config.update(extra_config)

    producer = cast(Any, Producer(config))

    # Wrap confluent-kafka Producer to match ProducerProtocol
    return _ConfluentProducerAdapter(producer)


class _ConfluentProducerAdapter:
    """Adapt confluent_kafka.Producer to our ProducerProtocol."""

    def __init__(self, producer: Any) -> None:
        self._producer = producer

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None = None,
        value: bytes | None = None,
    ) -> None:
        # confluent-kafka's produce() signature differs slightly
        self._producer.produce(topic, value=value, key=key)

    def flush(self, *, timeout: float | None = None) -> int:
        # confluent-kafka flush() returns unflushed count
        t = timeout if timeout is not None else -1.0
        return int(self._producer.flush(timeout=t))
