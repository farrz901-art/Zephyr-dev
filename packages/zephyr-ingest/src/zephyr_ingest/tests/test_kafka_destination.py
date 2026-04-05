"""Unit tests for KafkaDestination (no real Kafka required)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from zephyr_core import RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_ingest.destinations.kafka import KafkaDestination


def _default_calls() -> list[dict[str, Any]]:
    return []


@dataclass
class FakeProducer:
    """Mock Kafka producer for testing."""

    # 使用显式类型的工厂函数来消除 "list[Unknown]"
    calls: list[dict[str, Any]] = field(default_factory=_default_calls)
    should_raise: Exception | None = None
    unflushed_count: int = 0

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None = None,
        value: bytes | None = None,
    ) -> None:
        if self.should_raise:
            raise self.should_raise
        self.calls.append({"topic": topic, "key": key, "value": value})

    def flush(self, *, timeout: float | None = None) -> int:
        return self.unflushed_count


def test_kafka_destination_success(tmp_path: Path) -> None:
    """KafkaDestination sends DeliveryPayloadV1 with schema_version=1."""
    fake = FakeProducer()
    dest = KafkaDestination(topic="test-topic", producer=fake)

    ctx = RunContext.new()
    meta = RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        engine=None,
        document=None,
        # metrics=None,
        error=None,
        warnings=[],
    )

    sha = "abc123"
    receipt = dest(out_root=tmp_path, sha256=sha, meta=meta, result=None)

    assert receipt.ok is True
    assert receipt.destination == "kafka"
    assert receipt.details is not None
    assert receipt.details["retryable"] is False
    assert receipt.details["flush_attempted"] is True
    assert receipt.details["flush_completed"] is True
    assert len(fake.calls) == 1

    call = fake.calls[0]
    assert call["topic"] == "test-topic"
    assert call["key"] == f"{sha}:{meta.run_id}".encode("utf-8")

    # Verify value is valid JSON with schema_version
    payload = json.loads(call["value"])
    assert payload["schema_version"] == 1
    assert payload["sha256"] == sha
    assert "run_meta" in payload
    assert "artifacts" in payload


def test_kafka_destination_producer_exception() -> None:
    """Producer exception results in ok=False receipt with retryable=True."""
    fake = FakeProducer(should_raise=RuntimeError("broker unreachable"))
    dest = KafkaDestination(topic="test-topic", producer=fake)

    ctx = RunContext.new()
    meta = RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        engine=None,
        document=None,
        # metrics=None,
        error=None,
        warnings=[],
    )

    receipt = dest(out_root=Path("/tmp"), sha256="xyz", meta=meta, result=None)

    assert receipt.ok is False
    assert receipt.details is not None

    assert receipt.details["retryable"] is True
    assert receipt.details["exc_type"] == "RuntimeError"
    assert receipt.details["failure_kind"] == "producer_error"
    assert receipt.details["flush_attempted"] is False
    assert receipt.details["flush_completed"] is False
    assert "broker unreachable" in receipt.details["exc"]


def test_kafka_destination_unflushed() -> None:
    """Unflushed messages result in ok=False with retryable=True."""
    fake = FakeProducer(unflushed_count=5)
    dest = KafkaDestination(topic="test-topic", producer=fake)

    ctx = RunContext.new()
    meta = RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        engine=None,
        document=None,
        # metrics=None,
        error=None,
        warnings=[],
    )

    receipt = dest(out_root=Path("/tmp"), sha256="xyz", meta=meta, result=None)

    assert receipt.ok is False
    assert receipt.details is not None

    assert receipt.details["unflushed"] == 5
    assert receipt.details["retryable"] is True
    assert receipt.details["failure_kind"] == "flush_incomplete"
    assert receipt.details["flush_attempted"] is True
    assert receipt.details["flush_completed"] is True


def test_kafka_destination_timeout_exception_maps_to_timeout_failure_kind() -> None:
    fake = FakeProducer(should_raise=TimeoutError("flush timed out"))
    dest = KafkaDestination(topic="test-topic", producer=fake, flush_timeout_s=3.0)

    ctx = RunContext.new()
    meta = RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        engine=None,
        document=None,
        error=None,
        warnings=[],
    )

    receipt = dest(out_root=Path("/tmp"), sha256="xyz", meta=meta, result=None)

    assert receipt.ok is False
    assert receipt.details is not None
    assert receipt.details["retryable"] is True
    assert receipt.details["exc_type"] == "TimeoutError"
    assert receipt.details["failure_kind"] == "timeout"
    assert receipt.details["flush_attempted"] is False
    assert receipt.details["flush_completed"] is False
