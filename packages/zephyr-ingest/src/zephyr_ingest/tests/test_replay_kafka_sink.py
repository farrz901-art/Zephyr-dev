from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from zephyr_ingest.replay_delivery import KafkaReplaySink, replay_delivery_dlq


def _default_calls() -> list[dict[str, Any]]:
    return []


@dataclass
class FakeProducer:
    calls: list[dict[str, Any]] = field(default_factory=_default_calls)

    def produce(self, *, topic: str, key: bytes | None = None, value: bytes | None = None) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value})

    def flush(self, *, timeout: float | None = None) -> int:
        return 0


def test_replay_to_kafka_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    dlq_dir = out_root / "_dlq" / "delivery"
    dlq_dir.mkdir(parents=True, exist_ok=True)

    # Create a valid DLQ record (run_meta can be minimal dict for payload)
    (dlq_dir / "one.json").write_text(
        json.dumps({"sha256": "abc", "run_id": "r1", "run_meta": {"schema_version": 3}}),
        encoding="utf-8",
    )

    fake = FakeProducer()
    sink = KafkaReplaySink(topic="t", producer=fake)
    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)

    assert stats.total == 1
    assert stats.succeeded == 1
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["topic"] == "t"
    assert call["key"] == b"abc:r1"
    payload = json.loads(call["value"])
    assert payload["schema_version"] == 1
    assert payload["sha256"] == "abc"
