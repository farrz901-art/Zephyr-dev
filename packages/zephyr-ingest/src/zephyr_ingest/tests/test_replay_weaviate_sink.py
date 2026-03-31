from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, cast

from zephyr_ingest.replay_delivery import WeaviateReplaySink, replay_delivery_dlq


@dataclass
class FakeBatch:
    number_errors: int = 0
    added: list[dict[str, Any]] = field(default_factory=lambda: cast(list[dict[str, Any]], []))

    def add_object(self, properties: dict[str, Any], uuid: str | None = None) -> str:
        self.added.append({"properties": properties, "uuid": uuid})
        return uuid or "generated"


@dataclass
class FakeBatchManager:
    batch: FakeBatch
    failed_objects: list[Any] = field(default_factory=lambda: cast(list[Any], []))

    @contextmanager
    def dynamic(self) -> Iterator[FakeBatch]:
        yield self.batch


@dataclass
class FakeCollection:
    batch: FakeBatchManager


def test_replay_to_weaviate_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    sha = "abc"
    run_id = "r1"

    # Artifacts
    doc_dir = out_root / sha
    doc_dir.mkdir(parents=True, exist_ok=True)
    (doc_dir / "normalized.txt").write_text("hello", encoding="utf-8")
    (doc_dir / "elements.json").write_text(
        json.dumps([{"type": "Text", "text": "hello"}]), encoding="utf-8"
    )

    # DLQ record
    dlq_dir = out_root / "_dlq" / "delivery"
    dlq_dir.mkdir(parents=True, exist_ok=True)
    (dlq_dir / "one.json").write_text(
        json.dumps(
            {
                "sha256": sha,
                "run_id": run_id,
                "run_meta": {"run_id": run_id, "pipeline_version": "p1"},
            }
        ),
        encoding="utf-8",
    )

    fake_batch = FakeBatch()
    coll = FakeCollection(batch=FakeBatchManager(batch=fake_batch))
    sink = WeaviateReplaySink(collection_name="ZephyrDoc", collection=coll)

    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
    assert stats.total == 1
    assert stats.succeeded == 1
    assert len(fake_batch.added) == 1
    props = fake_batch.added[0]["properties"]
    assert props["sha256"] == sha
    assert props["normalized_text"] == "hello"
    assert props["elements_count"] == 1
