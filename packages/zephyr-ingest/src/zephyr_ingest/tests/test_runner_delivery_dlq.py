from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from zephyr_core import DocumentRef, RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.contracts.v1.models import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    ZephyrElement,
)
from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.runner import RunnerConfig, run_documents


def _doc(tmp_path: Path) -> DocumentRef:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")
    return DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-03-23T00:00:00Z",
        filename="a.txt",
        extension=".txt",
        size_bytes=f.stat().st_size,
    )


def _ok_partition(**kwargs: Any) -> PartitionResult:
    return PartitionResult(
        document=DocumentMetadata(
            filename="a.txt",
            mime_type="text/plain",
            sha256="dummy",
            size_bytes=1,
            created_at_utc="2026-03-23T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured", backend="local", version="0.0.0", strategy=PartitionStrategy.AUTO
        ),
        elements=[ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})],
        normalized_text="Hello",
        warnings=[],
    )


class FailingDest:
    @property
    def name(self) -> str:
        return "webhook"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        # simulate a delivery failure (we don't need to write artifacts for this test)
        return DeliveryReceipt(destination="webhook", ok=False, details={"status_code": 500})


def test_delivery_failed_writes_dlq(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    cfg = RunnerConfig(
        out_root=out_root, strategy=PartitionStrategy.AUTO, workers=1, destination=FailingDest()
    )
    ctx = RunContext.new(pipeline_version="p1", run_id="r1", timestamp_utc="2026-03-23T00:00:00Z")

    stats = run_documents(
        docs=[_doc(tmp_path)],
        cfg=cfg,
        ctx=ctx,
        partition_fn=_ok_partition,
        destination=FailingDest(),
    )
    assert stats.total == 1

    # DLQ file should exist
    dlq_dir = out_root / "_dlq" / "delivery"
    assert dlq_dir.exists()
    files = list(dlq_dir.glob("*.json"))
    assert len(files) == 1

    data = json.loads(files[0].read_text(encoding="utf-8"))
    assert data["run_id"] == "r1"
    assert data["destination_receipt"]["ok"] is False

    report = json.loads((out_root / "batch_report.json").read_text(encoding="utf-8"))
    assert report["delivery"]["failed"] == 1
    assert report["delivery"]["dlq_written_total"] == 1
    assert report["delivery"]["dlq_dir"]
