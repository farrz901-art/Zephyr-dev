from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    ZephyrElement,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.runner import RunnerConfig, run_documents


class OkDest:
    name = "okdest"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: Any, result: Any = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(destination="okdest", ok=True, details={})


def test_stage_durations_ms_written(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    doc = DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="a.txt",
        extension=".txt",
        size_bytes=f.stat().st_size,
    )

    ctx = RunContext.new()
    cfg = RunnerConfig(out_root=tmp_path / "out", destination=OkDest(), workers=1)

    def fake_partition_fn(*, filename: str, **kw: object) -> PartitionResult:
        dm = DocumentMetadata(
            filename="a.txt",
            mime_type="text/plain",
            sha256="x",
            size_bytes=5,
            created_at_utc="2026-01-01T00:00:00Z",
        )
        eng = EngineInfo(
            name="unstructured", backend="dummy", version="0", strategy=PartitionStrategy.AUTO
        )
        return PartitionResult(
            document=dm,
            engine=eng,
            elements=[ZephyrElement(element_id="e", type="Text", text="hello", metadata={})],
            normalized_text="hello",
            warnings=[],
        )

    run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=fake_partition_fn, destination=OkDest()
    )
    report = json.loads((cfg.out_root / "batch_report.json").read_text(encoding="utf-8"))
    sd = report["stage_durations_ms"]
    assert sd["hash_ms"]["min"] is not None
    assert sd["partition_ms"]["min"] is not None
    assert sd["delivery_ms"]["min"] is not None
