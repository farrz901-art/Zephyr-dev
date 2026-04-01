from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    ZephyrElement,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.runner import RunnerConfig, run_documents


class FailDest:
    name = "faildest"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: Any, result: Any = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(
            destination="faildest",
            ok=False,
            details={"retryable": True, "error_code": str(ErrorCode.DELIVERY_FAILED)},
        )


def fake_partition_fn(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: Any | None = None,
    **kw: object,
) -> PartitionResult:
    doc = DocumentMetadata(
        filename="a.txt",
        mime_type="text/plain",
        sha256="x",
        size_bytes=1,
        created_at_utc="2026-01-01T00:00:00Z",
    )
    eng = EngineInfo(
        name="unstructured", backend="dummy", version="0", strategy=PartitionStrategy.AUTO
    )
    return PartitionResult(
        document=doc,
        engine=eng,
        elements=[ZephyrElement(element_id="e", type="Text", text="x", metadata={})],
        normalized_text="x",
        warnings=[],
    )


def test_delivery_error_code_counted(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("x", encoding="utf-8")
    doc = DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="a.txt",
        extension=".txt",
        size_bytes=1,
    )
    ctx = RunContext.new()
    cfg = RunnerConfig(out_root=tmp_path / "out", destination=FailDest())
    run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=fake_partition_fn, destination=FailDest()
    )
    report = json.loads((cfg.out_root / "batch_report.json").read_text(encoding="utf-8"))
    assert report["counts_by_error_code"][str(ErrorCode.DELIVERY_FAILED)] >= 1
