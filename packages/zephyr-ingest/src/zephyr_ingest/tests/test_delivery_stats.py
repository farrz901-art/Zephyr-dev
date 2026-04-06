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


def _doc(tmp_path: Path, name: str, content: str) -> DocumentRef:
    f = tmp_path / name
    f.write_text(content, encoding="utf-8")
    return DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-03-22T00:00:00Z",
        filename=name,
        extension=Path(name).suffix.lower(),
        size_bytes=f.stat().st_size,
    )


def _ok_partition(**kwargs: Any) -> PartitionResult:
    return PartitionResult(
        document=DocumentMetadata(
            filename="x.txt",
            mime_type="text/plain",
            sha256="dummy",
            size_bytes=1,
            created_at_utc="2026-03-22T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured", backend="local", version="0.0.0", strategy=PartitionStrategy.AUTO
        ),
        elements=[ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})],
        normalized_text="Hello",
        warnings=[],
    )


class OkDest:
    name = "filesystem"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> DeliveryReceipt:
        # simulate filesystem destination wrote run_meta
        d = out_root / sha256
        d.mkdir(parents=True, exist_ok=True)
        (d / "run_meta.json").write_text(
            json.dumps(meta.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        if result is not None:
            (d / "elements.json").write_text("[]", encoding="utf-8")
            (d / "normalized.txt").write_text("", encoding="utf-8")
        return DeliveryReceipt(destination=self.name, ok=True, details=None)


class FanoutFailDest:
    name = "fanout"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(
            destination="fanout",
            ok=False,
            details={
                "receipts": [
                    {"destination": "filesystem", "ok": True, "details": None},
                    {"destination": "webhook", "ok": False, "details": {"status_code": 500}},
                ]
            },
        )


class FailureKindDest:
    name = "webhook"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(
            destination=self.name,
            ok=False,
            details={
                "retryable": True,
                "failure_kind": "server_error",
                "error_code": "delivery_http_failed",
            },
        )


def test_delivery_receipt_written_and_report_counts(tmp_path: Path) -> None:
    doc = _doc(tmp_path, "a.txt", "hello")
    out_root = tmp_path / "out"
    cfg = RunnerConfig(
        out_root=out_root, strategy=PartitionStrategy.AUTO, workers=1, destination=OkDest()
    )
    ctx = RunContext.new(pipeline_version="p1", run_id="r1", timestamp_utc="2026-03-22T00:00:00Z")

    stats = run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=_ok_partition, destination=OkDest()
    )
    assert stats.success == 1

    # delivery_receipt.json exists
    # sha is computed from file; locate by scanning out_root subdirs (only one)
    subdirs = [p for p in out_root.iterdir() if p.is_dir()]
    assert len(subdirs) == 1
    out_dir = subdirs[0]
    assert (out_dir / "delivery_receipt.json").exists()

    report = json.loads((out_root / "batch_report.json").read_text(encoding="utf-8"))
    assert report["delivery"]["total"] == 1
    assert report["delivery"]["ok"] == 1
    assert report["delivery"]["by_destination"]["filesystem"]["ok"] == 1


def test_fanout_children_stats(tmp_path: Path) -> None:
    doc = _doc(tmp_path, "a.txt", "hello")
    out_root = tmp_path / "out"
    cfg = RunnerConfig(
        out_root=out_root, strategy=PartitionStrategy.AUTO, workers=1, destination=FanoutFailDest()
    )
    ctx = RunContext.new(pipeline_version="p1", run_id="r2", timestamp_utc="2026-03-22T00:00:00Z")

    run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=_ok_partition, destination=FanoutFailDest()
    )

    report = json.loads((out_root / "batch_report.json").read_text(encoding="utf-8"))
    assert report["delivery"]["by_destination"]["fanout"]["failed"] == 1

    children = report["delivery"]["fanout_children_by_destination"]
    assert children["filesystem"]["ok"] == 1
    assert children["webhook"]["failed"] == 1


def test_delivery_failure_kind_counts_are_written_to_batch_report(tmp_path: Path) -> None:
    doc = _doc(tmp_path, "a.txt", "hello")
    out_root = tmp_path / "out"
    cfg = RunnerConfig(
        out_root=out_root,
        strategy=PartitionStrategy.AUTO,
        workers=1,
        destination=FailureKindDest(),
    )
    ctx = RunContext.new(pipeline_version="p1", run_id="r3", timestamp_utc="2026-03-22T00:00:00Z")

    run_documents(
        docs=[doc],
        cfg=cfg,
        ctx=ctx,
        partition_fn=_ok_partition,
        destination=FailureKindDest(),
    )

    report = json.loads((out_root / "batch_report.json").read_text(encoding="utf-8"))
    assert report["delivery"]["failure_kinds_by_destination"] == {"webhook": {"server_error": 1}}
