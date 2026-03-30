from __future__ import annotations

import logging
from pathlib import Path

import pytest

from zephyr_core import PartitionResult, RunContext
from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_ingest.runner import RunnerConfig, run_documents


def test_runner_emits_run_start_and_done_events(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    ctx = RunContext.new()
    cfg = RunnerConfig(out_root=tmp_path / "out", workers=1)

    run_documents(docs=[], cfg=cfg, ctx=ctx, config_snapshot=None)

    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("run_start ") for m in msgs)
    assert any(m.startswith("run_done ") for m in msgs)


def test_runner_delivery_failed_emits_dlq_written(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    # Create a minimal "doc" that will fail delivery by using a destination that always fails.
    from zephyr_core.contracts.v1.document_ref import DocumentRef
    from zephyr_ingest.destinations.base import DeliveryReceipt

    class FailingDest:
        name = "faildest"

        def __call__(
            self,
            *,
            out_root: Path,
            sha256: str,
            meta: RunMetaV1,
            result: PartitionResult | None = None,
        ):
            return DeliveryReceipt(destination="faildest", ok=False, details={"retryable": True})

    doc = DocumentRef(
        uri=str((tmp_path / "a.txt").resolve()),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="a.txt",
        extension=".txt",
        size_bytes=1,
    )
    (tmp_path / "a.txt").write_text("x", encoding="utf-8")

    ctx = RunContext.new()
    cfg = RunnerConfig(out_root=tmp_path / "out", destination=FailingDest(), workers=1)

    run_documents(docs=[doc], cfg=cfg, ctx=ctx)

    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("delivery_failed ") for m in msgs)
    assert any(m.startswith("dlq_written ") for m in msgs)
