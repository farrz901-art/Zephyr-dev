from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from zephyr_core import RunContext
from zephyr_ingest.obs.batch_report_v1 import BATCH_REPORT_SCHEMA_VERSION, BatchReportV1
from zephyr_ingest.runner import RunnerConfig, run_documents


def test_batch_report_has_schema_version(tmp_path: Path) -> None:
    ctx = RunContext.new()
    cfg = RunnerConfig(out_root=tmp_path / "out")

    run_documents(docs=[], cfg=cfg, ctx=ctx)

    report_path = (cfg.out_root / "batch_report.json").resolve()
    obj = json.loads(report_path.read_text(encoding="utf-8"))
    report = cast(BatchReportV1, obj)

    assert report["schema_version"] == BATCH_REPORT_SCHEMA_VERSION
    assert report["run_id"] == ctx.run_id
    assert report["pipeline_version"] == ctx.pipeline_version
    assert report["executor"] in ("serial", "thread")

    # stable sections
    assert "counts" in report
    assert "delivery" in report
    assert "retry" in report
    assert "durations_ms" in report
    assert "metrics" in report
    assert "stage_durations_ms" in report

    sd = report["stage_durations_ms"]
    assert "hash_ms" in sd
    assert "partition_ms" in sd
    assert "delivery_ms" in sd

    metrics = report["metrics"]
    assert isinstance(metrics["run_wall_ms"], int)
    assert metrics["docs_total"] == report["counts"]["total"]
    assert metrics["delivery_total"] == report["delivery"]["total"]
    assert metrics["dlq_written_total"] == report["delivery"]["dlq_written_total"]

    # delivery retryable breakdown exists and sums to delivery_failed_total
    assert report["delivery"]["failed"] == (
        report["delivery"]["failed_retryable"]
        + report["delivery"]["failed_non_retryable"]
        + report["delivery"]["failed_unknown"]
    )
    assert metrics["delivery_failed_total"] == (
        metrics["delivery_failed_retryable_total"]
        + metrics["delivery_failed_non_retryable_total"]
        + metrics["delivery_failed_unknown_total"]
    )

    assert (metrics["docs_per_min"] is None) or isinstance(metrics["docs_per_min"], float)
