from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from zephyr_ingest import cli


def test_bench_outputs_json_and_uses_iterations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("x", encoding="utf-8")

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        out_root = Path(cfg.out_root)
        out_root.mkdir(parents=True, exist_ok=True)
        # Write minimal batch_report.json with metrics
        payload: dict[str, Any] = {
            "schema_version": 1,
            "run_id": ctx.run_id,
            "pipeline_version": ctx.pipeline_version,
            "timestamp_utc": ctx.timestamp_utc,
            "strategy": "auto",
            "counts": {
                "total": 1,
                "success": 1,
                "failed": 0,
                "skipped_unsupported": 0,
                "skipped_existing": 0,
            },
            "delivery": {
                "total": 1,
                "ok": 1,
                "failed": 0,
                "failed_retryable": 0,
                "failed_non_retryable": 0,
                "failed_unknown": 0,
                "dlq_written_total": 0,
                "dlq_dir": str((out_root / "_dlq" / "delivery").resolve()),
                "by_destination": {},
                "fanout_children_by_destination": {},
            },
            "counts_by_extension": {".txt": 1},
            "counts_by_error_code": {},
            "retry": {
                "enabled": True,
                "max_attempts": 3,
                "base_backoff_ms": 200,
                "max_backoff_ms": 5000,
                "retry_attempts_total": 0,
                "retried_success": 0,
                "retryable_failed": 0,
            },
            "durations_ms": {"min": None, "max": None, "avg": None, "p95": None},
            "generated_at_utc": "2026-01-01T00:00:00Z",
            "workers": 1,
            "executor": "serial",
            "metrics": {
                "run_wall_ms": 123,
                "docs_per_min": 456.0,
                "docs_total": 1,
                "docs_success_total": 1,
                "docs_failed_total": 0,
                "docs_skipped_total": 0,
                "delivery_total": 1,
                "delivery_ok_total": 1,
                "delivery_failed_total": 0,
                "delivery_failed_retryable_total": 0,
                "delivery_failed_non_retryable_total": 0,
                "delivery_failed_unknown_total": 0,
                "dlq_written_total": 0,
            },
        }
        (out_root / "batch_report.json").write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "bench",
            "--path",
            str(inbox),
            "--glob",
            "*.txt",
            "--bench-out",
            str(tmp_path / "bench"),
            "--iterations",
            "2",
            "--warmup",
            "1",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    obj = json.loads(out)
    assert "session_dir" in obj
    assert "results" in obj
    assert len(obj["results"]) == 3  # warmup + iterations
    assert obj["summary"]["iterations"] == 2
    assert obj["summary"]["warmup"] == 1
    assert obj["summary"]["measured_count"] == 2
