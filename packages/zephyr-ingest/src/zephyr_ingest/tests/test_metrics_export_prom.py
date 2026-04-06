from __future__ import annotations

import json
from pathlib import Path

import pytest

from zephyr_ingest import cli


def test_metrics_export_prom_stdout(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out_root = tmp_path / "out"
    out_root.mkdir(parents=True, exist_ok=True)

    report = {
        "schema_version": 1,
        "run_id": "r1",
        "pipeline_version": "p1",
        "timestamp_utc": "2026-01-01T00:00:00Z",
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
            "by_destination": {"filesystem": {"ok": 1, "failed": 0}},
            "fanout_children_by_destination": {},
            "failure_kinds_by_destination": {
                "webhook": {"server_error": 2},
                "kafka": {"timeout": 1},
            },
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
        "durations_ms": {"min": 1, "max": 2, "avg": 1, "p95": 2},
        "stage_durations_ms": {
            "hash_ms": {"min": 1, "max": 1, "avg": 1, "p95": 1},
            "partition_ms": {"min": 2, "max": 2, "avg": 2, "p95": 2},
            "delivery_ms": {"min": 3, "max": 3, "avg": 3, "p95": 3},
        },
        "generated_at_utc": "2026-01-01T00:00:00Z",
        "workers": 1,
        "executor": "serial",
        "metrics": {
            "run_wall_ms": 1000,
            "docs_per_min": 60.0,
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
    (out_root / "batch_report.json").write_text(json.dumps(report), encoding="utf-8")

    rc = cli.main(["metrics", "export-prom", "--out", str(out_root)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "# HELP zephyr_ingest_run_wall_seconds" in out
    assert "zephyr_ingest_run_wall_seconds" in out
    assert "zephyr_ingest_run_stage_duration_seconds" in out
    assert 'destination="filesystem"' in out
    assert "zephyr_ingest_run_delivery_failure_kind_total" in out
    assert 'destination="webhook",failure_kind="server_error"' in out


def test_metrics_export_prom_textfile(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "batch_report.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_id": "r1",
                "pipeline_version": "p1",
                "timestamp_utc": "2026-01-01T00:00:00Z",
                "strategy": "auto",
                "counts": {
                    "total": 0,
                    "success": 0,
                    "failed": 0,
                    "skipped_unsupported": 0,
                    "skipped_existing": 0,
                },
                "delivery": {
                    "total": 0,
                    "ok": 0,
                    "failed": 0,
                    "failed_retryable": 0,
                    "failed_non_retryable": 0,
                    "failed_unknown": 0,
                    "dlq_written_total": 0,
                    "dlq_dir": str((out_root / "_dlq" / "delivery").resolve()),
                    "by_destination": {},
                    "fanout_children_by_destination": {},
                },
                "counts_by_extension": {},
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
                    "run_wall_ms": 1,
                    "docs_per_min": None,
                    "docs_total": 0,
                    "docs_success_total": 0,
                    "docs_failed_total": 0,
                    "docs_skipped_total": 0,
                    "delivery_total": 0,
                    "delivery_ok_total": 0,
                    "delivery_failed_total": 0,
                    "delivery_failed_retryable_total": 0,
                    "delivery_failed_non_retryable_total": 0,
                    "delivery_failed_unknown_total": 0,
                    "dlq_written_total": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    prom_path = tmp_path / "textfile" / "zephyr_ingest.prom"
    rc = cli.main(["metrics", "export-prom", "--out", str(out_root), "--textfile", str(prom_path)])
    assert rc == 0
    assert prom_path.exists()
    text = prom_path.read_text(encoding="utf-8")
    assert "zephyr_ingest_run_info" in text
