from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_recovery_operator_report import main as p5_recovery_main

from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_inspect import inspect_local_sqlite_queue
from zephyr_ingest.queue_recover import requeue_local_task
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.testing.p5_recovery_operator import (
    P5_RECOVERY_MATRIX_PATH,
    P5_RECOVERY_RUNBOOK_PATH,
    build_p5_recovery_scenario_matrix,
    format_p5_recovery_check_results,
    summarize_recovery_artifacts,
    validate_p5_recovery_artifacts,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_task(task_id: str) -> TaskV1:
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1(
                uri=f"/tmp/{task_id}.txt",
                source="local_file",
                discovered_at_utc="2026-04-19T00:00:00Z",
                filename=f"{task_id}.txt",
                extension=".txt",
                size_bytes=32,
            )
        ),
        execution=TaskExecutionV1(),
        identity=TaskIdentityV1(
            pipeline_version="p5-m3-recovery",
            sha256=f"sha-{task_id}",
        ),
    )


def _write_recovery_out_root(out_root: Path) -> None:
    artifact_dir = out_root / "sha-recovery"
    _write_json(
        artifact_dir / "run_meta.json",
        {
            "schema_version": 1,
            "outcome": "success",
            "run_id": "run-recovery",
            "pipeline_version": "p5-m3-recovery",
            "timestamp_utc": "2026-04-19T00:00:00Z",
            "document": {
                "filename": "records.jsonl",
                "mime_type": "application/x-ndjson",
                "sha256": "sha-recovery",
                "size_bytes": 12,
                "created_at_utc": "2026-04-19T00:00:00Z",
            },
            "engine": {
                "name": "recovery-test",
                "backend": "test",
                "version": "0",
                "strategy": "auto",
            },
            "metrics": {
                "duration_ms": 1,
                "elements_count": 1,
                "normalized_text_len": 8,
                "attempts": 1,
            },
            "warnings": [],
            "error": None,
            "provenance": {
                "run_origin": "resume",
                "delivery_origin": "replay",
                "execution_mode": "worker",
                "task_id": "task-recovery",
                "checkpoint_identity_key": "checkpoint-1",
                "task_identity_key": (
                    '{"kind":"it","pipeline_version":"p5-m3-recovery","sha256":"sha-recovery"}'
                ),
            },
        },
    )
    _write_json(
        artifact_dir / "delivery_receipt.json",
        {
            "destination": "sqlite",
            "ok": False,
            "details": {
                "retryable": True,
                "failure_kind": "connection",
                "error_code": "ZE-DELIVERY-FAILED",
            },
            "summary": {
                "delivery_outcome": "failed",
                "failure_retryability": "retryable",
                "failure_kind": "connection",
                "error_code": "ZE-DELIVERY-FAILED",
                "attempt_count": 1,
                "payload_count": 1,
            },
        },
    )
    _write_json(
        out_root / "batch_report.json",
        {
            "delivery": {
                "total": 1,
                "ok": 0,
                "failed": 1,
                "failed_retryable": 1,
            },
            "counts_by_error_code": {"ZE-DELIVERY-FAILED": 1},
        },
    )
    _write_json(out_root / "_dlq" / "delivery" / "pending.json", {"sha256": "sha-recovery"})
    _write_json(out_root / "_dlq" / "delivery_done" / "done.json", {"sha256": "sha-recovery"})


@pytest.mark.auth_contract
def test_p5_recovery_matrix_artifact_matches_helper_shape() -> None:
    artifact = json.loads(P5_RECOVERY_MATRIX_PATH.read_text(encoding="utf-8"))
    built = build_p5_recovery_scenario_matrix()
    built_scenarios = cast(list[dict[str, object]], built["scenarios"])

    assert artifact["phase"] == built["phase"]
    assert artifact["truth_surface"] == "repo-side artifact-backed operator evidence"
    assert artifact["decisive_history_fields"] == ["run_origin", "delivery_origin"]
    assert {scenario["name"] for scenario in artifact["scenarios"]} == {
        scenario["name"] for scenario in built_scenarios
    }
    assert P5_RECOVERY_RUNBOOK_PATH.exists()


@pytest.mark.auth_contract
def test_p5_recovery_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_recovery_artifacts()

    assert all(check.ok for check in checks), format_p5_recovery_check_results(checks)


@pytest.mark.auth_contract
def test_p5_recovery_summary_combines_queue_delivery_and_provenance(
    tmp_path: Path,
) -> None:
    queue_root = tmp_path / "queue"
    queue = build_local_queue_backend(kind="sqlite", root=queue_root)
    task = _make_task("task-poison")
    queue.enqueue(task)
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    out_root = tmp_path / "out"
    _write_recovery_out_root(out_root)

    summary = summarize_recovery_artifacts(out_root=out_root, queue_root=queue_root)
    queue_summary = cast(dict[str, object], cast(dict[str, object], summary["queue"])["summary"])
    delivery_summary = cast(dict[str, object], summary["delivery"])
    histories = cast(list[dict[str, object]], summary["provenance_histories"])
    actions = cast(list[str], summary["suggested_next_actions"])

    assert queue_summary["poison"] == 1
    assert delivery_summary["dlq_pending"] == 1
    assert delivery_summary["dlq_done"] == 1
    assert histories == [
        {
            "path": str((out_root / "sha-recovery" / "run_meta.json").resolve()),
            "run_origin": "resume",
            "delivery_origin": "replay",
            "task_id": "task-recovery",
            "checkpoint_identity_key": "checkpoint-1",
        }
    ]
    assert "inspect_poison_then_consider_requeue" in actions
    assert "replay_delivery_dlq" in actions
    assert "verify_replay_delivery_done" in actions


@pytest.mark.auth_contract
def test_p5_recovery_requeue_keeps_requeue_provenance_distinct(
    tmp_path: Path,
) -> None:
    queue_root = tmp_path / "queue"
    queue = build_local_queue_backend(kind="sqlite", root=queue_root)
    task = _make_task("task-requeue")
    queue.enqueue(task)
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    poison_view = inspect_local_sqlite_queue(root=queue_root, bucket="poison").to_dict()
    recovery = requeue_local_task(
        root=queue_root,
        source_bucket="poison",
        task_id="task-requeue",
        backend_kind="sqlite",
    )
    pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()

    assert poison_view["tasks"][0]["governance_problem"] == "poison_attempts_exhausted"
    assert recovery.to_dict()["audit_support"] == "result_only"
    assert recovery.to_dict()["redrive_support"] == "not_modeled"
    assert recovery.to_run_provenance().to_dict() == {
        "run_origin": "requeue",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-requeue",
        "task_identity_key": (
            '{"kind":"uns","pipeline_version":"p5-m3-recovery","sha256":"sha-task-requeue"}'
        ),
    }
    assert pending_view["summary"]["pending"] == 1


@pytest.mark.auth_contract
def test_p5_recovery_operator_cli_prints_paths_checks_and_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    queue_root = tmp_path / "queue"
    out_root = tmp_path / "out"
    queue = build_local_queue_backend(kind="sqlite", root=queue_root)
    task = _make_task("task-cli")
    queue.enqueue(task)
    _write_recovery_out_root(out_root)

    assert p5_recovery_main(["--print-matrix-path"]) == 0
    matrix_output = capsys.readouterr()
    assert Path(matrix_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_recovery_scenario_matrix.json",
    )

    assert p5_recovery_main(["--check-artifacts"]) == 0
    check_output = capsys.readouterr()
    assert "recovery_matrix_exists -> ok" in check_output.out

    assert p5_recovery_main(["--queue-root", str(queue_root), "--out-root", str(out_root)]) == 0
    summary_output = capsys.readouterr()
    assert "P5-M3 recovery operator summary:" in summary_output.out
    assert "dlq_pending=1" in summary_output.out

    assert (
        p5_recovery_main(["--queue-root", str(queue_root), "--out-root", str(out_root), "--json"])
        == 0
    )
    json_output = capsys.readouterr()
    payload_obj: object = json.loads(json_output.out)
    assert isinstance(payload_obj, dict)
    payload = cast(dict[str, object], payload_obj)
    assert "operator_first_steps" in payload
