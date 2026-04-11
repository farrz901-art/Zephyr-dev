from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping

import pytest

from zephyr_core import DocumentRef, PartitionStrategy
from zephyr_ingest.queue_inspect import inspect_local_spool_queue
from zephyr_ingest.queue_recover import QueueRecoveryError, requeue_local_spool_task
from zephyr_ingest.spool_queue import LocalSpoolQueue
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)

_NON_OPERATOR_SURFACE_KEYS = {
    "delivery_outcome",
    "failure_retryability",
    "failure_kind",
    "error_code",
    "attempt_count",
    "payload_count",
    "accepted_count",
    "rejected_count",
    "backend_error_code",
    "object_key",
    "document_id",
    "table",
    "row_count",
    "stream",
    "tenant_id",
    "line_count",
    "source_kind",
    "source_url",
    "source_page_id",
    "source_page_version",
    "source_relative_path",
    "cursor",
    "progress_kind",
    "checkpoint_identity_key",
    "parent_checkpoint_identity_key",
    "task_identity_key",
    "run_origin",
    "delivery_origin",
    "execution_mode",
}


def _make_task(task_id: str) -> TaskV1:
    doc = DocumentRef(
        uri=f"/tmp/{task_id}.pdf",
        source="local_file",
        discovered_at_utc="2026-04-05T00:00:00Z",
        filename=f"{task_id}.pdf",
        extension=".pdf",
        size_bytes=128,
    )
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
        execution=TaskExecutionV1(
            strategy=PartitionStrategy.AUTO,
            unique_element_ids=True,
        ),
    )


def _make_identified_task(task_id: str) -> TaskV1:
    return TaskV1(
        task_id=task_id,
        kind="it",
        inputs=_make_task(task_id).inputs,
        execution=TaskExecutionV1(
            strategy=PartitionStrategy.AUTO,
            unique_element_ids=True,
        ),
        identity=TaskIdentityV1(
            pipeline_version="2026.04.06",
            sha256=f"sha256-{task_id}",
        ),
    )


def _read_json(path: Path) -> dict[str, object]:
    obj: dict[str, object] = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(obj, dict)
    return obj


def _assert_no_non_operator_surface_leakage(payload: Mapping[str, object]) -> None:
    for key in _NON_OPERATOR_SURFACE_KEYS:
        assert key not in payload


def test_enqueue_writes_pending_task(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")
    task = _make_task("task-001")

    path = queue.enqueue(task)

    assert path == queue.pending_dir / "task-001.json"
    assert path.exists()
    assert _read_json(path) == {
        "task": task.to_dict(),
        "governance": {
            "failure_count": 0,
            "orphan_count": 0,
        },
    }


def test_claim_transitions_pending_to_inflight(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")
    task = _make_task("task-002")
    queue.enqueue(task)

    claimed = queue.claim_next()

    assert claimed is not None
    assert claimed.task == task
    assert not (queue.pending_dir / "task-002.json").exists()
    assert claimed.claim_ref == queue.inflight_dir / "task-002.json"
    assert Path(claimed.claim_ref).exists()


def test_ack_success_transitions_inflight_to_done(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")
    queue.enqueue(_make_task("task-003"))
    claimed = queue.claim_next()
    assert claimed is not None

    target = queue.ack_success(claimed)

    assert target == queue.done_dir / "task-003.json"
    assert target.exists()
    assert not (queue.inflight_dir / "task-003.json").exists()


def test_ack_failure_requeues_pending_and_increments_failure_count(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=2)
    queue.enqueue(_make_task("task-004"))
    claimed = queue.claim_next()
    assert claimed is not None

    target = queue.ack_failure(claimed)

    assert target == queue.pending_dir / "task-004.json"
    assert target.exists()
    assert not (queue.inflight_dir / "task-004.json").exists()
    assert _read_json(target)["governance"] == {
        "failure_count": 1,
        "orphan_count": 0,
    }


def test_ack_failure_transitions_to_poison_at_threshold(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=2)
    queue.enqueue(_make_task("task-004"))

    first_claim = queue.claim_next()
    assert first_claim is not None
    queue.ack_failure(first_claim)

    second_claim = queue.claim_next()
    assert second_claim is not None
    target = queue.ack_failure(second_claim)

    assert target == queue.poison_dir / "task-004.json"
    assert target.exists()
    assert _read_json(target)["governance"] == {
        "failure_count": 2,
        "orphan_count": 0,
    }
    assert not (queue.root / "_dlq").exists()


def test_recover_stale_inflight_moves_task_back_to_pending(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_orphan_requeues=2)
    queue.enqueue(_make_task("task-005"))
    claimed = queue.claim_next()
    assert claimed is not None

    os.utime(Path(claimed.claim_ref), (25, 25))

    recovered = queue.recover_stale_inflight(max_age_s=10, now_epoch_s=50)

    assert recovered == 1
    assert (queue.pending_dir / "task-005.json").exists()
    assert not (queue.inflight_dir / "task-005.json").exists()
    assert _read_json(queue.pending_dir / "task-005.json")["governance"] == {
        "failure_count": 0,
        "orphan_count": 1,
    }


def test_recover_stale_inflight_transitions_to_poison_at_orphan_threshold(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_orphan_requeues=1)
    queue.enqueue(_make_task("task-006"))
    claimed = queue.claim_next()
    assert claimed is not None

    os.utime(Path(claimed.claim_ref), (25, 25))

    recovered = queue.recover_stale_inflight(max_age_s=10, now_epoch_s=50)

    assert recovered == 1
    assert (queue.poison_dir / "task-006.json").exists()
    assert not (queue.pending_dir / "task-006.json").exists()
    assert _read_json(queue.poison_dir / "task-006.json")["governance"] == {
        "failure_count": 0,
        "orphan_count": 1,
    }


def test_recover_stale_inflight_requires_positive_ttl(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")

    with pytest.raises(ValueError, match="max_age_s must be > 0"):
        queue.recover_stale_inflight(max_age_s=0)


def test_inspect_queue_summary_reports_expected_buckets(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    queue.enqueue(_make_task("task-pending"))
    queue.enqueue(_make_task("task-inflight"))
    queue.enqueue(_make_task("task-done"))
    queue.enqueue(_make_task("task-poison"))

    inflight = queue.claim_next()
    assert inflight is not None

    done = queue.claim_next()
    assert done is not None
    queue.ack_success(done)

    poison = queue.claim_next()
    assert poison is not None
    queue.ack_failure(poison)

    result = inspect_local_spool_queue(root=queue.root)

    assert result.to_dict()["summary"] == {
        "pending": 1,
        "inflight": 1,
        "done": 1,
        "failed": 0,
        "poison": 1,
        "listed_tasks": 0,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "none",
    }
    assert result.bucket is None
    assert result.tasks == ()


def test_inspect_queue_bucket_list_surfaces_governance_and_identity_metadata(
    tmp_path: Path,
) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    queue.enqueue(_make_identified_task("task-identity"))
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    result = inspect_local_spool_queue(root=queue.root, bucket="poison")
    payload = result.to_dict()

    assert payload["bucket"] == "poison"
    assert len(payload["tasks"]) == 1
    assert payload["summary"]["poison"] == 1
    assert payload["summary"]["listed_tasks"] == 1
    assert payload["summary"]["governance_problem_tasks"] == 1
    assert payload["summary"]["visible_requeue_history_tasks"] == 0
    assert payload["summary"]["recovery_audit_support"] == "persisted_in_history"
    assert payload["tasks"][0] == {
        "bucket": "poison",
        "state": "poison",
        "governance_labels": [],
        "governance_problem": "poison_attempts_exhausted",
        "poison_kind": "attempts_exhausted",
        "handling_expectation": "requeue_supported",
        "recovery_audit_support": "persisted_in_history",
        "task_id": "task-identity",
        "kind": "it",
        "record_path": str((queue.poison_dir / "task-identity.json").resolve()),
        "updated_at_utc": payload["tasks"][0]["updated_at_utc"],
        "uri": "/tmp/task-identity.pdf",
        "source": "local_file",
        "filename": "task-identity.pdf",
        "discovered_at_utc": "2026-04-05T00:00:00Z",
        "identity": {
            "pipeline_version": "2026.04.06",
            "sha256": "sha256-task-identity",
        },
        "failure_count": 1,
        "orphan_count": 0,
        "latest_recovery": None,
    }


def test_inspect_queue_is_read_only(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    queue.enqueue(_make_identified_task("task-readonly"))
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    before = {
        str(path.relative_to(queue.root)): path.read_text(encoding="utf-8")
        for path in sorted(queue.root.rglob("*.json"))
    }

    result = inspect_local_spool_queue(root=queue.root, bucket="poison")

    after = {
        str(path.relative_to(queue.root)): path.read_text(encoding="utf-8")
        for path in sorted(queue.root.rglob("*.json"))
    }

    assert result.to_dict()["summary"]["poison"] == 1
    assert before == after


def test_requeue_poison_task_moves_to_pending_and_preserves_governance(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    queue.enqueue(_make_identified_task("task-requeue-poison"))
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    before = (queue.poison_dir / "task-requeue-poison.json").read_text(encoding="utf-8")

    result = requeue_local_spool_task(
        root=queue.root,
        source_bucket="poison",
        task_id="task-requeue-poison",
    )

    pending_path = queue.pending_dir / "task-requeue-poison.json"
    result_payload = result.to_dict()
    assert result_payload == {
        "action": "requeue",
        "support_status": "supported",
        "governance_result": "moved_to_pending",
        "redrive_support": "not_modeled",
        "audit_support": "persisted_in_history",
        "root": str(queue.root.resolve()),
        "task_id": "task-requeue-poison",
        "kind": "it",
        "source_bucket": "poison",
        "target_bucket": "pending",
        "source_path": str((queue.poison_dir / "task-requeue-poison.json").resolve()),
        "target_path": str(pending_path.resolve()),
        "failure_count": 1,
        "orphan_count": 0,
        "recorded_at_utc": result_payload["recorded_at_utc"],
    }
    assert not (queue.poison_dir / "task-requeue-poison.json").exists()
    assert pending_path.exists()
    before_payload = json.loads(before)
    pending_payload = json.loads(pending_path.read_text(encoding="utf-8"))
    assert pending_payload["task"] == before_payload["task"]
    assert pending_payload["governance"] == before_payload["governance"]
    assert pending_payload["provenance"] == [
        {
            "action": "requeue",
            "source_bucket": "poison",
            "target_bucket": "pending",
            "recorded_at_utc": result_payload["recorded_at_utc"],
        }
    ]
    expected_task_identity_key = (
        '{"kind":"it","pipeline_version":"2026.04.06","sha256":"sha256-task-requeue-poison"}'
    )
    assert result.to_run_provenance().to_dict() == {
        "run_origin": "requeue",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-requeue-poison",
        "task_identity_key": expected_task_identity_key,
    }

    inspected = inspect_local_spool_queue(root=queue.root, bucket="pending").to_dict()
    assert inspected["tasks"][0]["latest_recovery"] == pending_payload["provenance"][0]


def test_requeue_inflight_task_moves_to_pending_and_preserves_governance(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_orphan_requeues=2)
    queue.enqueue(_make_task("task-requeue-inflight"))
    claimed = queue.claim_next()
    assert claimed is not None
    inflight_path = queue.inflight_dir / "task-requeue-inflight.json"
    before = inflight_path.read_text(encoding="utf-8")

    result = requeue_local_spool_task(
        root=queue.root,
        source_bucket="inflight",
        task_id="task-requeue-inflight",
    )

    pending_path = queue.pending_dir / "task-requeue-inflight.json"
    result_payload = result.to_dict()
    assert result_payload == {
        "action": "requeue",
        "support_status": "supported",
        "governance_result": "moved_to_pending",
        "redrive_support": "not_modeled",
        "audit_support": "persisted_in_history",
        "root": str(queue.root.resolve()),
        "task_id": "task-requeue-inflight",
        "kind": "uns",
        "source_bucket": "inflight",
        "target_bucket": "pending",
        "source_path": str(inflight_path.resolve()),
        "target_path": str(pending_path.resolve()),
        "failure_count": 0,
        "orphan_count": 0,
        "recorded_at_utc": result_payload["recorded_at_utc"],
    }
    assert not inflight_path.exists()
    assert pending_path.exists()
    before_payload = json.loads(before)
    pending_payload = json.loads(pending_path.read_text(encoding="utf-8"))
    assert pending_payload["task"] == before_payload["task"]
    assert pending_payload["governance"] == before_payload["governance"]
    assert pending_payload["provenance"] == [
        {
            "action": "requeue",
            "source_bucket": "inflight",
            "target_bucket": "pending",
            "recorded_at_utc": result_payload["recorded_at_utc"],
        }
    ]


def test_cross_flow_queue_operator_surfaces_keep_shared_boundary_and_local_identity_explicit(
    tmp_path: Path,
) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1, max_orphan_requeues=2)

    queue.enqueue(_make_identified_task("task-it-operator"))
    claimed_it = queue.claim_next()
    assert claimed_it is not None
    queue.ack_failure(claimed_it)

    queue.enqueue(_make_task("task-uns-operator"))
    claimed_uns = queue.claim_next()
    assert claimed_uns is not None

    poison_view = inspect_local_spool_queue(root=queue.root, bucket="poison").to_dict()
    inflight_view = inspect_local_spool_queue(root=queue.root, bucket="inflight").to_dict()

    assert poison_view["summary"] == {
        "pending": 0,
        "inflight": 1,
        "done": 0,
        "failed": 0,
        "poison": 1,
        "listed_tasks": 1,
        "governance_problem_tasks": 1,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "persisted_in_history",
    }
    assert inflight_view["summary"] == {
        "pending": 0,
        "inflight": 1,
        "done": 0,
        "failed": 0,
        "poison": 1,
        "listed_tasks": 1,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "persisted_in_history",
    }

    poison_task = poison_view["tasks"][0]
    inflight_task = inflight_view["tasks"][0]

    assert poison_task["task_id"] == "task-it-operator"
    assert poison_task["kind"] == "it"
    assert poison_task["governance_problem"] == "poison_attempts_exhausted"
    assert poison_task["handling_expectation"] == "requeue_supported"
    assert poison_task["recovery_audit_support"] == "persisted_in_history"
    assert poison_task["identity"] == {
        "pipeline_version": "2026.04.06",
        "sha256": "sha256-task-it-operator",
    }
    assert poison_task["latest_recovery"] is None
    _assert_no_non_operator_surface_leakage(poison_task)

    assert inflight_task["task_id"] == "task-uns-operator"
    assert inflight_task["kind"] == "uns"
    assert inflight_task["governance_problem"] == "none"
    assert inflight_task["handling_expectation"] == "requeue_supported"
    assert inflight_task["recovery_audit_support"] == "persisted_in_history"
    assert inflight_task["identity"] is None
    assert inflight_task["latest_recovery"] is None
    _assert_no_non_operator_surface_leakage(inflight_task)

    it_result = requeue_local_spool_task(
        root=queue.root,
        source_bucket="poison",
        task_id="task-it-operator",
    )
    uns_result = requeue_local_spool_task(
        root=queue.root,
        source_bucket="inflight",
        task_id="task-uns-operator",
    )
    it_result_payload = it_result.to_dict()
    uns_result_payload = uns_result.to_dict()

    assert it_result_payload["action"] == "requeue"
    assert it_result_payload["support_status"] == "supported"
    assert it_result_payload["governance_result"] == "moved_to_pending"
    assert it_result_payload["redrive_support"] == "not_modeled"
    assert it_result_payload["audit_support"] == "persisted_in_history"
    assert it_result_payload["kind"] == "it"
    assert it_result_payload["source_bucket"] == "poison"
    assert it_result_payload["target_bucket"] == "pending"
    _assert_no_non_operator_surface_leakage(it_result_payload)

    assert uns_result_payload["action"] == "requeue"
    assert uns_result_payload["support_status"] == "supported"
    assert uns_result_payload["governance_result"] == "moved_to_pending"
    assert uns_result_payload["redrive_support"] == "not_modeled"
    assert uns_result_payload["audit_support"] == "persisted_in_history"
    assert uns_result_payload["kind"] == "uns"
    assert uns_result_payload["source_bucket"] == "inflight"
    assert uns_result_payload["target_bucket"] == "pending"
    _assert_no_non_operator_surface_leakage(uns_result_payload)

    assert it_result.to_run_provenance().to_dict() == {
        "run_origin": "requeue",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-it-operator",
        "task_identity_key": (
            '{"kind":"it","pipeline_version":"2026.04.06","sha256":"sha256-task-it-operator"}'
        ),
    }
    assert uns_result.to_run_provenance().to_dict() == {
        "run_origin": "requeue",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-uns-operator",
    }

    pending_view = inspect_local_spool_queue(root=queue.root, bucket="pending").to_dict()
    assert pending_view["summary"] == {
        "pending": 2,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 0,
        "listed_tasks": 2,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 2,
        "recovery_audit_support": "persisted_in_history",
    }

    pending_tasks = {task["task_id"]: task for task in pending_view["tasks"]}
    it_pending_task = pending_tasks["task-it-operator"]
    uns_pending_task = pending_tasks["task-uns-operator"]

    assert it_pending_task["governance_labels"] == ["requeued"]
    assert it_pending_task["governance_problem"] == "none"
    assert it_pending_task["handling_expectation"] == "none"
    assert it_pending_task["identity"] == {
        "pipeline_version": "2026.04.06",
        "sha256": "sha256-task-it-operator",
    }
    assert it_pending_task["latest_recovery"] is not None
    assert set(it_pending_task["latest_recovery"]) == {
        "action",
        "source_bucket",
        "target_bucket",
        "recorded_at_utc",
    }
    assert it_pending_task["latest_recovery"]["action"] == "requeue"
    assert it_pending_task["latest_recovery"]["source_bucket"] == "poison"
    assert it_pending_task["latest_recovery"]["target_bucket"] == "pending"
    _assert_no_non_operator_surface_leakage(it_pending_task)

    assert uns_pending_task["governance_labels"] == ["requeued"]
    assert uns_pending_task["governance_problem"] == "none"
    assert uns_pending_task["handling_expectation"] == "none"
    assert uns_pending_task["identity"] is None
    assert uns_pending_task["latest_recovery"] is not None
    assert set(uns_pending_task["latest_recovery"]) == {
        "action",
        "source_bucket",
        "target_bucket",
        "recorded_at_utc",
    }
    assert uns_pending_task["latest_recovery"]["action"] == "requeue"
    assert uns_pending_task["latest_recovery"]["source_bucket"] == "inflight"
    assert uns_pending_task["latest_recovery"]["target_bucket"] == "pending"
    _assert_no_non_operator_surface_leakage(uns_pending_task)


def test_requeue_task_rejects_invalid_source_state(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")
    queue.enqueue(_make_task("task-not-poison"))

    with pytest.raises(QueueRecoveryError, match="Task not found in poison: task-not-poison"):
        requeue_local_spool_task(
            root=queue.root,
            source_bucket="poison",
            task_id="task-not-poison",
        )
