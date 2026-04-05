from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from zephyr_core import DocumentRef, PartitionStrategy
from zephyr_ingest.spool_queue import LocalSpoolQueue
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskInputsV1,
    TaskV1,
)


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


def _read_json(path: Path) -> dict[str, object]:
    obj: dict[str, object] = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(obj, dict)
    return obj


def test_enqueue_writes_pending_task(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")
    task = _make_task("task-001")

    path = queue.enqueue(task)

    assert path == queue.pending_dir / "task-001.json"
    assert path.exists()
    assert _read_json(path) == task.to_dict()


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


def test_ack_failure_transitions_inflight_to_failed(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")
    queue.enqueue(_make_task("task-004"))
    claimed = queue.claim_next()
    assert claimed is not None

    target = queue.ack_failure(claimed)

    assert target == queue.failed_dir / "task-004.json"
    assert target.exists()
    assert not (queue.inflight_dir / "task-004.json").exists()


def test_recover_stale_inflight_moves_task_back_to_pending(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")
    queue.enqueue(_make_task("task-005"))
    claimed = queue.claim_next()
    assert claimed is not None

    os.utime(Path(claimed.claim_ref), (25, 25))

    recovered = queue.recover_stale_inflight(max_age_s=10, now_epoch_s=50)

    assert recovered == 1
    assert (queue.pending_dir / "task-005.json").exists()
    assert not (queue.inflight_dir / "task-005.json").exists()


def test_recover_stale_inflight_requires_positive_ttl(tmp_path: Path) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool")

    with pytest.raises(ValueError, match="max_age_s must be > 0"):
        queue.recover_stale_inflight(max_age_s=0)
