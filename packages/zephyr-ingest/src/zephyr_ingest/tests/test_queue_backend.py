from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Literal

import pytest

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_ingest.lock_provider import FileLockProvider, LockProvider
from zephyr_ingest.lock_provider_factory import build_local_lock_provider
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackend, QueueBackendWorkSource
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_inspect import inspect_local_queue, inspect_local_spool_queue
from zephyr_ingest.queue_recover import requeue_local_spool_task, requeue_local_task
from zephyr_ingest.spool_queue import (
    DEFAULT_MAX_ORPHAN_REQUEUES,
    DEFAULT_MAX_TASK_ATTEMPTS,
    LocalSpoolQueue,
)
from zephyr_ingest.sqlite_lock_provider import SqliteLockProvider
from zephyr_ingest.sqlite_queue import SqliteQueueBackend
from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.worker_runtime import WorkerRuntime


def _make_task(task_id: str, *, kind: Literal["uns", "it"] = "uns") -> TaskV1:
    source = "airbyte" if kind == "it" else "local_file"
    extension = ".json" if kind == "it" else ".pdf"
    filename = f"{task_id}{extension}"
    return TaskV1(
        task_id=task_id,
        kind=kind,
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1(
                uri=f"/tmp/{filename}",
                source=source,
                discovered_at_utc="2026-04-05T00:00:00Z",
                filename=filename,
                extension=extension,
                size_bytes=64 if kind == "it" else 128,
            )
        ),
        execution=TaskExecutionV1(),
        identity=TaskIdentityV1(
            pipeline_version="p-worker",
            sha256=f"sha-{task_id}",
        ),
    )


def make_task(task_id: str, *, kind: Literal["uns", "it"] = "uns") -> TaskV1:
    """
    公开的 Task 构造工具，主要用于测试。
    """
    return _make_task(task_id, kind=kind)


def _accept_backend(backend: QueueBackend) -> QueueBackend:
    return backend


def test_spool_queue_satisfies_queue_backend_protocol(tmp_path: Path) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool")

    accepted = _accept_backend(backend)

    assert accepted is backend


def test_sqlite_queue_satisfies_queue_backend_protocol_and_factory_selection(
    tmp_path: Path,
) -> None:
    backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue")

    accepted = _accept_backend(backend)
    selected = build_local_queue_backend(kind="sqlite", root=tmp_path / "selected-queue")

    assert accepted is backend
    assert isinstance(selected, SqliteQueueBackend)
    assert selected.db_path == (tmp_path / "selected-queue" / "queue.sqlite3")
    assert selected.db_path.exists()


def test_sqlite_queue_enqueue_claim_and_ack_lifecycle(tmp_path: Path) -> None:
    backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue", max_task_attempts=2)
    enqueue_ref = backend.enqueue(_make_task("task-sqlite"))

    claimed = backend.claim_next()

    assert enqueue_ref == "task-sqlite"
    assert claimed is not None
    assert claimed.task.task_id == "task-sqlite"
    assert claimed.claim_ref == "task-sqlite"
    assert backend.db_path.exists()
    assert not (tmp_path / "sqlite-queue" / "pending").exists()

    requeued_ref = backend.ack_failure(claimed)
    assert requeued_ref == "task-sqlite"

    second_claim = backend.claim_next()
    assert second_claim is not None
    done_ref = backend.ack_success(second_claim)
    assert done_ref == "task-sqlite"
    assert backend.claim_next() is None


def test_sqlite_queue_defaults_are_bounded_product_safe_not_first_failure_poison(
    tmp_path: Path,
) -> None:
    backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue")
    assert backend.max_task_attempts == DEFAULT_MAX_TASK_ATTEMPTS
    assert backend.max_orphan_requeues == DEFAULT_MAX_ORPHAN_REQUEUES

    backend.enqueue(_make_task("task-default-failure"))
    first_claim = backend.claim_next()
    assert first_claim is not None
    backend.ack_failure(first_claim)
    assert backend.queue_metrics_snapshot().pending == 1
    assert backend.queue_metrics_snapshot().poison == 0

    second_claim = backend.claim_next()
    assert second_claim is not None
    backend.ack_failure(second_claim)
    assert backend.queue_metrics_snapshot().pending == 1
    assert backend.queue_metrics_snapshot().poison == 0

    third_claim = backend.claim_next()
    assert third_claim is not None
    backend.ack_failure(third_claim)
    assert backend.queue_metrics_snapshot().pending == 0
    assert backend.queue_metrics_snapshot().poison == 1

    backend.enqueue(_make_task("task-default-orphan"))
    orphan_claim = backend.claim_next()
    assert orphan_claim is not None
    backend.requeue_orphaned(orphan_claim)
    assert backend.queue_metrics_snapshot().pending == 1
    assert backend.queue_metrics_snapshot().poison == 1


def test_sqlite_queue_backend_work_source_handles_lock_contention_and_stale_inflight(
    tmp_path: Path,
) -> None:
    backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue", max_orphan_requeues=2)
    backend.enqueue(_make_task("task-sqlite-stale"))

    class BusyLockProvider:
        def acquire(self, *, key: str, owner: str) -> None:
            return None

        def release(self, lock: object) -> None:
            raise AssertionError("release should not be called")

    source = QueueBackendWorkSource(
        backend=backend,
        handler=lambda task: None,
        lock_provider=BusyLockProvider(),
        lock_owner="worker-sqlite",
    )

    work = source.poll()

    assert work is None
    reclaimed = backend.claim_next()
    assert reclaimed is not None
    assert reclaimed.task.task_id == "task-sqlite-stale"

    stale_backend = SqliteQueueBackend(root=tmp_path / "sqlite-stale", max_orphan_requeues=2)
    stale_backend.enqueue(_make_task("task-sqlite-recover"))
    claimed_stale = stale_backend.claim_next()
    assert claimed_stale is not None

    with sqlite3.connect(stale_backend.db_path) as conn:
        conn.execute(
            "UPDATE queue_tasks SET claimed_at = ?, updated_at = ? WHERE task_id = ?",
            (25.0, 25.0, "task-sqlite-recover"),
        )

    recovered = stale_backend.recover_stale_inflight(max_age_s=10, now_epoch_s=50)
    recovered_claim = stale_backend.claim_next()

    assert recovered == 1
    assert recovered_claim is not None
    assert recovered_claim.task.task_id == "task-sqlite-recover"


def test_sqlite_queue_metrics_render_shared_queue_governance_subset(tmp_path: Path) -> None:
    backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue", max_orphan_requeues=3)
    backend.enqueue(_make_task("task-sqlite-stale-metrics"))
    claimed = backend.claim_next()
    assert claimed is not None

    with sqlite3.connect(backend.db_path) as conn:
        conn.execute(
            "UPDATE queue_tasks SET claimed_at = ?, updated_at = ? WHERE task_id = ?",
            (25.0, 25.0, "task-sqlite-stale-metrics"),
        )

    class BusyLockProvider:
        def acquire(self, *, key: str, owner: str) -> None:
            return None

        def release(self, lock: object) -> None:
            raise AssertionError("release should not be called")

    source = QueueBackendWorkSource(
        backend=backend,
        handler=lambda task: None,
        recover_inflight_after_s=10,
        lock_provider=BusyLockProvider(),
        lock_owner="worker-sqlite",
    )
    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    work = source.poll()

    assert work is None
    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="pending"} 1.0' in text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="inflight"} 0.0' in text
    assert 'zephyr_ingest_queue_policy_max_task_attempts{pipeline_version="p-worker"} 3.0' in text
    assert 'zephyr_ingest_queue_policy_max_orphan_requeues{pipeline_version="p-worker"} 3.0' in text
    assert 'zephyr_ingest_queue_orphan_requeues_total{pipeline_version="p-worker"} 2.0' in text
    assert (
        'zephyr_ingest_queue_stale_inflight_recoveries_total{pipeline_version="p-worker"} 1.0'
        in text
    )
    assert 'zephyr_ingest_queue_poison_transitions_total{pipeline_version="p-worker"} 0.0' in text
    assert 'zephyr_ingest_queue_lock_contention_total{pipeline_version="p-worker"} 1.0' in text


def test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset(
    tmp_path: Path,
) -> None:
    backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue", max_task_attempts=1)
    task = _make_task("task-sqlite-governed", kind="it")
    backend.enqueue(task)
    claimed = backend.claim_next()
    assert claimed is not None
    backend.ack_failure(claimed)

    poison_view = inspect_local_queue(
        root=backend.root,
        backend_kind="sqlite",
        bucket="poison",
    ).to_dict()
    assert poison_view["summary"] == {
        "pending": 0,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 1,
        "listed_tasks": 1,
        "governance_problem_tasks": 1,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "result_only",
    }
    assert poison_view["tasks"] == [
        {
            "bucket": "poison",
            "state": "poison",
            "governance_labels": [],
            "governance_problem": "poison_attempts_exhausted",
            "state_explanation": "operator_requeue_supported_after_attempt_or_orphan_threshold",
            "poison_kind": "attempts_exhausted",
            "handling_expectation": "requeue_supported",
            "recovery_audit_support": "result_only",
            "task_id": "task-sqlite-governed",
            "kind": "it",
            "record_path": (
                f"{backend.db_path.resolve()}#bucket=poison,task_id=task-sqlite-governed"
            ),
            "updated_at_utc": poison_view["tasks"][0]["updated_at_utc"],
            "uri": "/tmp/task-sqlite-governed.json",
            "source": "airbyte",
            "filename": "task-sqlite-governed.json",
            "discovered_at_utc": "2026-04-05T00:00:00Z",
            "identity": {
                "pipeline_version": "p-worker",
                "sha256": "sha-task-sqlite-governed",
            },
            "failure_count": 1,
            "orphan_count": 0,
            "latest_recovery": None,
        }
    ]

    recovery = requeue_local_task(
        root=backend.root,
        backend_kind="sqlite",
        source_bucket="poison",
        task_id="task-sqlite-governed",
    )
    assert recovery.governance_action.action == "requeue"
    assert recovery.governance_action.source_state == "poison"
    assert recovery.governance_action.target_state == "pending"
    assert recovery.governance_action.audit_support == "result_only"
    assert recovery.governance_action.redrive_semantics == "not_modeled"
    assert recovery.support_status == "supported"
    assert recovery.governance_result == "moved_to_pending"
    assert recovery.to_dict() == {
        "action": "requeue",
        "support_status": "supported",
        "governance_result": "moved_to_pending",
        "redrive_support": "not_modeled",
        "audit_support": "result_only",
        "root": str(backend.root.resolve()),
        "task_id": "task-sqlite-governed",
        "kind": "it",
        "source_bucket": "poison",
        "target_bucket": "pending",
        "source_path": f"{backend.db_path.resolve()}#bucket=poison,task_id=task-sqlite-governed",
        "target_path": f"{backend.db_path.resolve()}#bucket=pending,task_id=task-sqlite-governed",
        "failure_count": 1,
        "orphan_count": 0,
        "recorded_at_utc": recovery.recorded_at_utc,
        "source_contract_id": None,
    }
    assert recovery.to_run_provenance().to_dict() == {
        "run_origin": "requeue",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-sqlite-governed",
        "task_identity_key": (
            '{"kind":"it","pipeline_version":"p-worker","sha256":"sha-task-sqlite-governed"}'
        ),
    }

    pending_result = inspect_local_queue(
        root=backend.root,
        backend_kind="sqlite",
        bucket="pending",
    )
    assert pending_result.tasks[0].governance_action_audit_support == "result_only"
    assert pending_result.tasks[0].latest_governance_action is None
    pending_view = pending_result.to_dict()
    assert pending_view["summary"] == {
        "pending": 1,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 0,
        "listed_tasks": 1,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "result_only",
    }
    assert pending_view["tasks"][0]["state"] == "pending"
    assert pending_view["tasks"][0]["state_explanation"] == "ready_for_local_worker_claim"
    assert pending_view["tasks"][0]["governance_labels"] == []
    assert pending_view["tasks"][0]["governance_problem"] == "none"
    assert pending_view["tasks"][0]["poison_kind"] == "not_poison"
    assert pending_view["tasks"][0]["handling_expectation"] == "none"
    assert pending_view["tasks"][0]["recovery_audit_support"] == "result_only"
    assert pending_view["tasks"][0]["latest_recovery"] is None


def test_worker_runtime_consumes_sqlite_queue_backend_work_source(tmp_path: Path) -> None:
    backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue")
    backend.enqueue(_make_task("task-sqlite-runtime", kind="it"))
    handled: list[str] = []

    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    def handle_task(task: TaskV1) -> None:
        handled.append(task.task_id)
        runtime.request_draining()

    source = QueueBackendWorkSource(backend=backend, handler=handle_task)

    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert handled == ["task-sqlite-runtime"]
    assert backend.claim_next() is None


def test_worker_runtime_consumes_queue_backend_work_source_without_behavior_drift(
    tmp_path: Path,
) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool")
    task = _make_task("task-001")
    backend.enqueue(task)
    handled: list[str] = []

    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    def handle_task(task: TaskV1) -> None:
        handled.append(task.task_id)
        runtime.request_draining()

    source = QueueBackendWorkSource(backend=backend, handler=handle_task)

    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert handled == ["task-001"]
    assert not (backend.pending_dir / "task-001.json").exists()
    assert (backend.done_dir / "task-001.json").exists()


def test_worker_runtime_consumes_queue_backend_work_source_with_lock_provider(
    tmp_path: Path,
) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool")
    lock_provider = FileLockProvider(root=tmp_path / "locks")
    task = _make_task("task-locked")
    backend.enqueue(task)
    handled: list[str] = []

    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    def handle_task(task: TaskV1) -> None:
        handled.append(task.task_id)
        runtime.request_draining()

    source = QueueBackendWorkSource(
        backend=backend,
        handler=handle_task,
        lock_provider=lock_provider,
        lock_owner="worker-a",
    )

    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert handled == ["task-locked"]
    assert (backend.done_dir / "task-locked.json").exists()
    assert list((tmp_path / "locks").glob("*.lock")) == []


def test_queue_backend_work_source_requeues_when_lock_is_unavailable(tmp_path: Path) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool", max_orphan_requeues=2)
    task = _make_task("task-locked")
    backend.enqueue(task)

    class BusyLockProvider:
        def acquire(self, *, key: str, owner: str) -> None:
            return None

        def release(self, lock: object) -> None:
            raise AssertionError("release should not be called")

    busy_lock_provider = BusyLockProvider()
    lock_provider: LockProvider = busy_lock_provider
    source = QueueBackendWorkSource(
        backend=backend,
        handler=lambda task: None,
        lock_provider=lock_provider,
        lock_owner="worker-a",
    )

    work = source.poll()

    assert work is None
    assert (backend.pending_dir / "task-locked.json").exists()
    assert not (backend.inflight_dir / "task-locked.json").exists()
    assert source.work_source_metrics_snapshot().lock_contention_total == 1


def test_queue_backend_work_source_accepts_it_task_with_lock_provider(tmp_path: Path) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool")
    lock_provider = FileLockProvider(root=tmp_path / "locks")
    task = _make_task("task-it-001", kind="it")
    backend.enqueue(task)
    handled: list[str] = []

    source = QueueBackendWorkSource(
        backend=backend,
        handler=lambda task: handled.append(task.task_id),
        lock_provider=lock_provider,
        lock_owner="worker-it",
    )

    work = source.poll()

    assert work is not None

    work()

    assert handled == ["task-it-001"]
    assert (backend.done_dir / "task-it-001.json").exists()
    assert list((tmp_path / "locks").glob("*.lock")) == []


def test_queue_backend_work_source_accepts_sqlite_lock_provider(tmp_path: Path) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool")
    lock_provider = build_local_lock_provider(kind="sqlite", root=tmp_path / "sqlite-locks")
    task = _make_task("task-it-sqlite-lock", kind="it")
    backend.enqueue(task)
    handled: list[str] = []

    source = QueueBackendWorkSource(
        backend=backend,
        handler=lambda task: handled.append(task.task_id),
        lock_provider=lock_provider,
        lock_owner="worker-it",
    )

    work = source.poll()

    assert work is not None

    work()

    assert handled == ["task-it-sqlite-lock"]
    assert (backend.done_dir / "task-it-sqlite-lock.json").exists()


def test_queue_and_lock_backend_reference_shapes_remain_backend_specific(tmp_path: Path) -> None:
    spool_backend = LocalSpoolQueue(root=tmp_path / "spool")
    sqlite_backend = SqliteQueueBackend(root=tmp_path / "sqlite-queue")
    spool_lock = FileLockProvider(root=tmp_path / "file-locks")
    sqlite_lock = build_local_lock_provider(kind="sqlite", root=tmp_path / "sqlite-locks")

    spool_backend.enqueue(_make_task("task-spool-ref"))
    sqlite_backend.enqueue(_make_task("task-sqlite-ref"))

    spool_claimed = spool_backend.claim_next()
    sqlite_claimed = sqlite_backend.claim_next()
    assert spool_claimed is not None
    assert sqlite_claimed is not None

    spool_acquired = spool_lock.acquire(key="task-spool-ref", owner="worker-a")
    sqlite_acquired = sqlite_lock.acquire(key="task-sqlite-ref", owner="worker-b")
    assert spool_acquired is not None
    assert sqlite_acquired is not None

    assert isinstance(spool_claimed.claim_ref, Path)
    assert isinstance(sqlite_claimed.claim_ref, str)
    assert spool_acquired.lock_ref.suffix == ".lock"
    assert sqlite_acquired.lock_ref.name == "locks.sqlite3"


def test_worker_metrics_render_queue_state_orphan_and_lock_contention(tmp_path: Path) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool", max_orphan_requeues=3)
    backend.enqueue(_make_task("task-stale"))
    claimed = backend.claim_next()
    assert claimed is not None
    os.utime(Path(claimed.claim_ref), (25, 25))

    class BusyLockProvider:
        def acquire(self, *, key: str, owner: str) -> None:
            return None

        def release(self, lock: object) -> None:
            raise AssertionError("release should not be called")

    source = QueueBackendWorkSource(
        backend=backend,
        handler=lambda task: None,
        recover_inflight_after_s=10,
        lock_provider=BusyLockProvider(),
        lock_owner="worker-a",
    )
    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    work = source.poll()

    assert work is None
    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="pending"} 1.0' in text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="inflight"} 0.0' in text
    assert 'zephyr_ingest_queue_orphan_requeues_total{pipeline_version="p-worker"} 2.0' in text
    assert (
        'zephyr_ingest_queue_stale_inflight_recoveries_total{pipeline_version="p-worker"} 1.0'
        in text
    )
    assert 'zephyr_ingest_queue_lock_contention_total{pipeline_version="p-worker"} 1.0' in text


def test_worker_metrics_render_poison_and_stale_lock_recovery(tmp_path: Path) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    backend.enqueue(_make_task("task-poison"))
    claimed = backend.claim_next()
    assert claimed is not None
    backend.ack_failure(claimed)

    lock_provider = FileLockProvider(root=tmp_path / "locks", stale_after_s=10)
    first = lock_provider.acquire(key="task-key", owner="worker-a")
    assert first is not None
    os.utime(first.lock_ref, (25, 25))
    second = lock_provider.acquire(key="task-key", owner="worker-b")
    assert second is not None

    source = QueueBackendWorkSource(
        backend=backend,
        handler=lambda task: None,
        lock_provider=lock_provider,
        lock_owner="worker-a",
    )
    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="poison"} 1.0' in text
    assert 'zephyr_ingest_queue_poison_transitions_total{pipeline_version="p-worker"} 1.0' in text
    assert 'zephyr_ingest_lock_stale_recoveries_total{pipeline_version="p-worker"} 1.0' in text


def test_governance_recovery_runtime_metrics_and_inspection_stay_coherent(
    tmp_path: Path,
) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    task = _make_task("task-governed", kind="it")
    backend.enqueue(task)
    claimed = backend.claim_next()
    assert claimed is not None
    backend.ack_failure(claimed)

    poison_result = inspect_local_spool_queue(root=backend.root, bucket="poison")
    poison_view = poison_result.to_dict()
    assert poison_view["summary"]["poison"] == 1
    assert poison_view["tasks"][0]["state"] == "poison"
    assert poison_view["tasks"][0]["governance_labels"] == []
    assert poison_view["tasks"][0]["governance_problem"] == "poison_attempts_exhausted"
    assert poison_view["tasks"][0]["poison_kind"] == "attempts_exhausted"
    assert poison_view["tasks"][0]["handling_expectation"] == "requeue_supported"
    assert poison_view["tasks"][0]["task_id"] == "task-governed"
    assert poison_view["tasks"][0]["identity"] == {
        "pipeline_version": "p-worker",
        "sha256": "sha-task-governed",
    }
    assert poison_view["tasks"][0]["latest_recovery"] is None

    recovery = requeue_local_spool_task(
        root=backend.root,
        source_bucket="poison",
        task_id="task-governed",
    )
    assert recovery.governance_action.action == "requeue"
    assert recovery.governance_action.source_state == "poison"
    assert recovery.governance_action.target_state == "pending"
    assert recovery.governance_action.audit_support == "persisted_in_history"
    assert recovery.governance_action.redrive_semantics == "not_modeled"
    pending_result = inspect_local_spool_queue(root=backend.root, bucket="pending")
    assert pending_result.tasks[0].governance_action_audit_support == "persisted_in_history"
    latest_governance_action = pending_result.tasks[0].latest_governance_action
    assert latest_governance_action is not None
    assert latest_governance_action.action == "requeue"
    assert latest_governance_action.source_state == "poison"
    assert latest_governance_action.target_state == "pending"
    assert latest_governance_action.audit_support == "persisted_in_history"
    assert latest_governance_action.redrive_semantics == "not_modeled"
    pending_view = pending_result.to_dict()
    assert pending_view["summary"] == {
        "pending": 1,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 0,
        "listed_tasks": 1,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 1,
        "recovery_audit_support": "persisted_in_history",
    }
    assert pending_view["tasks"][0]["state"] == "pending"
    assert pending_view["tasks"][0]["governance_labels"] == ["requeued"]
    assert pending_view["tasks"][0]["governance_problem"] == "none"
    assert pending_view["tasks"][0]["poison_kind"] == "not_poison"
    assert pending_view["tasks"][0]["handling_expectation"] == "none"
    assert pending_view["tasks"][0]["latest_recovery"] == {
        "action": "requeue",
        "source_bucket": "poison",
        "target_bucket": "pending",
        "recorded_at_utc": recovery.recorded_at_utc,
    }

    handled: list[str] = []
    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    def handle_task(task: TaskV1) -> None:
        handled.append(task.task_id)
        runtime.request_draining()

    source = QueueBackendWorkSource(backend=backend, handler=handle_task)

    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert handled == ["task-governed"]
    assert not (backend.root / "_dlq" / "delivery").exists()

    done_view = inspect_local_spool_queue(root=backend.root, bucket="done").to_dict()
    assert done_view["summary"] == {
        "pending": 0,
        "inflight": 0,
        "done": 1,
        "failed": 0,
        "poison": 0,
        "listed_tasks": 1,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 1,
        "recovery_audit_support": "persisted_in_history",
    }
    assert done_view["tasks"][0]["state"] == "done"
    assert done_view["tasks"][0]["governance_labels"] == ["requeued"]
    assert done_view["tasks"][0]["governance_problem"] == "none"
    assert done_view["tasks"][0]["poison_kind"] == "not_poison"
    assert done_view["tasks"][0]["handling_expectation"] == "none"
    assert done_view["tasks"][0]["latest_recovery"] == pending_view["tasks"][0]["latest_recovery"]

    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="done"} 1.0' in text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="poison"} 0.0' in text
    assert 'zephyr_ingest_queue_policy_max_task_attempts{pipeline_version="p-worker"} 1.0' in text
    assert 'zephyr_ingest_queue_poison_transitions_total{pipeline_version="p-worker"} 1.0' in text


@pytest.mark.parametrize(
    ("queue_kind", "lock_kind", "expects_recovery_history"),
    [
        ("spool", "file", True),
        ("sqlite", "sqlite", False),
    ],
)
def test_supported_backend_pairs_keep_governance_surfaces_coherent(
    tmp_path: Path,
    queue_kind: Literal["spool", "sqlite"],
    lock_kind: Literal["file", "sqlite"],
    expects_recovery_history: bool,
) -> None:
    backend = build_local_queue_backend(
        kind=queue_kind,
        root=tmp_path / f"{queue_kind}-queue",
        max_task_attempts=1,
    )
    lock_provider = build_local_lock_provider(
        kind=lock_kind,
        root=tmp_path / f"{lock_kind}-locks",
        stale_after_s=10,
    )
    task = _make_task(f"task-governed-{queue_kind}", kind="it")
    backend.enqueue(task)
    claimed = backend.claim_next()
    assert claimed is not None
    backend.ack_failure(claimed)

    poison_result = inspect_local_queue(
        root=tmp_path / f"{queue_kind}-queue",
        backend_kind=queue_kind,
        bucket="poison",
    )
    poison_view = poison_result.to_dict()
    assert poison_view["summary"]["poison"] == 1
    assert poison_view["summary"]["listed_tasks"] == 1
    assert poison_view["summary"]["governance_problem_tasks"] == 1
    assert poison_view["summary"]["visible_requeue_history_tasks"] == 0
    if expects_recovery_history:
        assert poison_view["summary"]["recovery_audit_support"] == "persisted_in_history"
    else:
        assert poison_view["summary"]["recovery_audit_support"] == "result_only"
    assert poison_view["tasks"][0]["state"] == "poison"
    assert poison_view["tasks"][0]["governance_labels"] == []
    assert poison_view["tasks"][0]["governance_problem"] == "poison_attempts_exhausted"
    assert poison_view["tasks"][0]["poison_kind"] == "attempts_exhausted"
    assert poison_view["tasks"][0]["handling_expectation"] == "requeue_supported"
    assert poison_view["tasks"][0]["task_id"] == task.task_id
    assert poison_view["tasks"][0]["identity"] == {
        "pipeline_version": "p-worker",
        "sha256": f"sha-{task.task_id}",
    }
    assert poison_view["tasks"][0]["latest_recovery"] is None
    if expects_recovery_history:
        assert poison_view["tasks"][0]["recovery_audit_support"] == "persisted_in_history"
    else:
        assert poison_view["tasks"][0]["recovery_audit_support"] == "result_only"

    recovery = requeue_local_task(
        root=tmp_path / f"{queue_kind}-queue",
        backend_kind=queue_kind,
        source_bucket="poison",
        task_id=task.task_id,
    )
    assert recovery.governance_action.action == "requeue"
    assert recovery.governance_action.source_state == "poison"
    assert recovery.governance_action.target_state == "pending"
    assert recovery.support_status == "supported"
    assert recovery.governance_result == "moved_to_pending"
    if expects_recovery_history:
        assert recovery.governance_action.audit_support == "persisted_in_history"
        assert recovery.to_dict()["audit_support"] == "persisted_in_history"
    else:
        assert recovery.governance_action.audit_support == "result_only"
        assert recovery.to_dict()["audit_support"] == "result_only"
    assert recovery.governance_action.redrive_semantics == "not_modeled"
    assert recovery.to_dict()["support_status"] == "supported"
    assert recovery.to_dict()["governance_result"] == "moved_to_pending"
    assert recovery.to_dict()["redrive_support"] == "not_modeled"
    assert recovery.to_run_provenance().to_dict() == {
        "run_origin": "requeue",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": task.task_id,
        "task_identity_key": (
            '{"kind":"it","pipeline_version":"p-worker","sha256":"' + f"sha-{task.task_id}" + '"}'
        ),
    }

    pending_result = inspect_local_queue(
        root=tmp_path / f"{queue_kind}-queue",
        backend_kind=queue_kind,
        bucket="pending",
    )
    pending_view = pending_result.to_dict()
    assert pending_view["summary"] == {
        "pending": 1,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 0,
        "listed_tasks": 1,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 1 if expects_recovery_history else 0,
        "recovery_audit_support": (
            "persisted_in_history" if expects_recovery_history else "result_only"
        ),
    }
    if expects_recovery_history:
        assert pending_result.tasks[0].governance_action_audit_support == "persisted_in_history"
        latest_governance_action = pending_result.tasks[0].latest_governance_action
        assert latest_governance_action is not None
        assert latest_governance_action.action == "requeue"
        assert latest_governance_action.source_state == "poison"
        assert latest_governance_action.target_state == "pending"
        assert latest_governance_action.audit_support == "persisted_in_history"
        assert pending_view["tasks"][0]["state"] == "pending"
        assert pending_view["tasks"][0]["governance_labels"] == ["requeued"]
        assert pending_view["tasks"][0]["governance_problem"] == "none"
        assert pending_view["tasks"][0]["poison_kind"] == "not_poison"
        assert pending_view["tasks"][0]["handling_expectation"] == "none"
        assert pending_view["tasks"][0]["recovery_audit_support"] == "persisted_in_history"
        assert pending_view["tasks"][0]["latest_recovery"] == {
            "action": "requeue",
            "source_bucket": "poison",
            "target_bucket": "pending",
            "recorded_at_utc": recovery.recorded_at_utc,
        }
    else:
        assert pending_result.tasks[0].governance_action_audit_support == "result_only"
        assert pending_result.tasks[0].latest_governance_action is None
        assert pending_view["tasks"][0]["state"] == "pending"
        assert pending_view["tasks"][0]["governance_labels"] == []
        assert pending_view["tasks"][0]["governance_problem"] == "none"
        assert pending_view["tasks"][0]["poison_kind"] == "not_poison"
        assert pending_view["tasks"][0]["handling_expectation"] == "none"
        assert pending_view["tasks"][0]["recovery_audit_support"] == "result_only"
        assert pending_view["tasks"][0]["latest_recovery"] is None

    lock_key = normalize_task_idempotency_key(task)
    stale_lock = lock_provider.acquire(key=lock_key, owner="worker-stale")
    assert stale_lock is not None
    assert stale_lock.owner == "worker-stale"
    if lock_kind == "file":
        os.utime(stale_lock.lock_ref, (25, 25))
    else:
        assert isinstance(lock_provider, SqliteLockProvider)
        with sqlite3.connect(lock_provider.db_path) as conn:
            conn.execute(
                "UPDATE task_locks SET acquired_at_epoch_s = ? WHERE key = ?",
                (25, lock_key),
            )

    handled: list[str] = []
    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id=f"r-governed-{queue_kind}-{lock_kind}",
            timestamp_utc="2026-04-05T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    def handle_task(task: TaskV1) -> None:
        handled.append(task.task_id)
        runtime.request_draining()

    source = QueueBackendWorkSource(
        backend=backend,
        handler=handle_task,
        lock_provider=lock_provider,
        lock_owner="worker-runtime",
    )

    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert handled == [task.task_id]

    done_view = inspect_local_queue(
        root=tmp_path / f"{queue_kind}-queue",
        backend_kind=queue_kind,
        bucket="done",
    ).to_dict()
    assert done_view["summary"] == {
        "pending": 0,
        "inflight": 0,
        "done": 1,
        "failed": 0,
        "poison": 0,
        "listed_tasks": 1,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 1 if expects_recovery_history else 0,
        "recovery_audit_support": (
            "persisted_in_history" if expects_recovery_history else "result_only"
        ),
    }
    if expects_recovery_history:
        assert done_view["tasks"][0]["state"] == "done"
        assert done_view["tasks"][0]["governance_labels"] == ["requeued"]
        assert done_view["tasks"][0]["governance_problem"] == "none"
        assert done_view["tasks"][0]["poison_kind"] == "not_poison"
        assert done_view["tasks"][0]["handling_expectation"] == "none"
        assert done_view["tasks"][0]["recovery_audit_support"] == "persisted_in_history"
        assert (
            done_view["tasks"][0]["latest_recovery"] == pending_view["tasks"][0]["latest_recovery"]
        )
    else:
        assert done_view["tasks"][0]["state"] == "done"
        assert done_view["tasks"][0]["governance_labels"] == []
        assert done_view["tasks"][0]["governance_problem"] == "none"
        assert done_view["tasks"][0]["poison_kind"] == "not_poison"
        assert done_view["tasks"][0]["handling_expectation"] == "none"
        assert done_view["tasks"][0]["recovery_audit_support"] == "result_only"
        assert done_view["tasks"][0]["latest_recovery"] is None

    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="done"} 1.0' in text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="poison"} 0.0' in text
    assert 'zephyr_ingest_queue_poison_transitions_total{pipeline_version="p-worker"} 1.0' in text
    assert 'zephyr_ingest_lock_stale_recoveries_total{pipeline_version="p-worker"} 1.0' in text


@pytest.mark.parametrize("backend_kind", ["spool", "sqlite"])
def test_queue_inspection_exposes_orphan_as_governance_label_not_core_state(
    tmp_path: Path,
    backend_kind: Literal["spool", "sqlite"],
) -> None:
    backend: QueueBackend
    backend_root = tmp_path / f"{backend_kind}-queue"
    if backend_kind == "spool":
        backend = LocalSpoolQueue(root=backend_root, max_orphan_requeues=2)
    else:
        backend = SqliteQueueBackend(root=backend_root, max_orphan_requeues=2)
    task = _make_task(f"task-orphan-{backend_kind}", kind="it")
    backend.enqueue(task)
    claimed = backend.claim_next()
    assert claimed is not None
    backend.requeue_orphaned(claimed)

    pending_view = inspect_local_queue(
        root=backend_root,
        backend_kind=backend_kind,
        bucket="pending",
    ).to_dict()

    assert pending_view["summary"]["pending"] == 1
    assert pending_view["tasks"][0]["task_id"] == task.task_id
    assert pending_view["tasks"][0]["state"] == "pending"
    assert pending_view["tasks"][0]["governance_labels"] == ["orphaned"]
    assert pending_view["tasks"][0]["governance_problem"] == "orphaned"
    assert pending_view["tasks"][0]["poison_kind"] == "not_poison"
    assert pending_view["tasks"][0]["handling_expectation"] == "none"
    assert pending_view["tasks"][0]["orphan_count"] == 1


@pytest.mark.parametrize("backend_kind", ["spool", "sqlite"])
def test_queue_inspection_distinguishes_poison_from_attempts_and_orphan_thresholds(
    tmp_path: Path,
    backend_kind: Literal["spool", "sqlite"],
) -> None:
    backend_root = tmp_path / f"{backend_kind}-queue"
    if backend_kind == "spool":
        backend: QueueBackend = LocalSpoolQueue(
            root=backend_root,
            max_task_attempts=1,
            max_orphan_requeues=1,
        )
    else:
        backend = SqliteQueueBackend(
            root=backend_root,
            max_task_attempts=1,
            max_orphan_requeues=1,
        )

    failure_task = _make_task(f"task-poison-failure-{backend_kind}", kind="it")
    backend.enqueue(failure_task)
    failure_claimed = backend.claim_next()
    assert failure_claimed is not None
    backend.ack_failure(failure_claimed)

    orphan_task = _make_task(f"task-poison-orphan-{backend_kind}", kind="it")
    backend.enqueue(orphan_task)
    orphan_claimed = backend.claim_next()
    assert orphan_claimed is not None
    backend.requeue_orphaned(orphan_claimed)

    poison_view = inspect_local_queue(
        root=backend_root,
        backend_kind=backend_kind,
        bucket="poison",
    ).to_dict()

    assert poison_view["summary"]["poison"] == 2
    assert poison_view["summary"]["listed_tasks"] == 2
    assert poison_view["summary"]["governance_problem_tasks"] == 2
    assert poison_view["summary"]["visible_requeue_history_tasks"] == 0
    if backend_kind == "spool":
        assert poison_view["summary"]["recovery_audit_support"] == "persisted_in_history"
    else:
        assert poison_view["summary"]["recovery_audit_support"] == "result_only"
    by_task_id = {task["task_id"]: task for task in poison_view["tasks"]}

    assert by_task_id[failure_task.task_id]["state"] == "poison"
    assert by_task_id[failure_task.task_id]["governance_labels"] == []
    assert by_task_id[failure_task.task_id]["governance_problem"] == "poison_attempts_exhausted"
    assert by_task_id[failure_task.task_id]["poison_kind"] == "attempts_exhausted"
    assert by_task_id[failure_task.task_id]["handling_expectation"] == "requeue_supported"
    assert by_task_id[failure_task.task_id]["failure_count"] == 1
    assert by_task_id[failure_task.task_id]["orphan_count"] == 0

    assert by_task_id[orphan_task.task_id]["state"] == "poison"
    assert by_task_id[orphan_task.task_id]["governance_labels"] == ["orphaned"]
    assert by_task_id[orphan_task.task_id]["governance_problem"] == "poison_orphaned"
    assert by_task_id[orphan_task.task_id]["poison_kind"] == "orphaned"
    assert by_task_id[orphan_task.task_id]["handling_expectation"] == "requeue_supported"
    assert by_task_id[orphan_task.task_id]["failure_count"] == 0
    assert by_task_id[orphan_task.task_id]["orphan_count"] == 1


@pytest.mark.parametrize(
    ("backend_kind", "expects_recovery_history"),
    [
        ("spool", True),
        ("sqlite", False),
    ],
)
def test_task_governance_boundary_keeps_state_poison_and_recovery_action_distinct(
    tmp_path: Path,
    backend_kind: Literal["spool", "sqlite"],
    expects_recovery_history: bool,
) -> None:
    backend_root = tmp_path / f"{backend_kind}-queue"
    if backend_kind == "spool":
        backend: QueueBackend = LocalSpoolQueue(
            root=backend_root,
            max_task_attempts=1,
            max_orphan_requeues=1,
        )
    else:
        backend = SqliteQueueBackend(
            root=backend_root,
            max_task_attempts=1,
            max_orphan_requeues=1,
        )

    failure_task = _make_task(f"task-governance-failure-{backend_kind}", kind="it")
    backend.enqueue(failure_task)
    failure_claimed = backend.claim_next()
    assert failure_claimed is not None
    backend.ack_failure(failure_claimed)

    orphan_task = _make_task(f"task-governance-orphan-{backend_kind}", kind="it")
    backend.enqueue(orphan_task)
    orphan_claimed = backend.claim_next()
    assert orphan_claimed is not None
    backend.requeue_orphaned(orphan_claimed)

    poison_result = inspect_local_queue(
        root=backend_root,
        backend_kind=backend_kind,
        bucket="poison",
    )
    assert poison_result.summary.poison == 2
    assert poison_result.summary.listed_tasks == 2
    assert poison_result.summary.governance_problem_tasks == 2
    assert poison_result.summary.visible_requeue_history_tasks == 0
    assert poison_result.summary.recovery_audit_support == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )

    poison_tasks = {task.task_id: task for task in poison_result.tasks}
    failed_poison = poison_tasks[failure_task.task_id]
    orphan_poison = poison_tasks[orphan_task.task_id]

    assert failed_poison.state == "poison"
    assert failed_poison.governance_labels == ()
    assert failed_poison.governance_problem == "poison_attempts_exhausted"
    assert failed_poison.poison_kind == "attempts_exhausted"
    assert failed_poison.handling_expectation == "requeue_supported"
    assert failed_poison.latest_recovery is None
    assert failed_poison.recovery_audit_support == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )
    assert failed_poison.governance_action_audit_support == "result_only"
    assert failed_poison.latest_governance_action is None

    assert orphan_poison.state == "poison"
    assert orphan_poison.governance_labels == ("orphaned",)
    assert orphan_poison.governance_problem == "poison_orphaned"
    assert orphan_poison.poison_kind == "orphaned"
    assert orphan_poison.handling_expectation == "requeue_supported"
    assert orphan_poison.latest_recovery is None
    assert orphan_poison.recovery_audit_support == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )
    assert orphan_poison.governance_action_audit_support == "result_only"
    assert orphan_poison.latest_governance_action is None

    recovery = requeue_local_task(
        root=backend_root,
        backend_kind=backend_kind,
        source_bucket="poison",
        task_id=failure_task.task_id,
    )
    assert recovery.governance_action.action == "requeue"
    assert recovery.governance_action.source_state == "poison"
    assert recovery.governance_action.target_state == "pending"
    assert recovery.governance_action.redrive_semantics == "not_modeled"
    assert recovery.support_status == "supported"
    assert recovery.governance_result == "moved_to_pending"
    if expects_recovery_history:
        assert recovery.governance_action.audit_support == "persisted_in_history"
        assert recovery.to_dict()["audit_support"] == "persisted_in_history"
    else:
        assert recovery.governance_action.audit_support == "result_only"
        assert recovery.to_dict()["audit_support"] == "result_only"
    assert recovery.to_dict()["support_status"] == "supported"
    assert recovery.to_dict()["governance_result"] == "moved_to_pending"
    assert recovery.to_dict()["redrive_support"] == "not_modeled"

    pending_result = inspect_local_queue(
        root=backend_root,
        backend_kind=backend_kind,
        bucket="pending",
    )
    assert pending_result.summary.pending == 1
    assert pending_result.summary.listed_tasks == 1
    assert pending_result.summary.governance_problem_tasks == 0
    assert pending_result.summary.visible_requeue_history_tasks == (
        1 if expects_recovery_history else 0
    )
    assert pending_result.summary.recovery_audit_support == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )
    recovered_task = pending_result.tasks[0]

    assert recovered_task.task_id == failure_task.task_id
    assert recovered_task.state == "pending"
    assert recovered_task.governance_problem == "none"
    assert recovered_task.poison_kind == "not_poison"
    assert recovered_task.handling_expectation == "none"
    if expects_recovery_history:
        assert recovered_task.governance_labels == ("requeued",)
        assert recovered_task.recovery_audit_support == "persisted_in_history"
        assert recovered_task.governance_action_audit_support == "persisted_in_history"
        assert recovered_task.latest_recovery == {
            "action": "requeue",
            "source_bucket": "poison",
            "target_bucket": "pending",
            "recorded_at_utc": recovery.recorded_at_utc,
        }
        latest_governance_action = recovered_task.latest_governance_action
        assert latest_governance_action is not None
        assert latest_governance_action.action == "requeue"
        assert latest_governance_action.source_state == "poison"
        assert latest_governance_action.target_state == "pending"
        assert latest_governance_action.audit_support == "persisted_in_history"
        assert latest_governance_action.redrive_semantics == "not_modeled"
    else:
        assert recovered_task.governance_labels == ()
        assert recovered_task.recovery_audit_support == "result_only"
        assert recovered_task.governance_action_audit_support == "result_only"
        assert recovered_task.latest_recovery is None
        assert recovered_task.latest_governance_action is None


@pytest.mark.parametrize(
    ("backend_kind", "expects_recovery_history"),
    [
        ("spool", True),
        ("sqlite", False),
    ],
)
def test_operator_surface_boundary_keeps_inspection_recovery_and_summary_facts_distinct(
    tmp_path: Path,
    backend_kind: Literal["spool", "sqlite"],
    expects_recovery_history: bool,
) -> None:
    backend_root = tmp_path / f"{backend_kind}-queue"
    if backend_kind == "spool":
        backend: QueueBackend = LocalSpoolQueue(root=backend_root, max_task_attempts=1)
    else:
        backend = SqliteQueueBackend(root=backend_root, max_task_attempts=1)

    task = _make_task(f"task-operator-boundary-{backend_kind}", kind="it")
    backend.enqueue(task)
    claimed = backend.claim_next()
    assert claimed is not None
    backend.ack_failure(claimed)

    poison_result = inspect_local_queue(
        root=backend_root,
        backend_kind=backend_kind,
        bucket="poison",
    )
    poison_task = poison_result.tasks[0]
    assert poison_result.summary.listed_tasks == 1
    assert poison_result.summary.governance_problem_tasks == 1
    assert poison_result.summary.visible_requeue_history_tasks == 0
    assert poison_result.summary.recovery_audit_support == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )
    assert poison_task.state == "poison"
    assert poison_task.governance_problem == "poison_attempts_exhausted"
    assert poison_task.handling_expectation == "requeue_supported"
    assert poison_task.recovery_audit_support == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )
    assert poison_task.latest_recovery is None
    assert poison_task.latest_governance_action is None

    recovery = requeue_local_task(
        root=backend_root,
        backend_kind=backend_kind,
        source_bucket="poison",
        task_id=task.task_id,
    )
    assert recovery.governance_action.action == "requeue"
    assert recovery.support_status == "supported"
    assert recovery.governance_result == "moved_to_pending"
    assert recovery.to_dict()["redrive_support"] == "not_modeled"
    assert recovery.to_dict()["audit_support"] == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )

    pending_result = inspect_local_queue(
        root=backend_root,
        backend_kind=backend_kind,
        bucket="pending",
    )
    pending_task = pending_result.tasks[0]
    assert pending_result.summary.listed_tasks == 1
    assert pending_result.summary.governance_problem_tasks == 0
    assert pending_result.summary.visible_requeue_history_tasks == (
        1 if expects_recovery_history else 0
    )
    assert pending_result.summary.recovery_audit_support == (
        "persisted_in_history" if expects_recovery_history else "result_only"
    )
    assert pending_task.state == "pending"
    assert pending_task.governance_problem == "none"
    assert pending_task.handling_expectation == "none"
    if expects_recovery_history:
        latest_action = pending_task.latest_governance_action
        assert latest_action is not None
        assert pending_task.governance_labels == ("requeued",)
        assert pending_task.latest_recovery is not None
        assert latest_action.action == "requeue"
        assert latest_action.audit_support == "persisted_in_history"
    else:
        assert pending_task.governance_labels == ()
        assert pending_task.latest_recovery is None
        assert pending_task.latest_governance_action is None
