from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Literal

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_ingest.lock_provider import FileLockProvider, LockProvider
from zephyr_ingest.lock_provider_factory import build_local_lock_provider
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackend, QueueBackendWorkSource
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_inspect import inspect_local_spool_queue
from zephyr_ingest.queue_recover import requeue_local_spool_task
from zephyr_ingest.spool_queue import LocalSpoolQueue
from zephyr_ingest.sqlite_queue import SqliteQueueBackend
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

    poison_view = inspect_local_spool_queue(root=backend.root, bucket="poison").to_dict()
    assert poison_view["summary"]["poison"] == 1
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
    pending_view = inspect_local_spool_queue(root=backend.root, bucket="pending").to_dict()
    assert pending_view["summary"] == {
        "pending": 1,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 0,
    }
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
    }
    assert done_view["tasks"][0]["latest_recovery"] == pending_view["tasks"][0]["latest_recovery"]

    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="done"} 1.0' in text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="poison"} 0.0' in text
    assert 'zephyr_ingest_queue_poison_transitions_total{pipeline_version="p-worker"} 1.0' in text
