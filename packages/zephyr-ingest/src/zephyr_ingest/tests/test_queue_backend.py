from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_ingest.lock_provider import FileLockProvider, LockProvider
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackend, QueueBackendWorkSource
from zephyr_ingest.queue_inspect import inspect_local_spool_queue
from zephyr_ingest.queue_recover import requeue_local_spool_task
from zephyr_ingest.spool_queue import LocalSpoolQueue
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


def _accept_backend(backend: QueueBackend) -> QueueBackend:
    return backend


def test_spool_queue_satisfies_queue_backend_protocol(tmp_path: Path) -> None:
    backend = LocalSpoolQueue(root=tmp_path / "spool")

    accepted = _accept_backend(backend)

    assert accepted is backend


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
