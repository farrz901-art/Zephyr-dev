from __future__ import annotations

from pathlib import Path
from typing import Literal

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_ingest.lock_provider import FileLockProvider, LockProvider
from zephyr_ingest.queue_backend import QueueBackend, QueueBackendWorkSource
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
