from __future__ import annotations

from pathlib import Path

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_ingest.queue_backend import QueueBackend, QueueBackendWorkSource
from zephyr_ingest.spool_queue import LocalSpoolQueue
from zephyr_ingest.task_v1 import TaskDocumentInputV1, TaskExecutionV1, TaskInputsV1, TaskV1
from zephyr_ingest.worker_runtime import WorkerRuntime


def _make_task(task_id: str) -> TaskV1:
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1(
                uri=f"/tmp/{task_id}.pdf",
                source="local_file",
                discovered_at_utc="2026-04-05T00:00:00Z",
                filename=f"{task_id}.pdf",
                extension=".pdf",
                size_bytes=128,
            )
        ),
        execution=TaskExecutionV1(),
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
