from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from zephyr_core import DocumentMetadata, DocumentRef, PartitionStrategy, RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.run_meta import EngineMetaV1, MetricsV1, RunProvenanceV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.lock_provider import LockProvider
from zephyr_ingest.lock_provider_factory import build_local_lock_provider
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackend, QueueBackendWorkSource, TaskHandler
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_inspect import inspect_local_sqlite_queue
from zephyr_ingest.queue_recover import requeue_local_task
from zephyr_ingest.replay_delivery import SqliteReplaySink, replay_delivery_dlq
from zephyr_ingest.sqlite_lock_provider import SqliteLockProvider
from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.worker_runtime import WorkerRuntime


def _source_document(path: Path) -> DocumentRef:
    return DocumentRef(
        uri=str(path),
        source="local_file",
        discovered_at_utc="2026-04-21T00:00:00Z",
        filename=path.name,
        extension=path.suffix.lower(),
        size_bytes=path.stat().st_size,
    )


def _make_uns_task(
    *,
    task_id: str,
    source_path: Path,
    pipeline_version: str = "p5-m4-s8",
    identity_sha: str | None = None,
) -> TaskV1:
    doc = _source_document(source_path)
    resolved_sha = identity_sha if identity_sha is not None else f"sha-{task_id}"
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
        execution=TaskExecutionV1(strategy=PartitionStrategy.AUTO, unique_element_ids=True),
        identity=TaskIdentityV1(pipeline_version=pipeline_version, sha256=resolved_sha),
    )


@dataclass(slots=True)
class _ThreadResult:
    rc: int | None = None
    exc: BaseException | None = None


def _start_runtime_thread(
    *,
    runtime: WorkerRuntime,
    work_source: QueueBackendWorkSource,
    sleep_fn: Callable[[float], None],
) -> tuple[threading.Thread, _ThreadResult]:
    result = _ThreadResult()

    def _runner() -> None:
        try:
            result.rc = runtime.run(work_source=work_source, sleep_fn=sleep_fn)
        except BaseException as exc:  # pragma: no cover - surfaced by assertions below
            result.exc = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    return thread, result


def _assert_thread_done(thread: threading.Thread, result: _ThreadResult) -> None:
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert result.exc is None
    assert result.rc == 0


def _runtime(*, run_id: str) -> WorkerRuntime:
    return WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p5-m4-s8",
            run_id=run_id,
            timestamp_utc="2026-04-21T00:00:00Z",
        ),
        poll_interval_ms=1,
    )


def _ignore_task(task: TaskV1) -> None:
    del task


def _contention_sleep(
    *,
    runtime: WorkerRuntime,
    source: QueueBackendWorkSource,
    deadline: float,
) -> Callable[[float], None]:
    def _sleep(_: float) -> None:
        if source.work_source_metrics_snapshot().lock_contention_total >= 1:
            runtime.request_draining()
        if time.monotonic() >= deadline:
            runtime.request_draining()
        time.sleep(0.01)

    return _sleep


def _sqlite_payload_for_identity(
    *,
    db_path: Path,
    table_name: str,
    identity_key: str,
) -> dict[str, object]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            f"SELECT payload_json FROM {table_name} WHERE identity_key = ?",
            (identity_key,),
        ).fetchone()
    assert row is not None
    payload_obj: object = json.loads(cast(str, row[0]))
    assert isinstance(payload_obj, dict)
    return cast(dict[str, object], payload_obj)


def _write_replayable_delivery_failure(
    *,
    out_root: Path,
    identity_sha: str,
    run_id: str,
) -> None:
    artifact_dir = out_root / identity_sha
    artifact_dir.mkdir(parents=True, exist_ok=True)
    records_path = artifact_dir / "records.jsonl"
    records_path.write_text(
        '{"data":{"message":"s8-replay-record","row":1}}\n',
        encoding="utf-8",
    )
    (artifact_dir / "logs.jsonl").write_text(
        '{"event":"worker_stop","phase":"failed"}\n',
        encoding="utf-8",
    )
    (artifact_dir / "checkpoint.json").write_text(
        json.dumps({"progress_family": "cursor_v1", "checkpoints": []}, indent=2),
        encoding="utf-8",
    )
    (artifact_dir / "elements.json").write_text(
        json.dumps(
            [
                {
                    "element_id": "e1",
                    "type": "Text",
                    "text": "s8-replay-record",
                    "metadata": {"flow_kind": "it"},
                }
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "normalized.txt").write_text("s8-replay-record\n", encoding="utf-8")

    run_meta = RunMetaV1(
        run_id=run_id,
        pipeline_version="p5-m4-s8",
        timestamp_utc="2026-04-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=RunOutcome.SUCCESS,
        document=DocumentMetadata(
            filename="records.jsonl",
            mime_type="application/x-ndjson",
            sha256=identity_sha,
            size_bytes=records_path.stat().st_size,
            created_at_utc="2026-04-21T00:00:00Z",
        ),
        engine=EngineMetaV1(
            name="bounded-concurrency-test",
            backend="test",
            version="0",
            strategy="auto",
        ),
        metrics=MetricsV1(
            duration_ms=1,
            elements_count=1,
            normalized_text_len=16,
            attempts=1,
        ),
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="worker",
            task_id="task-s8-replay",
            task_identity_key=(
                '{"kind":"it","pipeline_version":"p5-m4-s8","sha256":"' + identity_sha + '"}'
            ),
        ),
    )
    (artifact_dir / "run_meta.json").write_text(
        json.dumps(run_meta.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_delivery_dlq(
        out_root=out_root,
        sha256=identity_sha,
        meta=run_meta,
        receipt=DeliveryReceipt(
            destination="sqlite",
            ok=False,
            details={
                "retryable": True,
                "failure_kind": "connection",
                "error_code": "ZE-DELIVERY-FAILED",
            },
        ),
    )


def _hold_first_task_and_create_contention(
    *,
    tmp_path: Path,
    max_orphan_requeues: int,
    contender_count: int,
    task_count: int | None = None,
) -> tuple[Path, QueueBackend, LockProvider, threading.Event, threading.Thread, _ThreadResult]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "contention-source.txt"
    source_path.write_text("contention-source", encoding="utf-8")
    queue_root = tmp_path / "queue"
    queue_backend = build_local_queue_backend(
        kind="sqlite",
        root=queue_root,
        max_orphan_requeues=max_orphan_requeues,
    )
    shared_sha = "sha-s8-shared-contention"
    resolved_task_count = task_count if task_count is not None else contender_count + 1
    for index in range(resolved_task_count):
        queue_backend.enqueue(
            _make_uns_task(
                task_id=f"task-s8-contention-{index + 1}",
                source_path=source_path,
                identity_sha=shared_sha,
            )
        )

    lock_provider = build_local_lock_provider(
        kind="sqlite",
        root=tmp_path / "locks",
        stale_after_s=10,
    )
    primary_runtime = _runtime(run_id="s8-primary-holder")
    first_started = threading.Event()
    release_first = threading.Event()

    def primary_handler(task: TaskV1) -> None:
        assert task.task_id == "task-s8-contention-1"
        first_started.set()
        assert release_first.wait(timeout=5.0)
        primary_runtime.request_draining()

    primary_source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, primary_handler),
        lock_provider=lock_provider,
        lock_owner="s8-primary",
    )
    primary_thread, primary_result = _start_runtime_thread(
        runtime=primary_runtime,
        work_source=primary_source,
        sleep_fn=lambda _: time.sleep(0.01),
    )
    assert first_started.wait(timeout=5.0)

    for index in range(contender_count):
        contender_runtime = _runtime(run_id=f"s8-contender-{index + 1}")
        contender_source = QueueBackendWorkSource(
            backend=queue_backend,
            handler=cast(TaskHandler, _ignore_task),
            lock_provider=lock_provider,
            lock_owner=f"s8-contender-{index + 1}",
        )
        contender_thread, contender_result = _start_runtime_thread(
            runtime=contender_runtime,
            work_source=contender_source,
            sleep_fn=_contention_sleep(
                runtime=contender_runtime,
                source=contender_source,
                deadline=time.monotonic() + 5.0,
            ),
        )
        _assert_thread_done(contender_thread, contender_result)
        assert contender_source.work_source_metrics_snapshot().lock_contention_total == 1

    return (
        queue_root,
        queue_backend,
        lock_provider,
        release_first,
        primary_thread,
        primary_result,
    )


@pytest.mark.auth_recovery_drill
def test_p5_m4_s8_four_worker_same_pending_contention_is_bounded_and_inspectable(
    tmp_path: Path,
) -> None:
    queue_root, queue_backend, lock_provider, release_first, primary_thread, primary_result = (
        _hold_first_task_and_create_contention(
            tmp_path=tmp_path,
            max_orphan_requeues=10,
            contender_count=3,
        )
    )

    pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()
    summary_view = inspect_local_sqlite_queue(root=queue_root).to_dict()

    assert summary_view["summary"]["inflight"] == 1
    assert summary_view["summary"]["pending"] == 3
    assert pending_view["summary"]["governance_problem_tasks"] >= 1
    assert any(task["governance_problem"] == "orphaned" for task in pending_view["tasks"])

    release_first.set()
    _assert_thread_done(primary_thread, primary_result)

    cleanup_runtime = _runtime(run_id="s8-cleanup-worker")
    handled: list[str] = []

    def cleanup_handler(task: TaskV1) -> None:
        handled.append(task.task_id)
        if len(handled) == 3:
            cleanup_runtime.request_draining()

    cleanup_source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, cleanup_handler),
        lock_provider=lock_provider,
        lock_owner="s8-cleanup",
    )
    cleanup_rc = cleanup_runtime.run(work_source=cleanup_source, sleep_fn=lambda _: None)
    done_view = inspect_local_sqlite_queue(root=queue_root, bucket="done").to_dict()
    prom_text = render_prometheus_text(
        families=build_worker_prom_families(
            ctx=cleanup_runtime.ctx,
            lifecycle=cleanup_runtime,
            work_source=cleanup_source,
        )
    )

    assert cleanup_rc == 0
    assert len(handled) == 3
    assert {task["task_id"] for task in done_view["tasks"]} == {
        "task-s8-contention-1",
        "task-s8-contention-2",
        "task-s8-contention-3",
        "task-s8-contention-4",
    }
    assert (
        'zephyr_ingest_worker_runtime_boundary_info{pipeline_version="p5-m4-s8",'
        'runtime_scope="single_process_local_polling",stop_intent="drain",'
        'accepts_new_work="false"} 1.0'
    ) in prom_text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p5-m4-s8",bucket="done"} 4.0' in prom_text


@pytest.mark.auth_recovery_drill
def test_p5_m4_s8_backlog_drain_under_three_workers_leaves_pending_inspectable(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "backlog-source.txt"
    source_path.write_text("backlog-source", encoding="utf-8")
    queue_root = tmp_path / "queue"
    queue_backend = build_local_queue_backend(kind="sqlite", root=queue_root)
    for index in range(6):
        queue_backend.enqueue(
            _make_uns_task(
                task_id=f"task-s8-backlog-{index + 1}",
                source_path=source_path,
                identity_sha=f"sha-s8-backlog-{index + 1}",
            )
        )

    barrier = threading.Barrier(3)
    handled: list[str] = []
    threads: list[tuple[threading.Thread, _ThreadResult]] = []
    sources: list[QueueBackendWorkSource] = []
    runtimes: list[WorkerRuntime] = []

    for index in range(3):
        runtime = _runtime(run_id=f"s8-drain-worker-{index + 1}")
        runtimes.append(runtime)

        def handler(task: TaskV1, *, current_runtime: WorkerRuntime = runtime) -> None:
            handled.append(task.task_id)
            barrier.wait(timeout=5.0)
            current_runtime.request_draining()

        source = QueueBackendWorkSource(
            backend=queue_backend,
            handler=cast(TaskHandler, handler),
        )
        sources.append(source)
        threads.append(
            _start_runtime_thread(
                runtime=runtime,
                work_source=source,
                sleep_fn=lambda _: time.sleep(0.01),
            )
        )

    for thread, result in threads:
        _assert_thread_done(thread, result)

    summary_view = inspect_local_sqlite_queue(root=queue_root).to_dict()
    pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()
    done_view = inspect_local_sqlite_queue(root=queue_root, bucket="done").to_dict()
    prom_text = render_prometheus_text(
        families=build_worker_prom_families(
            ctx=runtimes[0].ctx,
            lifecycle=runtimes[0],
            work_source=sources[0],
        )
    )

    assert len(set(handled)) == 3
    assert summary_view["summary"]["done"] == 3
    assert summary_view["summary"]["pending"] == 3
    assert pending_view["summary"]["listed_tasks"] == 3
    assert done_view["summary"]["listed_tasks"] == 3
    assert all(
        task["state_explanation"] == "ready_for_local_worker_claim"
        for task in pending_view["tasks"]
    )
    assert (
        'zephyr_ingest_worker_runtime_boundary_info{pipeline_version="p5-m4-s8",'
        'runtime_scope="single_process_local_polling",stop_intent="drain",'
        'accepts_new_work="false"} 1.0'
    ) in prom_text


@pytest.mark.auth_recovery_drill
def test_p5_m4_s8_stale_lock_recovery_and_contention_remain_local_and_auditable(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "stale-source.txt"
    source_path.write_text("stale-source", encoding="utf-8")
    queue_root = tmp_path / "queue"
    queue_backend = build_local_queue_backend(
        kind="sqlite",
        root=queue_root,
        max_orphan_requeues=10,
    )
    shared_sha = "sha-s8-stale-shared"
    task_one = _make_uns_task(
        task_id="task-s8-stale-1",
        source_path=source_path,
        identity_sha=shared_sha,
    )
    task_two = _make_uns_task(
        task_id="task-s8-stale-2",
        source_path=source_path,
        identity_sha=shared_sha,
    )
    queue_backend.enqueue(task_one)
    queue_backend.enqueue(task_two)

    lock_provider = build_local_lock_provider(
        kind="sqlite",
        root=tmp_path / "locks",
        stale_after_s=10,
    )
    assert isinstance(lock_provider, SqliteLockProvider)
    stale_lock = lock_provider.acquire(
        key=normalize_task_idempotency_key(task_one),
        owner="stale-owner",
    )
    assert stale_lock is not None
    with sqlite3.connect(lock_provider.db_path) as conn:
        conn.execute(
            "UPDATE task_locks SET acquired_at_epoch_s = ? WHERE key = ?",
            (25, normalize_task_idempotency_key(task_one)),
        )

    primary_runtime = _runtime(run_id="s8-stale-primary")
    first_started = threading.Event()
    release_first = threading.Event()

    def primary_handler(task: TaskV1) -> None:
        assert task.task_id == "task-s8-stale-1"
        first_started.set()
        assert release_first.wait(timeout=5.0)
        primary_runtime.request_draining()

    primary_source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, primary_handler),
        lock_provider=lock_provider,
        lock_owner="s8-stale-primary",
    )
    primary_thread, primary_result = _start_runtime_thread(
        runtime=primary_runtime,
        work_source=primary_source,
        sleep_fn=lambda _: time.sleep(0.01),
    )
    assert first_started.wait(timeout=5.0)

    contender_runtime = _runtime(run_id="s8-stale-contender")
    contender_source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, _ignore_task),
        lock_provider=lock_provider,
        lock_owner="s8-stale-contender",
    )
    contender_thread, contender_result = _start_runtime_thread(
        runtime=contender_runtime,
        work_source=contender_source,
        sleep_fn=_contention_sleep(
            runtime=contender_runtime,
            source=contender_source,
            deadline=time.monotonic() + 5.0,
        ),
    )
    _assert_thread_done(contender_thread, contender_result)

    pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()
    stale_metrics = lock_provider.lock_metrics_snapshot()
    prom_text = render_prometheus_text(
        families=build_worker_prom_families(
            ctx=contender_runtime.ctx,
            lifecycle=contender_runtime,
            work_source=contender_source,
        )
    )

    assert stale_metrics.stale_recovery_total == 1
    assert contender_source.work_source_metrics_snapshot().lock_contention_total == 1
    assert pending_view["tasks"][0]["governance_problem"] == "orphaned"
    assert 'zephyr_ingest_lock_stale_recoveries_total{pipeline_version="p5-m4-s8"} 1.0' in prom_text
    assert 'zephyr_ingest_queue_lock_contention_total{pipeline_version="p5-m4-s8"} 1.0' in prom_text

    release_first.set()
    _assert_thread_done(primary_thread, primary_result)


@pytest.mark.auth_recovery_drill
def test_p5_m4_s8_poison_requeue_under_contention_remains_operator_readable(
    tmp_path: Path,
) -> None:
    queue_root, _queue_backend, _lock_provider, release_first, primary_thread, primary_result = (
        _hold_first_task_and_create_contention(
            tmp_path=tmp_path,
            max_orphan_requeues=2,
            contender_count=2,
            task_count=2,
        )
    )
    poison_view = inspect_local_sqlite_queue(root=queue_root, bucket="poison").to_dict()

    assert poison_view["summary"]["poison"] == 1
    assert poison_view["tasks"][0]["task_id"] == "task-s8-contention-2"
    assert poison_view["tasks"][0]["poison_kind"] == "orphaned"
    assert poison_view["tasks"][0]["governance_problem"] == "poison_orphaned"
    assert poison_view["tasks"][0]["handling_expectation"] == "requeue_supported"

    recovery = requeue_local_task(
        root=queue_root,
        source_bucket="poison",
        task_id="task-s8-contention-2",
        backend_kind="sqlite",
    )
    pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()
    recovery_provenance = recovery.to_run_provenance()

    assert recovery.audit_support == "result_only"
    assert recovery.governance_action.redrive_semantics == "not_modeled"
    assert recovery_provenance.run_origin == "requeue"
    assert recovery_provenance.delivery_origin == "primary"
    assert pending_view["tasks"][0]["task_id"] == "task-s8-contention-2"
    assert pending_view["tasks"][0]["orphan_count"] == 2

    release_first.set()
    _assert_thread_done(primary_thread, primary_result)


@pytest.mark.auth_recovery_drill
def test_p5_m4_s8_replay_and_queue_governance_remain_distinct_under_contention(
    tmp_path: Path,
) -> None:
    queue_root, _queue_backend, _lock_provider, release_first, primary_thread, primary_result = (
        _hold_first_task_and_create_contention(
            tmp_path=tmp_path / "queue-side",
            max_orphan_requeues=2,
            contender_count=2,
            task_count=2,
        )
    )
    recovery = requeue_local_task(
        root=queue_root,
        source_bucket="poison",
        task_id="task-s8-contention-2",
        backend_kind="sqlite",
    )
    queue_provenance = recovery.to_run_provenance()

    out_root = tmp_path / "delivery-side"
    identity_sha = "sha-s8-replay"
    run_id = "s8-replay-run"
    _write_replayable_delivery_failure(
        out_root=out_root,
        identity_sha=identity_sha,
        run_id=run_id,
    )
    replay_db = out_root / "replay.db"
    replay_stats = replay_delivery_dlq(
        out_root=out_root,
        sink=SqliteReplaySink(db_path=replay_db, table_name="zephyr_delivery_records"),
        dry_run=False,
        move_done=True,
    )
    payload = _sqlite_payload_for_identity(
        db_path=replay_db,
        table_name="zephyr_delivery_records",
        identity_key=f"{identity_sha}:{run_id}",
    )
    payload_run_meta_obj = payload["run_meta"]
    assert isinstance(payload_run_meta_obj, dict)
    payload_run_meta = cast(dict[str, object], payload_run_meta_obj)
    payload_provenance_obj = payload_run_meta["provenance"]
    assert isinstance(payload_provenance_obj, dict)
    payload_provenance = cast(dict[str, object], payload_provenance_obj)
    dlq_done_files = list((out_root / "_dlq" / "delivery_done").glob("*.json"))
    queue_pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()

    assert queue_provenance.run_origin == "requeue"
    assert queue_provenance.delivery_origin == "primary"
    assert replay_stats.attempted == 1
    assert replay_stats.succeeded == 1
    assert replay_stats.moved_to_done == 1
    assert len(dlq_done_files) == 1
    assert payload_provenance["run_origin"] == "intake"
    assert payload_provenance["delivery_origin"] == "replay"
    assert payload_provenance["task_id"] == "task-s8-replay"
    assert queue_pending_view["tasks"][0]["task_id"] == "task-s8-contention-2"
    assert queue_pending_view["tasks"][0]["recovery_audit_support"] == "result_only"

    release_first.set()
    _assert_thread_done(primary_thread, primary_result)
