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
from zephyr_ingest.lock_provider_factory import build_local_lock_provider
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackendWorkSource, TaskHandler
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_inspect import inspect_local_sqlite_queue
from zephyr_ingest.replay_delivery import SqliteReplaySink, replay_delivery_dlq
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
        discovered_at_utc="2026-04-18T00:00:00Z",
        filename=path.name,
        extension=path.suffix.lower(),
        size_bytes=path.stat().st_size,
    )


def _make_uns_task(
    *,
    task_id: str,
    source_path: Path,
    pipeline_version: str,
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


@pytest.mark.auth_recovery_drill
def test_p5_m2_runtime_drain_leaves_pending_work_inspectable(tmp_path: Path) -> None:
    source_path = tmp_path / "drain-source.txt"
    source_path.write_text("drain-source", encoding="utf-8")

    queue_root = tmp_path / "drain-queue"
    queue_backend = build_local_queue_backend(kind="sqlite", root=queue_root)
    task_one = _make_uns_task(
        task_id="task-drain-1",
        source_path=source_path,
        pipeline_version="p5-m2-runtime",
    )
    task_two = _make_uns_task(
        task_id="task-drain-2",
        source_path=source_path,
        pipeline_version="p5-m2-runtime",
        identity_sha="sha-task-drain-2",
    )
    queue_backend.enqueue(task_one)
    queue_backend.enqueue(task_two)

    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p5-m2-runtime",
            run_id="m2-drain",
            timestamp_utc="2026-04-18T00:00:00Z",
        ),
        poll_interval_ms=1,
    )
    handled: list[str] = []

    def handle(task_obj: TaskV1) -> None:
        handled.append(task_obj.task_id)
        runtime.request_draining()

    work_source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, handle),
        lock_provider=build_local_lock_provider(
            kind="sqlite",
            root=tmp_path / "drain-locks",
            stale_after_s=10,
        ),
        lock_owner="worker-drain",
    )

    rc = runtime.run(work_source=work_source, sleep_fn=lambda _: None)
    summary_view = inspect_local_sqlite_queue(root=queue_root).to_dict()
    pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()
    done_view = inspect_local_sqlite_queue(root=queue_root, bucket="done").to_dict()
    metrics_text = render_prometheus_text(
        families=build_worker_prom_families(
            ctx=runtime.ctx,
            lifecycle=runtime,
            work_source=work_source,
        )
    )

    assert rc == 0
    assert handled == ["task-drain-1"]
    assert summary_view["summary"]["pending"] == 1
    assert summary_view["summary"]["done"] == 1
    assert pending_view["tasks"][0]["task_id"] == "task-drain-2"
    assert done_view["tasks"][0]["task_id"] == "task-drain-1"
    assert (
        'zephyr_ingest_queue_tasks{pipeline_version="p5-m2-runtime",bucket="pending"} 1.0'
        in metrics_text
    )
    assert (
        'zephyr_ingest_queue_tasks{pipeline_version="p5-m2-runtime",bucket="done"} 1.0'
        in metrics_text
    )


@pytest.mark.auth_recovery_drill
def test_p5_m2_two_worker_contention_is_bounded_and_inspectable(tmp_path: Path) -> None:
    source_path = tmp_path / "shared-source.txt"
    source_path.write_text("shared-source", encoding="utf-8")

    queue_root = tmp_path / "contention-queue"
    queue_backend = build_local_queue_backend(
        kind="sqlite",
        root=queue_root,
        max_orphan_requeues=2,
    )
    shared_sha = "sha-shared-contention"
    task_one = _make_uns_task(
        task_id="task-contention-1",
        source_path=source_path,
        pipeline_version="p5-m2-runtime",
        identity_sha=shared_sha,
    )
    task_two = _make_uns_task(
        task_id="task-contention-2",
        source_path=source_path,
        pipeline_version="p5-m2-runtime",
        identity_sha=shared_sha,
    )
    queue_backend.enqueue(task_one)
    queue_backend.enqueue(task_two)
    lock_provider = build_local_lock_provider(
        kind="sqlite",
        root=tmp_path / "contention-locks",
        stale_after_s=10,
    )

    runtime_one = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p5-m2-runtime",
            run_id="m2-contention-one",
            timestamp_utc="2026-04-18T00:00:00Z",
        ),
        poll_interval_ms=1,
    )
    runtime_two = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p5-m2-runtime",
            run_id="m2-contention-two",
            timestamp_utc="2026-04-18T00:00:01Z",
        ),
        poll_interval_ms=1,
    )

    first_started = threading.Event()
    release_first = threading.Event()
    handled_one: list[str] = []
    handled_two: list[str] = []

    def handle_one(task_obj: TaskV1) -> None:
        handled_one.append(task_obj.task_id)
        first_started.set()
        assert release_first.wait(timeout=5.0)
        runtime_one.request_draining()

    def handle_two(task_obj: TaskV1) -> None:
        handled_two.append(task_obj.task_id)
        runtime_two.request_draining()

    source_one = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, handle_one),
        lock_provider=lock_provider,
        lock_owner="worker-one",
    )
    source_two = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, handle_two),
        lock_provider=lock_provider,
        lock_owner="worker-two",
    )

    thread_one, result_one = _start_runtime_thread(
        runtime=runtime_one,
        work_source=source_one,
        sleep_fn=lambda _: time.sleep(0.01),
    )
    assert first_started.wait(timeout=5.0)

    def sleep_two(_: float) -> None:
        if source_two.work_source_metrics_snapshot().lock_contention_total >= 1:
            runtime_two.request_draining()
        time.sleep(0.01)

    thread_two, result_two = _start_runtime_thread(
        runtime=runtime_two,
        work_source=source_two,
        sleep_fn=sleep_two,
    )
    thread_two.join(timeout=5.0)
    assert not thread_two.is_alive()
    assert result_two.exc is None
    assert result_two.rc == 0

    pending_view = inspect_local_sqlite_queue(root=queue_root, bucket="pending").to_dict()
    summary_after_contention = inspect_local_sqlite_queue(root=queue_root).to_dict()
    contention_metrics = render_prometheus_text(
        families=build_worker_prom_families(
            ctx=runtime_two.ctx,
            lifecycle=runtime_two,
            work_source=source_two,
        )
    )

    assert handled_one == ["task-contention-1"]
    assert handled_two == []
    assert source_two.work_source_metrics_snapshot().lock_contention_total == 1
    assert summary_after_contention["summary"]["inflight"] == 1
    assert summary_after_contention["summary"]["pending"] == 1
    assert pending_view["tasks"][0]["task_id"] == "task-contention-2"
    assert pending_view["tasks"][0]["orphan_count"] == 1
    assert pending_view["tasks"][0]["governance_problem"] == "orphaned"
    assert (
        'zephyr_ingest_queue_lock_contention_total{pipeline_version="p5-m2-runtime"} 1.0'
        in contention_metrics
    )

    release_first.set()
    thread_one.join(timeout=5.0)
    assert not thread_one.is_alive()
    assert result_one.exc is None
    assert result_one.rc == 0

    runtime_three = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p5-m2-runtime",
            run_id="m2-contention-three",
            timestamp_utc="2026-04-18T00:00:02Z",
        ),
        poll_interval_ms=1,
    )
    handled_three: list[str] = []

    def handle_three(task_obj: TaskV1) -> None:
        handled_three.append(task_obj.task_id)
        runtime_three.request_draining()

    source_three = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(TaskHandler, handle_three),
        lock_provider=lock_provider,
        lock_owner="worker-three",
    )
    rc_three = runtime_three.run(work_source=source_three, sleep_fn=lambda _: None)
    done_view = inspect_local_sqlite_queue(root=queue_root, bucket="done").to_dict()

    assert rc_three == 0
    assert handled_three == ["task-contention-2"]
    assert {task["task_id"] for task in done_view["tasks"]} == {
        "task-contention-1",
        "task-contention-2",
    }


@pytest.mark.auth_recovery_drill
def test_p5_m2_replay_after_failed_delivery_keeps_operator_artifacts_aligned(
    tmp_path: Path,
) -> None:
    identity_sha = "sha-p5-m2-replay"
    run_id = "p5-m2-replay"
    artifact_dir = tmp_path / identity_sha
    artifact_dir.mkdir(parents=True, exist_ok=True)

    records_path = artifact_dir / "records.jsonl"
    records_path.write_text(
        '{"data":{"message":"replay-record","row":1}}\n',
        encoding="utf-8",
    )
    (artifact_dir / "logs.jsonl").write_text(
        '{"event":"worker_stop","phase":"failed"}\n',
        encoding="utf-8",
    )
    (artifact_dir / "checkpoint.json").write_text(
        json.dumps(
            {
                "progress_family": "cursor_v1",
                "checkpoints": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "elements.json").write_text(
        json.dumps(
            [
                {
                    "element_id": "e1",
                    "type": "Text",
                    "text": "replay-record",
                    "metadata": {"flow_kind": "it"},
                }
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "normalized.txt").write_text("replay-record\n", encoding="utf-8")

    run_meta = RunMetaV1(
        run_id=run_id,
        pipeline_version="p5-m2-runtime",
        timestamp_utc="2026-04-18T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=RunOutcome.SUCCESS,
        document=DocumentMetadata(
            filename="records.jsonl",
            mime_type="application/x-ndjson",
            sha256=identity_sha,
            size_bytes=records_path.stat().st_size,
            created_at_utc="2026-04-18T00:00:00Z",
        ),
        engine=EngineMetaV1(
            name="runtime-boundary-test",
            backend="test",
            version="0",
            strategy="auto",
        ),
        metrics=MetricsV1(
            duration_ms=1,
            elements_count=1,
            normalized_text_len=13,
            attempts=1,
        ),
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="worker",
            task_id="task-p5-m2-replay",
            task_identity_key=(
                '{"kind":"it","pipeline_version":"p5-m2-runtime","sha256":"' + identity_sha + '"}'
            ),
        ),
    )
    (artifact_dir / "run_meta.json").write_text(
        json.dumps(run_meta.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_delivery_dlq(
        out_root=tmp_path,
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

    replay_db = tmp_path / "replay.db"
    replay_stats = replay_delivery_dlq(
        out_root=tmp_path,
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

    dlq_done_files = list((tmp_path / "_dlq" / "delivery_done").glob("*.json"))
    dlq_pending_files = list((tmp_path / "_dlq" / "delivery").glob("*.json"))

    assert replay_stats.attempted == 1
    assert replay_stats.succeeded == 1
    assert replay_stats.failed == 0
    assert replay_stats.moved_to_done == 1
    assert len(dlq_done_files) == 1
    assert dlq_pending_files == []
    assert payload_provenance == {
        "run_origin": "intake",
        "delivery_origin": "replay",
        "execution_mode": "worker",
        "task_id": "task-p5-m2-replay",
        "task_identity_key": (
            '{"kind":"it","pipeline_version":"p5-m2-runtime","sha256":"' + identity_sha + '"}'
        ),
    }
