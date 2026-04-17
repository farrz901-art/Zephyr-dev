from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import cast

import pytest

from it_stream import (
    dump_it_artifacts,
    load_it_resume_selection,
    normalize_it_input_identity_sha,
    resume_file,
)
from it_stream import (
    process_file as process_it_file,
)
from it_stream.tests._p45_source_wave1_helpers import (
    http_fixture_base as it_http_fixture_base,
)
from it_stream.tests._p45_source_wave1_helpers import (
    postgresql_dsn,
    reset_postgresql_cursor_table,
    write_postgresql_source_spec,
)
from uns_stream.tests._p45_source_wave3_helpers import (
    http_fixture_base as uns_http_fixture_base,
)
from uns_stream.tests._p45_source_wave3_helpers import (
    write_http_source_spec,
)
from zephyr_core import DocumentRef, PartitionStrategy, RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.run_meta import EngineMetaV1, MetricsV1
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest._internal.utils import sha256_file
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.fanout import FanoutDestination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.webhook import DeliveryRetryConfig, WebhookDestination
from zephyr_ingest.flow_processor import normalize_flow_input_identity_sha
from zephyr_ingest.lock_provider import FileLockProvider
from zephyr_ingest.lock_provider_factory import build_local_lock_provider
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackendWorkSource, TaskHandler
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_inspect import inspect_local_sqlite_queue
from zephyr_ingest.queue_recover import requeue_local_task
from zephyr_ingest.replay_delivery import SqliteReplaySink, WebhookReplaySink, replay_delivery_dlq
from zephyr_ingest.runner import RunnerConfig, build_task_execution_handler, run_documents
from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.testing.p45 import LoadedP45Env
from zephyr_ingest.tests._p45_destination_wave1_helpers import (
    as_dict,
    read_json_dict,
    webhook_events,
    webhook_reset,
)
from zephyr_ingest.worker_runtime import WorkerRuntime


def _source_spec_document(path: Path) -> DocumentRef:
    return DocumentRef(
        uri=str(path),
        source="local_file",
        discovered_at_utc="2026-04-17T00:00:00Z",
        filename=path.name,
        extension=path.suffix.lower(),
        size_bytes=path.stat().st_size,
    )


def _uns_source_identity_sha(path: Path) -> str:
    return normalize_flow_input_identity_sha(
        flow_kind="uns",
        filename=str(path),
        default_sha=sha256_file(path),
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


def _make_uns_task(*, source_path: Path, task_id: str, pipeline_version: str) -> TaskV1:
    sha256 = _uns_source_identity_sha(source_path)
    source_doc = _source_spec_document(source_path)
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(source_doc)),
        execution=TaskExecutionV1(
            strategy=PartitionStrategy.AUTO,
            unique_element_ids=True,
        ),
        identity=TaskIdentityV1(
            pipeline_version=pipeline_version,
            sha256=sha256,
        ),
    )


def _raise_for_task(task_obj: TaskV1) -> None:
    raise RuntimeError(task_obj.task_id)


class _RuntimeDrainingHandler:
    def __init__(self, *, runtime: WorkerRuntime, delegate: object) -> None:
        self._runtime = runtime
        self._delegate = delegate

    def __call__(self, task_obj: TaskV1) -> None:
        delegate = self._delegate
        assert callable(delegate)
        delegate(task_obj)
        self._runtime.request_draining()


class _RaiseForTaskHandler:
    def __call__(self, task_obj: TaskV1) -> None:
        _raise_for_task(task_obj)


@pytest.mark.auth_recovery_drill
def test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    source_path = tmp_path / "uns-http-source.json"
    write_http_source_spec(
        source_path,
        url=f"{uns_http_fixture_base(p45_env)}/documents/plain",
        accept="text/plain",
        timeout_s=5.0,
    )
    source_doc = _source_spec_document(source_path)
    out_root = tmp_path / "out-uns-batch"
    webhook_base = p45_env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL").removesuffix("/ingest")
    retryable_url = f"{webhook_base}/status/503"
    success_url = p45_env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL")
    webhook_reset(base_url=webhook_base)

    destination = FanoutDestination(
        destinations=(
            FilesystemDestination(),
            WebhookDestination(
                url=retryable_url,
                retry=DeliveryRetryConfig(
                    enabled=True,
                    max_attempts=1,
                    base_backoff_ms=0,
                    max_backoff_ms=0,
                ),
            ),
        )
    )
    ctx = RunContext.new(
        pipeline_version="p45-m7-uns",
        run_id="m7-uns-batch",
        timestamp_utc="2026-04-17T00:00:00Z",
    )

    stats = run_documents(
        docs=[source_doc],
        cfg=RunnerConfig(out_root=out_root, destination=destination, force=True),
        ctx=ctx,
        flow_kind="uns",
    )

    sha256 = _uns_source_identity_sha(source_path)
    run_meta = read_json_dict(out_root / sha256 / "run_meta.json")
    elements_obj: object = json.loads(
        (out_root / sha256 / "elements.json").read_text(encoding="utf-8")
    )
    receipt = read_json_dict(out_root / sha256 / "delivery_receipt.json")
    report = read_json_dict(out_root / "batch_report.json")

    assert stats.success == 1
    assert run_meta["provenance"] == {
        "run_origin": "intake",
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "task_id": sha256,
        "task_identity_key": (
            '{"kind":"uns","pipeline_version":"p45-m7-uns","sha256":"' + sha256 + '"}'
        ),
    }
    assert isinstance(elements_obj, list)
    first_element = cast(list[object], elements_obj)[0]
    assert isinstance(first_element, dict)
    first_element_dict = cast(dict[str, object], first_element)
    first_metadata = as_dict(first_element_dict["metadata"])
    assert first_metadata["source_kind"] == "http_document_v1"
    assert first_metadata["source_url"] == f"{uns_http_fixture_base(p45_env)}/documents/plain"

    assert receipt["summary"] == {
        "delivery_outcome": "failed",
        "failure_retryability": "retryable",
        "failure_kind": "server_error",
        "error_code": "ZE-DELIVERY-HTTP-FAILED",
        "attempt_count": 1,
        "payload_count": 1,
    }
    delivery = as_dict(report["delivery"])
    assert delivery["total"] == 1
    assert delivery["ok"] == 0
    assert delivery["failed"] == 1
    assert delivery["failed_retryable"] == 1
    assert delivery["failed_unknown"] == 0
    assert as_dict(delivery["by_destination"])["fanout"] == {
        "total": 1,
        "ok": 0,
        "failed": 1,
    }
    assert as_dict(delivery["fanout_children_by_destination"]) == {
        "filesystem": {"total": 1, "ok": 1, "failed": 0},
        "webhook": {"total": 1, "ok": 0, "failed": 1},
    }
    assert as_dict(delivery["failure_kinds_by_destination"]) == {
        "fanout": {"server_error": 1},
        "webhook": {"server_error": 1},
    }
    assert report["counts_by_error_code"] == {"ZE-DELIVERY-HTTP-FAILED": 1}

    replay_sink_fail = WebhookReplaySink(url=retryable_url, timeout_s=5.0)
    replay_failed = replay_delivery_dlq(
        out_root=out_root,
        sink=replay_sink_fail,
        dry_run=False,
        move_done=False,
    )
    assert replay_failed.attempted == 1
    assert replay_failed.failed == 1
    assert replay_failed.succeeded == 0
    assert len(list((out_root / "_dlq" / "delivery").glob("*.json"))) == 1

    replay_sink_success = WebhookReplaySink(url=success_url, timeout_s=5.0)
    replay_succeeded = replay_delivery_dlq(
        out_root=out_root,
        sink=replay_sink_success,
        dry_run=False,
        move_done=True,
    )
    replay_done_files = list((out_root / "_dlq" / "delivery_done").glob("*.json"))
    events = webhook_events(base_url=webhook_base)
    ingest_events = [event for event in events if event["path"] == "/ingest"]
    payload = cast(dict[str, object], ingest_events[0]["payload"])
    replay_run_meta = as_dict(payload["run_meta"])
    replay_provenance = as_dict(replay_run_meta["provenance"])

    assert replay_succeeded.attempted == 1
    assert replay_succeeded.succeeded == 1
    assert replay_succeeded.failed == 0
    assert replay_succeeded.moved_to_done == 1
    assert len(replay_done_files) == 1
    assert replay_provenance == {
        "run_origin": "intake",
        "delivery_origin": "replay",
        "execution_mode": "batch",
        "task_id": sha256,
        "task_identity_key": (
            '{"kind":"uns","pipeline_version":"p45-m7-uns","sha256":"' + sha256 + '"}'
        ),
    }


@pytest.mark.auth_recovery_drill
def test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    pytest.importorskip("psycopg")
    table_name = "p45_m7_pg_resume"
    reset_postgresql_cursor_table(
        p45_env,
        table=table_name,
        rows=(
            ("cust-1", "cust-0001", "Ada"),
            ("cust-2", "cust-0002", "Bob"),
            ("cust-3", "cust-0003", "Cy"),
        ),
    )
    source_path = tmp_path / "postgres-source.json"
    write_postgresql_source_spec(
        source_path,
        stream="pg-customers",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(p45_env),
        schema="public",
        table=table_name,
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
    )

    identity_sha = normalize_it_input_identity_sha(
        filename=str(source_path),
        default_sha="fallback-m7-postgresql",
    )
    initial = process_it_file(
        filename=str(source_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "it-initial",
        result=initial,
        pipeline_version="p45-m7-it",
    )
    first_checkpoint = initial_artifacts.checkpoint.checkpoints[0]
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "it-initial" / "checkpoint.json",
        pipeline_version="p45-m7-it",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(source_path),
        checkpoint_path=tmp_path / "it-initial" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p45-m7-it",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    resumed_out = tmp_path / "it-resumed"
    run_provenance = selection.to_run_provenance(
        execution_mode="worker",
        task_id="task-m7-it-resume",
    )
    dump_it_artifacts(
        out_dir=resumed_out,
        result=resumed,
        pipeline_version="p45-m7-it",
        run_provenance=run_provenance,
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    run_meta = RunMetaV1(
        run_id="m7-it-resume",
        pipeline_version="p45-m7-it",
        timestamp_utc="2026-04-17T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=RunOutcome.SUCCESS,
        document=resumed.document,
        engine=EngineMetaV1(
            name=resumed.engine.name,
            backend=resumed.engine.backend,
            version=resumed.engine.version,
            strategy=str(resumed.engine.strategy),
        ),
        metrics=MetricsV1(
            duration_ms=1,
            elements_count=len(resumed.elements),
            normalized_text_len=len(resumed.normalized_text),
            attempts=1,
        ),
        provenance=run_provenance,
    )
    (resumed_out / "run_meta.json").write_text(
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

    # replay_delivery_dlq expects artifacts under out_root/<sha256>
    replay_artifact_dir = tmp_path / identity_sha
    replay_artifact_dir.mkdir(parents=True, exist_ok=True)
    for name in ("records.jsonl", "logs.jsonl", "checkpoint.json", "run_meta.json"):
        (replay_artifact_dir / name).write_text(
            (resumed_out / name).read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    sqlite_db_path = tmp_path / "replay.db"
    sqlite_table = "zephyr_delivery_records"
    replay_stats = replay_delivery_dlq(
        out_root=tmp_path,
        sink=SqliteReplaySink(db_path=sqlite_db_path, table_name=sqlite_table),
        dry_run=False,
        move_done=True,
    )
    payload = _sqlite_payload_for_identity(
        db_path=sqlite_db_path,
        table_name=sqlite_table,
        identity_key=f"{identity_sha}:m7-it-resume",
    )
    payload_run_meta = as_dict(payload["run_meta"])
    payload_provenance = as_dict(payload_run_meta["provenance"])
    payload_artifacts = as_dict(payload["artifacts"])
    records_path = cast(str, payload_artifacts["records_path"])
    state_path = cast(str, payload_artifacts["state_path"])
    logs_path = cast(str, payload_artifacts["logs_path"])

    assert replay_stats.succeeded == 1
    assert replay_stats.failed == 0
    assert payload_provenance["run_origin"] == "resume"
    assert payload_provenance["delivery_origin"] == "replay"
    assert payload_provenance["execution_mode"] == "worker"
    assert payload_provenance["task_id"] == "task-m7-it-resume"
    assert payload_provenance["checkpoint_identity_key"] == first_checkpoint.checkpoint_identity_key
    assert records_path.endswith("records.jsonl")
    assert state_path.endswith("checkpoint.json")
    assert logs_path.endswith("logs.jsonl")


@pytest.mark.auth_recovery_drill
def test_p45_m7_worker_queue_lock_and_operator_surfaces_remain_inspectable(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    source_path = tmp_path / "worker-http-source.json"
    write_http_source_spec(
        source_path,
        url=f"{it_http_fixture_base(p45_env)}/documents/plain",
        accept="text/plain",
        timeout_s=5.0,
    )
    queue_root = tmp_path / "sqlite-queue"
    out_root = tmp_path / "worker-out"
    task = _make_uns_task(
        source_path=source_path,
        task_id="task-m7-worker",
        pipeline_version="p45-m7-worker",
    )
    queue_backend = build_local_queue_backend(
        kind="sqlite",
        root=queue_root,
        max_task_attempts=1,
        max_orphan_requeues=1,
    )
    queue_backend.enqueue(task)
    lock_provider = build_local_lock_provider(
        kind="file",
        root=tmp_path / "file-locks",
        stale_after_s=10,
    )
    assert isinstance(lock_provider, FileLockProvider)
    lock_key = normalize_task_idempotency_key(task)
    stale_lock = lock_provider.acquire(key=lock_key, owner="worker-stale")
    assert stale_lock is not None
    os.utime(stale_lock.lock_ref, (25, 25))

    ctx = RunContext.new(
        pipeline_version="p45-m7-worker",
        run_id="m7-worker-runtime",
        timestamp_utc="2026-04-17T00:00:00Z",
    )
    runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=1)
    task_handler = build_task_execution_handler(
        cfg=RunnerConfig(out_root=out_root, destination=FilesystemDestination()),
        ctx=ctx,
        flow_kind="uns",
    )

    work_source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=cast(
            TaskHandler,
            _RuntimeDrainingHandler(runtime=runtime, delegate=task_handler),
        ),
        lock_provider=lock_provider,
        lock_owner="worker-m7",
    )

    rc = runtime.run(work_source=work_source, sleep_fn=lambda _: None)
    metrics_text = render_prometheus_text(
        families=build_worker_prom_families(ctx=ctx, lifecycle=runtime, work_source=work_source)
    )
    queue_view = inspect_local_sqlite_queue(root=queue_root, bucket="done").to_dict()
    assert task.identity is not None
    run_meta = read_json_dict(out_root / task.identity.sha256 / "run_meta.json")
    elements_obj: object = json.loads(
        (out_root / task.identity.sha256 / "elements.json").read_text(encoding="utf-8")
    )
    assert isinstance(elements_obj, list)
    first_element = cast(list[object], elements_obj)[0]
    assert isinstance(first_element, dict)
    first_element_dict = cast(dict[str, object], first_element)
    first_metadata = as_dict(first_element_dict["metadata"])

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert (
        'zephyr_ingest_lock_stale_recoveries_total{pipeline_version="p45-m7-worker"} 1.0'
        in metrics_text
    )
    assert (
        'zephyr_ingest_queue_tasks{pipeline_version="p45-m7-worker",bucket="done"} 1.0'
        in metrics_text
    )
    assert run_meta["provenance"] == {
        "run_origin": "intake",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-m7-worker",
        "task_identity_key": (
            '{"kind":"uns","pipeline_version":"p45-m7-worker","sha256":"'
            + task.identity.sha256
            + '"}'
        ),
    }
    assert first_metadata["source_kind"] == "http_document_v1"
    assert queue_view["summary"] == {
        "pending": 0,
        "inflight": 0,
        "done": 1,
        "failed": 0,
        "poison": 0,
        "listed_tasks": 1,
        "governance_problem_tasks": 0,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "result_only",
    }
    done_task = queue_view["tasks"][0]
    assert done_task["task_id"] == "task-m7-worker"
    assert done_task["kind"] == "uns"
    assert done_task["governance_problem"] == "none"
    assert done_task["latest_recovery"] is None

    failed_queue_root = tmp_path / "sqlite-queue-failed"
    failed_queue = build_local_queue_backend(
        kind="sqlite",
        root=failed_queue_root,
        max_task_attempts=1,
        max_orphan_requeues=1,
    )
    failed_task = _make_uns_task(
        source_path=source_path,
        task_id="task-m7-failed",
        pipeline_version="p45-m7-worker",
    )
    failed_queue.enqueue(failed_task)
    failed_runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=1)
    failed_source = QueueBackendWorkSource(
        backend=failed_queue,
        handler=cast(TaskHandler, _RaiseForTaskHandler()),
    )
    with pytest.raises(RuntimeError, match="task-m7-failed"):
        failed_runtime.run(work_source=failed_source, sleep_fn=lambda _: None)

    poison_view = inspect_local_sqlite_queue(root=failed_queue_root, bucket="poison").to_dict()
    recovery_result = requeue_local_task(
        root=failed_queue_root,
        source_bucket="poison",
        task_id="task-m7-failed",
        backend_kind="sqlite",
    )
    pending_view = inspect_local_sqlite_queue(root=failed_queue_root, bucket="pending").to_dict()

    assert poison_view["summary"]["poison"] == 1
    assert poison_view["tasks"][0]["governance_problem"] == "poison_attempts_exhausted"
    assert recovery_result.to_dict()["redrive_support"] == "not_modeled"
    assert failed_task.identity is not None
    assert recovery_result.to_run_provenance().to_dict() == {
        "run_origin": "requeue",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-m7-failed",
        "task_identity_key": (
            '{"kind":"uns","pipeline_version":"p45-m7-worker","sha256":"'
            + failed_task.identity.sha256
            + '"}'
        ),
    }
    assert pending_view["summary"]["pending"] == 1
    assert pending_view["tasks"][0]["task_id"] == "task-m7-failed"
