from __future__ import annotations

import json
import logging
import os
import sqlite3
from http.client import HTTPConnection
from pathlib import Path
from typing import Any

import pytest

from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    ZephyrElement,
)
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_ingest import cli
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.health_server import HealthHttpServer, LifecycleHealthProvider
from zephyr_ingest.lock_provider_factory import (
    LocalLockProviderKind,
    build_local_lock_provider,
)
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackendWorkSource
from zephyr_ingest.queue_backend_factory import (
    LocalQueueBackendKind,
    build_local_queue_backend,
)
from zephyr_ingest.runner import RunnerConfig, build_task_execution_handler, run_documents
from zephyr_ingest.sqlite_lock_provider import SqliteLockProvider
from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.tests.test_queue_backend import make_task
from zephyr_ingest.worker_runtime import (
    WORKER_DRAIN_ON_EMPTY_POLICY,
    WORKER_RUNTIME_SCOPE,
    DrainingOnIdleWorkSource,
    WorkerRuntime,
    run_worker,
)


def test_cli_worker_invokes_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: dict[str, object] = {}

    def fake_run_worker(
        *,
        ctx: RunContext,
        poll_interval_ms: int,
        health_host: str | None = None,
        health_port: int | None = None,
    ) -> int:
        called["run_id"] = ctx.run_id
        called["pipeline_version"] = ctx.pipeline_version
        called["timestamp_utc"] = ctx.timestamp_utc
        called["poll_interval_ms"] = poll_interval_ms
        called["health_host"] = health_host
        called["health_port"] = health_port
        return 0

    monkeypatch.setattr(cli, "run_worker", fake_run_worker)

    rc = cli.main(
        [
            "worker",
            "--poll-interval-ms",
            "25",
            "--health-host",
            "127.0.0.1",
            "--health-port",
            "18080",
            "--pipeline-version",
            "p-worker",
            "--run-id",
            "r-worker",
            "--timestamp-utc",
            "2026-04-04T00:00:00Z",
        ]
    )

    assert rc == 0
    assert called["run_id"] == "r-worker"
    assert called["pipeline_version"] == "p-worker"
    assert called["timestamp_utc"] == "2026-04-04T00:00:00Z"
    assert called["poll_interval_ms"] == 25
    assert called["health_host"] == "127.0.0.1"
    assert called["health_port"] == 18080


def test_worker_runtime_draining_stops_new_work_and_emits_events(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    ctx = RunContext.new(
        pipeline_version="p-worker",
        run_id="r-worker",
        timestamp_utc="2026-04-04T00:00:00Z",
    )
    runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=1)
    executed: list[str] = []

    class Source:
        def __init__(self) -> None:
            self.poll_calls = 0

        def poll(self) -> Any:
            self.poll_calls += 1
            if self.poll_calls == 1:

                def first_work() -> None:
                    executed.append("first")
                    runtime.request_draining()

                return first_work
            if self.poll_calls == 2:

                def second_work() -> None:
                    executed.append("second")

                return second_work
            return None

    source = Source()
    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert executed == ["first"]
    assert source.poll_calls == 1

    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("worker_start ") for m in msgs)
    assert any(m.startswith("worker_draining ") for m in msgs)
    assert any(m.startswith("worker_stop ") for m in msgs)


def test_worker_runtime_exposes_bounded_drain_stop_intent_and_runtime_scope() -> None:
    ctx = RunContext.new(
        pipeline_version="p-worker",
        run_id="r-worker",
        timestamp_utc="2026-04-04T00:00:00Z",
    )
    runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=1)

    assert runtime.runtime_scope == WORKER_RUNTIME_SCOPE
    assert runtime.stop_intent == "none"
    assert runtime.accepts_new_work is False

    runtime._phase = WorkerPhase.RUNNING  # noqa: SLF001 - narrow lifecycle surface test # pyright: ignore[reportPrivateUsage]
    assert runtime.accepts_new_work is True
    runtime.request_draining()

    assert runtime.phase == WorkerPhase.DRAINING
    assert runtime.stop_intent == "drain"
    assert runtime.accepts_new_work is False

    text = render_prometheus_text(families=build_worker_prom_families(ctx=ctx, lifecycle=runtime))
    assert (
        'zephyr_ingest_worker_runtime_boundary_info{pipeline_version="p-worker",'
        'runtime_scope="single_process_local_polling",stop_intent="drain",'
        'accepts_new_work="false"} 1.0'
    ) in text


def test_drain_on_empty_policy_only_drains_after_work_has_been_seen(
    tmp_path: Path,
) -> None:
    backend = build_local_queue_backend(kind="sqlite", root=tmp_path / "sqlite")
    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker",
            timestamp_utc="2026-04-04T00:00:00Z",
        ),
        poll_interval_ms=1,
    )
    source = DrainingOnIdleWorkSource(
        runtime=runtime,
        delegate=QueueBackendWorkSource(
            backend=backend,
            handler=lambda task: None,
        ),
    )

    assert source.drain_policy == WORKER_DRAIN_ON_EMPTY_POLICY
    assert source.poll() is None
    assert runtime.stop_intent == "none"

    backend.enqueue(make_task("task-drain-after-work", kind="it"))
    work = source.poll()
    assert work is not None
    work()

    assert source.poll() is None
    assert runtime.stop_intent == "drain"


def test_health_provider_http_endpoints_reflect_lifecycle_state() -> None:
    ctx = RunContext.new(
        pipeline_version="p-worker",
        run_id="r-worker",
        timestamp_utc="2026-04-04T00:00:00Z",
    )
    runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=1)

    with HealthHttpServer(
        provider=LifecycleHealthProvider(lifecycle=runtime),
        host="127.0.0.1",
        port=0,
    ) as server:
        conn = HTTPConnection("127.0.0.1", server.bound_port, timeout=5)
        conn.request("GET", "/healthz")
        healthz = conn.getresponse()
        assert healthz.status == 200
        healthz.read()

        conn.request("GET", "/readyz")
        readyz = conn.getresponse()
        assert readyz.status == 503
        readyz.read()

        conn.request("GET", "/startupz")
        startupz = conn.getresponse()
        assert startupz.status == 503
        startupz.read()
        conn.close()

    runtime._phase = WorkerPhase.RUNNING  # noqa: SLF001 - narrow runtime-state test # pyright: ignore[reportPrivateUsage]
    runtime.request_draining()

    with HealthHttpServer(
        provider=LifecycleHealthProvider(lifecycle=runtime),
        host="127.0.0.1",
        port=0,
    ) as server:
        conn = HTTPConnection("127.0.0.1", server.bound_port, timeout=5)
        conn.request("GET", "/healthz")
        healthz = conn.getresponse()
        assert healthz.status == 200
        healthz.read()

        conn.request("GET", "/readyz")
        readyz = conn.getresponse()
        assert readyz.status == 503
        readyz.read()

        conn.request("GET", "/startupz")
        startupz = conn.getresponse()
        assert startupz.status == 200
        startupz.read()
        conn.close()


def test_health_server_exposes_metrics_alongside_health_endpoints() -> None:
    ctx = RunContext.new(
        pipeline_version="p-worker",
        run_id="r-worker",
        timestamp_utc="2026-04-04T00:00:00Z",
    )
    runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=1)

    with HealthHttpServer(
        provider=LifecycleHealthProvider(lifecycle=runtime),
        host="127.0.0.1",
        port=0,
        metrics_text_provider=lambda: render_prometheus_text(
            families=build_worker_prom_families(ctx=ctx, lifecycle=runtime)
        ),
    ) as server:
        conn = HTTPConnection("127.0.0.1", server.bound_port, timeout=5)

        conn.request("GET", "/metrics")
        metrics = conn.getresponse()
        text = metrics.read().decode("utf-8")
        assert metrics.status == 200
        assert metrics.getheader("Content-Type") == "text/plain; version=0.0.4; charset=utf-8"
        assert "# HELP zephyr_ingest_worker_info" in text
        assert 'zephyr_ingest_worker_info{pipeline_version="p-worker"} 1.0' in text
        assert (
            'zephyr_ingest_worker_phase{pipeline_version="p-worker",phase="starting"} 1.0' in text
        )

        conn.request("GET", "/healthz")
        healthz = conn.getresponse()
        healthz_body = healthz.read().decode("utf-8")
        assert healthz.status == 200
        assert healthz.getheader("Content-Type") == "application/json; charset=utf-8"
        assert '"kind": "liveness"' in healthz_body
        conn.close()


def test_run_worker_executes_spool_queue_backend_when_selected(tmp_path: Path) -> None:
    backend = build_local_queue_backend(kind="spool", root=tmp_path / "spool")
    backend.enqueue(make_task("task-worker-spool"))
    handled: list[str] = []

    rc = run_worker(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker-spool",
            timestamp_utc="2026-04-04T00:00:00Z",
        ),
        poll_interval_ms=1,
        queue_backend=backend,
        task_handler=lambda task: handled.append(task.task_id),
        drain_on_empty=True,
        sleep_fn=lambda _: None,
    )

    assert rc == 0
    assert handled == ["task-worker-spool"]


def test_run_worker_executes_sqlite_queue_backend_when_selected(tmp_path: Path) -> None:
    backend = build_local_queue_backend(kind="sqlite", root=tmp_path / "sqlite")
    backend.enqueue(make_task("task-worker-sqlite", kind="it"))
    handled: list[str] = []

    rc = run_worker(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id="r-worker-sqlite",
            timestamp_utc="2026-04-04T00:00:00Z",
        ),
        poll_interval_ms=1,
        queue_backend=backend,
        task_handler=lambda task: handled.append(task.task_id),
        drain_on_empty=True,
        sleep_fn=lambda _: None,
    )

    assert rc == 0
    assert handled == ["task-worker-sqlite"]


def test_run_worker_rejects_partial_queue_backend_wiring(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="provided together"):
        run_worker(
            ctx=RunContext.new(
                pipeline_version="p-worker",
                run_id="r-worker-invalid",
                timestamp_utc="2026-04-04T00:00:00Z",
            ),
            poll_interval_ms=1,
            queue_backend=build_local_queue_backend(kind="sqlite", root=tmp_path / "sqlite"),
            sleep_fn=lambda _: None,
        )


def test_run_worker_uses_shared_task_processor_delivery_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import zephyr_ingest.runner as runner_mod

    task = make_task("task-shared-chain", kind="it")
    backend = build_local_queue_backend(kind="sqlite", root=tmp_path / "sqlite")
    backend.enqueue(task)
    ctx = RunContext.new(
        pipeline_version="p-worker",
        run_id="r-worker-shared-chain",
        timestamp_utc="2026-04-06T00:00:00Z",
    )
    cfg = RunnerConfig(
        out_root=tmp_path / "out",
        destination=FilesystemDestination(),
    )
    captured: dict[str, object] = {}

    class RecordingProcessor:
        def process(
            self,
            *,
            doc: DocumentRef,
            strategy: PartitionStrategy,
            unique_element_ids: bool,
            run_id: str | None,
            pipeline_version: str | None,
            sha256: str,
        ) -> PartitionResult:
            captured["doc"] = doc
            captured["strategy"] = strategy
            captured["unique_element_ids"] = unique_element_ids
            captured["run_id"] = run_id
            captured["pipeline_version"] = pipeline_version
            captured["sha256"] = sha256
            return PartitionResult(
                document=DocumentMetadata(
                    filename=task.inputs.document.filename,
                    mime_type="application/json",
                    sha256=sha256,
                    size_bytes=task.inputs.document.size_bytes,
                    created_at_utc="2026-04-06T00:00:00Z",
                ),
                engine=EngineInfo(
                    name="recording-processor",
                    backend="test",
                    version="0",
                    strategy=strategy,
                ),
                elements=[
                    ZephyrElement(
                        element_id="e1",
                        type="Text",
                        text="worker-task",
                        metadata={"flow_kind": "it"},
                    )
                ],
                normalized_text="worker-task",
                warnings=[],
            )

    def fake_build_processor_for_flow_kind(*, flow_kind: str, backend: object | None) -> object:
        captured["flow_kind"] = flow_kind
        captured["backend"] = backend
        return RecordingProcessor()

    monkeypatch.setattr(
        runner_mod,
        "build_processor_for_flow_kind",
        fake_build_processor_for_flow_kind,
    )

    handler = build_task_execution_handler(cfg=cfg, ctx=ctx)

    rc = run_worker(
        ctx=ctx,
        poll_interval_ms=1,
        queue_backend=backend,
        task_handler=handler,
        drain_on_empty=True,
        sleep_fn=lambda _: None,
    )

    task_sha = task.identity.sha256 if task.identity is not None else None
    assert rc == 0
    assert captured["flow_kind"] == "it"
    assert captured["backend"] is None
    assert captured["run_id"] == "r-worker-shared-chain"
    assert captured["pipeline_version"] == "p-worker"
    assert captured["strategy"] == PartitionStrategy.AUTO
    assert captured["unique_element_ids"] is True
    assert captured["sha256"] == task_sha
    assert task_sha is not None
    assert (cfg.out_root / task_sha / "run_meta.json").exists()
    assert (cfg.out_root / task_sha / "delivery_receipt.json").exists()
    run_meta = json.loads((cfg.out_root / task_sha / "run_meta.json").read_text(encoding="utf-8"))
    assert run_meta["provenance"] == {
        "run_origin": "intake",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-shared-chain",
        "task_identity_key": (
            '{"kind":"it","pipeline_version":"p-worker","sha256":"sha-task-shared-chain"}'
        ),
    }


def test_batch_and_worker_paths_share_primary_execution_facts_for_same_uns_task(
    tmp_path: Path,
) -> None:
    doc_path = tmp_path / "same-doc.txt"
    doc_path.write_text("same-doc", encoding="utf-8")
    doc = DocumentRef(
        uri=str(doc_path),
        source="local_file",
        discovered_at_utc="2026-04-06T00:00:00Z",
        filename="same-doc.txt",
        extension=".txt",
        size_bytes=doc_path.stat().st_size,
    )
    batch_out = tmp_path / "batch-out"
    worker_out = tmp_path / "worker-out"
    batch_ctx = RunContext.new(
        pipeline_version="p-shared",
        run_id="r-batch-shared",
        timestamp_utc="2026-04-06T00:00:00Z",
    )
    worker_ctx = RunContext.new(
        pipeline_version="p-shared",
        run_id="r-worker-shared",
        timestamp_utc="2026-04-06T00:00:00Z",
    )
    processor_calls: list[tuple[str | None, str | None, str]] = []

    class RecordingProcessor:
        def process(
            self,
            *,
            doc: DocumentRef,
            strategy: PartitionStrategy,
            unique_element_ids: bool,
            run_id: str | None,
            pipeline_version: str | None,
            sha256: str,
        ) -> PartitionResult:
            processor_calls.append((run_id, pipeline_version, sha256))
            return PartitionResult(
                document=DocumentMetadata(
                    filename=doc.filename,
                    mime_type="text/plain",
                    sha256=sha256,
                    size_bytes=doc.size_bytes,
                    created_at_utc="2026-04-06T00:00:00Z",
                ),
                engine=EngineInfo(
                    name="recording-processor",
                    backend="test",
                    version="0",
                    strategy=strategy,
                ),
                elements=[
                    ZephyrElement(
                        element_id="e1",
                        type="Text",
                        text="same-doc",
                        metadata={"flow_kind": "uns", "unique_element_ids": unique_element_ids},
                    )
                ],
                normalized_text="same-doc",
                warnings=[],
            )

    batch_cfg = RunnerConfig(out_root=batch_out, destination=FilesystemDestination())
    worker_cfg = RunnerConfig(out_root=worker_out, destination=FilesystemDestination())
    run_documents(
        docs=[doc],
        cfg=batch_cfg,
        ctx=batch_ctx,
        flow_kind="uns",
        processor=RecordingProcessor(),
    )

    batch_run_meta_paths = list(batch_out.glob("*/run_meta.json"))
    assert len(batch_run_meta_paths) == 1
    batch_run_meta = json.loads(batch_run_meta_paths[0].read_text(encoding="utf-8"))

    task = TaskV1(
        task_id="task-same-doc",
        kind="uns",
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
        execution=TaskExecutionV1(
            strategy=PartitionStrategy.AUTO,
            unique_element_ids=True,
        ),
        identity=TaskIdentityV1(
            pipeline_version="p-shared",
            sha256=batch_run_meta["document"]["sha256"],
        ),
    )
    queue_backend = build_local_queue_backend(kind="sqlite", root=tmp_path / "sqlite-queue")
    queue_backend.enqueue(task)

    rc = run_worker(
        ctx=worker_ctx,
        poll_interval_ms=1,
        queue_backend=queue_backend,
        task_handler=build_task_execution_handler(
            cfg=worker_cfg,
            ctx=worker_ctx,
            flow_kind="uns",
            processor=RecordingProcessor(),
        ),
        drain_on_empty=True,
        sleep_fn=lambda _: None,
    )

    assert rc == 0
    worker_run_meta_paths = list(worker_out.glob("*/run_meta.json"))
    assert len(worker_run_meta_paths) == 1
    worker_run_meta = json.loads(worker_run_meta_paths[0].read_text(encoding="utf-8"))

    assert len(processor_calls) == 2
    assert batch_run_meta["document"]["sha256"] == worker_run_meta["document"]["sha256"]
    assert batch_run_meta["provenance"] == {
        "run_origin": "intake",
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "task_id": batch_run_meta["document"]["sha256"],
        "task_identity_key": (
            '{"kind":"uns","pipeline_version":"p-shared","sha256":"'
            + batch_run_meta["document"]["sha256"]
            + '"}'
        ),
    }
    assert worker_run_meta["provenance"] == {
        "run_origin": "intake",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-same-doc",
        "task_identity_key": (
            '{"kind":"uns","pipeline_version":"p-shared","sha256":"'
            + batch_run_meta["document"]["sha256"]
            + '"}'
        ),
    }


@pytest.mark.parametrize(
    ("queue_kind", "lock_kind"),
    [
        ("spool", "file"),
        ("sqlite", "sqlite"),
    ],
)
def test_runtime_supports_supported_queue_and_lock_backend_pairs(
    tmp_path: Path,
    queue_kind: LocalQueueBackendKind,
    lock_kind: LocalLockProviderKind,
) -> None:
    queue_backend = build_local_queue_backend(
        kind=queue_kind,
        root=tmp_path / f"{queue_kind}-queue",
    )
    lock_provider = build_local_lock_provider(
        kind=lock_kind,
        root=tmp_path / f"{lock_kind}-locks",
        stale_after_s=10,
    )
    queue_backend.enqueue(make_task(f"task-{queue_kind}-{lock_kind}", kind="it"))
    handled: list[str] = []

    runtime = WorkerRuntime(
        ctx=RunContext.new(
            pipeline_version="p-worker",
            run_id=f"r-{queue_kind}-{lock_kind}",
            timestamp_utc="2026-04-04T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    def handle_task(task: Any) -> None:
        handled.append(task.task_id)
        runtime.request_draining()

    source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=handle_task,
        lock_provider=lock_provider,
        lock_owner="worker-pair",
    )

    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert handled == [f"task-{queue_kind}-{lock_kind}"]

    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_lock_stale_recoveries_total{pipeline_version="p-worker"} 0.0' in text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="done"} 1.0' in text


@pytest.mark.parametrize("lock_kind", ["file", "sqlite"])
def test_runtime_supports_stale_lock_recovery_for_supported_lock_backends(
    tmp_path: Path,
    lock_kind: LocalLockProviderKind,
) -> None:
    queue_backend = build_local_queue_backend(
        kind="sqlite",
        root=tmp_path / "sqlite-queue",
    )
    task = make_task(f"task-stale-{lock_kind}", kind="it")
    queue_backend.enqueue(task)
    lock_provider = build_local_lock_provider(
        kind=lock_kind,
        root=tmp_path / f"{lock_kind}-locks",
        stale_after_s=10,
    )
    lock_key = normalize_task_idempotency_key(task)
    stale_lock = lock_provider.acquire(key=lock_key, owner="worker-stale")
    assert stale_lock is not None
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
            run_id=f"r-stale-{lock_kind}",
            timestamp_utc="2026-04-04T00:00:00Z",
        ),
        poll_interval_ms=1,
    )

    def handle_task(task: Any) -> None:
        handled.append(task.task_id)
        runtime.request_draining()

    source = QueueBackendWorkSource(
        backend=queue_backend,
        handler=handle_task,
        lock_provider=lock_provider,
        lock_owner="worker-pair",
    )

    rc = runtime.run(work_source=source, sleep_fn=lambda _: None)

    assert rc == 0
    assert runtime.phase == WorkerPhase.STOPPED
    assert handled == [f"task-stale-{lock_kind}"]

    text = render_prometheus_text(
        families=build_worker_prom_families(ctx=runtime.ctx, lifecycle=runtime, work_source=source)
    )
    assert 'zephyr_ingest_lock_stale_recoveries_total{pipeline_version="p-worker"} 1.0' in text
    assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="done"} 1.0' in text
