from __future__ import annotations

import logging
from http.client import HTTPConnection
from pathlib import Path
from typing import Any

import pytest

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import WorkerPhase
from zephyr_ingest import cli
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
from zephyr_ingest.tests.test_queue_backend import make_task
from zephyr_ingest.worker_runtime import WorkerRuntime, run_worker


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
    if queue_kind == "spool":
        assert 'zephyr_ingest_queue_tasks{pipeline_version="p-worker",bucket="done"} 1.0' in text
    else:
        assert "zephyr_ingest_queue_tasks" not in text
