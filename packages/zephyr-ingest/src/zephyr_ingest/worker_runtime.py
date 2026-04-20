from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Literal, Protocol

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import Lifecycle, WorkerPhase
from zephyr_ingest.health_server import HealthHttpServer, LifecycleHealthProvider
from zephyr_ingest.obs.events import log_event
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text
from zephyr_ingest.queue_backend import QueueBackend, QueueBackendWorkSource, TaskHandler

logger = logging.getLogger(__name__)
WorkerStopIntent = Literal["none", "drain", "stop"]
WORKER_RUNTIME_SCOPE = "single_process_local_polling"
WORKER_DRAIN_ON_EMPTY_POLICY = "after_first_work_then_idle"


class WorkItem(Protocol):
    def __call__(self) -> None: ...


class WorkSource(Protocol):
    def poll(self) -> WorkItem | None: ...


class IdleWorkSource:
    def poll(self) -> WorkItem | None:
        return None


@dataclass(slots=True)
class DrainingOnIdleWorkSource:
    runtime: "WorkerRuntime"
    delegate: WorkSource
    _saw_work: bool = field(default=False, init=False)

    @property
    def drain_policy(self) -> str:
        return WORKER_DRAIN_ON_EMPTY_POLICY

    def poll(self) -> WorkItem | None:
        work = self.delegate.poll()
        if work is None:
            if self._saw_work:
                self.runtime.request_draining()
            return None
        self._saw_work = True
        return work


@dataclass(slots=True)
class WorkerRuntime(Lifecycle):
    ctx: RunContext
    poll_interval_ms: int
    _phase: WorkerPhase = field(default=WorkerPhase.STARTING, init=False)
    _draining_requested: bool = field(default=False, init=False)
    _stopping_requested: bool = field(default=False, init=False)
    _inflight: int = field(default=0, init=False)

    @property
    def phase(self) -> WorkerPhase:
        return self._phase

    @property
    def runtime_scope(self) -> str:
        return WORKER_RUNTIME_SCOPE

    @property
    def stop_intent(self) -> WorkerStopIntent:
        if self._stopping_requested:
            return "stop"
        if self._draining_requested:
            return "drain"
        return "none"

    @property
    def accepts_new_work(self) -> bool:
        return self.phase == WorkerPhase.RUNNING and self.stop_intent == "none"

    def request_draining(self) -> None:
        if self._phase in (WorkerPhase.STOPPED, WorkerPhase.FAILED):
            return
        self._draining_requested = True
        if self._phase != WorkerPhase.STARTING:
            self._phase = WorkerPhase.DRAINING

    def request_stopping(self) -> None:
        if self._phase in (WorkerPhase.STOPPED, WorkerPhase.FAILED):
            return
        self._stopping_requested = True
        if self._phase != WorkerPhase.STARTING:
            self._phase = WorkerPhase.STOPPING

    def run(
        self,
        *,
        work_source: WorkSource,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> int:
        log_event(
            logger,
            level=logging.INFO,
            event="worker_start",
            run_id=self.ctx.run_id,
            pipeline_version=self.ctx.pipeline_version,
            phase=str(self.phase),
            poll_interval_ms=self.poll_interval_ms,
            runtime_scope=self.runtime_scope,
        )
        self._phase = WorkerPhase.RUNNING
        draining_logged = False

        while True:
            if self._stopping_requested:
                self._phase = WorkerPhase.STOPPING
            elif self._draining_requested:
                self._phase = WorkerPhase.DRAINING
                if not draining_logged:
                    log_event(
                        logger,
                        level=logging.INFO,
                        event="worker_draining",
                        run_id=self.ctx.run_id,
                        pipeline_version=self.ctx.pipeline_version,
                        phase=str(self.phase),
                        stop_intent=self.stop_intent,
                        accepts_new_work=self.accepts_new_work,
                        runtime_scope=self.runtime_scope,
                    )
                    draining_logged = True
            else:
                self._phase = WorkerPhase.RUNNING

            if self._phase in (WorkerPhase.DRAINING, WorkerPhase.STOPPING) and self._inflight == 0:
                self._phase = WorkerPhase.STOPPED
                log_event(
                    logger,
                    level=logging.INFO,
                    event="worker_stop",
                    run_id=self.ctx.run_id,
                    pipeline_version=self.ctx.pipeline_version,
                    phase=str(self.phase),
                    stop_intent=self.stop_intent,
                    accepts_new_work=self.accepts_new_work,
                    runtime_scope=self.runtime_scope,
                )
                return 0

            if self._phase == WorkerPhase.RUNNING:
                try:
                    work = work_source.poll()
                except Exception:
                    self._phase = WorkerPhase.FAILED
                    log_event(
                        logger,
                        level=logging.ERROR,
                        event="worker_stop",
                        run_id=self.ctx.run_id,
                        pipeline_version=self.ctx.pipeline_version,
                        phase=str(self.phase),
                        stop_intent=self.stop_intent,
                        runtime_scope=self.runtime_scope,
                    )
                    raise

                if work is None:
                    sleep_fn(self.poll_interval_ms / 1000.0)
                    continue

                self._inflight += 1
                try:
                    work()
                except Exception:
                    self._phase = WorkerPhase.FAILED
                    log_event(
                        logger,
                        level=logging.ERROR,
                        event="worker_stop",
                        run_id=self.ctx.run_id,
                        pipeline_version=self.ctx.pipeline_version,
                        phase=str(self.phase),
                        stop_intent=self.stop_intent,
                        runtime_scope=self.runtime_scope,
                    )
                    raise
                finally:
                    self._inflight -= 1
                continue

            sleep_fn(self.poll_interval_ms / 1000.0)


def run_worker(
    *,
    ctx: RunContext,
    poll_interval_ms: int,
    work_source: WorkSource | None = None,
    queue_backend: QueueBackend | None = None,
    task_handler: TaskHandler | None = None,
    drain_on_empty: bool = False,
    health_host: str | None = None,
    health_port: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=poll_interval_ms)
    if work_source is not None and (queue_backend is not None or task_handler is not None):
        raise ValueError("work_source cannot be combined with queue_backend/task_handler")
    if (queue_backend is None) != (task_handler is None):
        raise ValueError("queue_backend and task_handler must be provided together")

    if work_source is not None:
        source = work_source
    elif queue_backend is not None and task_handler is not None:
        queue_source: WorkSource = QueueBackendWorkSource(
            backend=queue_backend,
            handler=task_handler,
        )
        if drain_on_empty:
            source = DrainingOnIdleWorkSource(runtime=runtime, delegate=queue_source)
        else:
            source = queue_source
    else:
        source = IdleWorkSource()

    if health_host is not None and health_port is not None and health_port > 0:
        with HealthHttpServer(
            provider=LifecycleHealthProvider(lifecycle=runtime),
            host=health_host,
            port=health_port,
            metrics_text_provider=lambda: render_prometheus_text(
                families=build_worker_prom_families(ctx=ctx, lifecycle=runtime, work_source=source)
            ),
        ):
            return runtime.run(work_source=source, sleep_fn=sleep_fn)

    return runtime.run(work_source=source, sleep_fn=sleep_fn)
