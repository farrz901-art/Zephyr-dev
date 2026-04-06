from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Protocol

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import Lifecycle, WorkerPhase
from zephyr_ingest.health_server import HealthHttpServer, LifecycleHealthProvider
from zephyr_ingest.obs.events import log_event
from zephyr_ingest.obs.prom_export import build_worker_prom_families, render_prometheus_text

logger = logging.getLogger(__name__)


class WorkItem(Protocol):
    def __call__(self) -> None: ...


class WorkSource(Protocol):
    def poll(self) -> WorkItem | None: ...


class IdleWorkSource:
    def poll(self) -> WorkItem | None:
        return None


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
    health_host: str | None = None,
    health_port: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    runtime = WorkerRuntime(ctx=ctx, poll_interval_ms=poll_interval_ms)
    source = IdleWorkSource() if work_source is None else work_source

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
