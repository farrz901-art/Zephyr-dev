from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Final

from zephyr_core.contracts.v2.healthz import (
    HealthCheckKind,
    HealthCheckProvider,
    HealthCheckResult,
)
from zephyr_core.contracts.v2.lifecycle import Lifecycle, WorkerPhase

_PATH_TO_KIND: Final[dict[str, HealthCheckKind]] = {
    "/healthz": HealthCheckKind.LIVENESS,
    "/readyz": HealthCheckKind.READINESS,
    "/startupz": HealthCheckKind.STARTUP,
}


@dataclass(frozen=True, slots=True)
class LifecycleHealthProvider(HealthCheckProvider):
    lifecycle: Lifecycle

    def check(self, kind: HealthCheckKind) -> HealthCheckResult:
        phase = self.lifecycle.phase

        if kind == HealthCheckKind.LIVENESS:
            healthy = phase not in (WorkerPhase.FAILED, WorkerPhase.STOPPED)
        elif kind == HealthCheckKind.READINESS:
            healthy = phase == WorkerPhase.RUNNING
        else:
            healthy = phase not in (WorkerPhase.STARTING, WorkerPhase.FAILED, WorkerPhase.STOPPED)

        reason: str | None = None
        if not healthy:
            reason = f"phase={phase.value}"

        return HealthCheckResult(
            kind=kind,
            healthy=healthy,
            reason=reason,
            details={"phase": phase.value},
        )


def _make_handler(
    provider: HealthCheckProvider,
    metrics_text_provider: Callable[[], str] | None,
) -> type[BaseHTTPRequestHandler]:
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/metrics":
                if metrics_text_provider is None:
                    self.send_error(404)
                    return

                payload = metrics_text_provider().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            kind = _PATH_TO_KIND.get(self.path)
            if kind is None:
                self.send_error(404)
                return

            result = provider.check(kind)
            payload = json.dumps(asdict(result), ensure_ascii=False, default=str).encode("utf-8")
            status = 200 if result.healthy else 503

            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            return

    return HealthHandler


@dataclass(slots=True)
class HealthHttpServer:
    provider: HealthCheckProvider
    host: str
    port: int
    metrics_text_provider: Callable[[], str] | None = None
    _server: ThreadingHTTPServer | None = None
    _thread: threading.Thread | None = None

    @property
    def bound_port(self) -> int:
        if self._server is None:
            raise RuntimeError("health server is not started")
        return int(self._server.server_address[1])

    def start(self) -> None:
        if self._server is not None:
            return
        server = ThreadingHTTPServer(
            (self.host, self.port),
            _make_handler(self.provider, self.metrics_text_provider),
        )
        thread = threading.Thread(target=server.serve_forever, name="zephyr-healthz", daemon=True)
        thread.start()
        self._server = server
        self._thread = thread

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._server = None
        self._thread = None

    def __enter__(self) -> "HealthHttpServer":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()
