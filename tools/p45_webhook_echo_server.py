from __future__ import annotations

import hashlib
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import urlparse

_STATE_LOCK = Lock()
_REQUESTS: list[dict[str, object]] = []
_SCENARIO_ATTEMPTS: dict[str, int] = {}

class WebhookEchoHandler(BaseHTTPRequestHandler):
    server_version = "ZephyrP45WebhookEcho/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._write_json(HTTPStatus.OK, {"ok": True, "service": "webhook-echo"})
            return
        if parsed.path == "/_events":
            with _STATE_LOCK:
                items = list(_REQUESTS)
            self._write_json(
                HTTPStatus.OK,
                {"ok": True, "count": len(items), "items": items},
            )
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"ok": False, "path": parsed.path})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/_reset":
            with _STATE_LOCK:
                _REQUESTS.clear()
                _SCENARIO_ATTEMPTS.clear()
            self._write_json(HTTPStatus.OK, {"ok": True, "reset": True})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        idempotency_key = self.headers.get("Idempotency-Key")
        payload_obj: object
        try:
            payload_obj = json.loads(body.decode("utf-8")) if body else None
        except Exception:
            payload_obj = None
        with _STATE_LOCK:
            request_index = len(_REQUESTS) + 1
            _REQUESTS.append(
                {
                    "request_index": request_index,
                    "path": parsed.path,
                    "idempotency_key": idempotency_key,
                    "content_length": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "payload": payload_obj,
                }
            )
        status = self._response_status(parsed.path, idempotency_key=idempotency_key)
        payload = {
            "ok": int(status) < 400,
            "path": parsed.path,
            "content_length": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "idempotency_key": idempotency_key,
            "request_index": request_index,
            "status_code": int(status),
        }
        extra_headers: dict[str, str] = {}
        if status == HTTPStatus.TOO_MANY_REQUESTS:
            extra_headers["Retry-After"] = "1"
        self._write_json(status, payload, headers=extra_headers)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _response_status(self, path: str, *, idempotency_key: str | None) -> HTTPStatus:
        if path.startswith("/status/"):
            code = int(path.rsplit("/", 1)[-1])
            return HTTPStatus(code)
        if path.startswith("/flaky-once/"):
            code = int(path.rsplit("/", 1)[-1])
            scenario_key = f"{path}|{idempotency_key or 'no-idempotency-key'}"
            with _STATE_LOCK:
                attempt = _SCENARIO_ATTEMPTS.get(scenario_key, 0) + 1
                _SCENARIO_ATTEMPTS[scenario_key] = attempt
            if attempt == 1:
                return HTTPStatus(code)
            return HTTPStatus.OK
        return HTTPStatus.OK

    def _write_json(
        self,
        status: HTTPStatus,
        payload: object,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if headers is not None:
            for name, value in headers.items():
                self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    host = os.environ.get(
        "ZEPHYR_P45_WEBHOOK_ECHO_BIND",
        os.environ.get("ZEPHYR_P45_WEBHOOK_ECHO_HOST", "127.0.0.1"),
    )
    port = int(
        os.environ.get(
            "ZEPHYR_P45_WEBHOOK_ECHO_LISTEN_PORT",
            os.environ.get("ZEPHYR_P45_WEBHOOK_ECHO_PORT", "18082"),
        )
    )
    with ThreadingHTTPServer((host, port), WebhookEchoHandler) as server:
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
