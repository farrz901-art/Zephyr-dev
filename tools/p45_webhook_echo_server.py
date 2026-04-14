from __future__ import annotations

import hashlib
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class WebhookEchoHandler(BaseHTTPRequestHandler):
    server_version = "ZephyrP45WebhookEcho/1.0"

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._write_json(HTTPStatus.OK, {"ok": True, "service": "webhook-echo"})
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"ok": False, "path": self.path})

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        payload = {
            "ok": True,
            "path": self.path,
            "content_length": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        }
        self._write_json(HTTPStatus.OK, payload)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_json(self, status: HTTPStatus, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
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
