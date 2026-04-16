from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class FixtureHandler(BaseHTTPRequestHandler):
    server_version = "ZephyrP45HttpFixture/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._write_json(HTTPStatus.OK, {"ok": True, "service": "http-fixture"})
            return
        if parsed.path == "/documents/plain":
            body = b"Zephyr P4.5 fixture document.\n"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/records/cursor":
            query = parse_qs(parsed.query)
            cursor = query.get("cursor", ["start"])[0]
            if cursor == "start":
                payload = {
                    "records": [{"id": 1, "kind": "fixture"}],
                    "next_cursor": "page-2",
                }
            else:
                payload = {
                    "records": [{"id": 2, "kind": "fixture"}],
                    "next_cursor": None,
                }
            self._write_json(HTTPStatus.OK, payload)
            return
        if parsed.path == "/records/cursor-empty":
            query = parse_qs(parsed.query)
            cursor = query.get("cursor", ["start"])[0]
            if cursor == "start":
                payload = {"records": [], "next_cursor": "page-2"}
            elif cursor == "page-2":
                payload = {"records": [], "next_cursor": "page-3"}
            else:
                payload = {"records": [], "next_cursor": None}
            self._write_json(HTTPStatus.OK, payload)
            return
        if parsed.path == "/records/malformed":
            body = b"{not-json"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path.startswith("/status/"):
            raw_code = parsed.path.rsplit("/", 1)[-1]
            if raw_code.isdigit():
                status = int(raw_code)
                self._write_json(
                    HTTPStatus(status),
                    {"ok": False, "status": status, "path": parsed.path},
                )
                return
        self._write_json(HTTPStatus.NOT_FOUND, {"ok": False, "path": parsed.path})

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
        "ZEPHYR_P45_HTTP_FIXTURE_BIND",
        os.environ.get("ZEPHYR_P45_HTTP_FIXTURE_HOST", "127.0.0.1"),
    )
    port = int(
        os.environ.get(
            "ZEPHYR_P45_HTTP_FIXTURE_LISTEN_PORT",
            os.environ.get("ZEPHYR_P45_HTTP_FIXTURE_PORT", "18081"),
        )
    )
    with ThreadingHTTPServer((host, port), FixtureHandler) as server:
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
