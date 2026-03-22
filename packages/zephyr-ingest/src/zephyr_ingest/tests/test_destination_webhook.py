from __future__ import annotations

import json
from pathlib import Path

import httpx

from zephyr_core import RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest.destinations.webhook import WebhookDestination


def test_webhook_destination_posts_run_meta(tmp_path: Path) -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["idempotency"] = request.headers.get("Idempotency-Key", "")
        data = json.loads(request.content.decode("utf-8"))
        assert data["sha256"] == "abc"
        assert "run_meta" in data
        assert data["run_meta"]["schema_version"] == RUN_META_SCHEMA_VERSION
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)

    meta = RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )

    dest = WebhookDestination(url="https://example.test/webhook", transport=transport)

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=meta, result=None)
    assert receipt.ok is True
    assert captured["idempotency"] == "abc:r1"
