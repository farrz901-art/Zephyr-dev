from __future__ import annotations

import json
from pathlib import Path

import httpx

from zephyr_core import RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest.destinations.webhook import DeliveryRetryConfig, WebhookDestination


def _meta() -> RunMetaV1:
    return RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )


def test_webhook_retries_on_5xx_then_succeeds(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        # validate payload
        data = json.loads(request.content.decode("utf-8"))
        assert data["sha256"] == "abc"
        if calls["n"] == 1:
            return httpx.Response(500, text="server error")
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    dest = WebhookDestination(
        url="https://example.test/webhook",
        transport=transport,
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=3, base_backoff_ms=0, max_backoff_ms=0
        ),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=_meta(), result=None)
    assert receipt.ok is True
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert calls["n"] == 2


def test_webhook_does_not_retry_on_4xx(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, text="bad request")

    transport = httpx.MockTransport(handler)
    dest = WebhookDestination(
        url="https://example.test/webhook",
        transport=transport,
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=3, base_backoff_ms=0, max_backoff_ms=0
        ),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=_meta(), result=None)
    assert receipt.ok is False
    assert receipt.details is not None
    assert receipt.details["attempts"] == 1
    assert receipt.details["status_code"] == 400
    assert calls["n"] == 1


def test_webhook_retries_exhausted(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503, text="unavailable")

    transport = httpx.MockTransport(handler)
    dest = WebhookDestination(
        url="https://example.test/webhook",
        transport=transport,
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=2, base_backoff_ms=0, max_backoff_ms=0
        ),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=_meta(), result=None)
    assert receipt.ok is False
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert calls["n"] == 2
