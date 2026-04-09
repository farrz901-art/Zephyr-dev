from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from zephyr_core import RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest.destinations import webhook as webhook_module
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
        assert request.headers["Idempotency-Key"] == "abc:r1"
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
    assert receipt.failure_retryability == "not_failed"
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert receipt.details["retryable"] is False
    assert calls["n"] == 2


def test_webhook_does_not_retry_on_4xx(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        assert request.headers["Idempotency-Key"] == "abc:r1"
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
    assert receipt.failure_retryability == "non_retryable"
    assert receipt.shared_failure_kind == "client_error"
    assert receipt.shared_error_code == "ZE-DELIVERY-HTTP-FAILED"
    assert receipt.shared_summary == {
        "delivery_outcome": "failed",
        "failure_retryability": "non_retryable",
        "failure_kind": "client_error",
        "error_code": "ZE-DELIVERY-HTTP-FAILED",
        "attempt_count": 1,
        "payload_count": 1,
    }
    assert receipt.details is not None
    assert receipt.details["attempts"] == 1
    assert receipt.details["status_code"] == 400
    assert receipt.details["retryable"] is False
    assert receipt.details["failure_kind"] == "client_error"
    assert calls["n"] == 1


def test_webhook_retries_exhausted(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        assert request.headers["Idempotency-Key"] == "abc:r1"
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
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "server_error"
    assert receipt.shared_error_code == "ZE-DELIVERY-HTTP-FAILED"
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert receipt.details["retryable"] is True
    assert receipt.details["failure_kind"] == "server_error"
    assert calls["n"] == 2


def test_webhook_retries_on_429_and_honors_retry_after(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, text="slow down", headers={"Retry-After": "2"})
        return httpx.Response(200, json={"ok": True})

    def fake_sleep(delay_s: float) -> None:
        sleep_calls.append(delay_s)

    monkeypatch.setattr(webhook_module.time, "sleep", fake_sleep)

    transport = httpx.MockTransport(handler)
    dest = WebhookDestination(
        url="https://example.test/webhook",
        transport=transport,
        rate_limit=5.0,
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=3, base_backoff_ms=0, max_backoff_ms=0
        ),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=_meta(), result=None)

    assert receipt.ok is True
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert calls["n"] == 2
    assert sleep_calls == [2.0]


def test_webhook_uses_rate_limit_floor_for_429_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, text="slow down")
        return httpx.Response(200, json={"ok": True})

    def fake_sleep(delay_s: float) -> None:
        sleep_calls.append(delay_s)

    monkeypatch.setattr(webhook_module.time, "sleep", fake_sleep)

    transport = httpx.MockTransport(handler)
    dest = WebhookDestination(
        url="https://example.test/webhook",
        transport=transport,
        rate_limit=2.0,
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=3, base_backoff_ms=0, max_backoff_ms=0
        ),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=_meta(), result=None)

    assert receipt.ok is True
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert calls["n"] == 2
    assert sleep_calls == [0.5]


def test_webhook_retries_timeout_failures_and_marks_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ReadTimeout("timed out", request=request)

    def fake_sleep(delay_s: float) -> None:
        sleep_calls.append(delay_s)

    monkeypatch.setattr(webhook_module.time, "sleep", fake_sleep)

    transport = httpx.MockTransport(handler)
    dest = WebhookDestination(
        url="https://example.test/webhook",
        transport=transport,
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=2, base_backoff_ms=10, max_backoff_ms=10
        ),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "timeout"
    assert receipt.shared_error_code == "ZE-DELIVERY-HTTP-FAILED"
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert receipt.details["exc_type"] == "ReadTimeout"
    assert receipt.details["retryable"] is True
    assert receipt.details["failure_kind"] == "timeout"
    assert calls["n"] == 2
    assert sleep_calls == [0.01]


def test_webhook_retries_connection_failures_and_marks_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("connection failed", request=request)

    def fake_sleep(delay_s: float) -> None:
        sleep_calls.append(delay_s)

    monkeypatch.setattr(webhook_module.time, "sleep", fake_sleep)

    transport = httpx.MockTransport(handler)
    dest = WebhookDestination(
        url="https://example.test/webhook",
        transport=transport,
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=2, base_backoff_ms=5, max_backoff_ms=5
        ),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "connection"
    assert receipt.shared_error_code == "ZE-DELIVERY-HTTP-FAILED"
    assert receipt.details is not None
    assert receipt.details["attempts"] == 2
    assert receipt.details["exc_type"] == "ConnectError"
    assert receipt.details["retryable"] is True
    assert receipt.details["failure_kind"] == "connection"
    assert calls["n"] == 2
    assert sleep_calls == [0.005]
