from __future__ import annotations

import json
from pathlib import Path

import httpx

from zephyr_core.contracts.v1.run_meta import ErrorInfoV1, MetricsV1, RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.replay_delivery import replay_delivery_dlq


def _make_failed_dlq(out_root: Path) -> Path:
    meta = RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-23T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=None,
        metrics=MetricsV1(duration_ms=10, attempts=1),
        error=ErrorInfoV1(
            code="ZE-UNS-PARTITION-FAILED", message="fail", details={"retryable": True}
        ),
    )
    receipt = DeliveryReceipt(destination="webhook", ok=False, details={"status_code": 500})
    return write_delivery_dlq(out_root=out_root, sha256="abc", meta=meta, receipt=receipt)


def test_replay_delivery_moves_dlq_to_done(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    out_root.mkdir(parents=True, exist_ok=True)

    dlq_path = _make_failed_dlq(out_root)
    assert dlq_path.exists()

    # Mock webhook to return 200 OK
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Idempotency-Key"] == "abc:r1"
        data = json.loads(request.content.decode("utf-8"))
        assert data["sha256"] == "abc"
        assert "run_meta" in data
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)

    stats = replay_delivery_dlq(
        out_root=out_root,
        webhook_url="https://example.test/hook",
        timeout_s=1.0,
        transport=transport,
        limit=None,
        dry_run=False,
        move_done=True,
    )

    assert stats.total == 1
    assert stats.attempted == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    assert stats.moved_to_done == 1

    done_dir = out_root / "_dlq" / "delivery_done"
    assert done_dir.exists()
    assert (done_dir / dlq_path.name).exists()
    assert not dlq_path.exists()


def test_replay_delivery_dry_run_does_not_move(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    out_root.mkdir(parents=True, exist_ok=True)

    dlq_path = _make_failed_dlq(out_root)
    assert dlq_path.exists()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)

    stats = replay_delivery_dlq(
        out_root=out_root,
        webhook_url="https://example.test/hook",
        timeout_s=1.0,
        transport=transport,
        limit=None,
        dry_run=True,
        move_done=True,
    )

    assert stats.total == 1
    assert stats.attempted == 1
    assert stats.succeeded == 0
    assert stats.failed == 0
    assert stats.moved_to_done == 0

    # Still in DLQ, not moved
    assert dlq_path.exists()
    assert not (out_root / "_dlq" / "delivery_done" / dlq_path.name).exists()
