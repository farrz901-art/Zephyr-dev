from __future__ import annotations

import json
from pathlib import Path

import httpx

from it_stream import dump_it_artifacts, load_it_resume_selection, process_file
from zephyr_core.contracts.v1.run_meta import (
    ErrorInfoV1,
    MetricsV1,
    RunMetaV1,
    RunProvenanceV1,
)
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
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="batch",
            task_id="abc",
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
        assert data["run_meta"]["provenance"] == {
            "run_origin": "intake",
            "delivery_origin": "replay",
            "execution_mode": "batch",
            "task_id": "abc",
        }
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


def test_replay_delivery_preserves_resume_checkpoint_provenance_while_marking_replay(
    tmp_path: Path,
) -> None:
    out_root = tmp_path / "out"
    out_root.mkdir(parents=True, exist_ok=True)

    meta = RunMetaV1(
        run_id="r-resume",
        pipeline_version="p-resume",
        timestamp_utc="2026-03-23T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=None,
        metrics=MetricsV1(duration_ms=10, attempts=1),
        error=ErrorInfoV1(
            code="ZE-UNS-PARTITION-FAILED", message="fail", details={"retryable": True}
        ),
        provenance=RunProvenanceV1(
            run_origin="resume",
            delivery_origin="primary",
            execution_mode="worker",
            task_id="task-resume",
            checkpoint_identity_key="cp-1",
            task_identity_key="task-1",
        ),
    )
    receipt = DeliveryReceipt(destination="webhook", ok=False, details={"status_code": 500})
    dlq_path = write_delivery_dlq(
        out_root=out_root,
        sha256="abc-resume",
        meta=meta,
        receipt=receipt,
    )
    assert dlq_path.exists()

    def handler(request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content.decode("utf-8"))
        assert data["run_meta"]["provenance"] == {
            "run_origin": "resume",
            "delivery_origin": "replay",
            "execution_mode": "worker",
            "task_id": "task-resume",
            "checkpoint_identity_key": "cp-1",
            "task_identity_key": "task-1",
        }
        return httpx.Response(200, json={"ok": True})

    stats = replay_delivery_dlq(
        out_root=out_root,
        webhook_url="https://example.test/hook",
        timeout_s=1.0,
        transport=httpx.MockTransport(handler),
        limit=None,
        dry_run=False,
        move_done=True,
    )

    assert stats.succeeded == 1
    assert stats.failed == 0
    assert (out_root / "_dlq" / "delivery_done" / dlq_path.name).exists()


def test_replay_delivery_keeps_it_resume_identity_boundaries_while_switching_delivery_origin(
    tmp_path: Path,
) -> None:
    messages_path = tmp_path / "messages.json"
    messages_path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "customers",
                            "data": {"id": 1},
                            "emitted_at": "2026-01-01T00:00:00Z",
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"cursor": "2026-01-01T00:00:00Z"}},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    result = process_file(
        filename=str(messages_path),
        sha256="sha-it-replay",
        size_bytes=messages_path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it-replay",
    )
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        pipeline_version="p-it-replay",
        sha256="sha-it-replay",
    )
    resume_provenance = selection.to_run_provenance(
        execution_mode="worker",
        task_id="task-it-replay",
    )

    out_root = tmp_path / "out"
    out_root.mkdir(parents=True, exist_ok=True)
    meta = RunMetaV1(
        run_id="r-it-resume",
        pipeline_version="p-it-replay",
        timestamp_utc="2026-04-06T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=None,
        metrics=MetricsV1(duration_ms=10, attempts=1),
        error=ErrorInfoV1(
            code="ZE-IT-DELIVERY-FAILED",
            message="delivery failed",
            details={"retryable": True},
        ),
        provenance=resume_provenance,
    )
    receipt = DeliveryReceipt(destination="webhook", ok=False, details={"status_code": 500})
    dlq_path = write_delivery_dlq(
        out_root=out_root,
        sha256="sha-it-replay",
        meta=meta,
        receipt=receipt,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content.decode("utf-8"))
        assert data["run_meta"]["provenance"] == {
            "run_origin": "resume",
            "delivery_origin": "replay",
            "execution_mode": "worker",
            "task_id": "task-it-replay",
            "checkpoint_identity_key": artifacts.checkpoint.checkpoints[0].checkpoint_identity_key,
            "task_identity_key": artifacts.checkpoint.task_identity_key,
        }
        return httpx.Response(200, json={"ok": True})

    stats = replay_delivery_dlq(
        out_root=out_root,
        webhook_url="https://example.test/hook",
        timeout_s=1.0,
        transport=httpx.MockTransport(handler),
        limit=None,
        dry_run=False,
        move_done=True,
    )

    assert stats.succeeded == 1
    assert stats.failed == 0
    assert (out_root / "_dlq" / "delivery_done" / dlq_path.name).exists()
