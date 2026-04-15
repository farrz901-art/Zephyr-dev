from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from zephyr_core import RunMetaV1
from zephyr_core.contracts.v1.run_meta import RunProvenanceV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.sqlite import SqliteDestination
from zephyr_ingest.replay_delivery import (
    FilesystemReplaySink,
    SqliteReplaySink,
    replay_delivery_dlq,
)
from zephyr_ingest.tests._p45_destination_wave1_helpers import (
    as_dict,
    read_json_dict,
    run_batch_case,
    run_worker_case,
)


def _minimal_meta(*, run_id: str) -> RunMetaV1:
    return RunMetaV1(
        run_id=run_id,
        pipeline_version="p45-m2-wave1",
        timestamp_utc="2026-04-15T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="batch",
            task_id="task",
        ),
    )


@pytest.mark.auth_local_real
def test_p45_wave1_filesystem_local_real_batch_worker_and_crash_recovery(tmp_path: Path) -> None:
    destination = FilesystemDestination()
    text = "filesystem local real"
    batch = run_batch_case(
        tmp_root=tmp_path / "filesystem-batch",
        destination=destination,
        run_id="filesystem-batch",
        text=text,
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "filesystem-worker",
        destination=destination,
        run_id="filesystem-worker",
        doc=batch.doc,
    )

    batch_run_meta = read_json_dict(batch.out_root / batch.sha256 / "run_meta.json")
    worker_run_meta = read_json_dict(worker.out_root / worker.sha256 / "run_meta.json")
    batch_receipt = read_json_dict(batch.out_root / batch.sha256 / "delivery_receipt.json")
    worker_receipt = read_json_dict(worker.out_root / worker.sha256 / "delivery_receipt.json")

    damaged_normalized = batch.out_root / batch.sha256 / "normalized.txt"
    damaged_normalized.write_text("partial-write", encoding="utf-8")
    rerun = run_batch_case(
        tmp_root=tmp_path / "filesystem-batch",
        destination=destination,
        run_id="filesystem-rerun",
        text=text,
        force=True,
    )
    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_provenance = as_dict(batch_run_meta["provenance"])
    worker_provenance = as_dict(worker_run_meta["provenance"])
    batch_summary = as_dict(batch_receipt["summary"])
    worker_summary = as_dict(worker_receipt["summary"])

    assert batch_by_destination["filesystem"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert batch_provenance["delivery_origin"] == "primary"
    assert batch_provenance["execution_mode"] == "batch"
    assert worker_provenance["delivery_origin"] == "primary"
    assert worker_provenance["execution_mode"] == "worker"
    assert batch_summary["delivery_outcome"] == "delivered"
    assert worker_summary["delivery_outcome"] == "delivered"
    assert (rerun.out_root / rerun.sha256 / "normalized.txt").read_text(encoding="utf-8") == text


@pytest.mark.auth_local_real
def test_p45_wave1_filesystem_local_real_path_failure_is_normalized(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    destination = FilesystemDestination()

    receipt = destination(
        out_root=blocker / "child",
        sha256="abc123",
        meta=_minimal_meta(run_id="filesystem-path-failure"),
        result=None,
    )

    assert receipt.ok is False
    assert receipt.failure_retryability == "non_retryable"
    assert receipt.shared_failure_kind == "operational"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"


@pytest.mark.auth_local_real
def test_p45_wave1_sqlite_local_real_batch_worker_and_lock_failure(tmp_path: Path) -> None:
    db_path = tmp_path / "sqlite-real" / "delivery.db"
    destination = SqliteDestination(db_path=db_path, table_name="delivery_rows")
    batch = run_batch_case(
        tmp_root=tmp_path / "sqlite-batch",
        destination=destination,
        run_id="sqlite-batch",
        text="sqlite local real",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "sqlite-worker",
        destination=destination,
        run_id="sqlite-worker",
        doc=batch.doc,
    )

    batch_receipt = read_json_dict(batch.out_root / batch.sha256 / "delivery_receipt.json")
    worker_receipt = read_json_dict(worker.out_root / worker.sha256 / "delivery_receipt.json")

    locked_db = tmp_path / "sqlite-lock" / "locked.db"
    locked_db.parent.mkdir(parents=True, exist_ok=True)
    lock_conn = sqlite3.connect(locked_db)
    lock_conn.execute("BEGIN EXCLUSIVE")
    try:
        locked_destination = SqliteDestination(
            db_path=locked_db,
            table_name="delivery_rows",
            timeout_s=0.01,
        )
        locked_batch = run_batch_case(
            tmp_root=tmp_path / "sqlite-lock-run",
            destination=locked_destination,
            run_id="sqlite-lock-run",
            text="sqlite lock",
        )
    finally:
        lock_conn.rollback()
        lock_conn.close()
    locked_receipt = read_json_dict(
        locked_batch.out_root / locked_batch.sha256 / "delivery_receipt.json"
    )
    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_summary = as_dict(batch_receipt["summary"])
    worker_summary = as_dict(worker_receipt["summary"])
    locked_summary = as_dict(locked_receipt["summary"])

    assert batch_by_destination["sqlite"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert batch_summary["delivery_outcome"] == "delivered"
    assert worker_summary["delivery_outcome"] == "delivered"
    assert locked_summary["failure_retryability"] == "retryable"
    assert locked_summary["failure_kind"] == "locked"
    assert locked_summary["error_code"] == "ZE-DELIVERY-FAILED"


@pytest.mark.auth_recovery_drill
def test_p45_wave1_local_real_replay_for_filesystem_and_sqlite_is_stable(tmp_path: Path) -> None:
    source = run_batch_case(
        tmp_root=tmp_path / "replay-source",
        destination=FilesystemDestination(),
        run_id="local-real-source",
        text="replay local real",
    )
    write_delivery_dlq(
        out_root=source.out_root,
        sha256=source.sha256,
        meta=_minimal_meta(run_id="local-real-source"),
        receipt=DeliveryReceipt(
            destination="filesystem",
            ok=False,
            details={
                "retryable": False,
                "failure_kind": "operational",
                "error_code": "ZE-DELIVERY-FAILED",
            },
        ),
    )

    filesystem_target = tmp_path / "filesystem-replay-target"
    filesystem_stats = replay_delivery_dlq(
        out_root=source.out_root,
        sink=FilesystemReplaySink(out_root=filesystem_target),
        dry_run=False,
        move_done=False,
    )
    filesystem_stats_dup = replay_delivery_dlq(
        out_root=source.out_root,
        sink=FilesystemReplaySink(out_root=filesystem_target),
        dry_run=False,
        move_done=False,
    )

    sqlite_db = tmp_path / "sqlite-replay" / "replay.db"
    sqlite_stats = replay_delivery_dlq(
        out_root=source.out_root,
        sink=SqliteReplaySink(db_path=sqlite_db, table_name="delivery_rows"),
        dry_run=False,
        move_done=False,
    )
    sqlite_stats_dup = replay_delivery_dlq(
        out_root=source.out_root,
        sink=SqliteReplaySink(db_path=sqlite_db, table_name="delivery_rows"),
        dry_run=False,
        move_done=False,
    )

    replay_run_meta = read_json_dict(filesystem_target / source.sha256 / "run_meta.json")
    conn = sqlite3.connect(sqlite_db)
    try:
        row_count = conn.execute("SELECT COUNT(*) FROM delivery_rows").fetchone()
        payload_row = conn.execute(
            "SELECT payload_json FROM delivery_rows WHERE identity_key = ?",
            (f"{source.sha256}:local-real-source",),
        ).fetchone()
    finally:
        conn.close()

    assert filesystem_stats.succeeded == 1
    assert filesystem_stats_dup.succeeded == 1
    assert sqlite_stats.succeeded == 1
    assert sqlite_stats_dup.succeeded == 1
    assert as_dict(replay_run_meta["provenance"])["delivery_origin"] == "replay"
    assert row_count is not None
    assert row_count[0] == 1
    assert payload_row is not None
    payload = json.loads(payload_row[0])
    payload_run_meta = as_dict(payload["run_meta"])
    assert as_dict(payload_run_meta["provenance"])["delivery_origin"] == "replay"
