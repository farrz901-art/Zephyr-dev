from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from zephyr_core import RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v2.delivery_payload import DeliveryPayloadV1
from zephyr_ingest.destinations.sqlite import (
    SqliteDestination,
    send_delivery_payload_v1_to_sqlite,
)


def _build_meta() -> RunMetaV1:
    ctx = RunContext.new()
    return RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        engine=None,
        document=None,
        error=None,
        warnings=[],
    )


def test_sqlite_destination_rejects_invalid_table_name(tmp_path: Path) -> None:
    dest = SqliteDestination(db_path=tmp_path / "out.db", table_name="bad-name")

    with pytest.raises(ValueError, match="table_name"):
        dest(out_root=tmp_path / "out", sha256="abc", meta=_build_meta(), result=None)


def test_sqlite_destination_writes_shared_delivery_payload_and_normalized_receipt(
    tmp_path: Path,
) -> None:
    dest = SqliteDestination(db_path=tmp_path / "delivery.db", table_name="delivery_rows")
    meta = _build_meta()

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=meta, result=None)

    assert receipt.ok is True
    assert receipt.destination == "sqlite"
    assert receipt.failure_retryability == "not_failed"
    assert receipt.shared_summary == {
        "delivery_outcome": "delivered",
        "failure_retryability": "not_failed",
        "failure_kind": "not_failed",
        "error_code": "not_failed",
        "attempt_count": 1,
        "payload_count": 1,
    }
    assert receipt.details is not None
    assert receipt.details["table"] == "delivery_rows"
    assert receipt.details["operation"] == "upsert_inserted"
    assert receipt.details["row_count"] == 1
    assert receipt.details["identity_key"] == f"abc123:{meta.run_id}"
    assert receipt.details["retryable"] is False

    conn = sqlite3.connect(tmp_path / "delivery.db")
    try:
        row = conn.execute(
            "SELECT identity_key, sha256, run_id, schema_version, payload_json FROM delivery_rows"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == f"abc123:{meta.run_id}"
    assert row[1] == "abc123"
    assert row[2] == meta.run_id
    assert row[3] == 1
    payload = json.loads(row[4])
    assert {"schema_version", "sha256", "run_meta", "artifacts"}.issubset(payload)
    assert payload["schema_version"] == 1
    assert payload["sha256"] == "abc123"
    assert payload["run_meta"]["run_id"] == meta.run_id
    assert payload["content_evidence"]["evidence_kind"] in {
        "artifact_reference_only_v1",
        "elements_count_only_v1",
        "normalized_text_preview_v1",
        "normalized_text_and_records_preview_v1",
        "records_preview_v1",
    }
    assert set(payload["artifacts"]) == {
        "out_dir",
        "run_meta_path",
        "elements_path",
        "normalized_path",
    }


def test_sqlite_destination_preserves_idempotent_upsert_for_same_identity(tmp_path: Path) -> None:
    dest = SqliteDestination(db_path=tmp_path / "delivery.db", table_name="delivery_rows")
    meta = _build_meta()

    first = dest(out_root=tmp_path / "out-a", sha256="abc123", meta=meta, result=None)
    second = dest(out_root=tmp_path / "out-b", sha256="abc123", meta=meta, result=None)

    assert first.ok is True
    assert second.ok is True
    assert first.failure_retryability == "not_failed"
    assert second.failure_retryability == "not_failed"
    assert second.details is not None
    assert second.details["operation"] == "upsert_replaced"
    assert second.details["identity_key"] == f"abc123:{meta.run_id}"

    conn = sqlite3.connect(tmp_path / "delivery.db")
    try:
        row_count = conn.execute("SELECT COUNT(*) FROM delivery_rows").fetchone()
        payload_json = conn.execute("SELECT payload_json FROM delivery_rows").fetchone()
    finally:
        conn.close()

    assert row_count is not None
    assert row_count[0] == 1
    assert payload_json is not None
    payload = json.loads(payload_json[0])
    out_dir = Path(payload["artifacts"]["out_dir"])
    assert out_dir.name == "abc123"
    assert out_dir.parent.name == "out-b"
    assert {"schema_version", "sha256", "run_meta", "artifacts"}.issubset(payload)
    assert set(payload["artifacts"]) == {
        "out_dir",
        "run_meta_path",
        "elements_path",
        "normalized_path",
    }


def test_sqlite_destination_locked_error_is_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    payload: DeliveryPayloadV1 = {
        "schema_version": 1,
        "sha256": "abc123",
        "run_meta": {"run_id": "r1"},
        "artifacts": {
            "out_dir": "out",
            "run_meta_path": "out/run_meta.json",
            "elements_path": "out/elements.json",
            "normalized_path": "out/normalized.txt",
        },
    }

    def fake_connect(path: object, timeout: float = 5.0) -> object:
        del path, timeout
        raise sqlite3.OperationalError("database is locked")

    import zephyr_ingest.destinations.sqlite as sqlite_mod

    monkeypatch.setattr(sqlite_mod.sqlite3, "connect", fake_connect)

    receipt = send_delivery_payload_v1_to_sqlite(
        db_path=Path("delivery.db"),
        table_name="delivery_rows",
        payload=payload,
        idempotency_key="abc123:r1",
        timeout_s=5.0,
    )

    assert receipt.ok is False
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "locked"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.shared_summary["attempt_count"] == 1
    assert receipt.shared_summary["payload_count"] == 1
    assert receipt.details is not None
    assert receipt.details["retryable"] is True
    assert receipt.details["failure_kind"] == "locked"
