from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from zephyr_ingest.replay_delivery import SqliteReplaySink, replay_delivery_dlq


def test_replay_to_sqlite_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    dlq_dir = out_root / "_dlq" / "delivery"
    dlq_dir.mkdir(parents=True, exist_ok=True)

    (dlq_dir / "one.json").write_text(
        json.dumps(
            {
                "sha256": "abc",
                "run_id": "r1",
                "run_meta": {"schema_version": 3, "run_id": "r1", "pipeline_version": "p1"},
            }
        ),
        encoding="utf-8",
    )

    sink = SqliteReplaySink(
        db_path=out_root / "replay.db",
        table_name="delivery_rows",
    )
    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)

    assert stats.total == 1
    assert stats.succeeded == 1

    conn = sqlite3.connect(out_root / "replay.db")
    try:
        row = conn.execute(
            "SELECT identity_key, sha256, run_id, schema_version, payload_json FROM delivery_rows"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "abc:r1"
    assert row[1] == "abc"
    assert row[2] == "r1"
    assert row[3] == 1
    payload = json.loads(row[4])
    assert {"schema_version", "sha256", "run_meta", "artifacts"}.issubset(payload)
    assert payload["schema_version"] == 1
    assert payload["run_meta"]["provenance"]["delivery_origin"] == "replay"
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
