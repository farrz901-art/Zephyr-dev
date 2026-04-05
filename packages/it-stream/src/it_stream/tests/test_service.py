from __future__ import annotations

import json
from pathlib import Path

from it_stream import dump_it_artifacts, process_file
from zephyr_core import PartitionStrategy


def test_process_file_normalizes_airbyte_style_messages(tmp_path: Path) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "type": "LOG",
                        "log": {"level": "INFO", "message": "starting sync"},
                    },
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "customers",
                            "data": {"id": 1, "name": "Ada"},
                            "emitted_at": "2026-01-01T00:00:00Z",
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"cursor": "2026-01-01"}},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-001",
        size_bytes=path.stat().st_size,
    )

    assert result.engine.name == "it-stream"
    assert result.engine.backend == "airbyte-message-json"
    assert [element.type for element in result.elements] == [
        "StructuredRecord",
        "StructuredState",
        "StructuredLog",
    ]
    assert result.elements[0].metadata["artifact_kind"] == "record"
    assert result.elements[1].metadata["artifact_kind"] == "state"
    assert result.elements[2].metadata["artifact_kind"] == "log"
    assert '"name": "Ada"' in result.normalized_text
    assert '"cursor": "2026-01-01"' in result.normalized_text
    assert "starting sync" in result.normalized_text


def test_dump_it_artifacts_writes_zephyr_owned_jsonl_files(tmp_path: Path) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "accounts",
                            "data": {"id": "acct-1", "status": "active"},
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"checkpoint": 7}},
                    },
                    {
                        "type": "LOG",
                        "log": {"level": "WARN", "message": "partial page"},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-002",
        size_bytes=path.stat().st_size,
    )

    out_dir = tmp_path / "artifacts"
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result)
    record_row = json.loads((out_dir / "records.jsonl").read_text(encoding="utf-8"))
    state_row = json.loads((out_dir / "state.jsonl").read_text(encoding="utf-8"))
    log_row = json.loads((out_dir / "logs.jsonl").read_text(encoding="utf-8"))

    assert len(artifacts.records) == 1
    assert len(artifacts.states) == 1
    assert len(artifacts.logs) == 1
    assert (out_dir / "records.jsonl").read_text(encoding="utf-8") == (
        '{"data":{"id":"acct-1","status":"active"},"emitted_at":null,'
        '"record_index":0,"stream":"accounts"}'
    )
    assert (out_dir / "state.jsonl").read_text(encoding="utf-8") == (
        '{"data":{"checkpoint":7},"state_index":0}'
    )
    assert (out_dir / "logs.jsonl").read_text(encoding="utf-8") == (
        '{"level":"WARN","log_index":0,"message":"partial page"}'
    )
    assert set(record_row) == {"data", "emitted_at", "record_index", "stream"}
    assert set(state_row) == {"data", "state_index"}
    assert set(log_row) == {"level", "log_index", "message"}
    assert "type" not in record_row
    assert "record" not in record_row
    assert "type" not in state_row
    assert "state" not in state_row
    assert "type" not in log_row
    assert "log" not in log_row
