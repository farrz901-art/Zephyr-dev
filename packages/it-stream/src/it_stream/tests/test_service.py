from __future__ import annotations

import json
from pathlib import Path

import pytest

from it_stream import (
    ItCheckpointIdentityV1,
    ItTaskIdentityV1,
    dump_it_artifacts,
    load_it_resume_selection,
    normalize_it_checkpoint_identity_key,
    normalize_it_task_identity_key,
    process_file,
    resume_file,
)
from it_stream.artifacts import load_it_checkpoint
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


def test_dump_it_artifacts_writes_zephyr_owned_checkpoint_artifact(tmp_path: Path) -> None:
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
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result, pipeline_version="p-it")
    record_row = json.loads((out_dir / "records.jsonl").read_text(encoding="utf-8"))
    log_row = json.loads((out_dir / "logs.jsonl").read_text(encoding="utf-8"))
    checkpoint_row = json.loads((out_dir / "checkpoint.json").read_text(encoding="utf-8"))

    assert len(artifacts.records) == 1
    assert len(artifacts.states) == 1
    assert len(artifacts.logs) == 1
    assert artifacts.checkpoint.schema_version == 1
    assert artifacts.checkpoint.task_identity_key == normalize_it_task_identity_key(
        identity=ItTaskIdentityV1(
            pipeline_version="p-it",
            sha256="sha-it-002",
        )
    )
    assert (out_dir / "records.jsonl").read_text(encoding="utf-8") == (
        '{"data":{"id":"acct-1","status":"active"},"emitted_at":null,'
        '"record_index":0,"stream":"accounts"}'
    )
    assert (out_dir / "logs.jsonl").read_text(encoding="utf-8") == (
        '{"level":"WARN","log_index":0,"message":"partial page"}'
    )
    assert checkpoint_row == {
        "checkpoints": [
            {
                "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                    identity=ItCheckpointIdentityV1(
                        task=ItTaskIdentityV1(
                            pipeline_version="p-it",
                            sha256="sha-it-002",
                        ),
                        stream="accounts",
                        progress={"checkpoint": 7},
                    )
                ),
                "checkpoint_index": 0,
                "progress": {"checkpoint": 7},
            }
        ],
        "flow_kind": "it",
        "schema_version": 1,
        "stream": "accounts",
        "task_identity_key": normalize_it_task_identity_key(
            identity=ItTaskIdentityV1(
                pipeline_version="p-it",
                sha256="sha-it-002",
            )
        ),
    }
    assert set(record_row) == {"data", "emitted_at", "record_index", "stream"}
    assert set(log_row) == {"level", "log_index", "message"}
    assert set(checkpoint_row) == {
        "checkpoints",
        "flow_kind",
        "schema_version",
        "stream",
        "task_identity_key",
    }
    checkpoint_entry = checkpoint_row["checkpoints"][0]
    assert checkpoint_entry["checkpoint_identity_key"] != checkpoint_row["task_identity_key"]
    assert "type" not in record_row
    assert "record" not in record_row
    assert "type" not in log_row
    assert "log" not in log_row
    assert "type" not in checkpoint_row
    assert "state" not in checkpoint_row
    assert "messages" not in checkpoint_row


def test_it_checkpoint_identity_normalization_is_deterministic_and_related_to_task_identity() -> (
    None
):
    task_identity = ItTaskIdentityV1(
        pipeline_version="p-it",
        sha256="sha-it-003",
    )

    first = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=task_identity,
            stream="customers",
            progress={"cursor": "2026-01-01", "page": 3},
        )
    )
    second = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=task_identity,
            stream="customers",
            progress={"page": 3, "cursor": "2026-01-01"},
        )
    )
    third = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=task_identity,
            stream="customers",
            progress={"cursor": "2026-01-02", "page": 3},
        )
    )

    task_key = normalize_it_task_identity_key(identity=task_identity)

    assert first == second
    assert first != third
    assert first != task_key


def test_load_it_resume_selection_validates_task_identity_and_defaults_to_latest_checkpoint(
    tmp_path: Path,
) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
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
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "customers",
                            "data": {"id": 2},
                            "emitted_at": "2026-01-02T00:00:00Z",
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"cursor": "2026-01-02T00:00:00Z"}},
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
        sha256="sha-it-004",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it",
    )

    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        pipeline_version="p-it",
        sha256="sha-it-004",
    )

    assert selection.entry.checkpoint_index == 1
    assert selection.entry.checkpoint_identity_key == (
        artifacts.checkpoint.checkpoints[-1].checkpoint_identity_key
    )


def test_it_resume_selection_exposes_run_provenance(tmp_path: Path) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
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
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-004b",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it",
    )

    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        pipeline_version="p-it",
        sha256="sha-it-004b",
    )

    assert selection.to_run_provenance(
        execution_mode="batch",
        task_id="sha-it-004b",
    ).to_dict() == {
        "run_origin": "resume",
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "task_id": "sha-it-004b",
        "checkpoint_identity_key": artifacts.checkpoint.checkpoints[0].checkpoint_identity_key,
        "task_identity_key": artifacts.checkpoint.task_identity_key,
    }


def test_load_it_checkpoint_round_trips_zephyr_owned_checkpoint_artifact(tmp_path: Path) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
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
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-007",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it",
    )

    loaded = load_it_checkpoint(path=tmp_path / "artifacts" / "checkpoint.json")

    assert loaded == artifacts.checkpoint
    assert loaded.to_dict() == artifacts.checkpoint.to_dict()


def test_load_it_resume_selection_rejects_mismatched_task_identity(tmp_path: Path) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
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
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-008",
        size_bytes=path.stat().st_size,
    )
    dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it",
    )

    with pytest.raises(ValueError, match="task identity"):
        load_it_resume_selection(
            checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
            pipeline_version="p-it-other",
            sha256="sha-it-008",
        )


def test_resume_file_continues_it_processing_from_selected_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
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
                    {
                        "type": "LOG",
                        "log": {"level": "INFO", "message": "after-first-page"},
                    },
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "customers",
                            "data": {"id": 2},
                            "emitted_at": "2026-01-02T00:00:00Z",
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"cursor": "2026-01-02T00:00:00Z"}},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-005",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=initial,
        pipeline_version="p-it",
    )

    resumed = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        checkpoint_identity_key=artifacts.checkpoint.checkpoints[0].checkpoint_identity_key,
        pipeline_version="p-it",
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-005",
        size_bytes=path.stat().st_size,
    )

    assert [element.type for element in resumed.elements] == [
        "StructuredRecord",
        "StructuredState",
    ]
    assert resumed.elements[0].metadata["data"] == {"id": 2}
    assert resumed.elements[1].metadata["data"] == {"cursor": "2026-01-02T00:00:00Z"}
    assert "after-first-page" not in resumed.normalized_text


def test_resume_file_rejects_checkpoint_progress_outside_supported_cursor_subset(
    tmp_path: Path,
) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "accounts",
                            "data": {"id": "acct-1"},
                            "emitted_at": "2026-01-01T00:00:00Z",
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"checkpoint": 7}},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-009",
        size_bytes=path.stat().st_size,
    )
    dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=initial,
        pipeline_version="p-it",
    )

    with pytest.raises(ValueError, match="cursor"):
        resume_file(
            filename=str(path),
            checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
            pipeline_version="p-it",
            strategy=PartitionStrategy.AUTO,
            sha256="sha-it-009",
            size_bytes=path.stat().st_size,
        )


def test_resume_file_is_deterministic_for_equivalent_inputs(tmp_path: Path) -> None:
    path = tmp_path / "messages.json"
    path.write_text(
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
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "customers",
                            "data": {"id": 2},
                            "emitted_at": "2026-01-03T00:00:00Z",
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"cursor": "2026-01-03T00:00:00Z"}},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-006",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=initial,
        pipeline_version="p-it",
    )
    checkpoint_key = artifacts.checkpoint.checkpoints[0].checkpoint_identity_key

    first = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        checkpoint_identity_key=checkpoint_key,
        pipeline_version="p-it",
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-006",
        size_bytes=path.stat().st_size,
    )
    second = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        checkpoint_identity_key=checkpoint_key,
        pipeline_version="p-it",
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-006",
        size_bytes=path.stat().st_size,
    )

    assert first.normalized_text == second.normalized_text
    assert [element.metadata for element in first.elements] == [
        element.metadata for element in second.elements
    ]
