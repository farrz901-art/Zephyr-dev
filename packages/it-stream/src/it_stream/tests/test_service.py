from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import pytest

from it_stream import (
    ItCheckpointCompatibilityError,
    ItCheckpointIdentityV1,
    ItCheckpointProvenanceV1,
    ItCheckpointResumeCursorContinuationV1,
    ItCheckpointResumeProvenanceV1,
    ItResumeRecoveryError,
    ItTaskIdentityV1,
    dump_it_artifacts,
    inspect_it_checkpoint_compatibility,
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
                        progress_kind="state_dict_v1",
                        progress={"checkpoint": 7},
                    )
                ),
                "checkpoint_index": 0,
                "parent_checkpoint_identity_key": None,
                "progress_kind": "state_dict_v1",
                "progress": {"checkpoint": 7},
            }
        ],
        "flow_kind": "it",
        "provenance": {
            "delivery_origin": "primary",
            "execution_mode": "batch",
            "resumed_from_checkpoint_identity_key": None,
            "run_origin": "intake",
            "task_identity_key": normalize_it_task_identity_key(
                identity=ItTaskIdentityV1(
                    pipeline_version="p-it",
                    sha256="sha-it-002",
                )
            ),
        },
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
        "provenance",
        "schema_version",
        "stream",
        "task_identity_key",
    }
    checkpoint_entry = checkpoint_row["checkpoints"][0]
    assert checkpoint_entry["checkpoint_identity_key"] != checkpoint_row["task_identity_key"]
    assert checkpoint_entry["parent_checkpoint_identity_key"] is None
    assert checkpoint_entry["progress_kind"] == "state_dict_v1"
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
            progress_kind="cursor_v1",
            progress={"cursor": "2026-01-01", "page": 3},
        )
    )
    second = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=task_identity,
            stream="customers",
            progress_kind="cursor_v1",
            progress={"page": 3, "cursor": "2026-01-01"},
        )
    )
    third = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=task_identity,
            stream="customers",
            progress_kind="cursor_v1",
            progress={"cursor": "2026-01-02", "page": 3},
        )
    )

    task_key = normalize_it_task_identity_key(identity=task_identity)

    assert first == second
    assert first != third
    assert first != task_key


def test_it_checkpoint_identity_normalization_distinguishes_progress_family() -> None:
    task_identity = ItTaskIdentityV1(
        pipeline_version="p-it",
        sha256="sha-it-003-family",
    )

    cursor_identity = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=task_identity,
            stream="customers",
            progress_kind="cursor_v1",
            progress={"cursor": "same-value"},
        )
    )
    token_identity = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=task_identity,
            stream="customers",
            progress_kind="token_v1",
            progress={"cursor": "same-value"},
        )
    )

    assert cursor_identity != token_identity


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

    assert selection.selected_checkpoint.mode == "latest_checkpoint"
    assert selection.selected_checkpoint.checkpoint_identity_key == (
        artifacts.checkpoint.checkpoints[-1].checkpoint_identity_key
    )
    assert selection.selected_checkpoint.checkpoint_index == 1
    assert selection.selected_checkpoint.parent_checkpoint_identity_key == (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.entry.checkpoint_index == 1
    assert selection.entry.checkpoint_identity_key == (
        artifacts.checkpoint.checkpoints[-1].checkpoint_identity_key
    )
    assert selection.entry.parent_checkpoint_identity_key == (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "2026-01-02T00:00:00Z"


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
    assert selection.to_checkpoint_resume_provenance() == ItCheckpointResumeProvenanceV1(
        checkpoint_identity_key=artifacts.checkpoint.checkpoints[0].checkpoint_identity_key,
        progress_kind="cursor_v1",
        continuation=ItCheckpointResumeCursorContinuationV1(
            exclusive_after_cursor="2026-01-01T00:00:00Z"
        ),
    )


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
    assert loaded.provenance == ItCheckpointProvenanceV1(
        task_identity_key=artifacts.checkpoint.task_identity_key,
        run_origin="intake",
        delivery_origin="primary",
        execution_mode="batch",
        resumed_from_checkpoint_identity_key=None,
    )


def test_it_checkpoint_compatibility_explicitly_accepts_supported_artifact(tmp_path: Path) -> None:
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
        sha256="sha-it-supported",
        size_bytes=path.stat().st_size,
    )
    dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it",
    )

    raw = json.loads((tmp_path / "artifacts" / "checkpoint.json").read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "supported"
    assert compatibility.schema_version == 1
    assert compatibility.flow_kind == "it"
    assert compatibility.reason is None


def test_load_it_checkpoint_rejects_unsupported_schema_explicitly(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 99,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": "task-key",
                "checkpoints": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "unsupported"
    assert compatibility.schema_version == 99
    assert compatibility.flow_kind == "it"

    with pytest.raises(ItCheckpointCompatibilityError, match="schema version") as exc_info:
        load_it_checkpoint(path=checkpoint_path)

    assert exc_info.value.compatibility.status == "unsupported"


def test_load_it_checkpoint_rejects_malformed_checkpoint_explicitly(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "checkpoints": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "malformed"
    assert compatibility.schema_version == 1
    assert compatibility.flow_kind == "it"
    assert compatibility.reason is not None

    with pytest.raises(ItCheckpointCompatibilityError, match="task_identity_key") as exc_info:
        load_it_checkpoint(path=checkpoint_path)

    assert exc_info.value.compatibility.status == "malformed"


def test_it_checkpoint_lineage_records_immediate_parent_only(tmp_path: Path) -> None:
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
        sha256="sha-it-lineage",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it",
    )

    first_entry, second_entry = artifacts.checkpoint.checkpoints

    assert first_entry.parent_checkpoint_identity_key is None
    assert first_entry.progress_kind == "cursor_v1"
    assert '"progress_kind":"cursor_v1"' in first_entry.checkpoint_identity_key
    assert second_entry.parent_checkpoint_identity_key == first_entry.checkpoint_identity_key
    assert second_entry.progress_kind == "cursor_v1"
    assert '"progress_kind":"cursor_v1"' in second_entry.checkpoint_identity_key
    assert second_entry.checkpoint_identity_key != first_entry.checkpoint_identity_key


def test_dump_it_artifacts_can_record_resume_checkpoint_provenance(tmp_path: Path) -> None:
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

    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-provenance",
        size_bytes=path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "initial-artifacts",
        result=initial,
        pipeline_version="p-it",
    )
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "initial-artifacts" / "checkpoint.json",
        pipeline_version="p-it",
        sha256="sha-it-provenance",
        checkpoint_identity_key=initial_artifacts.checkpoint.checkpoints[0].checkpoint_identity_key,
    )

    assert selection.selected_checkpoint.mode == "explicit_checkpoint_identity"
    assert selection.selected_checkpoint.checkpoint_identity_key == (
        initial_artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert selection.selected_checkpoint.checkpoint_index == 0
    assert selection.selected_checkpoint.parent_checkpoint_identity_key is None
    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "2026-01-01T00:00:00Z"

    resumed = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=initial_artifacts.checkpoint.checkpoints[0].checkpoint_identity_key,
        pipeline_version="p-it",
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it-provenance",
        size_bytes=path.stat().st_size,
    )

    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "resumed-artifacts",
        result=resumed,
        pipeline_version="p-it",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-it-provenance",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )

    assert resumed_artifacts.checkpoint.provenance == ItCheckpointProvenanceV1(
        task_identity_key=initial_artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=initial_artifacts.checkpoint.checkpoints[
            0
        ].checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=initial_artifacts.checkpoint.checkpoints[
                0
            ].checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(
                exclusive_after_cursor="2026-01-01T00:00:00Z"
            ),
        ),
    )
    loaded_resumed = load_it_checkpoint(path=tmp_path / "resumed-artifacts" / "checkpoint.json")
    assert loaded_resumed.provenance == resumed_artifacts.checkpoint.provenance
    resumed_provenance = loaded_resumed.provenance
    assert resumed_provenance is not None
    assert resumed_provenance.resume is not None
    assert resumed_provenance.resume.checkpoint_identity_key == (
        resumed_provenance.resumed_from_checkpoint_identity_key
    )
    assert resumed_provenance.resume.checkpoint_identity_key != loaded_resumed.task_identity_key
    assert resumed_provenance.resume.progress_kind == selection.selected_checkpoint.progress_kind
    assert resumed_provenance.resume.continuation is not None
    assert resumed_provenance.resume.continuation.exclusive_after_cursor == (
        selection.continuation.exclusive_after_cursor
    )
    assert loaded_resumed.checkpoints[0].checkpoint_identity_key != (
        resumed_provenance.resume.checkpoint_identity_key
    )


def test_load_it_checkpoint_accepts_legacy_entries_without_lineage_field(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256="sha-it-legacy",
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                            identity=ItCheckpointIdentityV1(
                                task=ItTaskIdentityV1(
                                    pipeline_version="p-it",
                                    sha256="sha-it-legacy",
                                ),
                                stream="customers",
                                progress_kind="cursor_v1",
                                progress={"cursor": "2026-01-01T00:00:00Z"},
                            )
                        ),
                        "progress": {"cursor": "2026-01-01T00:00:00Z"},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    loaded = load_it_checkpoint(path=checkpoint_path)

    assert loaded.checkpoints[0].parent_checkpoint_identity_key is None
    assert loaded.checkpoints[0].progress_kind == "cursor_v1"
    assert loaded.provenance is None


def test_load_it_checkpoint_infers_non_cursor_legacy_progress_kind(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "accounts",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256="sha-it-legacy-state",
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                            identity=ItCheckpointIdentityV1(
                                task=ItTaskIdentityV1(
                                    pipeline_version="p-it",
                                    sha256="sha-it-legacy-state",
                                ),
                                stream="accounts",
                                progress_kind="state_dict_v1",
                                progress={"checkpoint": 7},
                            )
                        ),
                        "progress": {"checkpoint": 7},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    loaded = load_it_checkpoint(path=checkpoint_path)

    assert loaded.checkpoints[0].progress_kind == "state_dict_v1"


def test_load_it_checkpoint_infers_token_progress_kind(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256="sha-it-legacy-token",
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                            identity=ItCheckpointIdentityV1(
                                task=ItTaskIdentityV1(
                                    pipeline_version="p-it",
                                    sha256="sha-it-legacy-token",
                                ),
                                stream="customers",
                                progress_kind="token_v1",
                                progress={"next_page_token": "tok-1"},
                            )
                        ),
                        "progress": {"next_page_token": "tok-1"},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    loaded = load_it_checkpoint(path=checkpoint_path)

    assert loaded.checkpoints[0].progress_kind == "token_v1"


def test_load_it_checkpoint_infers_page_progress_kind(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256="sha-it-legacy-page",
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                            identity=ItCheckpointIdentityV1(
                                task=ItTaskIdentityV1(
                                    pipeline_version="p-it",
                                    sha256="sha-it-legacy-page",
                                ),
                                stream="customers",
                                progress_kind="page_v1",
                                progress={"page_number": 3},
                            )
                        ),
                        "progress": {"page_number": 3},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    loaded = load_it_checkpoint(path=checkpoint_path)

    assert loaded.checkpoints[0].progress_kind == "page_v1"


@pytest.mark.parametrize(
    ("progress_kind", "progress", "sha256"),
    [
        ("cursor_v1", {"cursor": "2026-01-01T00:00:00Z"}, "sha-it-explicit-cursor"),
        ("token_v1", {"next_page_token": "tok-1"}, "sha-it-explicit-token"),
        ("page_v1", {"page_number": 3}, "sha-it-explicit-page"),
        ("state_dict_v1", {"checkpoint": 7}, "sha-it-explicit-state"),
    ],
)
def test_it_checkpoint_compatibility_explicitly_accepts_supported_progress_families(
    tmp_path: Path,
    progress_kind: Literal["cursor_v1", "token_v1", "page_v1", "state_dict_v1"],
    progress: dict[str, object],
    sha256: str,
) -> None:
    checkpoint_identity_key = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=ItTaskIdentityV1(
                pipeline_version="p-it",
                sha256=sha256,
            ),
            stream="customers",
            progress_kind=progress_kind,
            progress=progress,
        )
    )
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256=sha256,
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": checkpoint_identity_key,
                        "progress_kind": progress_kind,
                        "progress": progress,
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "supported"
    assert compatibility.schema_version == 1
    assert compatibility.flow_kind == "it"
    assert compatibility.reason is None

    loaded = load_it_checkpoint(path=checkpoint_path)

    assert loaded.checkpoints[0].checkpoint_identity_key == checkpoint_identity_key
    assert loaded.checkpoints[0].progress_kind == progress_kind
    assert loaded.checkpoints[0].progress == progress


@pytest.mark.parametrize(
    ("checkpoint_entry", "match"),
    [
        (
            {
                "checkpoint_index": 0,
                "checkpoint_identity_key": "checkpoint-invalid-kind",
                "progress_kind": "cursor_v2",
                "progress": {"cursor": "2026-01-01T00:00:00Z"},
            },
            "supported progress kind",
        ),
        (
            {
                "checkpoint_index": 0,
                "checkpoint_identity_key": "checkpoint-invalid-progress",
                "progress_kind": "cursor_v1",
                "progress": ["2026-01-01T00:00:00Z"],
            },
            "field 'progress' must be an object",
        ),
    ],
)
def test_load_it_checkpoint_rejects_malformed_progress_shape_explicitly(
    tmp_path: Path,
    checkpoint_entry: dict[str, object],
    match: str,
) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256="sha-it-malformed-progress",
                    )
                ),
                "checkpoints": [checkpoint_entry],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "malformed"
    assert compatibility.schema_version == 1
    assert compatibility.flow_kind == "it"
    assert compatibility.reason is not None

    with pytest.raises(ItCheckpointCompatibilityError, match=match) as exc_info:
        load_it_checkpoint(path=checkpoint_path)

    assert exc_info.value.compatibility.status == "malformed"


def test_legacy_checkpoint_without_lineage_or_provenance_is_still_explicitly_supported(
    tmp_path: Path,
) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256="sha-it-legacy-supported",
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                            identity=ItCheckpointIdentityV1(
                                task=ItTaskIdentityV1(
                                    pipeline_version="p-it",
                                    sha256="sha-it-legacy-supported",
                                ),
                                stream="customers",
                                progress_kind="cursor_v1",
                                progress={"cursor": "2026-01-01T00:00:00Z"},
                            )
                        ),
                        "progress": {"cursor": "2026-01-01T00:00:00Z"},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "supported"
    assert compatibility.schema_version == 1
    assert compatibility.flow_kind == "it"
    assert compatibility.reason is None


def test_load_it_checkpoint_rejects_unsupported_flow_kind_explicitly(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "uns",
                "stream": "customers",
                "task_identity_key": "task-key",
                "checkpoints": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "unsupported"
    assert compatibility.schema_version == 1
    assert compatibility.flow_kind == "uns"
    assert compatibility.reason is not None

    with pytest.raises(ItCheckpointCompatibilityError, match="flow kind") as exc_info:
        load_it_checkpoint(path=checkpoint_path)

    assert exc_info.value.compatibility.status == "unsupported"


def test_checkpoint_governance_fields_remain_distinct_and_inspectable(tmp_path: Path) -> None:
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
        sha256="sha-it-governance",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=result,
        pipeline_version="p-it-governance",
    )

    first_entry, second_entry = artifacts.checkpoint.checkpoints
    provenance = artifacts.checkpoint.provenance

    assert provenance is not None
    assert artifacts.checkpoint.task_identity_key == provenance.task_identity_key
    assert first_entry.checkpoint_identity_key != artifacts.checkpoint.task_identity_key
    assert second_entry.checkpoint_identity_key != artifacts.checkpoint.task_identity_key
    assert first_entry.parent_checkpoint_identity_key is None
    assert second_entry.parent_checkpoint_identity_key == first_entry.checkpoint_identity_key
    assert provenance.resumed_from_checkpoint_identity_key is None


def test_load_it_resume_selection_distinguishes_incompatible_checkpoint_contract(
    tmp_path: Path,
) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "uns",
                "stream": "customers",
                "task_identity_key": "task-key",
                "checkpoints": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ItResumeRecoveryError, match="flow kind") as exc_info:
        load_it_resume_selection(
            checkpoint_path=checkpoint_path,
            pipeline_version="p-it",
            sha256="sha-it-incompatible",
        )

    assert exc_info.value.issue.to_dict() == {
        "status": "incompatible",
        "code": "checkpoint_incompatible",
        "message": "Unsupported it-stream checkpoint flow kind: 'uns'",
        "checkpoint_identity_key": None,
        "progress_kind": None,
    }


def test_load_it_resume_selection_distinguishes_malformed_checkpoint_artifact(
    tmp_path: Path,
) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "checkpoints": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ItResumeRecoveryError, match="task_identity_key") as exc_info:
        load_it_resume_selection(
            checkpoint_path=checkpoint_path,
            pipeline_version="p-it",
            sha256="sha-it-malformed",
        )

    assert exc_info.value.issue.status == "malformed"
    assert exc_info.value.issue.code == "checkpoint_malformed"
    assert exc_info.value.issue.progress_kind is None


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

    with pytest.raises(ItResumeRecoveryError, match="task identity") as exc_info:
        load_it_resume_selection(
            checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
            pipeline_version="p-it-other",
            sha256="sha-it-008",
        )

    assert exc_info.value.issue.status == "incompatible"
    assert exc_info.value.issue.code == "task_identity_mismatch"


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
    checkpoint = load_it_checkpoint(path=tmp_path / "artifacts" / "checkpoint.json")

    assert checkpoint.checkpoints[0].progress_kind == "state_dict_v1"

    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        pipeline_version="p-it",
        sha256="sha-it-009",
    )

    assert selection.selected_checkpoint.mode == "latest_checkpoint"
    assert selection.selected_checkpoint.progress_kind == "state_dict_v1"
    assert selection.continuation is None

    with pytest.raises(ItResumeRecoveryError, match="cursor_v1") as exc_info:
        resume_file(
            filename=str(path),
            checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
            pipeline_version="p-it",
            strategy=PartitionStrategy.AUTO,
            sha256="sha-it-009",
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.issue.status == "unsupported"
    assert exc_info.value.issue.code == "unsupported_progress_kind"
    assert exc_info.value.issue.progress_kind == "state_dict_v1"


@pytest.mark.parametrize(
    ("progress_kind", "progress", "sha256"),
    [
        ("token_v1", {"token": "tok-1"}, "sha-it-token"),
        ("page_v1", {"page_number": 3}, "sha-it-page"),
    ],
)
def test_resume_file_rejects_explicit_non_cursor_progress_families(
    tmp_path: Path,
    progress_kind: Literal["token_v1", "page_v1"],
    progress: dict[str, object],
    sha256: str,
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
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256=sha256,
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                            identity=ItCheckpointIdentityV1(
                                task=ItTaskIdentityV1(
                                    pipeline_version="p-it",
                                    sha256=sha256,
                                ),
                                stream="customers",
                                progress_kind=progress_kind,
                                progress=progress,
                            )
                        ),
                        "progress_kind": progress_kind,
                        "progress": progress,
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    compatibility = inspect_it_checkpoint_compatibility(raw=raw)

    assert compatibility.status == "supported"
    assert compatibility.schema_version == 1
    assert compatibility.flow_kind == "it"
    assert compatibility.reason is None

    selection = load_it_resume_selection(
        checkpoint_path=checkpoint_path,
        pipeline_version="p-it",
        sha256=sha256,
    )

    assert selection.selected_checkpoint.mode == "latest_checkpoint"
    assert selection.selected_checkpoint.progress_kind == progress_kind
    assert selection.entry.progress_kind == progress_kind
    assert selection.entry.progress == progress
    assert selection.entry.checkpoint_identity_key != selection.checkpoint.task_identity_key
    assert selection.continuation is None
    assert selection.to_checkpoint_resume_provenance() == ItCheckpointResumeProvenanceV1(
        checkpoint_identity_key=selection.selected_checkpoint.checkpoint_identity_key,
        progress_kind=progress_kind,
        continuation=None,
    )

    with pytest.raises(ItResumeRecoveryError, match="cursor_v1") as exc_info:
        resume_file(
            filename=str(path),
            checkpoint_path=checkpoint_path,
            pipeline_version="p-it",
            strategy=PartitionStrategy.AUTO,
            sha256=sha256,
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.issue.status == "unsupported"
    assert exc_info.value.issue.code == "unsupported_progress_kind"
    assert exc_info.value.issue.progress_kind == progress_kind


def test_load_it_resume_selection_surfaces_blocked_cursor_continuation_facts(
    tmp_path: Path,
) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_identity_key = normalize_it_checkpoint_identity_key(
        identity=ItCheckpointIdentityV1(
            task=ItTaskIdentityV1(
                pipeline_version="p-it",
                sha256="sha-it-blocked-cursor",
            ),
            stream="customers",
            progress_kind="cursor_v1",
            progress={"page_number": 3},
        )
    )
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "flow_kind": "it",
                "stream": "customers",
                "task_identity_key": normalize_it_task_identity_key(
                    identity=ItTaskIdentityV1(
                        pipeline_version="p-it",
                        sha256="sha-it-blocked-cursor",
                    )
                ),
                "checkpoints": [
                    {
                        "checkpoint_index": 0,
                        "checkpoint_identity_key": checkpoint_identity_key,
                        "progress_kind": "cursor_v1",
                        "progress": {"page_number": 3},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ItResumeRecoveryError, match="field 'cursor'") as exc_info:
        load_it_resume_selection(
            checkpoint_path=checkpoint_path,
            pipeline_version="p-it",
            sha256="sha-it-blocked-cursor",
        )

    assert exc_info.value.issue.status == "blocked"
    assert exc_info.value.issue.code == "missing_checkpoint_cursor"
    assert exc_info.value.issue.checkpoint_identity_key == checkpoint_identity_key
    assert exc_info.value.issue.progress_kind == "cursor_v1"


def test_resume_file_surfaces_blocked_state_cursor_for_supported_cursor_subset(
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
                        "state": {"data": {"page_number": 2}},
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
        sha256="sha-it-blocked-state",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=initial,
        pipeline_version="p-it",
    )

    with pytest.raises(ItResumeRecoveryError, match="field 'cursor'") as exc_info:
        resume_file(
            filename=str(path),
            checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
            checkpoint_identity_key=artifacts.checkpoint.checkpoints[0].checkpoint_identity_key,
            pipeline_version="p-it",
            strategy=PartitionStrategy.AUTO,
            sha256="sha-it-blocked-state",
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.issue.status == "blocked"
    assert exc_info.value.issue.code == "missing_state_cursor"
    assert exc_info.value.issue.checkpoint_identity_key == (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert exc_info.value.issue.progress_kind == "cursor_v1"


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
