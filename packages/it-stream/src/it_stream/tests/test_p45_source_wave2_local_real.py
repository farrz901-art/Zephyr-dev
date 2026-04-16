from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from it_stream import dump_it_artifacts, normalize_it_input_identity_sha, process_file
from it_stream.tests._p45_source_wave2_helpers import (
    airbyte_fixture_dir,
    as_dict,
    file_sha256,
    read_json_dict,
)
from zephyr_core import PartitionStrategy


@pytest.mark.auth_local_real
def test_p45_m5_airbyte_message_json_local_real_corpus_and_protocol_failures(
    tmp_path: Path,
) -> None:
    fixture_dir = airbyte_fixture_dir()
    success_path = fixture_dir / "customer_sync_cursor.json"
    success_copy = tmp_path / "customer_sync_cursor_copy.json"
    success_copy.write_text(success_path.read_text(encoding="utf-8"), encoding="utf-8")

    identity_sha = normalize_it_input_identity_sha(
        filename=str(success_path),
        default_sha=file_sha256(success_path),
    )
    same_identity_sha = identity_sha
    assert identity_sha == same_identity_sha

    result = process_file(
        filename=str(success_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=success_path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "airbyte-artifacts",
        result=result,
        pipeline_version="p45-m5-airbyte",
    )
    checkpoint_row = read_json_dict(tmp_path / "airbyte-artifacts" / "checkpoint.json")
    checkpoint_entries = cast(list[object], checkpoint_row["checkpoints"])
    first_checkpoint = as_dict(checkpoint_entries[0])
    first_run_task_identity = artifacts.checkpoint.task_identity_key

    rerun = process_file(
        filename=str(success_copy),
        strategy=PartitionStrategy.AUTO,
        sha256=same_identity_sha,
        size_bytes=success_copy.stat().st_size,
    )
    rerun_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "airbyte-rerun-artifacts",
        result=rerun,
        pipeline_version="p45-m5-airbyte",
    )

    assert result.engine.backend == "airbyte-message-json"
    assert [element.metadata["artifact_kind"] for element in result.elements] == [
        "record",
        "record",
        "state",
        "state",
        "log",
        "log",
    ]
    assert result.elements[0].metadata["data"] == {"id": "cust-1", "status": "active"}
    assert result.elements[0].metadata["emitted_at"] == "2026-04-01T00:00:00Z"
    assert result.elements[1].metadata["data"] == {"id": "cust-2", "status": "paused"}
    assert first_checkpoint["progress_kind"] == "cursor_v1"
    assert checkpoint_row["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": first_run_task_identity,
    }
    assert artifacts.checkpoint.task_identity_key == rerun_artifacts.checkpoint.task_identity_key
    assert artifacts.checkpoint.task_identity_key != (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    records_row = (tmp_path / "airbyte-artifacts" / "records.jsonl").read_text(encoding="utf-8")
    logs_row = (tmp_path / "airbyte-artifacts" / "logs.jsonl").read_text(encoding="utf-8")
    assert '"type"' not in records_row
    assert '"record"' not in records_row
    assert '"type"' not in logs_row
    assert '"log"' not in logs_row

    malformed_path = fixture_dir / "malformed_record_data.json"
    with pytest.raises(ValueError, match="record.data"):
        process_file(
            filename=str(malformed_path),
            strategy=PartitionStrategy.AUTO,
            sha256=file_sha256(malformed_path),
            size_bytes=malformed_path.stat().st_size,
        )

    unsupported_path = fixture_dir / "unsupported_trace_message.json"
    with pytest.raises(ValueError, match="Unsupported it-stream message type"):
        process_file(
            filename=str(unsupported_path),
            strategy=PartitionStrategy.AUTO,
            sha256=file_sha256(unsupported_path),
            size_bytes=unsupported_path.stat().st_size,
        )
