from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from it_stream import (
    ItCheckpointProvenanceV1,
    ItCheckpointResumeCursorContinuationV1,
    ItCheckpointResumeProvenanceV1,
    dump_it_artifacts,
    load_it_resume_selection,
    normalize_it_input_identity_sha,
    process_file,
    resume_file,
)
from it_stream.artifacts import load_it_checkpoint
from it_stream.tests._p45_source_wave2_helpers import (
    airbyte_fixture_dir,
    create_kafka_topic,
    file_sha256,
    kafka_bootstrap_servers,
    make_kafka_topic_name,
    mongodb_uri,
    produce_kafka_messages,
    reset_mongodb_collection,
    write_kafka_source_spec,
    write_mongodb_source_spec,
)
from zephyr_core import PartitionStrategy
from zephyr_ingest.testing.p45 import LoadedP45Env


def _simple_identifier_suffix(path: Path) -> str:
    return "".join(character if character.isalnum() else "_" for character in path.name.lower())


@pytest.mark.auth_recovery_drill
def test_p45_m5_kafka_partition_offset_recovery_drill_preserves_checkpoint_lineage_and_resume_tail(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    suffix = f"{_simple_identifier_suffix(tmp_path)}-{uuid4().hex[:8]}"
    topic = make_kafka_topic_name(prefix="p45-m5-recovery", suffix=suffix)
    create_kafka_topic(env=p45_env, topic=topic)
    produce_kafka_messages(
        env=p45_env,
        topic=topic,
        partition=0,
        values=(
            {"order_id": "ord-1", "status": "created"},
            {"order_id": "ord-2", "status": "paid"},
            {"order_id": "ord-3", "status": "shipped"},
        ),
    )

    source_path = tmp_path / "kafka-source.json"
    write_kafka_source_spec(
        source_path,
        stream="orders",
        connection_name="events-primary",
        brokers=kafka_bootstrap_servers(p45_env),
        topic=topic,
        partition=0,
        offset_start=None,
        batch_size=2,
    )

    identity_sha = normalize_it_input_identity_sha(
        filename=str(source_path),
        default_sha="fallback-kafka-recovery",
    )
    initial = process_file(
        filename=str(source_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "kafka-initial-artifacts",
        result=initial,
        pipeline_version="p45-m5-kafka",
    )
    first_checkpoint, second_checkpoint = initial_artifacts.checkpoint.checkpoints
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "kafka-initial-artifacts" / "checkpoint.json",
        pipeline_version="p45-m5-kafka",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(source_path),
        checkpoint_path=tmp_path / "kafka-initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p45-m5-kafka",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "kafka-resumed-artifacts",
        result=resumed,
        pipeline_version="p45-m5-kafka",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-kafka-m5-recovery",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "kafka-resumed-artifacts" / "checkpoint.json"
    )

    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )
    assert selection.continuation is not None
    assert selection.continuation.exclusive_after_cursor == "00000000000000000001"
    assert [element.metadata["artifact_kind"] for element in resumed.elements] == [
        "record",
        "state",
    ]
    assert resumed.elements[0].metadata["data"] == {
        "order_id": "ord-3",
        "status": "shipped",
    }
    assert resumed.elements[0].metadata["emitted_at"] == "00000000000000000002"
    assert resumed.elements[1].metadata["data"]["cursor"] == "00000000000000000002"
    assert loaded_resumed.provenance == ItCheckpointProvenanceV1(
        task_identity_key=initial_artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(
                exclusive_after_cursor="00000000000000000001"
            ),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


@pytest.mark.auth_recovery_drill
def test_p45_m5_mongodb_incremental_recovery_drill_preserves_checkpoint_lineage_and_resume_tail(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    pytest.importorskip("pymongo")

    suffix = _simple_identifier_suffix(tmp_path)
    database = p45_env.require("ZEPHYR_P45_MONGODB_DATABASE")
    collection = f"p45_m5_recovery_docs_{suffix}"
    reset_mongodb_collection(
        p45_env,
        database=database,
        collection=collection,
        documents=(
            {"doc_id": "cust-1", "doc_cursor": "cust-0001", "segment": "gold"},
            {"doc_id": "cust-2", "doc_cursor": "cust-0002", "segment": "silver"},
            {"doc_id": "cust-3", "doc_cursor": "cust-0003", "segment": "bronze"},
        ),
    )

    source_path = tmp_path / "mongodb-source.json"
    write_mongodb_source_spec(
        source_path,
        stream="customer-docs",
        connection_name="mongo-primary",
        uri=mongodb_uri(p45_env),
        database=database,
        collection=collection,
        fields=("doc_id", "doc_cursor", "segment"),
        cursor_field="doc_cursor",
        cursor_start="cust-0000",
        batch_size=2,
    )

    identity_sha = normalize_it_input_identity_sha(
        filename=str(source_path),
        default_sha="fallback-mongo-recovery",
    )
    initial = process_file(
        filename=str(source_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "mongo-initial-artifacts",
        result=initial,
        pipeline_version="p45-m5-mongodb",
    )
    first_checkpoint, second_checkpoint = initial_artifacts.checkpoint.checkpoints
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "mongo-initial-artifacts" / "checkpoint.json",
        pipeline_version="p45-m5-mongodb",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(source_path),
        checkpoint_path=tmp_path / "mongo-initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p45-m5-mongodb",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "mongo-resumed-artifacts",
        result=resumed,
        pipeline_version="p45-m5-mongodb",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-mongo-m5-recovery",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "mongo-resumed-artifacts" / "checkpoint.json"
    )

    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )
    assert selection.continuation is not None
    assert selection.continuation.exclusive_after_cursor == "cust-0002"
    assert [element.metadata["artifact_kind"] for element in resumed.elements] == [
        "record",
        "state",
    ]
    assert resumed.elements[0].metadata["data"] == {
        "doc_id": "cust-3",
        "doc_cursor": "cust-0003",
        "segment": "bronze",
    }
    assert resumed.elements[0].metadata["emitted_at"] == "cust-0003"
    assert resumed.elements[1].metadata["data"]["cursor"] == "cust-0003"
    assert loaded_resumed.provenance == ItCheckpointProvenanceV1(
        task_identity_key=initial_artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(exclusive_after_cursor="cust-0002"),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


@pytest.mark.auth_recovery_drill
def test_p45_m5_airbyte_message_json_recovery_drill_preserves_cursor_resume_provenance(
    tmp_path: Path,
) -> None:
    source_path = airbyte_fixture_dir() / "customer_sync_cursor.json"
    identity_sha = normalize_it_input_identity_sha(
        filename=str(source_path),
        default_sha=file_sha256(source_path),
    )
    initial = process_file(
        filename=str(source_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "airbyte-initial-artifacts",
        result=initial,
        pipeline_version="p45-m5-airbyte",
    )
    first_checkpoint, second_checkpoint = initial_artifacts.checkpoint.checkpoints
    checkpoint = load_it_checkpoint(path=tmp_path / "airbyte-initial-artifacts" / "checkpoint.json")
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "airbyte-initial-artifacts" / "checkpoint.json",
        pipeline_version="p45-m5-airbyte",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )

    resumed = resume_file(
        filename=str(source_path),
        checkpoint_path=tmp_path / "airbyte-initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p45-m5-airbyte",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "airbyte-resumed-artifacts",
        result=resumed,
        pipeline_version="p45-m5-airbyte",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-airbyte-m5-recovery",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "airbyte-resumed-artifacts" / "checkpoint.json"
    )

    assert checkpoint.checkpoints[0].progress_kind == "cursor_v1"
    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )
    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.continuation is not None
    assert selection.continuation.exclusive_after_cursor == "2026-04-01T00:00:00Z"
    assert [element.metadata["artifact_kind"] for element in resumed.elements] == [
        "record",
        "state",
    ]
    assert resumed.elements[0].metadata["data"] == {"id": "cust-2", "status": "paused"}
    assert resumed.elements[0].metadata["emitted_at"] == "2026-04-02T00:00:00Z"
    assert resumed.elements[1].metadata["data"] == {"cursor": "2026-04-02T00:00:00Z"}
    assert "after-first-page" not in resumed.normalized_text
    assert loaded_resumed.provenance == ItCheckpointProvenanceV1(
        task_identity_key=initial_artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(
                exclusive_after_cursor="2026-04-01T00:00:00Z"
            ),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance
