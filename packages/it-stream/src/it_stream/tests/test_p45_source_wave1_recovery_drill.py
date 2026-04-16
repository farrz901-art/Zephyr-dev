from __future__ import annotations

from pathlib import Path

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
from it_stream.tests._p45_source_wave1_helpers import (
    clickhouse_url,
    http_fixture_base,
    postgresql_dsn,
    reset_clickhouse_cursor_table,
    reset_postgresql_cursor_table,
    write_clickhouse_source_spec,
    write_http_source_spec,
    write_postgresql_source_spec,
)
from zephyr_core import PartitionStrategy
from zephyr_ingest.testing.p45 import LoadedP45Env


@pytest.mark.auth_recovery_drill
def test_p45_m4_http_json_cursor_recovery_drill_preserves_state_only_resume_provenance(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    source_path = tmp_path / "http-source.json"
    write_http_source_spec(
        source_path,
        stream="fixture-empty",
        url=f"{http_fixture_base(p45_env)}/records/cursor-empty",
    )

    identity_sha = normalize_it_input_identity_sha(
        filename=str(source_path),
        default_sha="fallback-http-recovery",
    )
    initial = process_file(
        filename=str(source_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "http-initial-artifacts",
        result=initial,
        pipeline_version="p45-m4-http",
    )
    first_checkpoint, second_checkpoint = initial_artifacts.checkpoint.checkpoints

    assert [element.metadata["artifact_kind"] for element in initial.elements] == [
        "state",
        "state",
        "log",
        "log",
        "log",
        "log",
    ]
    assert first_checkpoint.parent_checkpoint_identity_key is None
    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )

    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "http-initial-artifacts" / "checkpoint.json",
        pipeline_version="p45-m4-http",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(source_path),
        checkpoint_path=tmp_path / "http-initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p45-m4-http",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "http-resumed-artifacts",
        result=resumed,
        pipeline_version="p45-m4-http",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-http-recovery",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "http-resumed-artifacts" / "checkpoint.json"
    )

    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "page-2"
    assert [element.metadata["artifact_kind"] for element in resumed.elements] == ["state"]
    assert resumed.elements[0].metadata["data"] == {
        "cursor": "page-3",
        "page_number": 2,
        "source_url": f"{http_fixture_base(p45_env)}/records/cursor-empty",
        "record_count": 0,
    }
    assert loaded_resumed.provenance == ItCheckpointProvenanceV1(
        task_identity_key=initial_artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(exclusive_after_cursor="page-2"),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


@pytest.mark.auth_recovery_drill
def test_p45_m4_postgresql_incremental_recovery_drill_preserves_checkpoint_lineage_and_resume_tail(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    table_name = "p45_m4_pg_recovery"
    reset_postgresql_cursor_table(
        p45_env,
        table=table_name,
        rows=(
            ("cust-1", "cust-0001", "Ada"),
            ("cust-2", "cust-0002", "Bob"),
            ("cust-3", "cust-0003", "Cy"),
        ),
    )

    source_path = tmp_path / "postgres-source.json"
    write_postgresql_source_spec(
        source_path,
        stream="pg-customers",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(p45_env),
        schema="public",
        table=table_name,
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
    )

    identity_sha = normalize_it_input_identity_sha(
        filename=str(source_path),
        default_sha="fallback-postgresql-recovery",
    )
    initial = process_file(
        filename=str(source_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "postgres-initial-artifacts",
        result=initial,
        pipeline_version="p45-m4-postgresql",
    )
    first_checkpoint, second_checkpoint = initial_artifacts.checkpoint.checkpoints
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "postgres-initial-artifacts" / "checkpoint.json",
        pipeline_version="p45-m4-postgresql",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(source_path),
        checkpoint_path=tmp_path / "postgres-initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p45-m4-postgresql",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "postgres-resumed-artifacts",
        result=resumed,
        pipeline_version="p45-m4-postgresql",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-postgresql-recovery",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "postgres-resumed-artifacts" / "checkpoint.json"
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
        "customer_id": "cust-3",
        "doc_cursor": "cust-0003",
        "name": "Cy",
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
def test_p45_m4_clickhouse_incremental_recovery_drill_preserves_checkpoint_lineage_and_resume_tail(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    table_name = "p45_m4_clickhouse_recovery"
    reset_clickhouse_cursor_table(
        p45_env,
        table=table_name,
        rows=(
            ("cust-1", "cust-0001", "gold"),
            ("cust-2", "cust-0002", "silver"),
            ("cust-3", "cust-0003", "bronze"),
        ),
    )

    source_path = tmp_path / "clickhouse-source.json"
    write_clickhouse_source_spec(
        source_path,
        stream="warehouse-customers",
        connection_name="analytics-primary",
        url=clickhouse_url(p45_env),
        database=p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
        table=table_name,
        columns=("customer_id", "doc_cursor", "segment"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
        username=p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
        password=p45_env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
    )

    identity_sha = normalize_it_input_identity_sha(
        filename=str(source_path),
        default_sha="fallback-clickhouse-recovery",
    )
    initial = process_file(
        filename=str(source_path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "clickhouse-initial-artifacts",
        result=initial,
        pipeline_version="p45-m4-clickhouse",
    )
    first_checkpoint, second_checkpoint = initial_artifacts.checkpoint.checkpoints
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "clickhouse-initial-artifacts" / "checkpoint.json",
        pipeline_version="p45-m4-clickhouse",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(source_path),
        checkpoint_path=tmp_path / "clickhouse-initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p45-m4-clickhouse",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=source_path.stat().st_size,
    )
    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "clickhouse-resumed-artifacts",
        result=resumed,
        pipeline_version="p45-m4-clickhouse",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-clickhouse-recovery",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "clickhouse-resumed-artifacts" / "checkpoint.json"
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
        "customer_id": "cust-3",
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
