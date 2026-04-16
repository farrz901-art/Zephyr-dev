from __future__ import annotations

import json
from pathlib import Path

import pytest

from it_stream import dump_it_artifacts, normalize_it_input_identity_sha, process_file
from it_stream.tests._p45_source_wave1_helpers import (
    clickhouse_url,
    http_fixture_base,
    postgresql_dsn,
    read_json_dict,
    reserve_unused_port,
    reset_clickhouse_cursor_table,
    reset_postgresql_cursor_table,
    write_clickhouse_source_spec,
    write_http_source_spec,
    write_postgresql_source_spec,
)
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError
from zephyr_ingest.testing.p45 import LoadedP45Env


@pytest.mark.auth_service_live
def test_p45_m4_http_json_cursor_live_success_failure_and_identity_stability(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    base_url = http_fixture_base(p45_env)
    left = tmp_path / "http-left.json"
    right = tmp_path / "http-right.json"
    third = tmp_path / "http-third.json"
    write_http_source_spec(
        left,
        stream="fixture-customers",
        url=f"{base_url}/records/cursor",
        query={"limit": "2", "region": "test"},
    )
    right.write_text(
        json.dumps(
            {
                "source": {
                    "query": {"region": "test", "limit": "2"},
                    "cursor_param": "cursor",
                    "url": f"{base_url}/records/cursor",
                    "stream": "fixture-customers",
                    "kind": "http_json_cursor_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_http_source_spec(
        third,
        stream="fixture-customers",
        url=f"{base_url}/records/cursor",
        query={"limit": "3", "region": "test"},
    )

    first_sha = normalize_it_input_identity_sha(
        filename=str(left),
        default_sha="fallback-http-left",
    )
    second_sha = normalize_it_input_identity_sha(
        filename=str(right),
        default_sha="fallback-http-right",
    )
    different_sha = normalize_it_input_identity_sha(
        filename=str(third),
        default_sha="fallback-http-third",
    )

    assert first_sha == second_sha
    assert first_sha != different_sha

    result = process_file(
        filename=str(left),
        strategy=PartitionStrategy.AUTO,
        sha256=first_sha,
        size_bytes=left.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "http-artifacts",
        result=result,
        pipeline_version="p45-m4-http",
    )
    record_lines = (
        (tmp_path / "http-artifacts" / "records.jsonl").read_text(encoding="utf-8").splitlines()
    )
    checkpoint_row = read_json_dict(tmp_path / "http-artifacts" / "checkpoint.json")

    assert result.engine.backend == "http-json-cursor"
    assert [element.metadata["artifact_kind"] for element in result.elements] == [
        "record",
        "record",
        "state",
        "log",
        "log",
        "log",
    ]
    assert result.elements[2].metadata["data"] == {
        "cursor": "page-2",
        "page_number": 1,
        "source_url": f"{base_url}/records/cursor",
        "record_count": 1,
    }
    assert artifacts.checkpoint.task_identity_key != (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert json.loads(record_lines[0]) == {
        "data": {"id": 1, "kind": "fixture"},
        "emitted_at": None,
        "record_index": 0,
        "stream": "fixture-customers",
    }
    assert checkpoint_row["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": artifacts.checkpoint.task_identity_key,
    }

    retryable_spec = tmp_path / "http-retryable.json"
    write_http_source_spec(
        retryable_spec,
        stream="fixture-customers",
        url=f"http://127.0.0.1:{reserve_unused_port()}/records/cursor",
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-http-retryable",
            size_bytes=retryable_spec.stat().st_size,
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    retryable_details = retryable_exc.value.details
    assert retryable_details is not None
    assert retryable_details["retryable"] is True
    assert retryable_details["source_kind"] == "http_json_cursor_v1"
    assert isinstance(retryable_details["url"], str)
    assert isinstance(retryable_details["reason"], str)

    malformed_spec = tmp_path / "http-malformed.json"
    write_http_source_spec(
        malformed_spec,
        stream="fixture-customers",
        url=f"{base_url}/records/malformed",
    )
    with pytest.raises(ZephyrError) as malformed_exc:
        process_file(
            filename=str(malformed_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-http-malformed",
            size_bytes=malformed_spec.stat().st_size,
        )

    assert malformed_exc.value.code == ErrorCode.IO_READ_FAILED
    assert malformed_exc.value.details == {
        "retryable": False,
        "source_kind": "http_json_cursor_v1",
        "url": f"{base_url}/records/malformed",
    }


@pytest.mark.auth_service_live
def test_p45_m4_postgresql_incremental_live_success_failure_and_identity_stability(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    table_name = "p45_m4_pg_customers"
    reset_postgresql_cursor_table(
        p45_env,
        table=table_name,
        rows=(
            ("cust-1", "cust-0001", "Ada"),
            ("cust-2", "cust-0002", "Bob"),
            ("cust-3", "cust-0003", "Cy"),
        ),
    )

    left = tmp_path / "postgres-left.json"
    right = tmp_path / "postgres-right.json"
    third = tmp_path / "postgres-third.json"
    write_postgresql_source_spec(
        left,
        stream="pg-customers",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(p45_env),
        schema="public",
        table=table_name,
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
    )
    right.write_text(
        json.dumps(
            {
                "source": {
                    "batch_size": 2,
                    "cursor_start": "cust-0000",
                    "cursor_column": "doc_cursor",
                    "columns": ["customer_id", "doc_cursor", "name"],
                    "table": table_name,
                    "schema": "public",
                    "dsn": postgresql_dsn(p45_env, password="rotated-secret"),
                    "connection_name": "warehouse-primary",
                    "stream": "pg-customers",
                    "kind": "postgresql_incremental_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_postgresql_source_spec(
        third,
        stream="pg-customers",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(p45_env),
        schema="public",
        table=table_name,
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0001",
    )

    first_sha = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-pg-left")
    second_sha = normalize_it_input_identity_sha(
        filename=str(right),
        default_sha="fallback-pg-right",
    )
    different_sha = normalize_it_input_identity_sha(
        filename=str(third),
        default_sha="fallback-pg-third",
    )

    assert first_sha == second_sha
    assert first_sha != different_sha

    result = process_file(
        filename=str(left),
        strategy=PartitionStrategy.AUTO,
        sha256=first_sha,
        size_bytes=left.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "postgres-artifacts",
        result=result,
        pipeline_version="p45-m4-postgresql",
    )
    record_lines = (
        (tmp_path / "postgres-artifacts" / "records.jsonl").read_text(encoding="utf-8").splitlines()
    )
    checkpoint_row = read_json_dict(tmp_path / "postgres-artifacts" / "checkpoint.json")

    assert result.engine.backend == "postgresql-incremental"
    assert [element.metadata["artifact_kind"] for element in result.elements] == [
        "record",
        "record",
        "record",
        "state",
        "state",
        "log",
        "log",
        "log",
    ]
    assert result.elements[0].metadata["emitted_at"] == "cust-0001"
    assert result.elements[3].metadata["data"] == {
        "cursor": "cust-0002",
        "connection_name": "warehouse-primary",
        "schema": "public",
        "table": table_name,
        "cursor_column": "doc_cursor",
        "columns": ["customer_id", "doc_cursor", "name"],
        "read_direction": "asc",
        "row_count": 2,
    }
    assert artifacts.checkpoint.task_identity_key != (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert json.loads(record_lines[0]) == {
        "data": {"customer_id": "cust-1", "doc_cursor": "cust-0001", "name": "Ada"},
        "emitted_at": "cust-0001",
        "record_index": 0,
        "stream": "pg-customers",
    }
    assert checkpoint_row["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": artifacts.checkpoint.task_identity_key,
    }

    bad_auth_spec = tmp_path / "postgres-bad-auth.json"
    write_postgresql_source_spec(
        bad_auth_spec,
        stream="pg-customers",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(p45_env, password="wrong-password"),
        schema="public",
        table=table_name,
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
    )
    with pytest.raises(ZephyrError) as bad_auth_exc:
        process_file(
            filename=str(bad_auth_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-pg-bad-auth",
            size_bytes=bad_auth_spec.stat().st_size,
        )

    assert bad_auth_exc.value.code == ErrorCode.IO_READ_FAILED
    assert bad_auth_exc.value.details is not None
    assert bad_auth_exc.value.details["retryable"] is False
    assert bad_auth_exc.value.details["source_kind"] == "postgresql_incremental_v1"
    assert bad_auth_exc.value.details["connection_name"] == "warehouse-primary"

    retryable_spec = tmp_path / "postgres-retryable.json"
    write_postgresql_source_spec(
        retryable_spec,
        stream="pg-customers",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(p45_env, port=reserve_unused_port()),
        schema="public",
        table=table_name,
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-pg-retryable",
            size_bytes=retryable_spec.stat().st_size,
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    assert retryable_exc.value.details is not None
    assert retryable_exc.value.details["retryable"] is True
    assert retryable_exc.value.details["source_kind"] == "postgresql_incremental_v1"

    missing_table_spec = tmp_path / "postgres-missing-table.json"
    write_postgresql_source_spec(
        missing_table_spec,
        stream="pg-customers",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(p45_env),
        schema="public",
        table="p45_m4_pg_missing",
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
    )
    with pytest.raises(ZephyrError) as missing_table_exc:
        process_file(
            filename=str(missing_table_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-pg-missing",
            size_bytes=missing_table_spec.stat().st_size,
        )

    assert missing_table_exc.value.code == ErrorCode.IO_READ_FAILED
    assert missing_table_exc.value.details is not None
    assert missing_table_exc.value.details["retryable"] is False
    assert missing_table_exc.value.details["source_kind"] == "postgresql_incremental_v1"
    assert missing_table_exc.value.details["table"] == "p45_m4_pg_missing"


@pytest.mark.auth_service_live
def test_p45_m4_clickhouse_incremental_live_success_failure_and_identity_stability(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    table_name = "p45_m4_clickhouse_customers"
    reset_clickhouse_cursor_table(
        p45_env,
        table=table_name,
        rows=(
            ("cust-1", "cust-0001", "gold"),
            ("cust-2", "cust-0002", "silver"),
            ("cust-3", "cust-0003", "bronze"),
        ),
    )

    left = tmp_path / "clickhouse-left.json"
    right = tmp_path / "clickhouse-right.json"
    third = tmp_path / "clickhouse-third.json"
    write_clickhouse_source_spec(
        left,
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
    right.write_text(
        json.dumps(
            {
                "source": {
                    "password": "rotated-secret",
                    "username": p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
                    "batch_size": 2,
                    "cursor_start": "cust-0000",
                    "cursor_column": "doc_cursor",
                    "columns": ["customer_id", "doc_cursor", "segment"],
                    "table": table_name,
                    "database": p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
                    "url": clickhouse_url(p45_env),
                    "connection_name": "analytics-primary",
                    "stream": "warehouse-customers",
                    "kind": "clickhouse_incremental_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_clickhouse_source_spec(
        third,
        stream="warehouse-customers",
        connection_name="analytics-primary",
        url=clickhouse_url(p45_env),
        database=p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
        table=table_name,
        columns=("customer_id", "doc_cursor", "segment"),
        cursor_column="doc_cursor",
        cursor_start="cust-0001",
        username=p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
        password=p45_env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
    )

    first_sha = normalize_it_input_identity_sha(
        filename=str(left),
        default_sha="fallback-clickhouse-left",
    )
    second_sha = normalize_it_input_identity_sha(
        filename=str(right),
        default_sha="fallback-clickhouse-right",
    )
    different_sha = normalize_it_input_identity_sha(
        filename=str(third),
        default_sha="fallback-clickhouse-third",
    )

    assert first_sha == second_sha
    assert first_sha != different_sha

    result = process_file(
        filename=str(left),
        strategy=PartitionStrategy.AUTO,
        sha256=first_sha,
        size_bytes=left.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "clickhouse-artifacts",
        result=result,
        pipeline_version="p45-m4-clickhouse",
    )
    record_lines = (
        (tmp_path / "clickhouse-artifacts" / "records.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    checkpoint_row = read_json_dict(tmp_path / "clickhouse-artifacts" / "checkpoint.json")

    assert result.engine.backend == "clickhouse-incremental"
    assert [element.metadata["artifact_kind"] for element in result.elements] == [
        "record",
        "record",
        "record",
        "state",
        "state",
        "log",
        "log",
        "log",
    ]
    assert result.elements[0].metadata["emitted_at"] == "cust-0001"
    assert result.elements[3].metadata["data"] == {
        "cursor": "cust-0002",
        "connection_name": "analytics-primary",
        "database": p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
        "table": table_name,
        "cursor_column": "doc_cursor",
        "columns": ["customer_id", "doc_cursor", "segment"],
        "query_mode": "incremental_table_query",
        "read_direction": "asc",
        "row_count": 2,
    }
    assert artifacts.checkpoint.task_identity_key != (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert json.loads(record_lines[0]) == {
        "data": {"customer_id": "cust-1", "doc_cursor": "cust-0001", "segment": "gold"},
        "emitted_at": "cust-0001",
        "record_index": 0,
        "stream": "warehouse-customers",
    }
    assert checkpoint_row["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": artifacts.checkpoint.task_identity_key,
    }

    bad_auth_spec = tmp_path / "clickhouse-bad-auth.json"
    write_clickhouse_source_spec(
        bad_auth_spec,
        stream="warehouse-customers",
        connection_name="analytics-primary",
        url=clickhouse_url(p45_env),
        database=p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
        table=table_name,
        columns=("customer_id", "doc_cursor", "segment"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
        username=p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
        password="wrong-password",
    )
    with pytest.raises(ZephyrError) as bad_auth_exc:
        process_file(
            filename=str(bad_auth_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-clickhouse-bad-auth",
            size_bytes=bad_auth_spec.stat().st_size,
        )

    assert bad_auth_exc.value.code == ErrorCode.IO_READ_FAILED
    assert bad_auth_exc.value.details is not None
    assert bad_auth_exc.value.details["retryable"] is False
    assert bad_auth_exc.value.details["source_kind"] == "clickhouse_incremental_v1"
    assert bad_auth_exc.value.details["connection_name"] == "analytics-primary"

    retryable_spec = tmp_path / "clickhouse-retryable.json"
    write_clickhouse_source_spec(
        retryable_spec,
        stream="warehouse-customers",
        connection_name="analytics-primary",
        url=clickhouse_url(p45_env, port=reserve_unused_port()),
        database=p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
        table=table_name,
        columns=("customer_id", "doc_cursor", "segment"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
        username=p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
        password=p45_env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-clickhouse-retryable",
            size_bytes=retryable_spec.stat().st_size,
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    assert retryable_exc.value.details is not None
    assert retryable_exc.value.details["retryable"] is True
    assert retryable_exc.value.details["source_kind"] == "clickhouse_incremental_v1"

    missing_table_spec = tmp_path / "clickhouse-missing-table.json"
    write_clickhouse_source_spec(
        missing_table_spec,
        stream="warehouse-customers",
        connection_name="analytics-primary",
        url=clickhouse_url(p45_env),
        database=p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
        table="p45_m4_clickhouse_missing",
        columns=("customer_id", "doc_cursor", "segment"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
        username=p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
        password=p45_env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
    )
    with pytest.raises(ZephyrError) as missing_table_exc:
        process_file(
            filename=str(missing_table_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-clickhouse-missing",
            size_bytes=missing_table_spec.stat().st_size,
        )

    assert missing_table_exc.value.code == ErrorCode.IO_READ_FAILED
    assert missing_table_exc.value.details is not None
    assert missing_table_exc.value.details["retryable"] is False
    assert missing_table_exc.value.details["source_kind"] == "clickhouse_incremental_v1"
    assert missing_table_exc.value.details["table"] == "p45_m4_clickhouse_missing"
