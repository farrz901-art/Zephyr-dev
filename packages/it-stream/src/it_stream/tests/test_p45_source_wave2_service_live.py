from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from it_stream import dump_it_artifacts, normalize_it_input_identity_sha, process_file
from it_stream.tests._p45_source_wave2_helpers import (
    create_kafka_topic,
    kafka_bootstrap_servers,
    make_kafka_topic_name,
    mongodb_uri,
    produce_kafka_messages,
    read_json_dict,
    reserve_unused_port,
    reset_mongodb_collection,
    write_kafka_source_spec,
    write_mongodb_source_spec,
)
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError
from zephyr_ingest.testing.p45 import LoadedP45Env


def _simple_identifier_suffix(path: Path) -> str:
    return "".join(character if character.isalnum() else "_" for character in path.name.lower())


@pytest.mark.auth_service_live
def test_p45_m5_kafka_partition_offset_live_success_failure_and_identity_stability(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    suffix = f"{_simple_identifier_suffix(tmp_path)}-{uuid4().hex[:8]}"
    topic = make_kafka_topic_name(prefix="p45-m5-orders", suffix=suffix)
    create_kafka_topic(env=p45_env, topic=topic)
    offsets = produce_kafka_messages(
        env=p45_env,
        topic=topic,
        partition=0,
        values=(
            {"order_id": "ord-1", "status": "created"},
            {"order_id": "ord-2", "status": "paid"},
            {"order_id": "ord-3", "status": "shipped"},
        ),
    )
    assert offsets == [0, 1, 2]

    brokers = kafka_bootstrap_servers(p45_env)
    left = tmp_path / "kafka-left.json"
    right = tmp_path / "kafka-right.json"
    third = tmp_path / "kafka-third.json"
    write_kafka_source_spec(
        left,
        stream="orders",
        connection_name="events-primary",
        brokers=brokers,
        topic=topic,
        partition=0,
        offset_start=None,
        batch_size=2,
    )
    right.write_text(
        json.dumps(
            {
                "source": {
                    "batch_size": 2,
                    "offset_start": None,
                    "partition": 0,
                    "topic": topic,
                    "brokers": list(brokers),
                    "connection_name": "events-primary",
                    "stream": "orders",
                    "kind": "kafka_partition_offset_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_kafka_source_spec(
        third,
        stream="orders",
        connection_name="events-primary",
        brokers=brokers,
        topic=topic,
        partition=0,
        offset_start=1,
        batch_size=2,
    )

    first_sha = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-kafka-1")
    second_sha = normalize_it_input_identity_sha(
        filename=str(right),
        default_sha="fallback-kafka-2",
    )
    different_sha = normalize_it_input_identity_sha(
        filename=str(third),
        default_sha="fallback-kafka-3",
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
        out_dir=tmp_path / "kafka-artifacts",
        result=result,
        pipeline_version="p45-m5-kafka",
    )
    checkpoint_row = read_json_dict(tmp_path / "kafka-artifacts" / "checkpoint.json")
    record_lines = (
        (tmp_path / "kafka-artifacts" / "records.jsonl").read_text(encoding="utf-8").splitlines()
    )

    assert result.engine.backend == "kafka-partition-offset"
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
    assert result.elements[0].metadata["emitted_at"] == "00000000000000000000"
    assert result.elements[3].metadata["data"] == {
        "cursor": "00000000000000000001",
        "connection_name": "events-primary",
        "topic": topic,
        "partition": 0,
        "read_mode": "explicit_partition_offset",
        "read_direction": "asc",
        "record_count": 2,
        "last_offset": 1,
    }
    assert artifacts.checkpoint.task_identity_key != (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert json.loads(record_lines[0]) == {
        "data": {"order_id": "ord-1", "status": "created"},
        "emitted_at": "00000000000000000000",
        "record_index": 0,
        "stream": "orders",
    }
    assert checkpoint_row["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": artifacts.checkpoint.task_identity_key,
    }

    retryable_spec = tmp_path / "kafka-retryable.json"
    write_kafka_source_spec(
        retryable_spec,
        stream="orders",
        connection_name="events-primary",
        brokers=(f"127.0.0.1:{reserve_unused_port()}",),
        topic=topic,
        partition=0,
        offset_start=None,
        batch_size=2,
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-kafka-retryable",
            size_bytes=retryable_spec.stat().st_size,
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    retryable_details = retryable_exc.value.details
    assert retryable_details is not None
    assert retryable_details["retryable"] is True
    assert retryable_details["source_kind"] == "kafka_partition_offset_v1"
    assert retryable_details["topic"] == topic
    assert retryable_details["partition"] == 0

    malformed_topic = make_kafka_topic_name(prefix="p45-m5-orders-bad", suffix=suffix)
    create_kafka_topic(env=p45_env, topic=malformed_topic)
    malformed_offsets = produce_kafka_messages(
        env=p45_env,
        topic=malformed_topic,
        partition=0,
        values=("not-json-object",),
    )
    malformed_spec = tmp_path / "kafka-malformed.json"
    write_kafka_source_spec(
        malformed_spec,
        stream="orders",
        connection_name="events-primary",
        brokers=brokers,
        topic=malformed_topic,
        partition=0,
        offset_start=None,
        batch_size=1,
    )
    with pytest.raises(ZephyrError) as malformed_exc:
        process_file(
            filename=str(malformed_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-kafka-malformed",
            size_bytes=malformed_spec.stat().st_size,
        )

    assert malformed_exc.value.code == ErrorCode.IO_READ_FAILED
    assert malformed_exc.value.details == {
        "retryable": False,
        "source_kind": "kafka_partition_offset_v1",
        "connection_name": "events-primary",
        "topic": malformed_topic,
        "partition": 0,
        "offset": malformed_offsets[0],
    }


@pytest.mark.auth_service_live
def test_p45_m5_mongodb_incremental_live_success_failure_and_identity_stability(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    pytest.importorskip("pymongo")

    suffix = _simple_identifier_suffix(tmp_path)
    database = p45_env.require("ZEPHYR_P45_MONGODB_DATABASE")
    collection = f"p45_m5_docs_{suffix}"
    fields = ("doc_id", "doc_cursor", "segment")
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

    left = tmp_path / "mongo-left.json"
    right = tmp_path / "mongo-right.json"
    third = tmp_path / "mongo-third.json"
    write_mongodb_source_spec(
        left,
        stream="customer-docs",
        connection_name="mongo-primary",
        uri=mongodb_uri(p45_env),
        database=database,
        collection=collection,
        fields=fields,
        cursor_field="doc_cursor",
        cursor_start="cust-0000",
        batch_size=2,
    )
    right.write_text(
        json.dumps(
            {
                "source": {
                    "batch_size": 2,
                    "cursor_start": "cust-0000",
                    "cursor_field": "doc_cursor",
                    "fields": list(fields),
                    "collection": collection,
                    "database": database,
                    "uri": mongodb_uri(p45_env, password="rotated-password"),
                    "connection_name": "mongo-primary",
                    "stream": "customer-docs",
                    "kind": "mongodb_incremental_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_mongodb_source_spec(
        third,
        stream="customer-docs",
        connection_name="mongo-primary",
        uri=mongodb_uri(p45_env),
        database=database,
        collection=collection,
        fields=fields,
        cursor_field="doc_cursor",
        cursor_start="cust-0001",
        batch_size=2,
    )

    first_sha = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-mongo-1")
    second_sha = normalize_it_input_identity_sha(
        filename=str(right),
        default_sha="fallback-mongo-2",
    )
    different_sha = normalize_it_input_identity_sha(
        filename=str(third),
        default_sha="fallback-mongo-3",
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
        out_dir=tmp_path / "mongo-artifacts",
        result=result,
        pipeline_version="p45-m5-mongodb",
    )
    checkpoint_row = read_json_dict(tmp_path / "mongo-artifacts" / "checkpoint.json")
    record_lines = (
        (tmp_path / "mongo-artifacts" / "records.jsonl").read_text(encoding="utf-8").splitlines()
    )

    assert result.engine.backend == "mongodb-incremental"
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
        "connection_name": "mongo-primary",
        "database": database,
        "collection": collection,
        "cursor_field": "doc_cursor",
        "fields": ["doc_id", "doc_cursor", "segment"],
        "read_direction": "asc",
        "document_count": 2,
    }
    assert artifacts.checkpoint.task_identity_key != (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert json.loads(record_lines[0]) == {
        "data": {"doc_id": "cust-1", "doc_cursor": "cust-0001", "segment": "gold"},
        "emitted_at": "cust-0001",
        "record_index": 0,
        "stream": "customer-docs",
    }
    assert checkpoint_row["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": artifacts.checkpoint.task_identity_key,
    }

    bad_auth_spec = tmp_path / "mongo-bad-auth.json"
    write_mongodb_source_spec(
        bad_auth_spec,
        stream="customer-docs",
        connection_name="mongo-primary",
        uri=mongodb_uri(p45_env, password="wrong-password"),
        database=database,
        collection=collection,
        fields=fields,
        cursor_field="doc_cursor",
        cursor_start="cust-0000",
        batch_size=2,
    )
    with pytest.raises(ZephyrError) as bad_auth_exc:
        process_file(
            filename=str(bad_auth_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-mongo-bad-auth",
            size_bytes=bad_auth_spec.stat().st_size,
        )

    assert bad_auth_exc.value.code == ErrorCode.IO_READ_FAILED
    bad_auth_details = bad_auth_exc.value.details
    assert bad_auth_details is not None
    assert bad_auth_details["retryable"] is False
    assert bad_auth_details["source_kind"] == "mongodb_incremental_v1"
    assert bad_auth_details["database"] == database
    assert bad_auth_details["collection"] == collection

    retryable_spec = tmp_path / "mongo-retryable.json"
    write_mongodb_source_spec(
        retryable_spec,
        stream="customer-docs",
        connection_name="mongo-primary",
        uri=mongodb_uri(p45_env, port=reserve_unused_port()),
        database=database,
        collection=collection,
        fields=fields,
        cursor_field="doc_cursor",
        cursor_start="cust-0000",
        batch_size=2,
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-mongo-retryable",
            size_bytes=retryable_spec.stat().st_size,
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    retryable_details = retryable_exc.value.details
    assert retryable_details is not None
    assert retryable_details["retryable"] is True
    assert retryable_details["source_kind"] == "mongodb_incremental_v1"
    assert retryable_details["database"] == database
    assert retryable_details["collection"] == collection
    assert retryable_details["cursor_field"] == "doc_cursor"
    assert retryable_details["after_cursor"] == "cust-0000"

    malformed_collection = f"p45_m5_bad_docs_{suffix}"
    reset_mongodb_collection(
        p45_env,
        database=database,
        collection=malformed_collection,
        documents=({"doc_id": "cust-1", "doc_cursor": "cust-0001"},),
    )
    malformed_spec = tmp_path / "mongo-malformed.json"
    write_mongodb_source_spec(
        malformed_spec,
        stream="customer-docs",
        connection_name="mongo-primary",
        uri=mongodb_uri(p45_env),
        database=database,
        collection=malformed_collection,
        fields=fields,
        cursor_field="doc_cursor",
        cursor_start="cust-0000",
        batch_size=2,
    )
    with pytest.raises(ZephyrError) as malformed_exc:
        process_file(
            filename=str(malformed_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-mongo-malformed",
            size_bytes=malformed_spec.stat().st_size,
        )

    assert malformed_exc.value.code == ErrorCode.IO_READ_FAILED
    assert malformed_exc.value.details == {
        "retryable": False,
        "source_kind": "mongodb_incremental_v1",
        "connection_name": "mongo-primary",
        "database": database,
        "collection": malformed_collection,
        "field": "segment",
    }
