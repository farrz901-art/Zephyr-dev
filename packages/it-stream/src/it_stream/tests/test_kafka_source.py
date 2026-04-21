from __future__ import annotations

import json
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
from it_stream.sources import kafka_source
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


class _FakeKafkaMessage:
    def __init__(self, *, offset: int, value: object) -> None:
        self._offset = offset
        self._value = value

    def value(self) -> object:
        return self._value

    def offset(self) -> int:
        return self._offset

    def error(self) -> object | None:
        return None


class _FakeKafkaConsumer:
    def __init__(self, pages: dict[int, list[_FakeKafkaMessage]]) -> None:
        self._pages = pages
        self._offset = 0

    def assign(self, partitions: list[kafka_source.KafkaPartitionAssignmentV1]) -> None:
        self._offset = partitions[0].offset

    def consume(self, num_messages: int, timeout: float) -> list[_FakeKafkaMessage]:
        del timeout
        return list(self._pages.get(self._offset, []))[:num_messages]

    def close(self) -> None:
        return None


def _expected_kafka_operational_contract() -> dict[str, object]:
    return {
        "source_kind": "kafka_partition_offset_v1",
        "max_source_batches": 1000,
        "offset_cursor_width": 20,
        "default_poll_timeout_s": 1.0,
        "consumer_group_id_shape": "zephyr-it-stream-{connection_name}",
        "consumer_group_id": "zephyr-it-stream-events-primary",
        "read_model": "single_topic_single_partition_explicit_offset",
        "support_scope": (
            "bounded local it-stream source lane; not consumer-group ownership or "
            "rebalance semantics"
        ),
    }


def _write_kafka_source_spec(
    path: Path,
    *,
    brokers: list[str] | None = None,
    reverse_order: bool = False,
) -> None:
    payload = {
        "source": {
            "kind": "kafka_partition_offset_v1",
            "stream": "orders",
            "connection_name": "events-primary",
            "brokers": brokers or ["broker-a:9092", "broker-b:9092"],
            "topic": "orders.events",
            "partition": 2,
            "offset_start": 8,
            "batch_size": 2,
        }
    }
    if reverse_order:
        payload = {
            "source": {
                "batch_size": 2,
                "offset_start": 8,
                "partition": 2,
                "topic": "orders.events",
                "brokers": brokers or ["broker-b:9092", "broker-a:9092"],
                "connection_name": "events-primary",
                "stream": "orders",
                "kind": "kafka_partition_offset_v1",
            }
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_kafka_source_operational_contract_exposes_bounded_limits() -> None:
    contract = kafka_source.kafka_partition_source_operational_contract()

    assert contract.source_kind == "kafka_partition_offset_v1"
    assert contract.max_source_batches == 1000
    assert contract.offset_cursor_width == 20
    assert contract.default_poll_timeout_s == 1.0
    assert contract.consumer_group_id_prefix == "zephyr-it-stream"
    assert contract.consumer_group_id_shape == "zephyr-it-stream-{connection_name}"
    assert contract.read_model == "single_topic_single_partition_explicit_offset"
    assert "not consumer-group ownership" in contract.support_scope
    assert (
        kafka_source.kafka_partition_source_group_id(connection_name="events-primary")
        == "zephyr-it-stream-events-primary"
    )


def test_kafka_source_identity_sha_is_stable_for_equivalent_specs(tmp_path: Path) -> None:
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    third = tmp_path / "third.json"

    _write_kafka_source_spec(left)
    _write_kafka_source_spec(
        right,
        brokers=["broker-b:9092", "broker-a:9092"],
        reverse_order=True,
    )
    _write_kafka_source_spec(third)
    third_payload = json.loads(third.read_text(encoding="utf-8"))
    third_payload["source"]["partition"] = 3
    third.write_text(json.dumps(third_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    first = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-left")
    second = normalize_it_input_identity_sha(filename=str(right), default_sha="fallback-right")
    different = normalize_it_input_identity_sha(filename=str(third), default_sha="fallback-third")

    assert first == second
    assert first != different


def test_kafka_source_builds_cursor_checkpoints_and_resume_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_kafka_source_spec(path)

    pages = {
        9: [
            _FakeKafkaMessage(offset=9, value={"order_id": "ord-1", "status": "created"}),
            _FakeKafkaMessage(offset=10, value={"order_id": "ord-2", "status": "paid"}),
        ],
        11: [_FakeKafkaMessage(offset=11, value={"order_id": "ord-3", "status": "shipped"})],
        12: [],
    }

    def fake_connect(
        *,
        config: kafka_source.KafkaPartitionSourceConfigV1,
    ) -> _FakeKafkaConsumer:
        assert config.connection_name == "events-primary"
        assert config.topic == "orders.events"
        assert config.partition == 2
        return _FakeKafkaConsumer(pages)

    monkeypatch.setattr(kafka_source, "_connect_kafka_partition_source", fake_connect)

    identity_sha = normalize_it_input_identity_sha(filename=str(path), default_sha="fallback")
    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert result.engine.name == "it-stream"
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
    assert result.elements[0].metadata["emitted_at"] == "00000000000000000009"
    assert result.elements[3].metadata["data"] == {
        "cursor": "00000000000000000010",
        "connection_name": "events-primary",
        "topic": "orders.events",
        "partition": 2,
        "read_mode": "explicit_partition_offset",
        "read_direction": "asc",
        "record_count": 2,
        "last_offset": 10,
        "source_operational_contract": _expected_kafka_operational_contract(),
    }

    out_dir = tmp_path / "artifacts"
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result, pipeline_version="p-kafka")
    first_checkpoint, second_checkpoint = artifacts.checkpoint.checkpoints

    assert first_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )

    selection = load_it_resume_selection(
        checkpoint_path=out_dir / "checkpoint.json",
        pipeline_version="p-kafka",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )

    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "00000000000000000010"

    resumed = resume_file(
        filename=str(path),
        checkpoint_path=out_dir / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-kafka",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert [element.metadata["artifact_kind"] for element in resumed.elements] == [
        "record",
        "state",
    ]
    assert resumed.elements[0].metadata["data"] == {
        "order_id": "ord-3",
        "status": "shipped",
    }
    assert resumed.elements[0].metadata["emitted_at"] == "00000000000000000011"
    assert resumed.elements[1].metadata["data"]["cursor"] == "00000000000000000011"
    assert (
        resumed.elements[1].metadata["data"]["source_operational_contract"]
        == _expected_kafka_operational_contract()
    )

    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "resumed-artifacts",
        result=resumed,
        pipeline_version="p-kafka",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-kafka-resume",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(path=tmp_path / "resumed-artifacts" / "checkpoint.json")

    assert loaded_resumed.provenance == ItCheckpointProvenanceV1(
        task_identity_key=artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(
                exclusive_after_cursor="00000000000000000010"
            ),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


def test_kafka_source_requires_strictly_advancing_offsets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_kafka_source_spec(path)

    pages = {
        9: [
            _FakeKafkaMessage(offset=10, value={"order_id": "ord-1"}),
            _FakeKafkaMessage(offset=10, value={"order_id": "ord-2"}),
        ]
    }

    def fake_connect(
        *,
        config: kafka_source.KafkaPartitionSourceConfigV1,
    ) -> _FakeKafkaConsumer:
        del config
        return _FakeKafkaConsumer(pages)

    monkeypatch.setattr(kafka_source, "_connect_kafka_partition_source", fake_connect)

    with pytest.raises(ZephyrError) as exc_info:
        process_file(
            filename=str(path),
            strategy=PartitionStrategy.AUTO,
            sha256=normalize_it_input_identity_sha(filename=str(path), default_sha="fallback"),
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details == {
        "retryable": False,
        "source_kind": "kafka_partition_offset_v1",
        "connection_name": "events-primary",
        "topic": "orders.events",
        "partition": 2,
        "offset": 10,
        "previous_offset": 10,
    }


def test_kafka_source_fetch_uses_bounded_group_id_and_poll_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeNativeConsumer:
        def __init__(self, config: dict[str, object]) -> None:
            captured["consumer_config"] = dict(config)

        def list_topics(self, topic: str | None = None, timeout: float = 0.0) -> object:
            captured["metadata_topic"] = topic
            captured["metadata_timeout"] = timeout
            return object()

        def assign(self, assignments: list[object]) -> None:
            captured["assignment_count"] = len(assignments)

        def consume(self, num_messages: int, timeout: float) -> list[object]:
            captured["consume_num_messages"] = num_messages
            captured["consume_timeout"] = timeout
            return []

        def close(self) -> None:
            captured["closed"] = True

    class _FakeTopicPartition:
        def __init__(self, topic: str, partition: int, offset: int) -> None:
            del topic, partition, offset

    class _FakeKafkaModule:
        Consumer = _FakeNativeConsumer
        TopicPartition = _FakeTopicPartition

    def fake_import_module(name: str) -> object:
        assert name == "confluent_kafka"
        return _FakeKafkaModule

    monkeypatch.setattr(kafka_source.importlib, "import_module", fake_import_module)

    result = kafka_source.fetch_kafka_partition_source(
        config=kafka_source.KafkaPartitionSourceConfigV1(
            stream="orders",
            connection_name="events-primary",
            brokers=("broker-a:9092", "broker-b:9092"),
            topic="orders.events",
            partition=2,
            offset_start=8,
            batch_size=2,
        )
    )

    assert result.logs == [("INFO", "source exhausted after batch=1")]
    assert captured["consumer_config"] == {
        "bootstrap.servers": "broker-a:9092,broker-b:9092",
        "group.id": "zephyr-it-stream-events-primary",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
        "auto.offset.reset": "error",
    }
    assert captured["metadata_topic"] == "orders.events"
    assert captured["metadata_timeout"] == 1.0
    assert captured["assignment_count"] == 1
    assert captured["consume_num_messages"] == 2
    assert captured["consume_timeout"] == 1.0
    assert captured["closed"] is True
