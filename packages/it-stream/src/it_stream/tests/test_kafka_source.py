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
