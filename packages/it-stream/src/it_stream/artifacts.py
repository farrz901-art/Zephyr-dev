from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from it_stream.identity import (
    ItCheckpointIdentityV1,
    ItTaskIdentityV1,
    normalize_it_checkpoint_identity_key,
    normalize_it_task_identity_key,
)
from zephyr_core import PartitionResult

IT_CHECKPOINT_SCHEMA_VERSION: Literal[1] = 1


@dataclass(frozen=True, slots=True)
class ItArtifactRecordV1:
    stream: str
    record_index: int
    data: dict[str, object]
    emitted_at: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "stream": self.stream,
            "record_index": self.record_index,
            "emitted_at": self.emitted_at,
            "data": self.data,
        }


@dataclass(frozen=True, slots=True)
class ItArtifactStateV1:
    state_index: int
    data: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "state_index": self.state_index,
            "data": self.data,
        }


@dataclass(frozen=True, slots=True)
class ItArtifactLogV1:
    log_index: int
    level: str
    message: str

    def to_dict(self) -> dict[str, object]:
        return {
            "log_index": self.log_index,
            "level": self.level,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class ItCheckpointEntryV1:
    checkpoint_index: int
    checkpoint_identity_key: str
    progress: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "checkpoint_index": self.checkpoint_index,
            "checkpoint_identity_key": self.checkpoint_identity_key,
            "progress": self.progress,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ItCheckpointEntryV1:
        checkpoint_identity_key = data.get("checkpoint_identity_key")
        if not isinstance(checkpoint_identity_key, str) or not checkpoint_identity_key:
            raise ValueError(
                "it-stream checkpoint field 'checkpoint_identity_key' must be a non-empty string"
            )
        return cls(
            checkpoint_index=_read_required_int(metadata=data, key="checkpoint_index"),
            checkpoint_identity_key=checkpoint_identity_key,
            progress=_read_required_object_dict(metadata=data, key="progress"),
        )


@dataclass(frozen=True, slots=True)
class ItCheckpointV1:
    schema_version: Literal[1]
    flow_kind: Literal["it"]
    stream: str | None
    task_identity_key: str
    checkpoints: list[ItCheckpointEntryV1]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "flow_kind": self.flow_kind,
            "stream": self.stream,
            "task_identity_key": self.task_identity_key,
            "checkpoints": [checkpoint.to_dict() for checkpoint in self.checkpoints],
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ItCheckpointV1:
        schema_version = data.get("schema_version")
        if schema_version != IT_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError(f"Unsupported it-stream checkpoint schema version: {schema_version!r}")

        flow_kind = data.get("flow_kind")
        if flow_kind != "it":
            raise ValueError(f"Unsupported it-stream checkpoint flow kind: {flow_kind!r}")

        stream = data.get("stream")
        if stream is not None and not isinstance(stream, str):
            raise ValueError("it-stream checkpoint field 'stream' must be a string or null")

        task_identity_key = data.get("task_identity_key")
        if not isinstance(task_identity_key, str) or not task_identity_key:
            raise ValueError(
                "it-stream checkpoint field 'task_identity_key' must be a non-empty string"
            )

        checkpoints_raw = data.get("checkpoints")
        if not isinstance(checkpoints_raw, list):
            raise ValueError("it-stream checkpoint field 'checkpoints' must be a list")

        checkpoints: list[ItCheckpointEntryV1] = []
        for item in cast(list[object], checkpoints_raw):
            if not isinstance(item, dict):
                raise ValueError("it-stream checkpoint entry must be an object")
            checkpoints.append(ItCheckpointEntryV1.from_dict(cast(dict[str, object], item)))

        return cls(
            schema_version=IT_CHECKPOINT_SCHEMA_VERSION,
            flow_kind="it",
            stream=stream,
            task_identity_key=task_identity_key,
            checkpoints=checkpoints,
        )


@dataclass(frozen=True, slots=True)
class ItArtifactsV1:
    records: list[ItArtifactRecordV1]
    states: list[ItArtifactStateV1]
    logs: list[ItArtifactLogV1]
    checkpoint: ItCheckpointV1


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    text = "\n".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for row in rows
    )
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, row: dict[str, object]) -> None:
    path.write_text(
        json.dumps(row, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _read_required_str(*, metadata: dict[str, object], key: str) -> str:
    value = metadata.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"it-stream element metadata field '{key}' must be a non-empty string")
    return value


def _read_optional_str(*, metadata: dict[str, object], key: str) -> str | None:
    value = metadata.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"it-stream element metadata field '{key}' must be a string")
    return value


def _read_required_int(*, metadata: dict[str, object], key: str) -> int:
    value = metadata.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"it-stream element metadata field '{key}' must be an integer")
    return value


def _read_required_object_dict(*, metadata: dict[str, object], key: str) -> dict[str, object]:
    value = metadata.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"it-stream element metadata field '{key}' must be an object")
    raw_items = cast(dict[object, object], value)
    typed: dict[str, object] = {}
    for item_key, item_value in raw_items.items():
        if not isinstance(item_key, str):
            raise ValueError(f"it-stream element metadata field '{key}' must have string keys")
        typed[item_key] = item_value
    return typed


def build_it_artifacts(*, result: PartitionResult, pipeline_version: str) -> ItArtifactsV1:
    records: list[ItArtifactRecordV1] = []
    states: list[ItArtifactStateV1] = []
    logs: list[ItArtifactLogV1] = []

    for element in result.elements:
        metadata = element.metadata
        flow_kind = metadata.get("flow_kind")
        artifact_kind = metadata.get("artifact_kind")
        if flow_kind != "it" or not isinstance(artifact_kind, str):
            continue

        if artifact_kind == "record":
            records.append(
                ItArtifactRecordV1(
                    stream=_read_required_str(metadata=metadata, key="stream"),
                    record_index=_read_required_int(metadata=metadata, key="record_index"),
                    emitted_at=_read_optional_str(metadata=metadata, key="emitted_at"),
                    data=_read_required_object_dict(metadata=metadata, key="data"),
                )
            )
            continue

        if artifact_kind == "state":
            states.append(
                ItArtifactStateV1(
                    state_index=_read_required_int(metadata=metadata, key="state_index"),
                    data=_read_required_object_dict(metadata=metadata, key="data"),
                )
            )
            continue

        if artifact_kind == "log":
            logs.append(
                ItArtifactLogV1(
                    log_index=_read_required_int(metadata=metadata, key="log_index"),
                    level=_read_required_str(metadata=metadata, key="level"),
                    message=element.text,
                )
            )

    task_identity = ItTaskIdentityV1(
        pipeline_version=pipeline_version,
        sha256=result.document.sha256,
    )
    stream = records[0].stream if records else None
    checkpoint = ItCheckpointV1(
        schema_version=IT_CHECKPOINT_SCHEMA_VERSION,
        flow_kind="it",
        stream=stream,
        task_identity_key=normalize_it_task_identity_key(identity=task_identity),
        checkpoints=[
            ItCheckpointEntryV1(
                checkpoint_index=state.state_index,
                checkpoint_identity_key=normalize_it_checkpoint_identity_key(
                    identity=ItCheckpointIdentityV1(
                        task=task_identity,
                        stream=stream,
                        progress=state.data,
                    )
                ),
                progress=state.data,
            )
            for state in states
        ],
    )

    return ItArtifactsV1(records=records, states=states, logs=logs, checkpoint=checkpoint)


def dump_it_artifacts(
    *,
    out_dir: Path,
    result: PartitionResult,
    pipeline_version: str,
) -> ItArtifactsV1:
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts = build_it_artifacts(result=result, pipeline_version=pipeline_version)
    _write_jsonl(out_dir / "records.jsonl", [record.to_dict() for record in artifacts.records])
    _write_jsonl(out_dir / "logs.jsonl", [log.to_dict() for log in artifacts.logs])
    _write_json(out_dir / "checkpoint.json", artifacts.checkpoint.to_dict())
    return artifacts


def load_it_checkpoint(*, path: Path) -> ItCheckpointV1:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("it-stream checkpoint artifact must be a JSON object")
    return ItCheckpointV1.from_dict(cast(dict[str, object], raw))
