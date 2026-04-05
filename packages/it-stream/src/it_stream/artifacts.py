from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from zephyr_core import PartitionResult


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
class ItArtifactsV1:
    records: list[ItArtifactRecordV1]
    states: list[ItArtifactStateV1]
    logs: list[ItArtifactLogV1]


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    text = "\n".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for row in rows
    )
    path.write_text(text, encoding="utf-8")


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


def build_it_artifacts(*, result: PartitionResult) -> ItArtifactsV1:
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

    return ItArtifactsV1(records=records, states=states, logs=logs)


def dump_it_artifacts(*, out_dir: Path, result: PartitionResult) -> ItArtifactsV1:
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts = build_it_artifacts(result=result)
    _write_jsonl(out_dir / "records.jsonl", [record.to_dict() for record in artifacts.records])
    _write_jsonl(out_dir / "state.jsonl", [state.to_dict() for state in artifacts.states])
    _write_jsonl(out_dir / "logs.jsonl", [log.to_dict() for log in artifacts.logs])
    return artifacts
