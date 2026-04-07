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
from zephyr_core.contracts.v1.run_meta import (
    DeliveryOriginV1,
    ExecutionModeV1,
    RunOriginV1,
    RunProvenanceV1,
)

IT_CHECKPOINT_SCHEMA_VERSION: Literal[1] = 1
ItCheckpointCompatibilityStatus = Literal["supported", "unsupported", "malformed"]
ItCheckpointProgressKind = Literal["cursor_v1", "token_v1", "page_v1", "state_dict_v1"]


@dataclass(frozen=True, slots=True)
class ItCheckpointCompatibilityV1:
    status: ItCheckpointCompatibilityStatus
    schema_version: int | None
    flow_kind: str | None
    reason: str | None = None

    def is_supported(self) -> bool:
        return self.status == "supported"


class ItCheckpointCompatibilityError(ValueError):
    def __init__(self, *, compatibility: ItCheckpointCompatibilityV1) -> None:
        self.compatibility = compatibility
        super().__init__(compatibility.reason or "Unsupported it-stream checkpoint artifact")


@dataclass(frozen=True, slots=True)
class ItCheckpointProvenanceV1:
    task_identity_key: str
    run_origin: RunOriginV1
    delivery_origin: DeliveryOriginV1
    execution_mode: ExecutionModeV1
    resumed_from_checkpoint_identity_key: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "task_identity_key": self.task_identity_key,
            "run_origin": self.run_origin,
            "delivery_origin": self.delivery_origin,
            "execution_mode": self.execution_mode,
            "resumed_from_checkpoint_identity_key": self.resumed_from_checkpoint_identity_key,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ItCheckpointProvenanceV1:
        task_identity_key = data.get("task_identity_key")
        if not isinstance(task_identity_key, str) or not task_identity_key:
            raise ValueError(
                "it-stream checkpoint provenance field 'task_identity_key' "
                "must be a non-empty string"
            )

        run_origin_raw = data.get("run_origin")
        if run_origin_raw == "intake":
            run_origin: RunOriginV1 = "intake"
        elif run_origin_raw == "resume":
            run_origin = "resume"
        elif run_origin_raw == "redrive":
            run_origin = "redrive"
        elif run_origin_raw == "requeue":
            run_origin = "requeue"
        else:
            raise ValueError(
                "it-stream checkpoint provenance field 'run_origin' must be a supported origin"
            )

        delivery_origin_raw = data.get("delivery_origin")
        if delivery_origin_raw == "primary":
            delivery_origin: DeliveryOriginV1 = "primary"
        elif delivery_origin_raw == "replay":
            delivery_origin = "replay"
        else:
            raise ValueError(
                "it-stream checkpoint provenance field 'delivery_origin' must be a supported origin"
            )

        execution_mode_raw = data.get("execution_mode")
        if execution_mode_raw == "batch":
            execution_mode: ExecutionModeV1 = "batch"
        elif execution_mode_raw == "worker":
            execution_mode = "worker"
        else:
            raise ValueError(
                "it-stream checkpoint provenance field 'execution_mode' must be a supported mode"
            )

        resumed_from_checkpoint_identity_key = data.get("resumed_from_checkpoint_identity_key")
        if resumed_from_checkpoint_identity_key is not None and (
            not isinstance(resumed_from_checkpoint_identity_key, str)
            or not resumed_from_checkpoint_identity_key
        ):
            raise ValueError(
                "it-stream checkpoint provenance field 'resumed_from_checkpoint_identity_key' "
                "must be a non-empty string or null"
            )

        return cls(
            task_identity_key=task_identity_key,
            run_origin=run_origin,
            delivery_origin=delivery_origin,
            execution_mode=execution_mode,
            resumed_from_checkpoint_identity_key=resumed_from_checkpoint_identity_key,
        )


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
    parent_checkpoint_identity_key: str | None
    progress_kind: ItCheckpointProgressKind
    progress: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "checkpoint_index": self.checkpoint_index,
            "checkpoint_identity_key": self.checkpoint_identity_key,
            "parent_checkpoint_identity_key": self.parent_checkpoint_identity_key,
            "progress_kind": self.progress_kind,
            "progress": self.progress,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ItCheckpointEntryV1:
        checkpoint_identity_key = data.get("checkpoint_identity_key")
        if not isinstance(checkpoint_identity_key, str) or not checkpoint_identity_key:
            raise ValueError(
                "it-stream checkpoint field 'checkpoint_identity_key' must be a non-empty string"
            )
        parent_checkpoint_identity_key = data.get("parent_checkpoint_identity_key")
        if parent_checkpoint_identity_key is not None and (
            not isinstance(parent_checkpoint_identity_key, str)
            or not parent_checkpoint_identity_key
        ):
            raise ValueError(
                "it-stream checkpoint field 'parent_checkpoint_identity_key' "
                "must be a non-empty string or null"
            )
        progress = _read_required_object_dict(metadata=data, key="progress")
        progress_kind_raw = data.get("progress_kind")
        progress_kind: ItCheckpointProgressKind
        if progress_kind_raw is None:
            progress_kind = infer_it_checkpoint_progress_kind(progress=progress)
        elif progress_kind_raw == "cursor_v1":
            progress_kind = "cursor_v1"
        elif progress_kind_raw == "token_v1":
            progress_kind = "token_v1"
        elif progress_kind_raw == "page_v1":
            progress_kind = "page_v1"
        elif progress_kind_raw == "state_dict_v1":
            progress_kind = "state_dict_v1"
        else:
            raise ValueError(
                "it-stream checkpoint field 'progress_kind' must be a supported progress kind"
            )
        return cls(
            checkpoint_index=_read_required_int(metadata=data, key="checkpoint_index"),
            checkpoint_identity_key=checkpoint_identity_key,
            parent_checkpoint_identity_key=parent_checkpoint_identity_key,
            progress_kind=progress_kind,
            progress=progress,
        )


@dataclass(frozen=True, slots=True)
class ItCheckpointV1:
    schema_version: Literal[1]
    flow_kind: Literal["it"]
    stream: str | None
    task_identity_key: str
    provenance: ItCheckpointProvenanceV1 | None
    checkpoints: list[ItCheckpointEntryV1]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "flow_kind": self.flow_kind,
            "stream": self.stream,
            "task_identity_key": self.task_identity_key,
            "provenance": None if self.provenance is None else self.provenance.to_dict(),
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

        provenance_raw = data.get("provenance")
        provenance: ItCheckpointProvenanceV1 | None
        if provenance_raw is None:
            provenance = None
        elif isinstance(provenance_raw, dict):
            provenance = ItCheckpointProvenanceV1.from_dict(cast(dict[str, object], provenance_raw))
        else:
            raise ValueError("it-stream checkpoint field 'provenance' must be an object or null")
        if provenance is not None and provenance.task_identity_key != task_identity_key:
            raise ValueError(
                "it-stream checkpoint provenance task identity must match 'task_identity_key'"
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
            provenance=provenance,
            checkpoints=checkpoints,
        )


@dataclass(frozen=True, slots=True)
class ItArtifactsV1:
    records: list[ItArtifactRecordV1]
    states: list[ItArtifactStateV1]
    logs: list[ItArtifactLogV1]
    checkpoint: ItCheckpointV1


def infer_it_checkpoint_progress_kind(*, progress: dict[str, object]) -> ItCheckpointProgressKind:
    cursor = progress.get("cursor")
    if isinstance(cursor, str) and cursor:
        return "cursor_v1"
    token = progress.get("token")
    if isinstance(token, str) and token:
        return "token_v1"
    page_token = progress.get("page_token")
    if isinstance(page_token, str) and page_token:
        return "token_v1"
    next_page_token = progress.get("next_page_token")
    if isinstance(next_page_token, str) and next_page_token:
        return "token_v1"
    page_number = progress.get("page_number")
    if isinstance(page_number, int) and not isinstance(page_number, bool):
        return "page_v1"
    page = progress.get("page")
    if isinstance(page, int) and not isinstance(page, bool):
        return "page_v1"
    return "state_dict_v1"


def _default_checkpoint_provenance(*, task_identity_key: str) -> ItCheckpointProvenanceV1:
    return ItCheckpointProvenanceV1(
        task_identity_key=task_identity_key,
        run_origin="intake",
        delivery_origin="primary",
        execution_mode="batch",
        resumed_from_checkpoint_identity_key=None,
    )


def build_it_checkpoint_provenance(
    *,
    task_identity_key: str,
    run_provenance: RunProvenanceV1 | None,
) -> ItCheckpointProvenanceV1:
    if run_provenance is None:
        return _default_checkpoint_provenance(task_identity_key=task_identity_key)

    return ItCheckpointProvenanceV1(
        task_identity_key=task_identity_key,
        run_origin=run_provenance.run_origin or "intake",
        delivery_origin=run_provenance.delivery_origin or "primary",
        execution_mode=run_provenance.execution_mode or "batch",
        resumed_from_checkpoint_identity_key=run_provenance.checkpoint_identity_key,
    )


def inspect_it_checkpoint_compatibility(*, raw: object) -> ItCheckpointCompatibilityV1:
    if not isinstance(raw, dict):
        return ItCheckpointCompatibilityV1(
            status="malformed",
            schema_version=None,
            flow_kind=None,
            reason="it-stream checkpoint artifact must be a JSON object",
        )

    data = cast("dict[str, object]", raw)
    schema_version_raw = data.get("schema_version")
    if not isinstance(schema_version_raw, int) or isinstance(schema_version_raw, bool):
        return ItCheckpointCompatibilityV1(
            status="malformed",
            schema_version=None,
            flow_kind=None,
            reason="it-stream checkpoint field 'schema_version' must be an integer",
        )

    flow_kind_raw = data.get("flow_kind")
    if not isinstance(flow_kind_raw, str) or not flow_kind_raw:
        return ItCheckpointCompatibilityV1(
            status="malformed",
            schema_version=schema_version_raw,
            flow_kind=None,
            reason="it-stream checkpoint field 'flow_kind' must be a non-empty string",
        )

    if schema_version_raw != IT_CHECKPOINT_SCHEMA_VERSION:
        return ItCheckpointCompatibilityV1(
            status="unsupported",
            schema_version=schema_version_raw,
            flow_kind=flow_kind_raw,
            reason=f"Unsupported it-stream checkpoint schema version: {schema_version_raw!r}",
        )

    if flow_kind_raw != "it":
        return ItCheckpointCompatibilityV1(
            status="unsupported",
            schema_version=schema_version_raw,
            flow_kind=flow_kind_raw,
            reason=f"Unsupported it-stream checkpoint flow kind: {flow_kind_raw!r}",
        )

    try:
        ItCheckpointV1.from_dict(data)
    except ValueError as err:
        return ItCheckpointCompatibilityV1(
            status="malformed",
            schema_version=schema_version_raw,
            flow_kind=flow_kind_raw,
            reason=str(err),
        )

    return ItCheckpointCompatibilityV1(
        status="supported",
        schema_version=schema_version_raw,
        flow_kind=flow_kind_raw,
    )


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


def build_it_artifacts(
    *,
    result: PartitionResult,
    pipeline_version: str,
    run_provenance: RunProvenanceV1 | None = None,
) -> ItArtifactsV1:
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
    task_identity_key = normalize_it_task_identity_key(identity=task_identity)
    stream = records[0].stream if records else None
    checkpoint_entries: list[ItCheckpointEntryV1] = []
    parent_checkpoint_identity_key: str | None = None
    for state in states:
        progress_kind = infer_it_checkpoint_progress_kind(progress=state.data)
        checkpoint_identity_key = normalize_it_checkpoint_identity_key(
            identity=ItCheckpointIdentityV1(
                task=task_identity,
                stream=stream,
                progress_kind=progress_kind,
                progress=state.data,
            )
        )
        checkpoint_entries.append(
            ItCheckpointEntryV1(
                checkpoint_index=state.state_index,
                checkpoint_identity_key=checkpoint_identity_key,
                parent_checkpoint_identity_key=parent_checkpoint_identity_key,
                progress_kind=progress_kind,
                progress=state.data,
            )
        )
        parent_checkpoint_identity_key = checkpoint_identity_key

    checkpoint = ItCheckpointV1(
        schema_version=IT_CHECKPOINT_SCHEMA_VERSION,
        flow_kind="it",
        stream=stream,
        task_identity_key=task_identity_key,
        provenance=build_it_checkpoint_provenance(
            task_identity_key=task_identity_key,
            run_provenance=run_provenance,
        ),
        checkpoints=checkpoint_entries,
    )

    return ItArtifactsV1(records=records, states=states, logs=logs, checkpoint=checkpoint)


def dump_it_artifacts(
    *,
    out_dir: Path,
    result: PartitionResult,
    pipeline_version: str,
    run_provenance: RunProvenanceV1 | None = None,
) -> ItArtifactsV1:
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts = build_it_artifacts(
        result=result,
        pipeline_version=pipeline_version,
        run_provenance=run_provenance,
    )
    _write_jsonl(out_dir / "records.jsonl", [record.to_dict() for record in artifacts.records])
    _write_jsonl(out_dir / "logs.jsonl", [log.to_dict() for log in artifacts.logs])
    _write_json(out_dir / "checkpoint.json", artifacts.checkpoint.to_dict())
    return artifacts


def load_it_checkpoint(*, path: Path) -> ItCheckpointV1:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        raise ItCheckpointCompatibilityError(
            compatibility=ItCheckpointCompatibilityV1(
                status="malformed",
                schema_version=None,
                flow_kind=None,
                reason="it-stream checkpoint artifact must be valid JSON",
            )
        ) from err

    compatibility = inspect_it_checkpoint_compatibility(raw=raw)
    if not compatibility.is_supported():
        raise ItCheckpointCompatibilityError(compatibility=compatibility)

    return ItCheckpointV1.from_dict(cast(dict[str, object], raw))
