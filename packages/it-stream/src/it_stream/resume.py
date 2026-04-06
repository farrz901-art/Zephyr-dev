from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from it_stream.artifacts import ItCheckpointEntryV1, ItCheckpointV1, load_it_checkpoint
from it_stream.identity import ItTaskIdentityV1, normalize_it_task_identity_key
from it_stream.service import (
    ItNormalizedInputDocumentV1,
    ItRecordV1,
    ItStateV1,
    load_input_document,
    partition_input_document,
)
from zephyr_core import PartitionResult, PartitionStrategy


@dataclass(frozen=True, slots=True)
class ItResumeSelectionV1:
    checkpoint: ItCheckpointV1
    entry: ItCheckpointEntryV1


def _read_cursor(*, progress: dict[str, object], context: str) -> str:
    cursor = progress.get("cursor")
    if not isinstance(cursor, str) or not cursor:
        raise ValueError(f"{context} must contain a non-empty string field 'cursor'")
    return cursor


def load_it_resume_selection(
    *,
    checkpoint_path: Path,
    pipeline_version: str,
    sha256: str,
    checkpoint_identity_key: str | None = None,
) -> ItResumeSelectionV1:
    checkpoint = load_it_checkpoint(path=checkpoint_path)
    expected_task_identity_key = normalize_it_task_identity_key(
        identity=ItTaskIdentityV1(
            pipeline_version=pipeline_version,
            sha256=sha256,
        )
    )
    if checkpoint.task_identity_key != expected_task_identity_key:
        raise ValueError("it-stream checkpoint task identity does not match the requested work")

    if not checkpoint.checkpoints:
        raise ValueError("it-stream checkpoint artifact does not contain any checkpoints")

    if checkpoint_identity_key is None:
        return ItResumeSelectionV1(checkpoint=checkpoint, entry=checkpoint.checkpoints[-1])

    for entry in checkpoint.checkpoints:
        if entry.checkpoint_identity_key == checkpoint_identity_key:
            return ItResumeSelectionV1(checkpoint=checkpoint, entry=entry)

    raise ValueError(f"Unknown it-stream checkpoint identity key: {checkpoint_identity_key!r}")


def _resume_input_document(
    *,
    doc: ItNormalizedInputDocumentV1,
    selection: ItResumeSelectionV1,
) -> ItNormalizedInputDocumentV1:
    checkpoint_cursor = _read_cursor(
        progress=selection.entry.progress,
        context="it-stream resume checkpoint progress",
    )

    resumed_records: list[ItRecordV1] = []
    for record in doc.records:
        if record.emitted_at is None:
            raise ValueError("it-stream resume currently requires 'record.emitted_at' for records")
        if record.emitted_at > checkpoint_cursor:
            resumed_records.append(record)

    resumed_states: list[ItStateV1] = []
    for state in doc.states:
        state_cursor = _read_cursor(
            progress=state.data,
            context="it-stream resume state progress",
        )
        if state_cursor > checkpoint_cursor:
            resumed_states.append(state)

    return ItNormalizedInputDocumentV1(
        stream=doc.stream or selection.checkpoint.stream,
        records=resumed_records,
        states=resumed_states,
        logs=[],
    )


def resume_file(
    *,
    filename: str,
    checkpoint_path: str | Path,
    pipeline_version: str,
    sha256: str,
    checkpoint_identity_key: str | None = None,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    size_bytes: int | None = None,
) -> PartitionResult:
    path = Path(filename)
    selection = load_it_resume_selection(
        checkpoint_path=Path(checkpoint_path),
        pipeline_version=pipeline_version,
        sha256=sha256,
        checkpoint_identity_key=checkpoint_identity_key,
    )
    doc = load_input_document(path)
    resumed_doc = _resume_input_document(doc=doc, selection=selection)
    size = size_bytes if size_bytes is not None else path.stat().st_size
    return partition_input_document(
        doc=resumed_doc,
        filename=path.name,
        strategy=strategy,
        sha256=sha256,
        size_bytes=size,
    )
