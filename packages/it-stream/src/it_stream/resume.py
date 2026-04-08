from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn

from it_stream.artifacts import (
    ItCheckpointCompatibilityError,
    ItCheckpointEntryV1,
    ItCheckpointProgressKind,
    ItCheckpointResumeCursorContinuationV1,
    ItCheckpointResumeProvenanceV1,
    ItCheckpointV1,
    load_it_checkpoint,
)
from it_stream.identity import ItTaskIdentityV1, normalize_it_task_identity_key
from it_stream.service import (
    ItNormalizedInputDocumentV1,
    ItRecordV1,
    ItStateV1,
    load_input_document,
    partition_input_document,
)
from zephyr_core import PartitionResult, PartitionStrategy
from zephyr_core.contracts.v1.run_meta import ExecutionModeV1, RunProvenanceV1

ItResumeSelectionModeV1 = Literal["latest_checkpoint", "explicit_checkpoint_identity"]
ItResumeRecoveryStatusV1 = Literal["malformed", "incompatible", "unsupported", "blocked"]
ItResumeRecoveryCodeV1 = Literal[
    "checkpoint_malformed",
    "checkpoint_incompatible",
    "task_identity_mismatch",
    "empty_checkpoint_set",
    "unknown_checkpoint_identity",
    "unsupported_progress_kind",
    "missing_checkpoint_cursor",
    "missing_record_emitted_at",
    "missing_state_cursor",
]


@dataclass(frozen=True, slots=True)
class ItResumeRecoveryIssueV1:
    status: ItResumeRecoveryStatusV1
    code: ItResumeRecoveryCodeV1
    message: str
    checkpoint_identity_key: str | None = None
    progress_kind: ItCheckpointProgressKind | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "code": self.code,
            "message": self.message,
            "checkpoint_identity_key": self.checkpoint_identity_key,
            "progress_kind": self.progress_kind,
        }


class ItResumeRecoveryError(ValueError):
    def __init__(self, *, issue: ItResumeRecoveryIssueV1) -> None:
        self.issue = issue
        super().__init__(issue.message)


@dataclass(frozen=True, slots=True)
class ItResumeCheckpointSelectionV1:
    mode: ItResumeSelectionModeV1
    checkpoint_identity_key: str
    checkpoint_index: int
    parent_checkpoint_identity_key: str | None
    progress_kind: ItCheckpointProgressKind


@dataclass(frozen=True, slots=True)
class ItResumeCursorContinuationV1:
    progress_kind: Literal["cursor_v1"]
    exclusive_after_cursor: str


@dataclass(frozen=True, slots=True)
class ItResumeSelectionV1:
    checkpoint: ItCheckpointV1
    entry: ItCheckpointEntryV1
    selected_checkpoint: ItResumeCheckpointSelectionV1
    continuation: ItResumeCursorContinuationV1 | None

    def to_run_provenance(
        self,
        *,
        execution_mode: ExecutionModeV1 = "batch",
        task_id: str | None = None,
    ) -> RunProvenanceV1:
        return RunProvenanceV1(
            run_origin="resume",
            delivery_origin="primary",
            execution_mode=execution_mode,
            task_id=task_id,
            checkpoint_identity_key=self.selected_checkpoint.checkpoint_identity_key,
            task_identity_key=self.checkpoint.task_identity_key,
        )

    def to_checkpoint_resume_provenance(self) -> ItCheckpointResumeProvenanceV1:
        continuation: ItCheckpointResumeCursorContinuationV1 | None = None
        if self.continuation is not None:
            continuation = ItCheckpointResumeCursorContinuationV1(
                exclusive_after_cursor=self.continuation.exclusive_after_cursor
            )
        return ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=self.selected_checkpoint.checkpoint_identity_key,
            progress_kind=self.selected_checkpoint.progress_kind,
            continuation=continuation,
        )


def _raise_resume_recovery_error(
    *,
    status: ItResumeRecoveryStatusV1,
    code: ItResumeRecoveryCodeV1,
    message: str,
    checkpoint_identity_key: str | None = None,
    progress_kind: ItCheckpointProgressKind | None = None,
) -> NoReturn:
    raise ItResumeRecoveryError(
        issue=ItResumeRecoveryIssueV1(
            status=status,
            code=code,
            message=message,
            checkpoint_identity_key=checkpoint_identity_key,
            progress_kind=progress_kind,
        )
    )


def _raise_resume_recovery_error_from_compatibility(
    err: ItCheckpointCompatibilityError,
) -> NoReturn:
    compatibility = err.compatibility
    if compatibility.status == "malformed":
        _raise_resume_recovery_error(
            status="malformed",
            code="checkpoint_malformed",
            message=compatibility.reason or "Malformed it-stream checkpoint artifact",
        )
    _raise_resume_recovery_error(
        status="incompatible",
        code="checkpoint_incompatible",
        message=compatibility.reason or "Incompatible it-stream checkpoint artifact",
    )


def _read_cursor(*, progress: dict[str, object], context: str) -> str:
    cursor = progress.get("cursor")
    if not isinstance(cursor, str) or not cursor:
        raise ValueError(f"{context} must contain a non-empty string field 'cursor'")
    return cursor


def _read_checkpoint_cursor_or_raise(*, entry: ItCheckpointEntryV1) -> str:
    try:
        return _read_cursor(
            progress=entry.progress,
            context="it-stream resume checkpoint progress",
        )
    except ValueError as err:
        _raise_resume_recovery_error(
            status="blocked",
            code="missing_checkpoint_cursor",
            message=str(err),
            checkpoint_identity_key=entry.checkpoint_identity_key,
            progress_kind=entry.progress_kind,
        )


def _read_state_cursor_or_raise(
    *,
    state: ItStateV1,
    selection: ItResumeSelectionV1,
) -> str:
    try:
        return _read_cursor(
            progress=state.data,
            context="it-stream resume state progress",
        )
    except ValueError as err:
        _raise_resume_recovery_error(
            status="blocked",
            code="missing_state_cursor",
            message=str(err),
            checkpoint_identity_key=selection.selected_checkpoint.checkpoint_identity_key,
            progress_kind=selection.selected_checkpoint.progress_kind,
        )


def _build_resume_selection(
    *,
    checkpoint: ItCheckpointV1,
    entry: ItCheckpointEntryV1,
    mode: ItResumeSelectionModeV1,
) -> ItResumeSelectionV1:
    continuation: ItResumeCursorContinuationV1 | None = None
    if entry.progress_kind == "cursor_v1":
        continuation = ItResumeCursorContinuationV1(
            progress_kind="cursor_v1",
            exclusive_after_cursor=_read_checkpoint_cursor_or_raise(entry=entry),
        )
    return ItResumeSelectionV1(
        checkpoint=checkpoint,
        entry=entry,
        selected_checkpoint=ItResumeCheckpointSelectionV1(
            mode=mode,
            checkpoint_identity_key=entry.checkpoint_identity_key,
            checkpoint_index=entry.checkpoint_index,
            parent_checkpoint_identity_key=entry.parent_checkpoint_identity_key,
            progress_kind=entry.progress_kind,
        ),
        continuation=continuation,
    )


def load_it_resume_selection(
    *,
    checkpoint_path: Path,
    pipeline_version: str,
    sha256: str,
    checkpoint_identity_key: str | None = None,
) -> ItResumeSelectionV1:
    try:
        checkpoint = load_it_checkpoint(path=checkpoint_path)
    except ItCheckpointCompatibilityError as err:
        _raise_resume_recovery_error_from_compatibility(err)
    expected_task_identity_key = normalize_it_task_identity_key(
        identity=ItTaskIdentityV1(
            pipeline_version=pipeline_version,
            sha256=sha256,
        )
    )
    if checkpoint.task_identity_key != expected_task_identity_key:
        _raise_resume_recovery_error(
            status="incompatible",
            code="task_identity_mismatch",
            message="it-stream checkpoint task identity does not match the requested work",
        )

    if not checkpoint.checkpoints:
        _raise_resume_recovery_error(
            status="blocked",
            code="empty_checkpoint_set",
            message="it-stream checkpoint artifact does not contain any checkpoints",
        )

    if checkpoint_identity_key is None:
        return _build_resume_selection(
            checkpoint=checkpoint,
            entry=checkpoint.checkpoints[-1],
            mode="latest_checkpoint",
        )

    for entry in checkpoint.checkpoints:
        if entry.checkpoint_identity_key == checkpoint_identity_key:
            return _build_resume_selection(
                checkpoint=checkpoint,
                entry=entry,
                mode="explicit_checkpoint_identity",
            )

    _raise_resume_recovery_error(
        status="blocked",
        code="unknown_checkpoint_identity",
        message=f"Unknown it-stream checkpoint identity key: {checkpoint_identity_key!r}",
        checkpoint_identity_key=checkpoint_identity_key,
    )


def _resume_input_document(
    *,
    doc: ItNormalizedInputDocumentV1,
    selection: ItResumeSelectionV1,
) -> ItNormalizedInputDocumentV1:
    continuation = selection.continuation
    if continuation is None:
        _raise_resume_recovery_error(
            status="unsupported",
            code="unsupported_progress_kind",
            message="it-stream resume currently requires 'cursor_v1' checkpoint progress",
            checkpoint_identity_key=selection.selected_checkpoint.checkpoint_identity_key,
            progress_kind=selection.selected_checkpoint.progress_kind,
        )
    checkpoint_cursor = continuation.exclusive_after_cursor

    resumed_records: list[ItRecordV1] = []
    for record in doc.records:
        if record.emitted_at is None:
            _raise_resume_recovery_error(
                status="blocked",
                code="missing_record_emitted_at",
                message="it-stream resume currently requires 'record.emitted_at' for records",
                checkpoint_identity_key=selection.selected_checkpoint.checkpoint_identity_key,
                progress_kind=selection.selected_checkpoint.progress_kind,
            )
        emitted_at = record.emitted_at
        if emitted_at > checkpoint_cursor:
            resumed_records.append(record)

    resumed_states: list[ItStateV1] = []
    for state in doc.states:
        state_cursor = _read_state_cursor_or_raise(state=state, selection=selection)
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
