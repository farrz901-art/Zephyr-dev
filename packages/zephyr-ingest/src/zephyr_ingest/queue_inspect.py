from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

from zephyr_ingest.spool_queue import (
    QueueRecoveryProvenanceV1Dict,
    SpoolBucket,
    list_spool_bucket_paths,
    load_spool_record,
)
from zephyr_ingest.task_v1 import TaskIdentityV1Dict

SPOOL_BUCKETS: tuple[SpoolBucket, ...] = ("pending", "inflight", "done", "failed", "poison")


class QueueInspectSummaryDict(TypedDict):
    pending: int
    inflight: int
    done: int
    failed: int
    poison: int


class QueueInspectTaskDict(TypedDict):
    bucket: SpoolBucket
    task_id: str
    kind: str
    record_path: str
    updated_at_utc: str
    uri: str
    source: str
    filename: str
    discovered_at_utc: str
    identity: TaskIdentityV1Dict | None
    failure_count: int
    orphan_count: int
    latest_recovery: QueueRecoveryProvenanceV1Dict | None


class QueueInspectResultDict(TypedDict):
    root: str
    summary: QueueInspectSummaryDict
    bucket: SpoolBucket | None
    tasks: list[QueueInspectTaskDict]


@dataclass(frozen=True, slots=True)
class QueueInspectSummaryV1:
    pending: int
    inflight: int
    done: int
    failed: int
    poison: int

    def to_dict(self) -> QueueInspectSummaryDict:
        return {
            "pending": self.pending,
            "inflight": self.inflight,
            "done": self.done,
            "failed": self.failed,
            "poison": self.poison,
        }


@dataclass(frozen=True, slots=True)
class QueueInspectTaskV1:
    bucket: SpoolBucket
    task_id: str
    kind: str
    record_path: str
    updated_at_utc: str
    uri: str
    source: str
    filename: str
    discovered_at_utc: str
    identity: TaskIdentityV1Dict | None
    failure_count: int
    orphan_count: int
    latest_recovery: QueueRecoveryProvenanceV1Dict | None

    def to_dict(self) -> QueueInspectTaskDict:
        return {
            "bucket": self.bucket,
            "task_id": self.task_id,
            "kind": self.kind,
            "record_path": self.record_path,
            "updated_at_utc": self.updated_at_utc,
            "uri": self.uri,
            "source": self.source,
            "filename": self.filename,
            "discovered_at_utc": self.discovered_at_utc,
            "identity": self.identity,
            "failure_count": self.failure_count,
            "orphan_count": self.orphan_count,
            "latest_recovery": self.latest_recovery,
        }


@dataclass(frozen=True, slots=True)
class QueueInspectResultV1:
    root: str
    summary: QueueInspectSummaryV1
    bucket: SpoolBucket | None
    tasks: tuple[QueueInspectTaskV1, ...] = ()

    def to_dict(self) -> QueueInspectResultDict:
        return {
            "root": self.root,
            "summary": self.summary.to_dict(),
            "bucket": self.bucket,
            "tasks": [task.to_dict() for task in self.tasks],
        }


def inspect_local_spool_queue(
    *,
    root: Path,
    bucket: SpoolBucket | None = None,
    limit: int | None = None,
) -> QueueInspectResultV1:
    if limit is not None and limit <= 0:
        raise ValueError("limit must be > 0")

    resolved_root = root.expanduser().resolve()
    summary = QueueInspectSummaryV1(
        pending=len(list_spool_bucket_paths(root=resolved_root, bucket="pending")),
        inflight=len(list_spool_bucket_paths(root=resolved_root, bucket="inflight")),
        done=len(list_spool_bucket_paths(root=resolved_root, bucket="done")),
        failed=len(list_spool_bucket_paths(root=resolved_root, bucket="failed")),
        poison=len(list_spool_bucket_paths(root=resolved_root, bucket="poison")),
    )

    tasks: list[QueueInspectTaskV1] = []
    if bucket is not None:
        selected_paths = list_spool_bucket_paths(root=resolved_root, bucket=bucket)
        if limit is not None:
            selected_paths = selected_paths[:limit]
        tasks = [_build_task_inspection(bucket=bucket, path=path) for path in selected_paths]

    return QueueInspectResultV1(
        root=str(resolved_root),
        summary=summary,
        bucket=bucket,
        tasks=tuple(tasks),
    )


def _build_task_inspection(*, bucket: SpoolBucket, path: Path) -> QueueInspectTaskV1:
    record = load_spool_record(path)
    task = record.task
    doc = task.inputs.document
    identity = None if task.identity is None else task.identity.to_dict()
    latest_recovery = None if not record.provenance else record.provenance[-1].to_dict()
    return QueueInspectTaskV1(
        bucket=bucket,
        task_id=task.task_id,
        kind=task.kind,
        record_path=str(path.resolve()),
        updated_at_utc=_mtime_to_utc(path),
        uri=doc.uri,
        source=doc.source,
        filename=doc.filename,
        discovered_at_utc=doc.discovered_at_utc,
        identity=identity,
        failure_count=record.governance.failure_count,
        orphan_count=record.governance.orphan_count,
        latest_recovery=latest_recovery,
    )


def _mtime_to_utc(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat().replace("+00:00", "Z")
