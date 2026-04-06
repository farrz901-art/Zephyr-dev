from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypedDict

from zephyr_ingest.spool_queue import (
    QueueRecoveryProvenanceV1,
    load_spool_record,
    now_utc_isoformat,
    spool_bucket_dir,
    write_spool_record,
)

RecoverableSpoolBucket = Literal["poison", "inflight"]


class QueueRecoveryResultDict(TypedDict):
    action: Literal["requeue"]
    root: str
    task_id: str
    kind: str
    source_bucket: RecoverableSpoolBucket
    target_bucket: Literal["pending"]
    source_path: str
    target_path: str
    failure_count: int
    orphan_count: int
    recorded_at_utc: str


class QueueRecoveryError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class QueueRecoveryResultV1:
    root: str
    task_id: str
    kind: str
    source_bucket: RecoverableSpoolBucket
    source_path: str
    target_path: str
    failure_count: int
    orphan_count: int
    recorded_at_utc: str

    def to_dict(self) -> QueueRecoveryResultDict:
        return {
            "action": "requeue",
            "root": self.root,
            "task_id": self.task_id,
            "kind": self.kind,
            "source_bucket": self.source_bucket,
            "target_bucket": "pending",
            "source_path": self.source_path,
            "target_path": self.target_path,
            "failure_count": self.failure_count,
            "orphan_count": self.orphan_count,
            "recorded_at_utc": self.recorded_at_utc,
        }


def requeue_local_spool_task(
    *,
    root: Path,
    source_bucket: RecoverableSpoolBucket,
    task_id: str,
) -> QueueRecoveryResultV1:
    resolved_root = root.expanduser().resolve()
    source_path = spool_bucket_dir(root=resolved_root, bucket=source_bucket) / f"{task_id}.json"
    target_path = spool_bucket_dir(root=resolved_root, bucket="pending") / f"{task_id}.json"

    if not source_path.exists():
        raise QueueRecoveryError(f"Task not found in {source_bucket}: {task_id}")
    if target_path.exists():
        raise QueueRecoveryError(f"Task already exists in pending: {task_id}")

    record = load_spool_record(source_path)
    if record.task.task_id != task_id:
        raise QueueRecoveryError(
            "Task record id mismatch for "
            f"{source_bucket}: expected {task_id}, got {record.task.task_id}"
        )

    recorded_at_utc = now_utc_isoformat()
    updated_record = record.__class__(
        task=record.task,
        governance=record.governance,
        provenance=(
            *record.provenance,
            QueueRecoveryProvenanceV1(
                action="requeue",
                source_bucket=source_bucket,
                target_bucket="pending",
                recorded_at_utc=recorded_at_utc,
            ),
        ),
    )
    write_spool_record(path=target_path, record=updated_record)
    source_path.unlink(missing_ok=True)
    return QueueRecoveryResultV1(
        root=str(resolved_root),
        task_id=record.task.task_id,
        kind=record.task.kind,
        source_bucket=source_bucket,
        source_path=str(source_path),
        target_path=str(target_path),
        failure_count=record.governance.failure_count,
        orphan_count=record.governance.orphan_count,
        recorded_at_utc=recorded_at_utc,
    )
