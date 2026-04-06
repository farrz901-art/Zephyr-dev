from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, NotRequired, Protocol, TypedDict, cast, runtime_checkable

from zephyr_ingest.queue_backend import ClaimedTask, QueueBackendWorkSource, QueueWorkItem
from zephyr_ingest.task_v1 import TaskV1


class TaskGovernanceV1Dict(TypedDict):
    failure_count: int
    orphan_count: int


class SpoolTaskRecordDict(TypedDict):
    task: dict[str, object]
    governance: TaskGovernanceV1Dict
    provenance: NotRequired[list["QueueRecoveryProvenanceV1Dict"]]


SpoolBucket = Literal["pending", "inflight", "done", "failed", "poison"]


@dataclass(frozen=True, slots=True)
class QueueMetricsSnapshotV1:
    pending: int
    inflight: int
    done: int
    failed: int
    poison: int
    poison_transition_total: int
    orphan_requeue_total: int
    stale_inflight_recovery_total: int


@runtime_checkable
class SupportsQueueMetricsSnapshot(Protocol):
    def queue_metrics_snapshot(self) -> QueueMetricsSnapshotV1: ...


class QueueRecoveryProvenanceV1Dict(TypedDict):
    action: Literal["requeue"]
    source_bucket: Literal["poison", "inflight"]
    target_bucket: Literal["pending"]
    recorded_at_utc: str


@dataclass(frozen=True, slots=True)
class QueueRecoveryProvenanceV1:
    action: Literal["requeue"]
    source_bucket: Literal["poison", "inflight"]
    target_bucket: Literal["pending"]
    recorded_at_utc: str

    def to_dict(self) -> QueueRecoveryProvenanceV1Dict:
        return {
            "action": self.action,
            "source_bucket": self.source_bucket,
            "target_bucket": self.target_bucket,
            "recorded_at_utc": self.recorded_at_utc,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> QueueRecoveryProvenanceV1:
        action = data.get("action")
        source_bucket = data.get("source_bucket")
        target_bucket = data.get("target_bucket")
        recorded_at_utc = data.get("recorded_at_utc")
        if action != "requeue":
            raise ValueError("spool provenance field 'action' must be 'requeue'")
        normalized_source_bucket: Literal["poison", "inflight"]
        if source_bucket == "poison":
            normalized_source_bucket = "poison"
        elif source_bucket == "inflight":
            normalized_source_bucket = "inflight"
        else:
            raise ValueError("spool provenance field 'source_bucket' must be recoverable")
        if target_bucket != "pending":
            raise ValueError("spool provenance field 'target_bucket' must be 'pending'")
        if not isinstance(recorded_at_utc, str) or not recorded_at_utc:
            raise ValueError("spool provenance field 'recorded_at_utc' must be a non-empty string")
        return cls(
            action="requeue",
            source_bucket=normalized_source_bucket,
            target_bucket="pending",
            recorded_at_utc=recorded_at_utc,
        )


@dataclass(frozen=True, slots=True)
class TaskGovernanceV1:
    failure_count: int = 0
    orphan_count: int = 0

    def to_dict(self) -> TaskGovernanceV1Dict:
        return {
            "failure_count": self.failure_count,
            "orphan_count": self.orphan_count,
        }


@dataclass(frozen=True, slots=True)
class SpoolTaskRecord:
    task: TaskV1
    governance: TaskGovernanceV1 = TaskGovernanceV1()
    provenance: tuple[QueueRecoveryProvenanceV1, ...] = ()

    def to_dict(self) -> SpoolTaskRecordDict:
        payload: SpoolTaskRecordDict = {
            "task": cast("dict[str, object]", self.task.to_dict()),
            "governance": self.governance.to_dict(),
        }
        if self.provenance:
            payload["provenance"] = [entry.to_dict() for entry in self.provenance]
        return payload


def _load_record(path: Path) -> SpoolTaskRecord:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"Spool record must be a JSON object: {path}")

    raw = cast("dict[str, object]", obj)
    task_obj = raw.get("task")
    governance_obj = raw.get("governance")

    if isinstance(task_obj, dict):
        failure_count = 0
        orphan_count = 0
        if governance_obj is not None:
            if not isinstance(governance_obj, dict):
                raise TypeError(f"Spool record field 'governance' must be a mapping: {path}")
            gov = cast("dict[str, object]", governance_obj)
            failure_count = _read_non_bool_int(
                data=gov,
                key="failure_count",
                default=0,
            )
            orphan_count = _read_non_bool_int(
                data=gov,
                key="orphan_count",
                default=0,
            )
        provenance_obj = raw.get("provenance")
        provenance: tuple[QueueRecoveryProvenanceV1, ...] = ()
        if provenance_obj is not None:
            if not isinstance(provenance_obj, list):
                raise TypeError(f"Spool record field 'provenance' must be a list: {path}")
            typed_provenance_obj = cast("list[object]", provenance_obj)
            provenance = tuple(
                QueueRecoveryProvenanceV1.from_dict(cast("Mapping[str, object]", item))
                for item in typed_provenance_obj
                if isinstance(item, Mapping)
            )
        return SpoolTaskRecord(
            task=TaskV1.from_dict(cast("dict[str, object]", task_obj)),
            governance=TaskGovernanceV1(
                failure_count=failure_count,
                orphan_count=orphan_count,
            ),
            provenance=provenance,
        )

    return SpoolTaskRecord(task=TaskV1.from_dict(raw))


def _load_task(path: Path) -> TaskV1:
    return _load_record(path).task


def load_spool_record(path: Path) -> SpoolTaskRecord:
    return _load_record(path)


def write_spool_record(*, path: Path, record: SpoolTaskRecord) -> None:
    _write_json_atomic(path=path, data=record.to_dict())


def spool_bucket_dir(*, root: Path, bucket: SpoolBucket) -> Path:
    return root / bucket


def list_spool_bucket_paths(*, root: Path, bucket: SpoolBucket) -> list[Path]:
    bucket_root = spool_bucket_dir(root=root, bucket=bucket)
    if not bucket_root.exists():
        return []
    return sorted_spool_task_files(bucket_root)


def sorted_spool_task_files(root: Path) -> list[Path]:
    return sorted(
        [path for path in root.glob("*.json") if path.is_file()],
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )


def now_utc_isoformat() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _read_non_bool_int(*, data: Mapping[str, object], key: str, default: int) -> int:
    value = data.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Spool record field '{key}' must be an integer")
    return value


def _write_json_atomic(*, path: Path, data: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.rename(path)
    finally:
        tmp_path.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class LocalSpoolQueue:
    root: Path
    max_task_attempts: int = 1
    max_orphan_requeues: int = 1
    _poison_transition_total: int = field(default=0, init=False, repr=False)
    _orphan_requeue_total: int = field(default=0, init=False, repr=False)
    _stale_inflight_recovery_total: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_task_attempts <= 0:
            raise ValueError("max_task_attempts must be > 0")
        if self.max_orphan_requeues <= 0:
            raise ValueError("max_orphan_requeues must be > 0")
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.inflight_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        self.poison_dir.mkdir(parents=True, exist_ok=True)

    @property
    def pending_dir(self) -> Path:
        return self.root / "pending"

    @property
    def inflight_dir(self) -> Path:
        return self.root / "inflight"

    @property
    def done_dir(self) -> Path:
        return self.root / "done"

    @property
    def failed_dir(self) -> Path:
        return self.root / "failed"

    @property
    def poison_dir(self) -> Path:
        return self.root / "poison"

    def bucket_dir(self, bucket: SpoolBucket) -> Path:
        return spool_bucket_dir(root=self.root, bucket=bucket)

    def list_bucket_paths(self, *, bucket: SpoolBucket) -> list[Path]:
        return list_spool_bucket_paths(root=self.root, bucket=bucket)

    def queue_metrics_snapshot(self) -> QueueMetricsSnapshotV1:
        return QueueMetricsSnapshotV1(
            pending=len(self.list_bucket_paths(bucket="pending")),
            inflight=len(self.list_bucket_paths(bucket="inflight")),
            done=len(self.list_bucket_paths(bucket="done")),
            failed=len(self.list_bucket_paths(bucket="failed")),
            poison=len(self.list_bucket_paths(bucket="poison")),
            poison_transition_total=self._poison_transition_total,
            orphan_requeue_total=self._orphan_requeue_total,
            stale_inflight_recovery_total=self._stale_inflight_recovery_total,
        )

    def enqueue(self, task: TaskV1) -> Path:
        target = self.pending_dir / f"{task.task_id}.json"
        if self._task_exists(task.task_id):
            raise FileExistsError(f"Task already exists in spool queue: {task.task_id}")
        record = SpoolTaskRecord(task=task)
        _write_json_atomic(path=target, data=record.to_dict())
        return target

    def claim_next(self) -> ClaimedTask | None:
        for pending_path in self._sorted_task_files(self.pending_dir):
            inflight_path = self.inflight_dir / pending_path.name
            try:
                claimed_path = pending_path.rename(inflight_path)
            except FileNotFoundError:
                continue
            os.utime(claimed_path, None)
            return ClaimedTask(task=_load_task(claimed_path), claim_ref=claimed_path)
        return None

    def ack_success(self, claimed: ClaimedTask) -> Path:
        claim_path = self._claim_path(claimed)
        return claim_path.rename(self.done_dir / claim_path.name)

    def ack_failure(self, claimed: ClaimedTask) -> Path:
        claim_path = self._claim_path(claimed)
        record = _load_record(claim_path)
        failure_count = record.governance.failure_count + 1
        updated = SpoolTaskRecord(
            task=record.task,
            governance=TaskGovernanceV1(
                failure_count=failure_count,
                orphan_count=record.governance.orphan_count,
            ),
        )
        target_dir = (
            self.poison_dir if failure_count >= self.max_task_attempts else self.pending_dir
        )
        if target_dir == self.poison_dir:
            self._bump_counter("_poison_transition_total")
        return self._rewrite_record(path=claim_path, record=updated, target_dir=target_dir)

    def requeue_orphaned(self, claimed: ClaimedTask) -> Path:
        return self._requeue_orphaned_path(self._claim_path(claimed))

    def recover_stale_inflight(
        self,
        *,
        max_age_s: int,
        now_epoch_s: float | None = None,
    ) -> int:
        if max_age_s <= 0:
            raise ValueError("max_age_s must be > 0")

        now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
        recovered = 0

        for inflight_path in self._sorted_task_files(self.inflight_dir):
            age_s = now_s - inflight_path.stat().st_mtime
            if age_s < max_age_s:
                continue
            self._requeue_orphaned_path(inflight_path)
            recovered += 1

        if recovered > 0:
            self._bump_counter("_stale_inflight_recovery_total", recovered)

        return recovered

    @staticmethod
    def _sorted_task_files(root: Path) -> list[Path]:
        return sorted_spool_task_files(root)

    def _task_exists(self, task_id: str) -> bool:
        name = f"{task_id}.json"
        return any(
            path.exists()
            for path in (
                self.pending_dir / name,
                self.inflight_dir / name,
                self.done_dir / name,
                self.failed_dir / name,
                self.poison_dir / name,
            )
        )

    @staticmethod
    def _claim_path(claimed: ClaimedTask) -> Path:
        return Path(claimed.claim_ref)

    def _requeue_orphaned_path(self, path: Path) -> Path:
        record = _load_record(path)
        orphan_count = record.governance.orphan_count + 1
        updated = SpoolTaskRecord(
            task=record.task,
            governance=TaskGovernanceV1(
                failure_count=record.governance.failure_count,
                orphan_count=orphan_count,
            ),
        )
        target_dir = (
            self.poison_dir if orphan_count >= self.max_orphan_requeues else self.pending_dir
        )
        if target_dir == self.pending_dir:
            self._bump_counter("_orphan_requeue_total")
        else:
            self._bump_counter("_poison_transition_total")
        return self._rewrite_record(path=path, record=updated, target_dir=target_dir)

    def _bump_counter(self, attr: str, value: int = 1) -> None:
        object.__setattr__(self, attr, getattr(self, attr) + value)

    @staticmethod
    def _rewrite_record(*, path: Path, record: SpoolTaskRecord, target_dir: Path) -> Path:
        target = target_dir / path.name
        _write_json_atomic(path=target, data=record.to_dict())
        path.unlink(missing_ok=True)
        return target


SpoolQueueWorkSource = QueueBackendWorkSource
TaskWorkItem = QueueWorkItem
