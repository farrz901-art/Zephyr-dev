from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, cast

from zephyr_ingest.queue_backend import ClaimedTask, QueueBackendWorkSource, QueueWorkItem
from zephyr_ingest.task_v1 import TaskV1


class TaskGovernanceV1Dict(TypedDict):
    failure_count: int
    orphan_count: int


class SpoolTaskRecordDict(TypedDict):
    task: dict[str, object]
    governance: TaskGovernanceV1Dict


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

    def to_dict(self) -> SpoolTaskRecordDict:
        return {
            "task": cast("dict[str, object]", self.task.to_dict()),
            "governance": self.governance.to_dict(),
        }


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
        return SpoolTaskRecord(
            task=TaskV1.from_dict(cast("dict[str, object]", task_obj)),
            governance=TaskGovernanceV1(
                failure_count=failure_count,
                orphan_count=orphan_count,
            ),
        )

    return SpoolTaskRecord(task=TaskV1.from_dict(raw))


def _load_task(path: Path) -> TaskV1:
    return _load_record(path).task


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

        return recovered

    @staticmethod
    def _sorted_task_files(root: Path) -> list[Path]:
        return sorted(
            [path for path in root.glob("*.json") if path.is_file()],
            key=lambda path: (path.stat().st_mtime_ns, path.name),
        )

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
        return self._rewrite_record(path=path, record=updated, target_dir=target_dir)

    @staticmethod
    def _rewrite_record(*, path: Path, record: SpoolTaskRecord, target_dir: Path) -> Path:
        target = target_dir / path.name
        _write_json_atomic(path=target, data=record.to_dict())
        path.unlink(missing_ok=True)
        return target


SpoolQueueWorkSource = QueueBackendWorkSource
TaskWorkItem = QueueWorkItem
