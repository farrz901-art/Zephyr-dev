from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from zephyr_ingest.queue_backend import ClaimedTask, QueueBackendWorkSource, QueueWorkItem
from zephyr_ingest.task_v1 import TaskV1


def _load_task(path: Path) -> TaskV1:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"Spool record must be a JSON object: {path}")
    return TaskV1.from_dict(cast("dict[str, object]", obj))


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

    def __post_init__(self) -> None:
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.inflight_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)

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

    def enqueue(self, task: TaskV1) -> Path:
        target = self.pending_dir / f"{task.task_id}.json"
        if self._task_exists(task.task_id):
            raise FileExistsError(f"Task already exists in spool queue: {task.task_id}")
        _write_json_atomic(path=target, data=task.to_dict())
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
        return claim_path.rename(self.failed_dir / claim_path.name)

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
            inflight_path.rename(self.pending_dir / inflight_path.name)
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
            )
        )

    @staticmethod
    def _claim_path(claimed: ClaimedTask) -> Path:
        return Path(claimed.claim_ref)


SpoolQueueWorkSource = QueueBackendWorkSource
TaskWorkItem = QueueWorkItem
