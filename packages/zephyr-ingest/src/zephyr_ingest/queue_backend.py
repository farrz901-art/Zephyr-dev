from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from zephyr_ingest.task_v1 import TaskV1

QueueRecordRef = str | Path


@dataclass(frozen=True, slots=True)
class ClaimedTask:
    task: TaskV1
    claim_ref: QueueRecordRef

    @property
    def task_id(self) -> str:
        return self.task.task_id


@runtime_checkable
class QueueBackend(Protocol):
    def enqueue(self, task: TaskV1) -> QueueRecordRef: ...

    def claim_next(self) -> ClaimedTask | None: ...

    def ack_success(self, claimed: ClaimedTask) -> QueueRecordRef: ...

    def ack_failure(self, claimed: ClaimedTask) -> QueueRecordRef: ...

    def recover_stale_inflight(
        self,
        *,
        max_age_s: int,
        now_epoch_s: float | None = None,
    ) -> int: ...


class TaskHandler(Protocol):
    def __call__(self, task: TaskV1) -> None: ...


@dataclass(frozen=True, slots=True)
class QueueBackendWorkSource:
    backend: QueueBackend
    handler: TaskHandler
    recover_inflight_after_s: int | None = None

    def poll(self) -> QueueWorkItem | None:
        if self.recover_inflight_after_s is not None:
            self.backend.recover_stale_inflight(max_age_s=self.recover_inflight_after_s)

        claimed = self.backend.claim_next()
        if claimed is None:
            return None
        return QueueWorkItem(backend=self.backend, claimed=claimed, handler=self.handler)


@dataclass(frozen=True, slots=True)
class QueueWorkItem:
    backend: QueueBackend
    claimed: ClaimedTask
    handler: TaskHandler

    def __call__(self) -> None:
        try:
            self.handler(self.claimed.task)
        except Exception:
            self.backend.ack_failure(self.claimed)
            raise
        self.backend.ack_success(self.claimed)
