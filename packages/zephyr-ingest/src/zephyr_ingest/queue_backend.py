from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from zephyr_ingest.lock_provider import AcquiredLock, LockProvider, acquire_lock
from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
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

    def requeue_orphaned(self, claimed: ClaimedTask) -> QueueRecordRef: ...

    def recover_stale_inflight(
        self,
        *,
        max_age_s: int,
        now_epoch_s: float | None = None,
    ) -> int: ...


class TaskHandler(Protocol):
    def __call__(self, task: TaskV1) -> None: ...


@dataclass(frozen=True, slots=True)
class QueueWorkSourceMetricsV1:
    lock_contention_total: int


@dataclass(frozen=True, slots=True)
class QueueBackendWorkSource:
    backend: QueueBackend
    handler: TaskHandler
    recover_inflight_after_s: int | None = None
    lock_provider: LockProvider | None = None
    lock_owner: str = "worker"
    _lock_contention_total: int = field(default=0, init=False, repr=False)

    def work_source_metrics_snapshot(self) -> QueueWorkSourceMetricsV1:
        return QueueWorkSourceMetricsV1(lock_contention_total=self._lock_contention_total)

    def poll(self) -> QueueWorkItem | None:
        if self.recover_inflight_after_s is not None:
            self.backend.recover_stale_inflight(max_age_s=self.recover_inflight_after_s)

        claimed = self.backend.claim_next()
        if claimed is None:
            return None
        acquired_lock = None
        if self.lock_provider is not None:
            acquisition = acquire_lock(
                provider=self.lock_provider,
                key=normalize_task_idempotency_key(claimed.task),
                owner=self.lock_owner,
            )
            if acquisition.is_contended:
                object.__setattr__(
                    self,
                    "_lock_contention_total",
                    self._lock_contention_total + 1,
                )
                self.backend.requeue_orphaned(claimed)
                return None
            acquired_lock = acquisition.acquired_lock
        return QueueWorkItem(
            backend=self.backend,
            claimed=claimed,
            handler=self.handler,
            lock_provider=self.lock_provider,
            acquired_lock=acquired_lock,
        )


@dataclass(frozen=True, slots=True)
class QueueWorkItem:
    backend: QueueBackend
    claimed: ClaimedTask
    handler: TaskHandler
    lock_provider: LockProvider | None = None
    acquired_lock: AcquiredLock | None = None

    def __call__(self) -> None:
        try:
            self.handler(self.claimed.task)
        except Exception:
            self.backend.ack_failure(self.claimed)
            raise
        else:
            self.backend.ack_success(self.claimed)
        finally:
            if self.lock_provider is not None and self.acquired_lock is not None:
                self.lock_provider.release(self.acquired_lock)
