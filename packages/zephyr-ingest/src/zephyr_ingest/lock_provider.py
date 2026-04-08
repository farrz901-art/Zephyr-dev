from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class AcquiredLock:
    key: str
    owner: str
    lock_ref: Path


LockAcquireStatus = Literal["acquired", "contended"]
LockLeaseLikeStatus = Literal["not_modeled"]


@dataclass(frozen=True, slots=True)
class LockHolderFactsV1:
    key: str
    owner: str
    holder: str | None
    lease_like_status: LockLeaseLikeStatus = "not_modeled"


@dataclass(frozen=True, slots=True)
class LockAcquireResultV1:
    key: str
    owner: str
    status: LockAcquireStatus
    acquired_lock: AcquiredLock | None = None
    stale_recovered: bool = False

    @property
    def holder_facts(self) -> LockHolderFactsV1:
        holder = None if self.acquired_lock is None else self.acquired_lock.owner
        return LockHolderFactsV1(
            key=self.key,
            owner=self.owner,
            holder=holder,
        )

    @property
    def is_acquired(self) -> bool:
        return self.status == "acquired"

    @property
    def is_contended(self) -> bool:
        return self.status == "contended"


def acquire_lock(
    *,
    provider: "LockProvider",
    key: str,
    owner: str,
) -> LockAcquireResultV1:
    stale_recovery_total_before: int | None = None
    if isinstance(provider, SupportsLockMetricsSnapshot):
        stale_recovery_total_before = provider.lock_metrics_snapshot().stale_recovery_total

    acquired_lock = provider.acquire(key=key, owner=owner)
    if acquired_lock is None:
        return LockAcquireResultV1(key=key, owner=owner, status="contended")

    stale_recovered = False
    if stale_recovery_total_before is not None and isinstance(
        provider, SupportsLockMetricsSnapshot
    ):
        stale_recovery_total_after = provider.lock_metrics_snapshot().stale_recovery_total
        stale_recovered = stale_recovery_total_after > stale_recovery_total_before

    return LockAcquireResultV1(
        key=key,
        owner=owner,
        status="acquired",
        acquired_lock=acquired_lock,
        stale_recovered=stale_recovered,
    )


@runtime_checkable
class LockProvider(Protocol):
    def acquire(self, *, key: str, owner: str) -> AcquiredLock | None: ...

    def release(self, lock: AcquiredLock) -> None: ...


@dataclass(frozen=True, slots=True)
class LockMetricsSnapshotV1:
    stale_recovery_total: int


@runtime_checkable
class SupportsLockMetricsSnapshot(Protocol):
    def lock_metrics_snapshot(self) -> LockMetricsSnapshotV1: ...


@dataclass(frozen=True, slots=True)
class FileLockProvider:
    root: Path
    stale_after_s: int | None = None
    _stale_recovery_total: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if self.stale_after_s is not None and self.stale_after_s <= 0:
            raise ValueError("stale_after_s must be > 0")

    def acquire(self, *, key: str, owner: str) -> AcquiredLock | None:
        lock_path = self._lock_path_for_key(key)
        payload = {
            "key": key,
            "owner": owner,
            "acquired_at_epoch_s": int(time.time()),
        }

        try:
            self._write_lock(path=lock_path, payload=payload)
            return AcquiredLock(key=key, owner=owner, lock_ref=lock_path)
        except FileExistsError:
            if self.stale_after_s is None:
                return None
            if not self._break_stale_lock(path=lock_path):
                return None
            try:
                self._write_lock(path=lock_path, payload=payload)
            except FileExistsError:
                return None
            return AcquiredLock(key=key, owner=owner, lock_ref=lock_path)

    def release(self, lock: AcquiredLock) -> None:
        lock.lock_ref.unlink(missing_ok=True)

    def lock_metrics_snapshot(self) -> LockMetricsSnapshotV1:
        return LockMetricsSnapshotV1(stale_recovery_total=self._stale_recovery_total)

    def _break_stale_lock(self, *, path: Path) -> bool:
        try:
            stale_after_s = self.stale_after_s
            if stale_after_s is None:
                return False
            age_s = time.time() - path.stat().st_mtime
            if age_s < stale_after_s:
                return False
            path.unlink(missing_ok=True)
            object.__setattr__(self, "_stale_recovery_total", self._stale_recovery_total + 1)
            return True
        except FileNotFoundError:
            return True

    def _lock_path_for_key(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.root / f"{digest}.lock"

    @staticmethod
    def _write_lock(*, path: Path, payload: Mapping[str, object]) -> None:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
