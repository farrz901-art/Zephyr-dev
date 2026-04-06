from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

from zephyr_ingest.lock_provider import AcquiredLock, LockMetricsSnapshotV1


@dataclass(frozen=True, slots=True)
class SqliteLockProvider:
    root: Path
    stale_after_s: int | None = None
    db_filename: str = "locks.sqlite3"
    _stale_recovery_total: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if self.stale_after_s is not None and self.stale_after_s <= 0:
            raise ValueError("stale_after_s must be > 0")
        self._init_db()

    @property
    def db_path(self) -> Path:
        return self.root / self.db_filename

    def acquire(self, *, key: str, owner: str) -> AcquiredLock | None:
        acquired_at_epoch_s = int(time.time())
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO task_locks(key, owner, acquired_at_epoch_s)
                    VALUES (?, ?, ?)
                    """,
                    (key, owner, acquired_at_epoch_s),
                )
            return AcquiredLock(key=key, owner=owner, lock_ref=self.db_path)
        except sqlite3.IntegrityError:
            if self.stale_after_s is None:
                return None
            if not self._break_stale_lock(key=key):
                return None
            try:
                with self._connect() as conn:
                    conn.execute(
                        """
                        INSERT INTO task_locks(key, owner, acquired_at_epoch_s)
                        VALUES (?, ?, ?)
                        """,
                        (key, owner, acquired_at_epoch_s),
                    )
            except sqlite3.IntegrityError:
                return None
            return AcquiredLock(key=key, owner=owner, lock_ref=self.db_path)

    def release(self, lock: AcquiredLock) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM task_locks WHERE key = ?", (lock.key,))

    def lock_metrics_snapshot(self) -> LockMetricsSnapshotV1:
        return LockMetricsSnapshotV1(stale_recovery_total=self._stale_recovery_total)

    def _break_stale_lock(self, *, key: str) -> bool:
        stale_after_s = self.stale_after_s
        if stale_after_s is None:
            return False
        cutoff = int(time.time()) - stale_after_s
        with self._connect() as conn:
            deleted = conn.execute(
                """
                DELETE FROM task_locks
                WHERE key = ?
                  AND acquired_at_epoch_s <= ?
                """,
                (key, cutoff),
            ).rowcount
        if deleted > 0:
            object.__setattr__(self, "_stale_recovery_total", self._stale_recovery_total + deleted)
            return True
        return False

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_locks (
                    key TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    acquired_at_epoch_s INTEGER NOT NULL
                )
                """
            )
