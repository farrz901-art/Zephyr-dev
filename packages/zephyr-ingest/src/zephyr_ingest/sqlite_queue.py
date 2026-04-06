from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from zephyr_ingest.queue_backend import ClaimedTask
from zephyr_ingest.task_v1 import TaskV1

SqliteQueueBucket = Literal["pending", "inflight", "done", "failed", "poison"]


@dataclass(frozen=True, slots=True)
class SqliteQueueBackend:
    root: Path
    max_task_attempts: int = 1
    max_orphan_requeues: int = 1
    db_filename: str = "queue.sqlite3"

    def __post_init__(self) -> None:
        if self.max_task_attempts <= 0:
            raise ValueError("max_task_attempts must be > 0")
        if self.max_orphan_requeues <= 0:
            raise ValueError("max_orphan_requeues must be > 0")
        self.root.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @property
    def db_path(self) -> Path:
        return self.root / self.db_filename

    def enqueue(self, task: TaskV1) -> str:
        now = time.time()
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO queue_tasks (
                        task_id,
                        bucket,
                        task_json,
                        failure_count,
                        orphan_count,
                        claimed_at,
                        created_at,
                        updated_at
                    ) VALUES (?, 'pending', ?, 0, 0, NULL, ?, ?)
                    """,
                    (task.task_id, self._dump_task(task), now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise FileExistsError(f"Task already exists in sqlite queue: {task.task_id}") from exc
        return task.task_id

    def claim_next(self) -> ClaimedTask | None:
        while True:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT task_id, task_json
                    FROM queue_tasks
                    WHERE bucket = 'pending'
                    ORDER BY updated_at, task_id
                    LIMIT 1
                    """
                ).fetchone()
                if row is None:
                    return None
                task_id = self._read_str(row, "task_id")
                now = time.time()
                updated = conn.execute(
                    """
                    UPDATE queue_tasks
                    SET bucket = 'inflight',
                        claimed_at = ?,
                        updated_at = ?
                    WHERE task_id = ?
                      AND bucket = 'pending'
                    """,
                    (now, now, task_id),
                ).rowcount
                if updated == 0:
                    continue
                return ClaimedTask(
                    task=self._load_task(self._read_str(row, "task_json")),
                    claim_ref=task_id,
                )

    def ack_success(self, claimed: ClaimedTask) -> str:
        self._set_bucket(
            task_id=self._claim_task_id(claimed),
            expected_bucket="inflight",
            target_bucket="done",
            failure_count=None,
            orphan_count=None,
        )
        return self._claim_task_id(claimed)

    def ack_failure(self, claimed: ClaimedTask) -> str:
        task_id = self._claim_task_id(claimed)
        row = self._load_row(task_id=task_id, expected_bucket="inflight")
        failure_count = self._read_int(row, "failure_count") + 1
        target_bucket: SqliteQueueBucket = (
            "poison" if failure_count >= self.max_task_attempts else "pending"
        )
        self._set_bucket(
            task_id=task_id,
            expected_bucket="inflight",
            target_bucket=target_bucket,
            failure_count=failure_count,
            orphan_count=self._read_int(row, "orphan_count"),
        )
        return task_id

    def requeue_orphaned(self, claimed: ClaimedTask) -> str:
        return self._requeue_orphaned_task_id(task_id=self._claim_task_id(claimed))

    def recover_stale_inflight(
        self,
        *,
        max_age_s: int,
        now_epoch_s: float | None = None,
    ) -> int:
        if max_age_s <= 0:
            raise ValueError("max_age_s must be > 0")

        cutoff = (time.time() if now_epoch_s is None else float(now_epoch_s)) - max_age_s
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT task_id
                FROM queue_tasks
                WHERE bucket = 'inflight'
                  AND claimed_at IS NOT NULL
                  AND claimed_at <= ?
                ORDER BY claimed_at, task_id
                """,
                (cutoff,),
            ).fetchall()

        recovered = 0
        for row in rows:
            task_id = self._read_str(row, "task_id")
            try:
                self._requeue_orphaned_task_id(task_id=task_id)
            except FileNotFoundError:
                continue
            recovered += 1
        return recovered

    def _requeue_orphaned_task_id(self, *, task_id: str) -> str:
        row = self._load_row(task_id=task_id, expected_bucket="inflight")
        orphan_count = self._read_int(row, "orphan_count") + 1
        target_bucket: SqliteQueueBucket = (
            "poison" if orphan_count >= self.max_orphan_requeues else "pending"
        )
        self._set_bucket(
            task_id=task_id,
            expected_bucket="inflight",
            target_bucket=target_bucket,
            failure_count=self._read_int(row, "failure_count"),
            orphan_count=orphan_count,
        )
        return task_id

    def _set_bucket(
        self,
        *,
        task_id: str,
        expected_bucket: SqliteQueueBucket,
        target_bucket: SqliteQueueBucket,
        failure_count: int | None,
        orphan_count: int | None,
    ) -> None:
        now = time.time()
        with self._connect() as conn:
            updated = conn.execute(
                """
                UPDATE queue_tasks
                SET bucket = ?,
                    claimed_at = NULL,
                    updated_at = ?,
                    failure_count = COALESCE(?, failure_count),
                    orphan_count = COALESCE(?, orphan_count)
                WHERE task_id = ?
                  AND bucket = ?
                """,
                (target_bucket, now, failure_count, orphan_count, task_id, expected_bucket),
            ).rowcount
        if updated == 0:
            raise FileNotFoundError(f"Task not found in {expected_bucket}: {task_id}")

    def _load_row(
        self,
        *,
        task_id: str,
        expected_bucket: SqliteQueueBucket,
    ) -> sqlite3.Row:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT task_id, task_json, failure_count, orphan_count
                FROM queue_tasks
                WHERE task_id = ?
                  AND bucket = ?
                """,
                (task_id, expected_bucket),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"Task not found in {expected_bucket}: {task_id}")
        return row

    @staticmethod
    def _claim_task_id(claimed: ClaimedTask) -> str:
        claim_ref = claimed.claim_ref
        if not isinstance(claim_ref, str):
            raise TypeError("sqlite queue claim_ref must be a task id string")
        return claim_ref

    @staticmethod
    def _dump_task(task: TaskV1) -> str:
        return json.dumps(task.to_dict(), sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _load_task(payload: str) -> TaskV1:
        obj = json.loads(payload)
        if not isinstance(obj, dict):
            raise ValueError("sqlite queue task payload must be a JSON object")
        return TaskV1.from_dict(cast("dict[str, object]", obj))

    @staticmethod
    def _read_str(row: sqlite3.Row, key: str) -> str:
        value = row[key]
        if not isinstance(value, str):
            raise TypeError(f"sqlite queue field '{key}' must be a string")
        return value

    @staticmethod
    def _read_int(row: sqlite3.Row, key: str) -> int:
        value = row[key]
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"sqlite queue field '{key}' must be an integer")
        return value

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue_tasks (
                    task_id TEXT PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    task_json TEXT NOT NULL,
                    failure_count INTEGER NOT NULL,
                    orphan_count INTEGER NOT NULL,
                    claimed_at REAL NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_queue_tasks_bucket_updated
                ON queue_tasks(bucket, updated_at, task_id)
                """
            )
