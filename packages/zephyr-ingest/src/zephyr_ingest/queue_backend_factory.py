from __future__ import annotations

from pathlib import Path
from typing import Literal

from zephyr_ingest.queue_backend import QueueBackend
from zephyr_ingest.spool_queue import (
    DEFAULT_MAX_ORPHAN_REQUEUES,
    DEFAULT_MAX_TASK_ATTEMPTS,
    LocalSpoolQueue,
)
from zephyr_ingest.sqlite_queue import SqliteQueueBackend

LocalQueueBackendKind = Literal["spool", "sqlite"]


def build_local_queue_backend(
    *,
    kind: LocalQueueBackendKind,
    root: Path,
    max_task_attempts: int = DEFAULT_MAX_TASK_ATTEMPTS,
    max_orphan_requeues: int = DEFAULT_MAX_ORPHAN_REQUEUES,
) -> QueueBackend:
    if kind == "spool":
        return LocalSpoolQueue(
            root=root,
            max_task_attempts=max_task_attempts,
            max_orphan_requeues=max_orphan_requeues,
        )
    return SqliteQueueBackend(
        root=root,
        max_task_attempts=max_task_attempts,
        max_orphan_requeues=max_orphan_requeues,
    )
