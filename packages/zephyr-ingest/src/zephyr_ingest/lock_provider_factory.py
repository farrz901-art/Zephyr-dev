from __future__ import annotations

from pathlib import Path
from typing import Literal

from zephyr_ingest.lock_provider import FileLockProvider, LockProvider
from zephyr_ingest.sqlite_lock_provider import SqliteLockProvider

LocalLockProviderKind = Literal["file", "sqlite"]


def build_local_lock_provider(
    *,
    kind: LocalLockProviderKind,
    root: Path,
    stale_after_s: int | None = None,
) -> LockProvider:
    if kind == "file":
        return FileLockProvider(root=root, stale_after_s=stale_after_s)
    return SqliteLockProvider(root=root, stale_after_s=stale_after_s)
