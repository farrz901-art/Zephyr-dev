from __future__ import annotations

import os
from pathlib import Path

import pytest

from zephyr_ingest.lock_provider import FileLockProvider


def test_file_lock_provider_acquires_and_releases_lock(tmp_path: Path) -> None:
    provider = FileLockProvider(root=tmp_path / "locks")

    acquired = provider.acquire(key="task-key", owner="worker-a")

    assert acquired is not None
    assert acquired.key == "task-key"
    assert acquired.owner == "worker-a"
    assert acquired.lock_ref.exists()

    provider.release(acquired)

    assert not acquired.lock_ref.exists()


def test_file_lock_provider_returns_none_on_contention(tmp_path: Path) -> None:
    provider = FileLockProvider(root=tmp_path / "locks")
    first = provider.acquire(key="task-key", owner="worker-a")
    assert first is not None

    second = provider.acquire(key="task-key", owner="worker-b")

    assert second is None


def test_file_lock_provider_breaks_stale_lock(tmp_path: Path) -> None:
    provider = FileLockProvider(root=tmp_path / "locks", stale_after_s=10)
    first = provider.acquire(key="task-key", owner="worker-a")
    assert first is not None
    os.utime(first.lock_ref, (25, 25))

    second = provider.acquire(key="task-key", owner="worker-b")

    assert second is not None
    assert second.owner == "worker-b"
    assert second.lock_ref.exists()


def test_file_lock_provider_rejects_non_positive_stale_ttl(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="stale_after_s must be > 0"):
        FileLockProvider(root=tmp_path / "locks", stale_after_s=0)
