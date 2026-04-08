from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from zephyr_ingest.lock_provider import FileLockProvider
from zephyr_ingest.lock_provider_factory import LocalLockProviderKind, build_local_lock_provider
from zephyr_ingest.sqlite_lock_provider import SqliteLockProvider


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


def test_sqlite_lock_provider_satisfies_factory_selection(tmp_path: Path) -> None:
    selected = build_local_lock_provider(kind="sqlite", root=tmp_path / "sqlite-locks")

    assert isinstance(selected, SqliteLockProvider)
    assert selected.db_path == (tmp_path / "sqlite-locks" / "locks.sqlite3")
    assert selected.db_path.exists()


def test_sqlite_lock_provider_acquires_and_releases_lock(tmp_path: Path) -> None:
    provider = SqliteLockProvider(root=tmp_path / "sqlite-locks")

    acquired = provider.acquire(key="task-key", owner="worker-a")

    assert acquired is not None
    assert acquired.key == "task-key"
    assert acquired.owner == "worker-a"
    assert acquired.lock_ref == provider.db_path
    assert provider.db_path.exists()

    provider.release(acquired)

    with sqlite3.connect(provider.db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM task_locks WHERE key = ?",
            ("task-key",),
        ).fetchone()
    assert row is not None
    assert row[0] == 0


def test_sqlite_lock_provider_returns_none_on_contention(tmp_path: Path) -> None:
    provider = SqliteLockProvider(root=tmp_path / "sqlite-locks")
    first = provider.acquire(key="task-key", owner="worker-a")
    assert first is not None

    second = provider.acquire(key="task-key", owner="worker-b")

    assert second is None


def test_sqlite_lock_provider_breaks_stale_lock(tmp_path: Path) -> None:
    provider = SqliteLockProvider(root=tmp_path / "sqlite-locks", stale_after_s=10)
    first = provider.acquire(key="task-key", owner="worker-a")
    assert first is not None

    with sqlite3.connect(provider.db_path) as conn:
        conn.execute(
            "UPDATE task_locks SET acquired_at_epoch_s = ? WHERE key = ?",
            (25, "task-key"),
        )

    original_time = os.path.getmtime(provider.db_path)
    os.utime(provider.db_path, (original_time, original_time))
    second = provider.acquire(key="task-key", owner="worker-b")

    assert second is not None
    assert second.owner == "worker-b"
    assert provider.lock_metrics_snapshot().stale_recovery_total == 1


def test_sqlite_lock_provider_rejects_non_positive_stale_ttl(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="stale_after_s must be > 0"):
        SqliteLockProvider(root=tmp_path / "sqlite-locks", stale_after_s=0)


def _make_stale_lock(
    *,
    provider: FileLockProvider | SqliteLockProvider,
    key: str,
) -> None:
    if isinstance(provider, FileLockProvider):
        acquired = provider.acquire(key=key, owner="worker-a")
        assert acquired is not None
        os.utime(acquired.lock_ref, (25, 25))
        return

    acquired = provider.acquire(key=key, owner="worker-a")
    assert acquired is not None
    with sqlite3.connect(provider.db_path) as conn:
        conn.execute(
            "UPDATE task_locks SET acquired_at_epoch_s = ? WHERE key = ?",
            (25, key),
        )


@pytest.mark.parametrize("kind", ["file", "sqlite"])
def test_lock_provider_shared_contention_stale_and_owner_governance(
    tmp_path: Path,
    kind: LocalLockProviderKind,
) -> None:
    if kind == "file":
        provider: FileLockProvider | SqliteLockProvider = FileLockProvider(
            root=tmp_path / "locks",
            stale_after_s=10,
        )
    else:
        provider = SqliteLockProvider(
            root=tmp_path / "sqlite-locks",
            stale_after_s=10,
        )

    first = provider.acquire(key="task-key", owner="worker-a")

    assert first is not None
    assert first.key == "task-key"
    assert first.owner == "worker-a"

    contended = provider.acquire(key="task-key", owner="worker-b")

    assert contended is None

    _make_stale_lock(provider=provider, key="task-stale")

    recovered = provider.acquire(key="task-stale", owner="worker-c")

    assert recovered is not None
    assert recovered.key == "task-stale"
    assert recovered.owner == "worker-c"
    assert provider.lock_metrics_snapshot().stale_recovery_total == 1
