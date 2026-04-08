from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from zephyr_core import DocumentRef, PartitionStrategy
from zephyr_ingest import cli
from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1
from zephyr_ingest.spool_queue import LocalSpoolQueue
from zephyr_ingest.sqlite_queue import SqliteQueueBackend
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)


def _make_queue_task(task_id: str) -> TaskV1:
    doc = DocumentRef(
        uri=f"/tmp/{task_id}.pdf",
        source="local_file",
        discovered_at_utc="2026-04-05T00:00:00Z",
        filename=f"{task_id}.pdf",
        extension=".pdf",
        size_bytes=128,
    )
    return TaskV1(
        task_id=task_id,
        kind="it",
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
        execution=TaskExecutionV1(
            strategy=PartitionStrategy.AUTO,
            unique_element_ids=True,
        ),
        identity=TaskIdentityV1(
            pipeline_version="2026.04.06",
            sha256=f"sha256-{task_id}",
        ),
    )


def test_cli_run_invokes_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        assert cfg.out_root == tmp_path / "out"
        assert cfg.retry.enabled is False
        assert cfg.retry.max_attempts == 1
        assert cfg.retry.base_backoff_ms == 0
        assert cfg.retry.max_backoff_ms == 0
        assert cfg.workers == 4

        dest = kwargs.get("destination")
        assert "destination" in kwargs
        assert dest is not None
        assert getattr(dest, "name").startswith("filesystem")

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert isinstance(snap, dict)
        assert snap["schema_version"] == 1

        # 顶层 key
        assert "destinations" in snap
        assert "backend" in snap
        assert "runner" in snap

        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.kind") in ("cli", "env", "file", "default")

        # backend 脱敏规则（默认 local）
        backend = snap["backend"]
        assert backend["kind"] == "local"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--glob",
            "*.txt",
            "--out",
            str(tmp_path / "out"),
            "--strategy",
            "auto",
            "--no-retry",
            "--max-attempts",
            "1",
            "--base-backoff-ms",
            "0",
            "--max-backoff-ms",
            "0",
            "--workers",
            "4",
            # 移除 --destination filesystem，因为新版 cli 默认开启 fs 且不支持该参数
        ]
    )

    assert rc == 0
    assert called["ok"] is True


def test_cli_run_webhook_fanout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """测试 Webhook 触发的 Fanout 逻辑"""
    called = {"ok": False}
    # 创建 dummy 目录避免 Source 报错
    (tmp_path / "dummy.txt").touch()

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True

        dest = kwargs.get("destination")
        assert dest is not None
        # 当同时存在 Filesystem 和 Webhook 时，会自动使用 fanout
        assert getattr(dest, "name") == "fanout"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert isinstance(snap, dict)
        assert snap["schema_version"] == 1

        assert "destinations" in snap
        assert "backend" in snap
        assert "runner" in snap

        # destinations 必须包含 filesystem + webhook
        dests = snap["destinations"]
        # assert isinstance(dests, dict)
        assert "filesystem" in dests
        assert "webhook" in dests

        # backend 默认 local
        backend = snap["backend"]
        assert backend["kind"] == "local"
        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("destinations.webhook.url") == "cli"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(tmp_path / "dummy.txt"),
            "--out",
            str(tmp_path / "out"),
            "--strategy",
            "auto",
            "--webhook-url",
            "http://test.com",
        ]
    )
    assert rc == 0
    assert called["ok"] is True


def test_cli_run_webhook_controls_flow_into_snapshot_and_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called = {"ok": False}
    (tmp_path / "dummy.txt").touch()

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True

        dest = kwargs.get("destination")
        assert dest is not None
        assert getattr(dest, "name") == "fanout"
        children = getattr(dest, "destinations")
        webhook_dest = next(d for d in children if getattr(d, "name") == "webhook")
        assert getattr(webhook_dest, "timeout_s") == 4.5
        assert getattr(webhook_dest, "max_inflight") == 3
        assert getattr(webhook_dest, "rate_limit") == 1.5

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        webhook = cast(dict[str, object], snap["destinations"].get("webhook", {}))
        assert webhook["timeout_s"] == 4.5
        assert webhook["max_inflight"] == 3
        assert webhook["rate_limit"] == 1.5

        sources = snap.get("sources")
        assert sources is not None
        assert sources.get("destinations.webhook.timeout_s") == "cli"
        assert sources.get("destinations.webhook.max_inflight") == "cli"
        assert sources.get("destinations.webhook.rate_limit") == "cli"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(tmp_path / "dummy.txt"),
            "--out",
            str(tmp_path / "out"),
            "--webhook-url",
            "http://test.com",
            "--webhook-timeout-s",
            "4.5",
            "--webhook-max-inflight",
            "3",
            "--webhook-rate-limit",
            "1.5",
        ]
    )
    assert rc == 0
    assert called["ok"] is True


def test_cli_run_backend_uns_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """测试 --backend uns-api 是否正确构造了后端实例"""
    called = {"ok": False}
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "dummy.txt").touch()

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        assert cfg.backend is not None
        # 验证 HttpUnsApiBackend 的属性
        assert getattr(cfg.backend, "backend") == "uns_api"
        assert getattr(cfg.backend, "url") == "https://api.test.com/v0"
        assert getattr(cfg.backend, "timeout_s") == 12.5

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert isinstance(snap, dict)
        assert snap["schema_version"] == 1

        assert "destinations" in snap
        assert "backend" in snap
        assert "runner" in snap

        backend = snap["backend"]
        assert backend["kind"] == "uns-api"

        # api_key 必须脱敏
        assert backend["api_key"] in ("***", None)
        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.kind") == "cli"
        assert sources.get("backend.api_key") == "cli"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox / "dummy.txt"),
            "--out",
            str(tmp_path / "out"),
            "--backend",
            "uns-api",
            "--uns-api-url",
            "https://api.test.com/v0",
            "--uns-api-timeout-s",
            "12.5",
            "--uns-api-key",
            "test-key-123",
        ]
    )

    assert rc == 0
    assert called["ok"] is True


def test_cli_run_backend_local_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """测试默认情况下 backend 应该是 None (本地模式)"""
    called = {"ok": False}
    (tmp_path / "dummy.txt").touch()

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        assert cfg.backend is None

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert isinstance(snap, dict)
        assert snap["schema_version"] == 1

        assert "destinations" in snap
        assert "backend" in snap
        assert "runner" in snap

        backend = snap["backend"]
        assert backend["kind"] == "local"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(tmp_path / "dummy.txt"),
            "--out",
            str(tmp_path / "out"),
            "--backend",
            "local",
        ]
    )

    assert rc == 0
    assert called["ok"] is True


def test_cli_queue_inspect_prints_summary_and_bucket_tasks(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    queue.enqueue(_make_queue_task("task-cli"))
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    rc = cli.main(
        [
            "queue",
            "inspect",
            "--root",
            str(queue.root),
            "--bucket",
            "poison",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"] == {
        "pending": 0,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 1,
        "listed_tasks": 1,
        "governance_problem_tasks": 1,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "persisted_in_history",
    }
    assert payload["bucket"] == "poison"
    assert payload["tasks"] == [
        {
            "bucket": "poison",
            "state": "poison",
            "governance_labels": [],
            "governance_problem": "poison_attempts_exhausted",
            "poison_kind": "attempts_exhausted",
            "handling_expectation": "requeue_supported",
            "recovery_audit_support": "persisted_in_history",
            "task_id": "task-cli",
            "kind": "it",
            "record_path": str((queue.poison_dir / "task-cli.json").resolve()),
            "updated_at_utc": payload["tasks"][0]["updated_at_utc"],
            "uri": "/tmp/task-cli.pdf",
            "source": "local_file",
            "filename": "task-cli.pdf",
            "discovered_at_utc": "2026-04-05T00:00:00Z",
            "identity": {
                "pipeline_version": "2026.04.06",
                "sha256": "sha256-task-cli",
            },
            "failure_count": 1,
            "orphan_count": 0,
            "latest_recovery": None,
        }
    ]


def test_cli_queue_requeue_prints_recovery_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    queue = LocalSpoolQueue(root=tmp_path / "spool", max_task_attempts=1)
    queue.enqueue(_make_queue_task("task-recover-cli"))
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    rc = cli.main(
        [
            "queue",
            "requeue",
            "--root",
            str(queue.root),
            "--bucket",
            "poison",
            "--task-id",
            "task-recover-cli",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "action": "requeue",
        "support_status": "supported",
        "governance_result": "moved_to_pending",
        "redrive_support": "not_modeled",
        "audit_support": "persisted_in_history",
        "root": str(queue.root.resolve()),
        "task_id": "task-recover-cli",
        "kind": "it",
        "source_bucket": "poison",
        "target_bucket": "pending",
        "source_path": str((queue.poison_dir / "task-recover-cli.json").resolve()),
        "target_path": str((queue.pending_dir / "task-recover-cli.json").resolve()),
        "failure_count": 1,
        "orphan_count": 0,
        "recorded_at_utc": payload["recorded_at_utc"],
    }
    assert (queue.pending_dir / "task-recover-cli.json").exists()
    assert not (queue.poison_dir / "task-recover-cli.json").exists()


def test_cli_queue_sqlite_backend_supports_shared_inspection_and_requeue_subset(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    queue = SqliteQueueBackend(root=tmp_path / "sqlite-queue", max_task_attempts=1)
    queue.enqueue(_make_queue_task("task-sqlite-cli"))
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    inspect_rc = cli.main(
        [
            "queue",
            "inspect",
            "--backend",
            "sqlite",
            "--root",
            str(queue.root),
            "--bucket",
            "poison",
        ]
    )

    assert inspect_rc == 0
    inspect_payload = json.loads(capsys.readouterr().out)
    assert inspect_payload["summary"] == {
        "pending": 0,
        "inflight": 0,
        "done": 0,
        "failed": 0,
        "poison": 1,
        "listed_tasks": 1,
        "governance_problem_tasks": 1,
        "visible_requeue_history_tasks": 0,
        "recovery_audit_support": "result_only",
    }
    assert inspect_payload["tasks"][0]["task_id"] == "task-sqlite-cli"
    assert inspect_payload["tasks"][0]["record_path"] == (
        f"{queue.db_path.resolve()}#bucket=poison,task_id=task-sqlite-cli"
    )
    assert inspect_payload["tasks"][0]["governance_problem"] == "poison_attempts_exhausted"
    assert inspect_payload["tasks"][0]["recovery_audit_support"] == "result_only"
    assert inspect_payload["tasks"][0]["latest_recovery"] is None

    requeue_rc = cli.main(
        [
            "queue",
            "requeue",
            "--backend",
            "sqlite",
            "--root",
            str(queue.root),
            "--bucket",
            "poison",
            "--task-id",
            "task-sqlite-cli",
        ]
    )

    assert requeue_rc == 0
    requeue_payload = json.loads(capsys.readouterr().out)
    assert requeue_payload == {
        "action": "requeue",
        "support_status": "supported",
        "governance_result": "moved_to_pending",
        "redrive_support": "not_modeled",
        "audit_support": "result_only",
        "root": str(queue.root.resolve()),
        "task_id": "task-sqlite-cli",
        "kind": "it",
        "source_bucket": "poison",
        "target_bucket": "pending",
        "source_path": f"{queue.db_path.resolve()}#bucket=poison,task_id=task-sqlite-cli",
        "target_path": f"{queue.db_path.resolve()}#bucket=pending,task_id=task-sqlite-cli",
        "failure_count": 1,
        "orphan_count": 0,
        "recorded_at_utc": requeue_payload["recorded_at_utc"],
    }
