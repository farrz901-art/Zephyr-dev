from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from zephyr_ingest import cli
from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1


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
