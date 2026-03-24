from __future__ import annotations

from pathlib import Path
from typing import Any  # ← 必须导入

import pytest  # ← 必须导入

from zephyr_ingest import cli


def test_cli_run_invokes_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        # Just verify we got docs iterable and cfg/out_root
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
        # 使用 getattr 规避 strict 模式下对 Mock 对象的属性访问限制
        assert getattr(dest, "name").startswith("filesystem")

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
            "--destination",
            "filesystem",
        ]
    )

    assert rc == 0
    assert called["ok"] is True


def test_cli_run_webhook_fanout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """测试 Webhook 触发的 Fanout 逻辑"""
    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        dest = kwargs.get("destination")
        assert dest is not None
        # 验证是否成功启用了分叉目的地
        assert getattr(dest, "name") == "fanout"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(tmp_path),
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


def test_cli_run_backend_uns_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """测试 --backend uns-api 是否正确构造了后端实例并注入 Config"""
    called = {"ok": False}
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "dummy.txt").touch()

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        # 核心断言：验证后端实例是否正确生成
        assert cfg.backend is not None
        # 验证 backend 属性 (HttpUnsApiBackend 类中定义的固定属性)
        assert getattr(cfg.backend, "backend") == "uns_api"
        assert getattr(cfg.backend, "url") == "https://api.test.com/v0"
        assert getattr(cfg.backend, "timeout_s") == 12.5

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
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

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        # 核心断言：默认或显式指定 local 时，backend 应该为 None
        assert cfg.backend is None

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        ["run", "--path", str(tmp_path), "--out", str(tmp_path / "out"), "--backend", "local"]
    )

    assert rc == 0
    assert called["ok"] is True
