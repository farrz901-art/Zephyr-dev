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
        ]
    )
    assert rc == 0
    assert called["ok"] is True
