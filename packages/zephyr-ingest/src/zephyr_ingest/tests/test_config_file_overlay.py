from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from zephyr_ingest import cli
from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1


def _write_config(path: Path, toml_text: str) -> None:
    path.write_text(toml_text, encoding="utf-8")


def test_config_file_enables_webhook_and_uns_api_env_overrides_file_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[run]
backend = "uns-api"
uns_api_url = "https://api.test.com/v0"
uns_api_timeout_s = 12.5
uns_api_key = "file-key"

[retry]
enabled = false
max_attempts = 1
base_backoff_ms = 0
max_backoff_ms = 0

[destinations.webhook]
url = "http://test.com/hook"
timeout_s = 7.0
""".strip(),
    )

    # ENV should override FILE secret when CLI did not explicitly pass --uns-api-key
    monkeypatch.setenv("ZEPHYR_UNS_API_KEY", "env-key")

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True

        # Backend is constructed as HttpUnsApiBackend with api_key applied
        assert cfg.backend is not None
        assert getattr(cfg.backend, "backend") == "uns_api"
        assert getattr(cfg.backend, "url") == "https://api.test.com/v0"
        assert getattr(cfg.backend, "timeout_s") == 12.5
        assert getattr(cfg.backend, "api_key") == "env-key"

        # Destination should be fanout (filesystem + webhook)
        dest = kwargs.get("destination")
        assert dest is not None
        assert getattr(dest, "name") == "fanout"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert snap["schema_version"] == 1
        assert snap["backend"]["kind"] == "uns-api"
        # redacted
        assert snap["backend"]["api_key"] == "***"
        assert "webhook" in snap["destinations"]
        assert snap["destinations"]["webhook"]["url"] == "http://test.com/hook"
        assert snap["destinations"]["webhook"]["timeout_s"] == 7.0

        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.kind") == "file"
        assert sources.get("backend.api_key") == "env"
        assert sources.get("destinations.webhook.url") == "file"
        assert sources.get("destinations.webhook.timeout_s") == "file"

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
            "--config",
            str(cfg_path),
        ]
    )
    assert rc == 0
    assert called["ok"] is True


def test_cli_flag_overrides_file_field_without_requiring_cli_enable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[destinations.webhook]
url = "http://test.com/hook"
timeout_s = 7.0
""".strip(),
    )

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert snap["schema_version"] == 1
        assert "webhook" in snap["destinations"]
        # CLI explicitly overrides timeout_s, while url comes from file
        assert snap["destinations"]["webhook"]["timeout_s"] == 9.0

        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.kind") == "default"
        assert sources.get("destinations.webhook.url") == "file"
        assert sources.get("destinations.webhook.timeout_s") == "cli"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
            "--webhook-timeout-s",
            "9.0",
        ]
    )
    assert rc == 0
    assert called["ok"] is True


def test_cli_secret_overrides_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[run]
backend = "uns-api"
uns_api_url = "https://api.test.com/v0"
uns_api_timeout_s = 12.5
uns_api_key = "file-key"
""".strip(),
    )

    monkeypatch.setenv("ZEPHYR_UNS_API_KEY", "env-key")

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        assert cfg.backend is not None
        # CLI explicit --uns-api-key must win over ENV and FILE
        assert getattr(cfg.backend, "api_key") == "cli-key"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.api_key") == "cli"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
            "--backend",
            "uns-api",
            "--uns-api-url",
            "https://api.test.com/v0",
            "--uns-api-timeout-s",
            "12.5",
            "--uns-api-key",
            "cli-key",
        ]
    )
    assert rc == 0
    assert called["ok"] is True
