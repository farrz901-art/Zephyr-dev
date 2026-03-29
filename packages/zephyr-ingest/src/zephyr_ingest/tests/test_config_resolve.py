from __future__ import annotations

import json
from pathlib import Path

import pytest

from zephyr_ingest import cli


def _write(p: Path, text: str) -> None:
    p.write_text(text, encoding="utf-8")


def test_config_resolve_outputs_sources(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cfg = tmp_path / "cfg.toml"
    _write(
        cfg,
        """
[run]
backend = "local"

[destinations.webhook]
url = "http://example.test/hook"
timeout_s = 3.0
""".strip(),
    )

    rc = cli.main(["config", "resolve", "--config", str(cfg)])
    assert rc == 0

    out = capsys.readouterr().out
    obj = json.loads(out)
    assert "config_snapshot" in obj
    snap = obj["config_snapshot"]
    assert "sources" in snap
    assert isinstance(snap["sources"], dict)
    # at least one stable source key exists
    assert snap["sources"].get("destinations.webhook.url") in ("cli", "env", "file", "default")


def test_config_resolve_strict_fails_when_schema_version_missing(tmp_path: Path) -> None:
    cfg = tmp_path / "cfg.toml"
    _write(
        cfg,
        """
[run]
backend = "local"
""".strip(),
    )

    rc = cli.main(["config", "resolve", "--config", str(cfg), "--strict"])
    assert rc == 2


def test_config_resolve_fails_on_unsupported_schema_version(tmp_path: Path) -> None:
    cfg = tmp_path / "cfg.toml"
    _write(
        cfg,
        """
schema_version = 2

[run]
backend = "local"
""".strip(),
    )

    rc = cli.main(["config", "resolve", "--config", str(cfg)])
    assert rc == 2
