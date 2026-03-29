from __future__ import annotations

from pathlib import Path

import pytest

from zephyr_ingest import cli


def test_config_init_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["config", "init"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "schema_version = 1" in out
    assert "[run]" in out
    assert 'backend = "local"' in out


def test_config_init_writes_file(tmp_path: Path) -> None:
    out_path = tmp_path / "cfg.toml"
    rc = cli.main(["config", "init", "--out", str(out_path)])
    assert rc == 0
    text = out_path.read_text(encoding="utf-8")
    assert "schema_version = 1" in text
    assert "[retry]" in text


def test_config_init_refuses_overwrite(tmp_path: Path) -> None:
    out_path = tmp_path / "cfg.toml"
    out_path.write_text("x", encoding="utf-8")
    rc = cli.main(["config", "init", "--out", str(out_path)])
    assert rc == 2
