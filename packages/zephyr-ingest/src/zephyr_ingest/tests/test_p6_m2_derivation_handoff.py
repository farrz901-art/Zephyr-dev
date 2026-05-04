from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m2_derivation_handoff as handoff_tool


def _repo_root() -> Path:
    current = Path(__file__).resolve()
    for candidate in current.parents:
        if (
            ((candidate / "pyproject.toml").exists() or (candidate / ".git").exists())
            and (candidate / "docs/p6").exists()
            and (candidate / "packages/zephyr-ingest").exists()
        ):
            return candidate
    raise RuntimeError("Could not locate repository root from test file path")


def _as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


@pytest.mark.auth_contract
def test_p6_m2_handoff_build_report_and_markdown(tmp_path: Path) -> None:
    repo_root = _repo_root()
    derivation_root = tmp_path / "derivation"
    out_root = tmp_path / "handoff"
    report = handoff_tool.build_report(
        root=repo_root,
        out_root=out_root,
        derivation_root=derivation_root,
    )
    summary = _as_dict(report["summary"])
    export_mechanics = _as_dict(report["export_mechanics"])
    assert summary["overall"] == "pass"
    assert export_mechanics["actual_export_performed"] is False
    assert export_mechanics["code_migration_performed"] is False
    markdown = handoff_tool.render_markdown(report)
    assert "## What M2 completed" in markdown
    assert "## What M2 did not do" in markdown
    assert "## Next step" in markdown


@pytest.mark.auth_contract
def test_p6_m2_handoff_cli_outputs_and_check_artifacts(tmp_path: Path) -> None:
    repo_root = _repo_root()
    handoff_out = tmp_path / "handoff_cli"
    assert (
        handoff_tool.main(["--root", str(repo_root), "--out-root", str(handoff_out), "--json"]) == 0
    )
    assert (
        handoff_tool.main(["--root", str(repo_root), "--out-root", str(handoff_out), "--markdown"])
        == 0
    )
    assert (
        handoff_tool.main(
            ["--root", str(repo_root), "--out-root", str(handoff_out), "--check-artifacts"]
        )
        == 0
    )
    report = _as_dict(json.loads((handoff_out / "report.json").read_text(encoding="utf-8")))
    summary = _as_dict(report["summary"])
    assert summary["overall"] == "pass"
