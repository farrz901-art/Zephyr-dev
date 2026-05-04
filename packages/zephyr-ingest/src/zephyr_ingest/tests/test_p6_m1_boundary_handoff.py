from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m1_boundary_handoff as handoff_tool


def _as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


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


@pytest.mark.auth_contract
def test_p6_m1_handoff_build_report_and_markdown(tmp_path: Path) -> None:
    repo_root = _repo_root()
    out_root = tmp_path / "handoff_build"
    report = handoff_tool.build_report(root=repo_root, out_root=out_root)
    summary = _as_dict(report["summary"])
    six_repo_boundary = _as_dict(report["six_repo_boundary"])
    export_boundary = _as_dict(report["export_boundary"])
    security_review = _as_dict(report["security_review"])
    assert summary["overall"] == "pass"
    assert summary["active_blockers"] == 0
    assert six_repo_boundary["repo_count"] == 6
    assert six_repo_boundary["web_core_is_only_commercial_authority"] is True
    assert export_boundary["actual_code_migration_performed"] is False
    assert security_review["actual_codeowners_modified"] is False
    markdown = handoff_tool.render_markdown(report)
    assert "# P6-M1 boundary handoff" in markdown
    assert "## What M1 completed" in markdown
    assert "## Next step" in markdown


@pytest.mark.auth_contract
def test_p6_m1_handoff_cli_outputs_and_check_artifacts(tmp_path: Path) -> None:
    repo_root = _repo_root()
    out_root = tmp_path / "handoff_cli"
    assert handoff_tool.main(["--root", str(repo_root), "--out-root", str(out_root), "--json"]) == 0
    assert (
        handoff_tool.main(["--root", str(repo_root), "--out-root", str(out_root), "--markdown"])
        == 0
    )
    assert (
        handoff_tool.main(
            ["--root", str(repo_root), "--out-root", str(out_root), "--check-artifacts"]
        )
        == 0
    )
    report = _as_dict(json.loads((out_root / "report.json").read_text(encoding="utf-8")))
    summary = _as_dict(report["summary"])
    assert summary["overall"] == "pass"
