from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_prep_handoff as handoff_tool


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
def test_m3_prep_handoff_build_report_freezes_base_route_decisions() -> None:
    repo_root = _repo_root()
    report = handoff_tool.build_report(root=repo_root)
    summary = _as_dict(report["summary"])
    decisions = _as_dict(report["decisions"])
    ui_design = _as_dict(report["ui_design"])
    acceptance = _as_dict(report["acceptance_gates"])
    assert summary["overall"] == "pass"
    assert decisions["desktop_stack"] == "tauri_rust_shell_web_ui"
    assert decisions["python_to_rust_full_rewrite_required"] is False
    assert decisions["requires_p45_substrate"] is False
    assert decisions["base_license_allowed"] is False
    assert decisions["base_entitlement_allowed"] is False
    assert decisions["base_private_core_allowed"] is False
    assert ui_design["design_window_input_required"] is True
    assert ui_design["design_tasks_count"] == 12
    assert acceptance["ci_only_sufficient"] is False
    assert acceptance["source_sha_manifest_hash_required"] is True


@pytest.mark.auth_contract
def test_m3_prep_handoff_cli_outputs_and_check_artifacts(tmp_path: Path) -> None:
    repo_root = _repo_root()
    out_root = tmp_path / "handoff"
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
    assert summary["m3_prep_status"] == "sealed"
