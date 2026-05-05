from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s2_base_repo_init_handoff as handoff_tool


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


def _workspace_case_dir(case_name: str) -> Path:
    root = _repo_root() / ".tmp" / "p6_m3_s2_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_base_fixture(base_root: Path) -> None:
    (base_root / ".git").mkdir(parents=True, exist_ok=True)
    (base_root / ".github/workflows").mkdir(parents=True, exist_ok=True)
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / "manifests").mkdir(parents=True, exist_ok=True)
    (base_root / "scripts").mkdir(parents=True, exist_ok=True)
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / "README.md").write_text(
        "Boundary-negative mention: no entitlement, no payment.\n",
        encoding="utf-8",
    )
    (base_root / "PRODUCT_BOUNDARY.md").write_text(
        "No private_core_export. No web_core.internal.\n",
        encoding="utf-8",
    )
    (base_root / "docs/SOURCE_LINEAGE.md").write_text(
        "Initialized from Zephyr-dev scaffold only.\n",
        encoding="utf-8",
    )
    _write_json(
        base_root / "manifests/public_export_lineage.json",
        {
            "zephyr_dev_source_sha": handoff_tool.EXPECTED_SOURCE_SHA,
            "p5_1_final_sha": handoff_tool.P5_FINAL_SHA,
            "public_manifest_sha256": "abc",
            "base_repo_scaffold_manifest_sha256": "def",
            "base_local_bridge_contract_sha256": "ghi",
            "combined_base_scaffold_lineage_sha256": "xyz",
            "actual_runtime_source_copied": False,
            "actual_product_runtime_implemented": False,
            "created_from_shadow_scaffold": True,
        },
    )
    (base_root / "scripts/check_boundary.py").write_text(
        "print('placeholder boundary check')\n",
        encoding="utf-8",
    )
    (base_root / ".github/workflows/boundary.yml").write_text(
        "name: boundary\n",
        encoding="utf-8",
    )
    _write_json(base_root / ".tmp/base_boundary_check.json", {"summary": {"pass": True}})


@pytest.mark.auth_contract
def test_s2_handoff_reads_base_workspace_and_records_source_sha(tmp_path: Path) -> None:
    repo_root = _repo_root()
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    _emit_base_fixture(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    scope = _as_dict(report["scope"])
    lineage = _as_dict(report["lineage"])
    boundary = _as_dict(report["boundary"])
    assert summary["overall"] == "conditional"
    assert lineage["zephyr_dev_source_sha"] == handoff_tool.EXPECTED_SOURCE_SHA
    assert scope["actual_runtime_source_copied"] is False
    assert scope["production_tauri_app_implemented"] is False
    assert boundary["base_boundary_check_pass"] is True


@pytest.mark.auth_contract
def test_s2_handoff_manual_commands_and_secret_safe_outputs(tmp_path: Path) -> None:
    repo_root = _repo_root()
    base_root = _workspace_case_dir(f"{tmp_path.name}_manual")
    _emit_base_fixture(base_root)
    out_root = _workspace_case_dir(f"{tmp_path.name}_handoff")
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    manual_commands = cast(list[str], report["manual_commands_if_needed"])
    assert manual_commands
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=False)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=True)
    json_text = (out_root / "report.json").read_text(encoding="utf-8")
    markdown_text = (out_root / "report.md").read_text(encoding="utf-8")
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text
