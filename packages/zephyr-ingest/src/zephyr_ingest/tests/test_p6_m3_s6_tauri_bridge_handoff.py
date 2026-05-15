from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s6_tauri_bridge_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s6_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_fake_base_root(base_root: Path) -> None:
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / ".github/workflows").mkdir(parents=True, exist_ok=True)
    (base_root / "src-tauri/src").mkdir(parents=True, exist_ok=True)
    (base_root / "runtime/public-core-bundle/manifest").mkdir(parents=True, exist_ok=True)
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/TAURI_COMMAND_BRIDGE.md").write_text("doc\n", encoding="utf-8")
    (base_root / ".github/workflows/boundary.yml").write_text(
        "python scripts/check_tauri_command_bridge.py --json\n",
        encoding="utf-8",
    )
    (base_root / "src-tauri/Cargo.toml").write_text("[package]\nname='bridge'\n", encoding="utf-8")
    (base_root / "src-tauri/tauri.conf.json").write_text("{}\n", encoding="utf-8")
    (base_root / "src-tauri/src/main.rs").write_text("// main\n", encoding="utf-8")
    (base_root / "src-tauri/src/commands.rs").write_text("// commands\n", encoding="utf-8")
    (base_root / "src-tauri/src/bridge.rs").write_text("// bridge\n", encoding="utf-8")
    (base_root / "src-tauri/src/errors.rs").write_text("// errors\n", encoding="utf-8")
    (base_root / "src-tauri/src/lineage.rs").write_text("// lineage\n", encoding="utf-8")
    _write_json(
        base_root / ".tmp/tauri_command_bridge_check.json",
        {
            "summary": {
                "pass": True,
                "cargo_available": False,
                "cargo_check_pass": False,
                "static_bridge_check_pass": True,
                "commercial_terms_blocked": 0,
            },
            "bridge": {
                "commands_exist": True,
                "bundled_adapter_invocation_detected": True,
                "uses_zephyr_dev_root": False,
                "fixture_fallback_used": False,
                "run_local_file_command": True,
                "run_local_text_command": True,
                "read_run_result_command": True,
            },
        },
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True, "blocked_count": 0}},
    )
    _write_json(
        base_root / ".tmp/public_core_bundle_surface_audit.json",
        {"summary": {"overall": "pass"}},
    )
    _write_json(
        base_root / ".tmp/bundle_unsupported_surface_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/bundled_adapter_flow_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json",
        {"requires_p45_substrate": False, "requires_network": False},
    )


@pytest.mark.auth_contract
def test_s6_handoff_is_conditional_without_cargo(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    tauri_bridge = _as_dict(report["tauri_bridge"])
    assert summary["overall"] == "conditional"
    assert tauri_bridge["bundled_adapter_invocation"] is True
    assert tauri_bridge["run_local_file_command"] is True


@pytest.mark.auth_contract
def test_s6_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    out_root = _workspace_case_dir(f"{tmp_path.name}_out")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=False)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=True)
    json_text = (out_root / "report.json").read_text(encoding="utf-8")
    markdown_text = (out_root / "report.md").read_text(encoding="utf-8")
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text
