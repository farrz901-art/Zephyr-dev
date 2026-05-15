from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s9_tauri_app_flow_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s9_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_fake_base_root(base_root: Path) -> None:
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / "ui").mkdir(parents=True, exist_ok=True)
    (base_root / "src-tauri/src").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/BASE_LOCAL_APP_FLOW.md").write_text("local app flow\n", encoding="utf-8")
    (base_root / "docs/UI_TAURI_INVOKE_INTEGRATION.md").write_text(
        "invoke integration\n",
        encoding="utf-8",
    )
    (base_root / "ui/package-lock.json").write_text("{}\n", encoding="utf-8")
    (base_root / "src-tauri/src/main.rs").write_text(
        "tauri::Builder::default()\n",
        encoding="utf-8",
    )
    (base_root / "src-tauri/src/commands.rs").write_text("#[tauri::command]\n", encoding="utf-8")
    _write_json(
        base_root / ".tmp/no_bom_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True}},
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
        base_root / ".tmp/tauri_command_bridge_check.json",
        {
            "summary": {
                "pass": True,
                "cargo_available": True,
                "cargo_check_pass": True,
                "static_bridge_check_pass": True,
                "commercial_terms_blocked": 0,
            },
            "bridge": {
                "commands_exist": True,
                "bundled_adapter_invocation_detected": True,
                "uses_zephyr_dev_root": False,
                "fixture_fallback_used": False,
            },
        },
    )
    _write_json(
        base_root / ".tmp/ui_shell_check.json",
        {
            "summary": {
                "pass": True,
                "commercial_terms_blocked": 0,
                "network_calls_blocked": 0,
            },
            "unsupported_hits": [],
        },
    )
    _write_json(
        base_root / ".tmp/ui_result_lifecycle_check.json",
        {
            "summary": {
                "pass": True,
            }
        },
    )
    _write_json(
        base_root / ".tmp/ui_build_check.json",
        {
            "summary": {
                "pass": True,
                "node_available": True,
                "npm_available": True,
                "lock_file_exists": True,
                "ui_build_pass": True,
                "ui_dist_exists": True,
            }
        },
    )
    _write_json(
        base_root / ".tmp/tauri_app_path_check.json",
        {
            "summary": {
                "pass": True,
                "ui_build_pass": True,
                "cargo_check_pass": True,
                "tauri_command_registration_pass": True,
                "rust_cli_lifecycle_pass": True,
                "tauri_window_launch_attempted": False,
                "tauri_window_click_e2e_verified": False,
            },
            "tauri_app_path": {
                "cargo_available": True,
                "cargo_check_pass": True,
                "tauri_command_registration_pass": True,
                "rust_cli_lifecycle_pass": True,
                "tauri_window_launch_attempted": False,
                "tauri_window_click_e2e_verified": False,
            },
            "local_run_lifecycle": {
                "marker_found": True,
                "run_result_exists": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
            },
            "cargo": {
                "check_detail": (
                    "BridgeErrorKind::Input currently unused\n"
                    "associated function `input` is never used"
                ),
            },
        },
    )
    _write_json(
        base_root / ".tmp/s9_repo_status.json",
        {
            "previous_head_sha": handoff_tool.PREVIOUS_HEAD_SHA,
            "current_head_sha": "s9-current",
            "pushed": True,
        },
    )


@pytest.mark.auth_contract
def test_s9_handoff_reports_tauri_app_path_success(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root)

    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    tauri_app_path = _as_dict(report["tauri_app_path"])
    local_run_lifecycle = _as_dict(report["local_run_lifecycle"])

    assert summary["overall"] == "pass"
    assert summary["m3_s9_status"] == "sealed"
    assert tauri_app_path["cargo_check_pass"] is True
    assert local_run_lifecycle["marker_found"] is True


@pytest.mark.auth_contract
def test_s9_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
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
