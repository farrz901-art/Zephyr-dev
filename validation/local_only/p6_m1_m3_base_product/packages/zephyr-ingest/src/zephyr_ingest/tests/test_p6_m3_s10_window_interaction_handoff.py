from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s10_window_interaction_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s10_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_fake_base_root(base_root: Path, *, proof_pass: bool) -> None:
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/TAURI_WINDOW_INTERACTION_PROOF.md").write_text(
        "proof policy\n",
        encoding="utf-8",
    )
    (base_root / "docs/MANUAL_TAURI_WINDOW_PROOF.md").write_text(
        "manual proof\n",
        encoding="utf-8",
    )
    _write_json(base_root / ".tmp/no_bom_check.json", {"summary": {"pass": True}})
    _write_json(base_root / ".tmp/base_boundary_check.json", {"summary": {"pass": True}})
    _write_json(
        base_root / ".tmp/tauri_invoke_payload_shape_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/python_runtime_dependencies_check.json",
        {
            "summary": {"pass": True},
            "selected_python": "E:/Github_Projects/Zephyr/.venv/Scripts/python.EXE",
            "pydantic_importable": True,
            "unstructured_importable": True,
        },
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
            }
        },
    )
    _write_json(
        base_root / ".tmp/ui_shell_check.json",
        {
            "summary": {
                "pass": True,
                "commercial_terms_blocked": 0,
                "network_calls_blocked": 0,
            }
        },
    )
    _write_json(
        base_root / ".tmp/ui_result_lifecycle_check.json",
        {"summary": {"pass": True}},
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
                "tauri_window_launch_attempted": True,
                "tauri_window_click_e2e_verified": False,
            },
            "tauri_app_path": {
                "cargo_available": True,
                "cargo_check_pass": True,
                "tauri_command_registration_pass": True,
                "rust_cli_lifecycle_pass": True,
                "tauri_window_launch_attempted": True,
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
        },
    )
    _write_json(
        base_root / ".tmp/local_result_lifecycle_ux_check.json",
        {
            "summary": {
                "pass": True,
                "real_run_mode_exists": True,
                "sample_mode_retained": True,
                "run_status_timeline_exists": True,
                "runtime_preflight_card_exists": True,
                "supported_formats_notice_exists": True,
                "interaction_proof_panel_exists": True,
            }
        },
    )
    _write_json(
        base_root / ".tmp/tauri_window_launch_attempt.json",
        {
            "summary": {
                "pass": True,
                "ui_build_pass": True,
                "cargo_check_pass": True,
                "tauri_window_launch_attempted": True,
                "tauri_window_launch_process_started": True,
                "tauri_window_click_e2e_verified": False,
            },
            "launch": {
                "reason": "visible process started",
            },
        },
    )
    _write_json(
        base_root / ".tmp/tauri_window_interaction_proof_check.json",
        {
            "summary": {
                "pass": proof_pass,
                "proof_exists": proof_pass,
                "run_result_exists": proof_pass,
                "marker_found": proof_pass,
            },
            "proof": (
                {
                    "proof_kind": "manual_window_click",
                    "ui_mode": "real_tauri_local_text",
                    "run_result_path": ".tmp/s10_tauri_window_interaction/run_result.json",
                }
                if proof_pass
                else {}
            ),
        },
    )
    _write_json(
        base_root / ".tmp/s10_repo_status.json",
        {
            "previous_head_sha": handoff_tool.PREVIOUS_HEAD_SHA,
            "current_head_sha": "s10-current",
            "pushed": True,
        },
    )


@pytest.mark.auth_contract
def test_s10_handoff_is_conditional_without_click_proof(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root, proof_pass=False)

    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    fix_summary = _as_dict(report["s10_p_fix"])
    window_interaction = _as_dict(report["window_interaction"])

    assert summary["overall"] == "conditional"
    assert summary["m3_s10_status"] == "conditional"
    assert summary["major_gaps"] == 1
    assert fix_summary["invoke_payload_casing_fixed"] is True
    assert fix_summary["python_runtime_preflight_added"] is True
    assert window_interaction["tauri_window_launch_attempted"] is True
    assert window_interaction["manual_or_automated_proof_check_pass"] is False


@pytest.mark.auth_contract
def test_s10_handoff_is_sealed_when_click_proof_exists(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    out_root = _workspace_case_dir(f"{tmp_path.name}_out")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root, proof_pass=True)

    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=False)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=True)

    summary = _as_dict(report["summary"])
    fix_summary = _as_dict(report["s10_p_fix"])
    window_interaction = _as_dict(report["window_interaction"])
    json_text = (out_root / "report.json").read_text(encoding="utf-8")
    markdown_text = (out_root / "report.md").read_text(encoding="utf-8")

    assert summary["overall"] == "pass"
    assert summary["m3_s10_status"] == "sealed"
    assert summary["major_gaps"] == 0
    assert fix_summary["manual_proof_pack_exists"] is True
    assert window_interaction["tauri_window_click_e2e_verified"] is True
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text
