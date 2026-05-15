from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s8_ui_invoke_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s8_test" / case_name
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
    (base_root / ".github/workflows").mkdir(parents=True, exist_ok=True)
    (base_root / "runtime/public-core-bundle/manifest").mkdir(parents=True, exist_ok=True)
    (base_root / "ui/src/services").mkdir(parents=True, exist_ok=True)
    (base_root / "ui/src/contracts").mkdir(parents=True, exist_ok=True)
    (base_root / "src-tauri/src").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/UI_TAURI_INVOKE_INTEGRATION.md").write_text("invoke doc\n", encoding="utf-8")
    (base_root / ".github/workflows/boundary.yml").write_text(
        (
            "python scripts/check_ui_result_lifecycle.py --json\n"
            "python scripts/check_rust_bridge_cli_flow.py --json\n"
        ),
        encoding="utf-8",
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/ui_shell_check.json",
        {
            "summary": {
                "pass": True,
                "commercial_terms_blocked": 0,
                "network_calls_blocked": 0,
                "ui_does_not_call_python_directly": True,
                "ui_does_not_use_zephyr_dev_root": True,
            },
            "unsupported_hits": [],
        },
    )
    _write_json(
        base_root / ".tmp/tauri_command_bridge_check.json",
        {"summary": {"cargo_check_pass": True}},
    )
    _write_json(
        base_root / ".tmp/s8_tauri_bridge_cli_flow_check.json",
        {
            "summary": {
                "pass": True,
                "cargo_available": True,
                "cargo_check_pass": True,
                "cargo_run_local_text_pass": True,
                "run_result_exists": True,
                "marker_found": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
            }
        },
    )
    _write_json(
        base_root / ".tmp/ui_result_lifecycle_check.json",
        {
            "summary": {
                "pass": True,
                "real_run_result_consumed": True,
                "sample_success_consumed": True,
                "sample_error_consumed": True,
                "display_model_fields_covered": True,
            }
        },
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json",
        {"requires_network": False},
    )
    _write_json(
        base_root / ".tmp/s8_repo_status.json",
        {
            "previous_head_sha": handoff_tool.PREVIOUS_HEAD_SHA,
            "current_head_sha": "s8-current",
            "pushed": True,
        },
    )
    (base_root / "ui/src/services/baseBridgeClient.ts").write_text(
        (
            'export const TAURI_COMMANDS = { run_local_file: "run_local_file", '
            'run_local_text: "run_local_text", read_run_result: "read_run_result", '
            'open_output_folder_plan: "open_output_folder_plan", '
            'read_lineage_snapshot: "read_lineage_snapshot" };\n'
            "const payload = { input_path: 'a', output_dir: 'b', inline_text: 'c' };\n"
        ),
        encoding="utf-8",
    )
    (base_root / "ui/src/contracts/baseRunResult.ts").write_text(
        "export interface BaseRunResultV1 {}\n",
        encoding="utf-8",
    )
    (base_root / "src-tauri/src/commands.rs").write_text(
        (
            "pub fn run_local_file() {}\n"
            "pub fn run_local_text() {}\n"
            "pub fn read_run_result() {}\n"
            "pub fn open_output_folder_plan() {}\n"
            "pub fn read_lineage_snapshot() {}\n"
        ),
        encoding="utf-8",
    )
    (base_root / "src-tauri/src/main.rs").write_text(
        (
            "run-local-file\nrun-local-text\nread-run-result\n"
            "open-output-folder-plan\nread-lineage-snapshot\n"
        ),
        encoding="utf-8",
    )
    (base_root / "src-tauri/src/bridge.rs").write_text(
        "input_path output_dir inline_text runtime/public-core-bundle\n",
        encoding="utf-8",
    )


@pytest.mark.auth_contract
def test_s8_handoff_reports_alignment_and_lifecycle(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    invoke_alignment = _as_dict(report["invoke_alignment"])
    ui_result_lifecycle = _as_dict(report["ui_result_lifecycle"])
    assert summary["overall"] == "pass"
    assert invoke_alignment["ui_command_names_match_rust"] is True
    assert invoke_alignment["ui_payload_shapes_match_rust"] is True
    assert ui_result_lifecycle["real_run_result_consumed"] is True


@pytest.mark.auth_contract
def test_s8_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
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
