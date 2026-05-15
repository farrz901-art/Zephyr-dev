from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s11_runtime_packaging_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s11_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_fake_base_root(base_root: Path, *, managed_flow_pass: bool) -> None:
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / "runtime/python-runtime").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/PACKAGED_RUNTIME_BASELINE.md").write_text(
        "runtime packaging baseline\n",
        encoding="utf-8",
    )
    (base_root / "runtime/python-runtime/README.md").write_text(
        "runtime readme\n",
        encoding="utf-8",
    )
    (base_root / "runtime/python-runtime/base-runtime-requirements.in").write_text(
        "pydantic\n",
        encoding="utf-8",
    )
    (base_root / "runtime/python-runtime/base-runtime-requirements.txt").write_text(
        "pydantic==2.12.5\n",
        encoding="utf-8",
    )
    _write_json(base_root / ".tmp/no_bom_check.json", {"summary": {"pass": True}})
    _write_json(base_root / ".tmp/base_boundary_check.json", {"summary": {"pass": True}})
    _write_json(
        base_root / ".tmp/runtime_packaging_baseline_check.json",
        {
            "summary": {
                "pass": True,
                "runtime_manifest_exists": True,
                "requirements_exist": True,
                "bootstrap_script_exists": True,
                "managed_runtime_checker_exists": True,
                "managed_venv_supported": True,
                "installer_runtime_complete": False,
                "embedded_python_runtime": False,
                "venv_committed": False,
                "wheelhouse_committed": False,
            }
        },
    )
    _write_json(
        base_root / "runtime/python-runtime/runtime_manifest.json",
        {
            "schema_version": 1,
            "report_id": "zephyr.base.s11.runtime_manifest.v1",
            "supported_formats": [".txt", ".text", ".log", ".md", ".markdown"],
            "requires_network_at_runtime": False,
            "requires_p45_substrate": False,
            "uses_zephyr_dev_working_tree": False,
            "commercial_logic_allowed": False,
            "installer_runtime_complete": False,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "managed_venv_supported": True,
        },
    )
    _write_json(
        base_root / ".tmp/python_runtime_dependencies_check.json",
        {
            "summary": {
                "pass": True,
                "selected_python": (
                    "E:/Github_Projects/Zephyr-base/.tmp/base_runtime_venv/Scripts/python.exe"
                ),
                "selected_python_is_managed_runtime": True,
                "selected_python_differs_from_current_shell_python": True,
                "pydantic_importable": True,
                "unstructured_importable": True,
            },
            "selected_python": (
                "E:/Github_Projects/Zephyr-base/.tmp/base_runtime_venv/Scripts/python.exe"
            ),
            "selected_python_is_managed_runtime": True,
            "selected_python_differs_from_current_shell_python": True,
        },
    )
    _write_json(
        base_root / ".tmp/base_runtime_bootstrap.json",
        {
            "summary": {
                "pass": True,
                "managed_venv_created": True,
                "selected_python": "E:/Github_Projects/Zephyr/.venv/Scripts/python.exe",
                "requirements_installed": True,
            },
            "runtime": {
                "managed_python": (
                    "E:/Github_Projects/Zephyr-base/.tmp/base_runtime_venv/Scripts/python.exe"
                ),
                "managed_runtime_root": "E:/Github_Projects/Zephyr-base/.tmp/base_runtime_venv",
            },
        },
    )
    _write_json(
        base_root / ".tmp/managed_runtime_flow_check.json",
        {
            "summary": {
                "pass": managed_flow_pass,
                "managed_runtime_available": True,
                "managed_runtime_flow_pass": managed_flow_pass,
                "selected_python_is_managed_runtime": managed_flow_pass,
                "uses_current_shell_python_for_verified_flow": False,
            },
            "runtime": {
                "managed_python": (
                    "E:/Github_Projects/Zephyr-base/.tmp/base_runtime_venv/Scripts/python.exe"
                ),
                "selected_python_path": (
                    "E:/Github_Projects/Zephyr-base/.tmp/base_runtime_venv/Scripts/python.exe"
                    if managed_flow_pass
                    else None
                ),
            },
            "evidence": {
                "run_result_exists": managed_flow_pass,
                "marker_found": managed_flow_pass,
                "billing_semantics": False if managed_flow_pass else None,
                "bundled_runtime_used": True if managed_flow_pass else None,
                "fixture_runner_used": False if managed_flow_pass else None,
                "zephyr_dev_working_tree_required": False if managed_flow_pass else None,
                "requires_network": False if managed_flow_pass else None,
                "requires_p45_substrate": False if managed_flow_pass else None,
            },
        },
    )
    _write_json(
        base_root / ".tmp/tauri_invoke_payload_shape_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/ui_build_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/tauri_app_path_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/s11_repo_status.json",
        {
            "previous_head_sha": handoff_tool.PREVIOUS_HEAD_SHA,
            "current_head_sha": "s11-current",
            "pushed": True,
        },
    )


@pytest.mark.auth_contract
def test_s11_handoff_is_sealed_when_managed_runtime_flow_passes(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    out_root = _workspace_case_dir(f"{tmp_path.name}_out")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root, managed_flow_pass=True)

    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=False)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=True)

    summary = _as_dict(report["summary"])
    runtime_packaging = _as_dict(report["runtime_packaging"])
    runtime_evidence = _as_dict(report["runtime_evidence"])
    json_text = (out_root / "report.json").read_text(encoding="utf-8")
    markdown_text = (out_root / "report.md").read_text(encoding="utf-8")

    assert summary["overall"] == "pass"
    assert summary["m3_s11_status"] == "sealed"
    assert runtime_packaging["managed_runtime_flow_pass"] is True
    assert runtime_packaging["selected_python_is_managed_runtime"] is True
    assert runtime_evidence["marker_found"] is True
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text


@pytest.mark.auth_contract
def test_s11_handoff_fails_when_managed_runtime_flow_is_missing(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _emit_fake_base_root(base_root, managed_flow_pass=False)

    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    issues = cast(list[object], report["issues"])

    assert summary["overall"] == "fail"
    assert summary["m3_s11_status"] == "open"
    assert any(item == "Managed runtime flow did not pass." for item in issues)
