from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_base_boundary_gate as boundary_tool
from tools import p6_m3_base_lineage_gate as lineage_tool
from tools import p6_m3_base_repo_scaffold_plan as scaffold_tool
from tools import p6_m3_s1_base_scaffold_handoff as handoff_tool


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


def _as_int(value: object) -> int:
    assert isinstance(value, int)
    return value


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _workspace_case_dir(case_name: str) -> Path:
    root = _repo_root() / ".tmp" / "p6_m3_s1_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.mark.auth_contract
def test_base_scaffold_manifest_and_bridge_contract_freeze_boundary() -> None:
    repo_root = _repo_root()
    scaffold_manifest = _as_dict(
        json.loads((repo_root / "docs/p6/base_repo_scaffold_manifest.json").read_text("utf-8"))
    )
    assert scaffold_manifest["actual_repo_created"] is False
    assert scaffold_manifest["actual_code_migration_performed"] is False
    assert scaffold_manifest["desktop_stack"] == "tauri_rust_shell_web_ui"
    assert scaffold_manifest["bridge_strategy"] == "json_artifact_or_process_bridge"
    assert scaffold_manifest["license_allowed"] is False
    assert scaffold_manifest["entitlement_allowed"] is False
    assert scaffold_manifest["private_core_allowed"] is False
    assert scaffold_manifest["requires_p45_substrate"] is False

    bridge_contract = _as_dict(
        json.loads((repo_root / "docs/p6/base_local_bridge_contract.json").read_text("utf-8"))
    )
    contracts = _as_dict(bridge_contract["contracts"])
    assert "base_run_request_v1" in contracts
    assert "base_run_result_v1" in contracts
    run_result = _as_dict(contracts["base_run_result_v1"])
    usage_fact = _as_dict(run_result["usage_fact"])
    assert usage_fact["billing_semantics"] is False


@pytest.mark.auth_contract
def test_base_scaffold_shadow_emit_and_lineage_hashes(tmp_path: Path) -> None:
    repo_root = _repo_root()
    scaffold_out = _workspace_case_dir(f"{tmp_path.name}_scaffold")
    report = scaffold_tool.build_report(
        root=repo_root,
        out_root=scaffold_out,
        emit_shadow_scaffold=True,
    )
    summary = _as_dict(report["summary"])
    shadow = _as_dict(report["shadow_scaffold"])
    assert summary["shadow_scaffold_emitted"] is True
    assert shadow["files_created"]
    readme = scaffold_out / "Zephyr-base-shadow/README.md"
    assert readme.exists()
    readme_text = readme.read_text(encoding="utf-8")
    assert "placeholder only" in readme_text
    assert "not production-ready yet" in readme_text

    lineage = lineage_tool.build_report(root=repo_root)
    lineage_data = _as_dict(lineage["lineage"])
    assert lineage_data["public_manifest_sha256"]
    assert lineage_data["base_local_bridge_contract_sha256"]
    assert lineage_data["combined_base_scaffold_lineage_sha256"]
    assert lineage_data["public_manifest_hash_recorded"] is True
    assert lineage_data["bridge_contract_hash_recorded"] is True


@pytest.mark.auth_contract
def test_base_boundary_gate_fails_on_commercial_blocker_fixture(tmp_path: Path) -> None:
    repo_root = _repo_root()
    case_root = _workspace_case_dir(f"{tmp_path.name}_boundary")
    commercial_scan_path = case_root / "commercial_scan.json"
    forbidden_scan_path = case_root / "forbidden_scan.json"
    security_scan_path = case_root / "security_scan.json"

    _write_json(
        commercial_scan_path,
        {
            "summary": {
                "blocker_count": 1,
                "blocked_hits": 1,
                "review_required_hits": 0,
                "overall": "fail",
            }
        },
    )
    _write_json(
        forbidden_scan_path,
        {"summary": {"blocker_count": 0, "blocked_hits": 0, "overall": "pass"}},
    )
    _write_json(
        security_scan_path,
        {"summary": {"blocked_runtime_paths": 0, "overall": "pass"}},
    )

    report = boundary_tool.build_report(
        root=repo_root,
        out_root=case_root,
        lineage_path=case_root / "missing_lineage.json",
        commercial_scan_path=commercial_scan_path,
        forbidden_import_scan_path=forbidden_scan_path,
        security_scan_path=security_scan_path,
    )
    summary = _as_dict(report["summary"])
    assert summary["overall"] == "fail"
    assert _as_int(summary["active_blockers"]) >= 1
    assert summary["commercial_contamination_blockers"] == 1


@pytest.mark.auth_contract
def test_base_handoff_requires_artifacts_and_is_secret_safe(tmp_path: Path) -> None:
    repo_root = _repo_root()
    scaffold_root = _workspace_case_dir(f"{tmp_path.name}_missing")
    _write_json(scaffold_root / "report.json", {"summary": {"active_blockers": 0}})
    _write_json(
        scaffold_root / "base_lineage_gate.json",
        {
            "lineage": {
                "source_sha_recorded": True,
                "public_manifest_hash_recorded": True,
                "bridge_contract_hash_recorded": True,
                "combined_base_scaffold_lineage_sha256": "abc",
            }
        },
    )
    with pytest.raises(FileNotFoundError):
        handoff_tool.build_report(root=repo_root, scaffold_root=scaffold_root)

    scaffold_root_ok = _workspace_case_dir(f"{tmp_path.name}_ok")
    scaffold_report = scaffold_tool.build_report(
        root=repo_root,
        out_root=scaffold_root_ok,
        emit_shadow_scaffold=True,
    )
    scaffold_tool.emit_outputs(report=scaffold_report, out_root=scaffold_root_ok, markdown=False)
    scaffold_tool.emit_outputs(report=scaffold_report, out_root=scaffold_root_ok, markdown=True)

    lineage_report = lineage_tool.build_report(root=repo_root)
    lineage_tool.emit_outputs(report=lineage_report, out_root=scaffold_root_ok, markdown=False)
    lineage_tool.emit_outputs(report=lineage_report, out_root=scaffold_root_ok, markdown=True)

    boundary_report = boundary_tool.build_report(
        root=repo_root,
        out_root=scaffold_root_ok,
        lineage_path=scaffold_root_ok / "base_lineage_gate.json",
        commercial_scan_path=repo_root / ".tmp/p6_m1_boundary/commercial_contamination_scan.json",
        forbidden_import_scan_path=repo_root / ".tmp/p6_m1_boundary/forbidden_import_scan.json",
        security_scan_path=repo_root / ".tmp/p6_m1_boundary/security_sensitive_path_scan.json",
    )
    boundary_tool.emit_outputs(report=boundary_report, out_root=scaffold_root_ok, markdown=False)
    boundary_tool.emit_outputs(report=boundary_report, out_root=scaffold_root_ok, markdown=True)

    handoff = handoff_tool.build_report(root=repo_root, scaffold_root=scaffold_root_ok)
    summary = _as_dict(handoff["summary"])
    scope = _as_dict(handoff["scope"])
    assert summary["overall"] == "pass"
    assert scope["actual_repo_created"] is False
    assert scope["actual_code_migration_performed"] is False
    json_text = json.dumps(handoff, ensure_ascii=False, indent=2)
    markdown_text = handoff_tool.render_markdown(handoff)
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text
