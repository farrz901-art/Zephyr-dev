from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s5r_bundle_surface_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s5r_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_fake_repo_root(repo_root: Path) -> None:
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / ".tmp/p6_m3_public_core_bundle").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _write_json(
        repo_root / "docs/p6/p6_m3_s5_public_core_bundle_manifest.json",
        {"schema_version": 1},
    )
    (repo_root / "tools").mkdir(parents=True, exist_ok=True)
    (repo_root / "tools/p6_m3_public_core_bundle_export.py").write_text(
        "# tool\n",
        encoding="utf-8",
    )
    _write_json(
        repo_root / ".tmp/p6_m3_public_core_bundle/report.json",
        {
            "summary": {
                "bundle_exported": True,
                "bundle_execution_requires_zephyr_dev_working_tree": False,
                "bundle_generation_requires_zephyr_dev_working_tree": True,
            }
        },
    )


def _emit_fake_base_root(base_root: Path) -> None:
    (base_root / ".github/workflows").mkdir(parents=True, exist_ok=True)
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / "public-core-bridge").mkdir(parents=True, exist_ok=True)
    (base_root / "runtime/public-core-bundle/manifest").mkdir(parents=True, exist_ok=True)
    (base_root / "scripts").mkdir(parents=True, exist_ok=True)
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/BRIDGE_RUNTIME_MODES.md").write_text("modes\n", encoding="utf-8")
    (base_root / "public-core-bridge/run_public_core_adapter.py").write_text(
        "# adapter\n",
        encoding="utf-8",
    )
    (base_root / "runtime/public-core-bundle/README.md").write_text("bundle\n", encoding="utf-8")
    (base_root / "scripts/audit_public_core_bundle_surface.py").write_text(
        "# audit\n",
        encoding="utf-8",
    )
    (base_root / "scripts/check_bundle_unsupported_surface.py").write_text(
        "# negative\n",
        encoding="utf-8",
    )
    (base_root / ".github/workflows/boundary.yml").write_text(
        "python scripts/audit_public_core_bundle_surface.py --json\n"
        "python scripts/check_bundle_unsupported_surface.py --json\n",
        encoding="utf-8",
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json",
        {
            "supported_input_extensions": [".txt", ".md"],
            "allowed_partition_kinds": ["text", "md"],
            "allowed_sources": ["local_file"],
            "allowed_destinations": ["filesystem"],
            "removed_surface": {
                "non_txt_md_partition_modules_removed": True,
                "remote_sources_removed": True,
                "remote_destinations_removed": True,
                "testing_helpers_removed": True,
                "queue_worker_observability_governance_removed": True,
            },
        },
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/bundle_file_hashes.json",
        {"recorded_files_excluding_self": []},
    )
    _write_json(
        base_root / ".tmp/public_core_bundle_surface_audit.json",
        {
            "summary": {
                "bundle_surface_matches_manifest": True,
                "blocked_count": 0,
                "review_required_count": 0,
            }
        },
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True, "blocked_count": 0}},
    )
    _write_json(base_root / ".tmp/local_fixture_flow_check.json", {"summary": {"pass": True}})
    _write_json(base_root / ".tmp/local_adapter_flow_check.json", {"summary": {"pass": True}})
    _write_json(
        base_root / ".tmp/bundled_adapter_flow_check.json",
        {
            "summary": {
                "pass": True,
                "fixture_runner_used_for_s5_pass": False,
                "contains_dev_root": False,
                "zephyr_dev_working_tree_required": False,
            }
        },
    )
    _write_json(
        base_root / ".tmp/bundle_unsupported_surface_check.json",
        {
            "summary": {
                "pass": True,
                "unsupported_pdf_rejected": True,
                "hidden_pdf_route_available": False,
                "secret_safe_error": True,
            }
        },
    )


@pytest.mark.auth_contract
def test_s5r_handoff_reports_trimmed_surface(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    _emit_fake_repo_root(repo_root)
    _emit_fake_base_root(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    bundle_surface = _as_dict(report["bundle_surface"])
    runtime = _as_dict(report["runtime"])
    negative = _as_dict(report["negative_smoke"])
    assert summary["overall"] == "conditional"
    assert bundle_surface["bundle_surface_matches_manifest"] is True
    assert bundle_surface["blocked_count"] == 0
    assert runtime["zephyr_dev_root_used_for_s5r_pass"] is False
    assert negative["unsupported_pdf_rejected"] is True


@pytest.mark.auth_contract
def test_s5r_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    out_root = _workspace_case_dir(f"{tmp_path.name}_out")
    _emit_fake_repo_root(repo_root)
    _emit_fake_base_root(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=False)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=True)
    json_text = (out_root / "report.json").read_text(encoding="utf-8")
    markdown_text = (out_root / "report.md").read_text(encoding="utf-8")
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text
