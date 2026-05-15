from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_s12_install_layout_handoff.py"
    spec = importlib.util.spec_from_file_location("p6_m3_s12_install_layout_handoff", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_render_markdown_includes_required_sections() -> None:
    module = _load_module()
    report = {
        "summary": {
            "overall": "pass",
            "m3_s12_status": "sealed",
            "active_blockers": 0,
            "major_gaps": 0,
        },
        "install_layout": {
            "layout_built": True,
            "layout_audit_pass": True,
            "layout_runtime_smoke_pass": True,
            "ui_dist_present": True,
            "public_core_bundle_present": True,
            "runtime_manifest_present": True,
            "bootstrap_script_present": True,
            "forbidden_dirs_present": False,
        },
        "generated_artifacts": {
            "src_tauri_gen_classified_generated": True,
            "src_tauri_gen_committed": False,
            "generated_artifact_hygiene_pass": True,
        },
        "runtime_evidence": {
            "run_result_exists": True,
            "marker_found": True,
            "billing_semantics": False,
            "bundled_runtime_used": True,
            "fixture_runner_used": False,
            "zephyr_dev_working_tree_required": False,
            "requires_network": False,
            "requires_p45_substrate": False,
        },
        "boundary": {
            "boundary_check_pass": True,
            "install_layout_baseline_check_pass": True,
            "commercial_terms_blocked": 0,
        },
        "scope": {
            "installer_built": False,
            "release_created": False,
            "signed_installer": False,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "clean_machine_runtime_proven": False,
        },
        "issues": [],
        "non_blocking_notes": [],
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3-S12 Install layout first slice handoff" in rendered
    assert "## Generated artifact hygiene" in rendered
    assert "## Layout runtime smoke" in rendered
    assert "## Current limitations" in rendered


def test_build_report_reads_layout_artifacts(tmp_path: Path) -> None:
    module = _load_module()
    base_root = tmp_path / "Zephyr-base"
    (base_root / ".tmp").mkdir(parents=True)
    (base_root / "runtime/python-runtime").mkdir(parents=True)

    reports = {
        ".tmp/no_bom_check.json": {"summary": {"pass": True}},
        ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        ".tmp/generated_artifact_hygiene_check.json": {
            "summary": {
                "src_tauri_gen_classified_generated": True,
                "src_tauri_gen_committed": False,
                "generated_artifact_hygiene_pass": True,
            }
        },
        ".tmp/install_layout_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/base_install_layout_build.json": {
            "summary": {"pass": True},
            "manifest": {
                "ui_dist_present": True,
                "public_core_bundle_present": True,
                "runtime_bootstrap_present": True,
                "installer_built": False,
                "release_created": False,
                "embedded_python_runtime": False,
                "wheelhouse_bundled": False,
                "clean_machine_runtime_proven": False,
            },
        },
        ".tmp/base_install_layout_audit.json": {
            "summary": {
                "pass": True,
                "forbidden_dirs_present": False,
                "commercial_terms_blocked": 0,
            }
        },
        ".tmp/base_install_layout_runtime_smoke.json": {
            "summary": {"pass": True},
            "evidence": {
                "marker_found": True,
                "run_result_exists": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_network": False,
                "requires_p45_substrate": False,
            },
        },
        ".tmp/runtime_packaging_baseline_check.json": {
            "summary": {"pass": True, "runtime_manifest_exists": True}
        },
        ".tmp/managed_runtime_flow_check.json": {"summary": {"pass": True}},
        ".tmp/ui_build_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_path_check.json": {"summary": {"pass": True}},
        ".tmp/s12_repo_status.json": {
            "previous_head_sha": module.PREVIOUS_HEAD_SHA,
            "current_head_sha": "4b19d216ac76829a333712f74dbcdf285e940ad9",
            "pushed": True,
        },
    }
    for rel, payload in reports.items():
        path = base_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = module.build_report(root=tmp_path, base_root=base_root)
    summary = report["summary"]
    assert summary["overall"] == "pass"
    assert summary["m3_s12_status"] == "sealed"
    assert report["generated_artifacts"]["src_tauri_gen_committed"] is False
    assert report["install_layout"]["layout_runtime_smoke_pass"] is True
    assert report["runtime_evidence"]["marker_found"] is True
