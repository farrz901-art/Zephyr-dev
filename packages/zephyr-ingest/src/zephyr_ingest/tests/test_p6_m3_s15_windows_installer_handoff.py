from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_s15_windows_installer_handoff.py"
    spec = importlib.util.spec_from_file_location(
        "p6_m3_s15_windows_installer_handoff",
        module_path,
    )
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
            "m3_s15_status": "sealed",
            "active_blockers": 0,
            "major_gaps": 0,
        },
        "installer_package": {
            "package_kind": "portable_zip",
            "package_built": True,
            "package_path": "pkg.zip",
            "installer_manifest_exists": True,
            "installer_built": True,
            "signed_installer": False,
            "release_created": False,
            "package_audit_pass": True,
            "install_smoke_pass": True,
        },
        "package_contents": {
            "ui_dist_present": True,
            "tauri_app_present": True,
            "public_core_bundle_present": True,
            "runtime_manifest_present": True,
            "wheelhouse_in_package": True,
            "bootstrap_present": True,
            "docs_present": True,
            "forbidden_dirs_present": False,
        },
        "runtime_evidence": {
            "text_flow_pass": True,
            "file_flow_pass": True,
            "text_marker_found": True,
            "file_marker_found": True,
            "billing_semantics": False,
            "bundled_runtime_used": True,
            "fixture_runner_used": False,
            "zephyr_dev_working_tree_required": False,
            "requires_network_for_dependency_install": False,
            "requires_network_at_runtime": False,
            "requires_p45_substrate": False,
        },
        "wheelhouse": {
            "wheelhouse_in_package": True,
            "wheelhouse_committed_to_repo": False,
            "wheel_only_ready": False,
            "sdist_artifacts": ["langdetect-1.0.9.tar.gz"],
        },
        "scope": {
            "signed_installer": False,
            "release_created": False,
            "official_release": False,
            "embedded_python_runtime": False,
            "auto_update": False,
        },
        "boundary": {
            "commercial_terms_blocked": 0,
            "network_runtime_calls_blocked": 0,
        },
        "issues": [],
        "non_blocking_notes": [],
        "selected_python": "python.exe",
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3-S15 Windows installer package handoff" in rendered
    assert "## Installer package" in rendered
    assert "## Install smoke" in rendered
    assert "## Wheelhouse" in rendered


def test_build_report_is_pass_when_required_s15_artifacts_exist(tmp_path: Path) -> None:
    module = _load_module()
    base_root = tmp_path / "Zephyr-base"
    reports = {
        ".tmp/no_bom_check.json": {"summary": {"pass": True}},
        ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        ".tmp/windows_installer_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_build_baseline.json": {"summary": {"pass": True}},
        ".tmp/windows_installer_package_report.json": {
            "summary": {"pass": True, "package_built": True},
            "package_path": "pkg.zip",
        },
        ".tmp/windows_installer_package/installer_manifest.json": {
            "package_kind": "portable_zip",
            "ui_dist_present": True,
            "tauri_app_present": True,
            "public_core_bundle_present": True,
            "runtime_manifest_present": True,
            "wheelhouse_in_package": True,
            "wheelhouse_committed_to_repo": False,
            "embedded_python_runtime": False,
            "installer_built": True,
            "signed_installer": False,
            "release_created": False,
        },
        ".tmp/windows_installer_package_audit.json": {"summary": {"pass": True}},
        ".tmp/windows_install_smoke_report.json": {
            "summary": {"pass": True, "install_smoke_pass": True},
            "uses_no_index": True,
            "uses_find_links": True,
            "text_flow": {
                "pass": True,
                "marker_found": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_network": False,
                "requires_p45_substrate": False,
            },
            "file_flow": {
                "pass": True,
                "marker_found": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_network": False,
                "requires_p45_substrate": False,
            },
            "runtime": {"managed_python": "python.exe"},
        },
        ".tmp/wheelhouse_wheel_only_readiness.json": {
            "summary": {"pass": True, "wheel_only_ready": False},
            "wheel_only_ready": False,
            "sdist_artifacts": ["langdetect-1.0.9.tar.gz"],
        },
        ".tmp/offline_runtime_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/offline_install_network_guard.json": {"summary": {"pass": True}},
        ".tmp/ui_build_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_path_check.json": {"summary": {"pass": True}},
        ".tmp/managed_runtime_flow_check.json": {"summary": {"pass": True}},
        ".tmp/install_layout_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_proof_pack_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/s15_repo_status.json": {
            "previous_head_sha": module.PREVIOUS_HEAD_SHA,
            "current_head_sha": "e62d301deadbeef",
            "pushed": True,
        },
    }
    for rel, payload in reports.items():
        path = base_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = module.build_report(root=tmp_path, base_root=base_root)
    assert report["summary"]["overall"] == "pass"
    assert report["summary"]["m3_s15_status"] == "sealed"
    assert report["installer_package"]["install_smoke_pass"] is True
    assert report["scope"]["signed_installer"] is False
    assert any("langdetect" in note for note in report["non_blocking_notes"])
