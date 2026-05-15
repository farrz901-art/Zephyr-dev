from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_s17_external_ux_handoff.py"
    spec = importlib.util.spec_from_file_location(
        "p6_m3_s17_external_ux_handoff",
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
            "overall": "conditional",
            "m3_s17_status": "conditional",
            "active_blockers": 0,
            "major_gaps": 1,
        },
        "external_package": {
            "package_kind": "portable_zip",
            "package_built": True,
            "package_audit_pass": True,
            "external_runtime_smoke_pass": True,
            "external_gui_runtime_bootstrap_pass": True,
            "manual_gui_proof_imported": False,
            "manual_gui_proof_pass": False,
        },
        "ux": {
            "bilingual_ui": True,
            "language_toggle_visible": False,
            "english_visible": False,
            "chinese_visible": False,
            "primary_run_button_visible": False,
            "advanced_collapsed_by_default": False,
            "advanced_expanded": False,
        },
        "runtime_evidence": {
            "text_flow_pass": True,
            "file_flow_pass": True,
            "text_marker_found": True,
            "file_marker_found": True,
            "billing_semantics": False,
        },
        "scope": {
            "signed_installer": False,
            "official_release": False,
            "release_created": False,
            "auto_update": False,
        },
        "issues": [],
        "non_blocking_notes": [],
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3-S17 External package UX handoff" in rendered
    assert "## External package" in rendered
    assert "## UX proof status" in rendered
    assert "## Runtime evidence" in rendered
    assert "Import a validated manual external GUI proof" in rendered


def test_render_markdown_sealed_report_updates_next_step() -> None:
    module = _load_module()
    report = {
        "summary": {
            "overall": "pass",
            "m3_s17_status": "sealed",
            "active_blockers": 0,
            "major_gaps": 0,
        },
        "external_package": {
            "package_kind": "portable_zip",
            "package_built": True,
            "package_audit_pass": True,
            "external_runtime_smoke_pass": True,
            "external_gui_runtime_bootstrap_pass": True,
            "manual_gui_proof_imported": True,
            "manual_gui_proof_pass": True,
        },
        "ux": {
            "bilingual_ui": True,
            "language_toggle_visible": True,
            "english_visible": True,
            "chinese_visible": True,
            "primary_run_button_visible": True,
            "advanced_collapsed_by_default": True,
            "advanced_expanded": True,
        },
        "runtime_evidence": {
            "text_flow_pass": True,
            "file_flow_pass": True,
            "text_marker_found": True,
            "file_marker_found": True,
            "billing_semantics": False,
        },
        "scope": {
            "signed_installer": False,
            "official_release": False,
            "release_created": False,
            "auto_update": False,
        },
        "issues": [],
        "non_blocking_notes": [],
    }
    rendered = module.render_markdown(report)
    assert "S17 external package GUI UX proof is sealed" in rendered
    assert "Import a validated manual external GUI proof" not in rendered


def test_build_report_is_conditional_without_manual_proof(tmp_path: Path) -> None:
    module = _load_module()
    base_root = tmp_path / "Zephyr-base"
    reports = {
        ".tmp/no_bom_check.json": {"summary": {"pass": True}},
        ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        ".tmp/base_ux_shell_check.json": {"summary": {"pass": True, "bilingual_ui": True}},
        ".tmp/external_package_ux_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/external_package_ux_proof_pack_report.json": {"summary": {"pass": True}},
        ".tmp/external_package_runtime_smoke_report.json": {
            "summary": {"pass": True, "external_runtime_smoke_pass": True},
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
        },
        ".tmp/external_gui_runtime_bootstrap_check.json": {
            "summary": {
                "pass": True,
                "managed_runtime_created": True,
                "pointer_exists": True,
                "text_flow_pass": True,
                "file_flow_pass": True,
            }
        },
        ".tmp/windows_installer_package_report.json": {
            "summary": {"pass": True, "package_built": True}
        },
        ".tmp/windows_installer_package_audit.json": {"summary": {"pass": True}},
        ".tmp/ui_build_check.json": {"summary": {"pass": True}},
        ".tmp/ui_result_lifecycle_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_path_check.json": {"summary": {"pass": True}},
        ".tmp/s17_repo_status.json": {
            "previous_head_sha": module.PREVIOUS_HEAD_SHA,
            "current_head_sha": "abcdef1234567890",
            "pushed": True,
        },
    }
    for rel, payload in reports.items():
        path = base_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    report = module.build_report(root=tmp_path, base_root=base_root)
    assert report["summary"]["overall"] == "conditional"
    assert report["summary"]["m3_s17_status"] == "conditional"
    assert report["external_package"]["external_runtime_smoke_pass"] is True
    assert report["external_package"]["external_gui_runtime_bootstrap_pass"] is True
    assert report["external_package"]["manual_gui_proof_imported"] is False
