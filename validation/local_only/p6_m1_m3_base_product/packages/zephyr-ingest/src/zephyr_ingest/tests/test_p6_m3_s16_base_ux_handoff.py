from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_s16_base_ux_handoff.py"
    spec = importlib.util.spec_from_file_location(
        "p6_m3_s16_base_ux_handoff",
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
            "m3_s16_status": "sealed",
            "active_blockers": 0,
            "major_gaps": 0,
        },
        "ux": {
            "bilingual_ui": True,
            "english_supported": True,
            "chinese_supported": True,
            "language_toggle_present": True,
            "primary_run_button_present": True,
            "advanced_diagnostics_collapsed_by_default": True,
            "sample_mode_demoted": True,
            "proof_export_demoted": True,
            "lineage_demoted": True,
            "user_friendly_runtime_copy": True,
            "supported_formats_truthful": True,
        },
        "runtime_scope": {
            "runtime_contract_changed": False,
            "supported_formats_changed": False,
            "pdf_claimed": False,
            "docx_claimed": False,
            "image_or_ocr_claimed": False,
            "network_or_cloud_claimed": False,
            "commercial_claimed": False,
        },
        "validation": {
            "ux_shell_check_pass": True,
            "ui_build_pass": True,
            "ui_typecheck_pass": True,
            "ui_result_lifecycle_pass": True,
            "tauri_app_path_pass": True,
            "installer_package_regression_pass": True,
            "install_smoke_pass": True,
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
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3-S16 Base UX handoff" in rendered
    assert "## UX shell" in rendered
    assert "## Runtime scope" in rendered
    assert "## Validation" in rendered


def test_build_report_is_pass_when_required_s16_artifacts_exist(tmp_path: Path) -> None:
    module = _load_module()
    base_root = tmp_path / "Zephyr-base"
    reports = {
        ".tmp/no_bom_check.json": {"summary": {"pass": True}},
        ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        ".tmp/base_ux_shell_check.json": {
            "summary": {
                "pass": True,
                "bilingual_ui": True,
                "english_supported": True,
                "chinese_supported": True,
                "language_toggle_present": True,
                "primary_run_button_present": True,
                "advanced_diagnostics_collapsed_by_default": True,
                "sample_mode_demoted": True,
                "proof_export_demoted": True,
                "lineage_demoted": True,
                "user_friendly_runtime_copy": True,
                "supported_formats_truthful": True,
            }
        },
        ".tmp/ui_shell_check.json": {"summary": {"pass": True}},
        ".tmp/ui_build_check.json": {"summary": {"pass": True, "typecheck_pass": True}},
        ".tmp/ui_result_lifecycle_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_invoke_payload_shape_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_path_check.json": {"summary": {"pass": True}},
        ".tmp/windows_installer_package_report.json": {"summary": {"pass": True}},
        ".tmp/windows_installer_package_audit.json": {"summary": {"pass": True}},
        ".tmp/windows_install_smoke_report.json": {
            "summary": {"pass": True, "install_smoke_pass": True},
            "text_flow": {
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_network": False,
                "requires_p45_substrate": False,
            },
        },
        ".tmp/windows_installer_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/offline_runtime_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/offline_install_network_guard.json": {"summary": {"pass": True}},
        ".tmp/managed_runtime_flow_check.json": {"summary": {"pass": True}},
        ".tmp/wheelhouse_wheel_only_readiness.json": {
            "wheel_only_ready": False,
            "sdist_artifacts": ["langdetect-1.0.9.tar.gz"],
        },
        ".tmp/s16_repo_status.json": {
            "previous_head_sha": module.PREVIOUS_HEAD_SHA,
            "current_head_sha": "19387f1deadbeef",
            "pushed": True,
        },
    }
    for rel, payload in reports.items():
        path = base_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = module.build_report(root=tmp_path, base_root=base_root)
    assert report["summary"]["overall"] == "pass"
    assert report["summary"]["m3_s16_status"] == "sealed"
    assert report["validation"]["install_smoke_pass"] is True
    assert report["runtime_scope"]["runtime_contract_changed"] is False
