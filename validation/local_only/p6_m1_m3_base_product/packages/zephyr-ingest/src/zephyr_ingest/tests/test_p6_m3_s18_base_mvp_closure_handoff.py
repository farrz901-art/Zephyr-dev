from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_s18_base_mvp_closure_handoff.py"
    spec = importlib.util.spec_from_file_location(
        "p6_m3_s18_base_mvp_closure_handoff",
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
            "m3_s18_status": "sealed",
            "active_blockers": 0,
            "major_gaps": 0,
        },
        "m3_readiness": {
            "base_mvp_runnable": True,
            "external_package_ux_proven": True,
            "portable_zip_package_proven": True,
            "managed_runtime_bootstrap_proven": True,
            "offline_runtime_proven": True,
            "clean_machine_runtime_proven": True,
            "bilingual_ui_proven": True,
            "m3_distribution_decision": "unsigned_portable_zip_preview",
        },
        "scope": {
            "signed_installer": False,
            "official_release": False,
            "release_created": False,
            "auto_update": False,
            "embedded_python_runtime": False,
            "runtime_capability_changed": False,
        },
        "boundary": {
            "pdf_claimed": False,
            "docx_claimed": False,
            "image_or_ocr_claimed": False,
            "html_claimed": False,
            "cloud_claimed": False,
            "pro_claimed": False,
            "license_or_entitlement_claimed": False,
            "payment_or_billing_claimed": False,
        },
        "next": {
            "recommended_next_step": "P6-M3-S19 real user UX walkthrough",
        },
        "issues": [],
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3-S18 Base MVP closure handoff" in rendered
    assert "## M3 readiness" in rendered
    assert "unsigned_portable_zip_preview" in rendered


def test_build_report_is_pass_when_required_artifacts_exist(tmp_path: Path) -> None:
    module = _load_module()
    module._git_output = lambda _repo, *args: "abc123"  # type: ignore[attr-defined]
    root = tmp_path / "Zephyr"
    base_root = tmp_path / "Zephyr-base"
    (root / ".tmp/p6_m3_s17_external_ux_handoff").mkdir(parents=True, exist_ok=True)
    reports = {
        base_root / ".tmp/base_m3_readiness/report.json": {
            "summary": {"pass": True, "overall": "pass"},
            "m3_readiness": {
                "base_mvp_runnable": True,
                "external_package_ux_proven": True,
                "portable_zip_package_proven": True,
                "managed_runtime_bootstrap_proven": True,
                "offline_runtime_proven": True,
                "clean_machine_runtime_proven": True,
                "bilingual_ui_proven": True,
                "m3_distribution_decision": "unsigned_portable_zip_preview",
            },
            "scope": {
                "signed_installer": False,
                "official_release": False,
                "release_created": False,
                "auto_update": False,
                "embedded_python_runtime": False,
                "runtime_capability_changed": False,
            },
            "boundary": {
                "pdf_claimed": False,
                "docx_claimed": False,
                "image_or_ocr_claimed": False,
                "html_claimed": False,
                "cloud_claimed": False,
                "pro_claimed": False,
                "license_or_entitlement_claimed": False,
                "payment_or_billing_claimed": False,
                "private_core_allowed": False,
                "web_core_dependency_allowed": False,
                "secrets_printed": False,
            },
        },
        base_root / ".tmp/base_m3_overclaim_check.json": {
            "summary": {"pass": True},
        },
        base_root / ".tmp/windows_installer_package_report.json": {
            "summary": {"pass": True, "package_built": True},
            "manifest": {
                "package_kind": "portable_zip",
                "signed_installer": False,
                "release_created": False,
                "embedded_python_runtime": False,
            },
        },
        base_root / ".tmp/windows_installer_package_audit.json": {"summary": {"pass": True}},
        base_root / ".tmp/windows_install_smoke_report.json": {
            "summary": {"install_smoke_pass": True}
        },
        base_root / ".tmp/external_package_runtime_smoke_report.json": {
            "summary": {"external_runtime_smoke_pass": True}
        },
        base_root / ".tmp/external_gui_runtime_bootstrap_check.json": {"summary": {"pass": True}},
        base_root / ".tmp/base_ux_shell_check.json": {"summary": {"pass": True}},
        base_root / ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        root / ".tmp/p6_m3_s17_external_ux_handoff/report.json": {
            "summary": {"m3_s17_status": "sealed"}
        },
    }
    for path, payload in reports.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    report = module.build_report(root=root, base_root=base_root)
    assert report["summary"]["overall"] == "pass"
    assert report["summary"]["m3_s18_status"] == "sealed"
    assert report["m3_readiness"]["external_package_ux_proven"] is True
