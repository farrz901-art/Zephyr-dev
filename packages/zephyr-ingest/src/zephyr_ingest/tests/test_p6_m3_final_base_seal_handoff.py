from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_final_base_seal_handoff.py"
    spec = importlib.util.spec_from_file_location(
        "p6_m3_final_base_seal_handoff",
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
            "p6_m3_final_status": "sealed",
            "go": True,
            "active_blockers": 0,
            "major_gaps": 0,
            "allowed_next_phase": "P6-M4",
        },
        "proof_reuse": {
            "s17_external_user_proof_reused": True,
            "s17_external_user_proof_pass": True,
            "s18_closure_audit_pass": True,
            "manual_gui_retest_required": False,
        },
        "delivery": {
            "artifact_kind": "unsigned_portable_zip_preview",
            "recommended_filename": "ZephyrBase-windows-unsigned.zip",
            "recommended_hosting": "GitHub Release or repository artifact",
            "website_strategy": "Site links to GitHub Release",
        },
        "scope": {
            "signed_installer": False,
            "official_release": False,
            "release_created": False,
            "auto_update": False,
        },
        "issues": [],
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3 Final Base seal handoff" in rendered
    assert "## Proof reuse" in rendered
    assert "P6-M4" in rendered


def test_build_report_is_pass_when_required_artifacts_exist(tmp_path: Path) -> None:
    module = _load_module()
    module._git_output = lambda _repo, *args: "abc123"  # type: ignore[attr-defined]
    root = tmp_path / "Zephyr"
    base_root = tmp_path / "Zephyr-base"
    (root / ".tmp/p6_m3_s18_base_mvp_closure_handoff").mkdir(parents=True, exist_ok=True)
    reports = {
        base_root / ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        base_root / ".tmp/base_m3_overclaim_check.json": {"summary": {"pass": True}},
        base_root / ".tmp/base_m3_readiness/report.json": {"summary": {"pass": True}},
        base_root / ".tmp/base_m3_final_seal_check.json": {
            "summary": {
                "pass": True,
                "go": True,
                "p6_m3_final_status": "sealed",
                "allowed_next_phase": "P6-M4",
            }
        },
        base_root / ".tmp/windows_installer_package_report.json": {"summary": {"pass": True}},
        base_root / ".tmp/windows_installer_package_audit.json": {"summary": {"pass": True}},
        base_root / ".tmp/windows_install_smoke_report.json": {
            "summary": {"install_smoke_pass": True}
        },
        base_root / ".tmp/external_package_runtime_smoke_report.json": {
            "summary": {"external_runtime_smoke_pass": True}
        },
        base_root / ".tmp/external_gui_runtime_bootstrap_check.json": {"summary": {"pass": True}},
        base_root / ".tmp/imported_external_package_ux_proof_report.json": {
            "summary": {"manual_gui_proof_pass": True}
        },
        root / ".tmp/p6_m3_s18_base_mvp_closure_handoff/report.json": {
            "summary": {"m3_s18_status": "sealed"}
        },
        base_root / "manifests/base_m3_final_seal.json": {
            "summary": {
                "go": True,
                "allowed_next_phase": "P6-M4",
            },
            "proof_reuse": {
                "s17_external_user_proof_reused": True,
                "s17_external_user_proof_pass": True,
                "s18_closure_audit_pass": True,
                "manual_gui_retest_required": False,
            },
            "delivery": {
                "artifact_kind": "unsigned_portable_zip_preview",
                "recommended_filename": "ZephyrBase-windows-unsigned.zip",
                "recommended_hosting": "GitHub Release or repository artifact",
                "website_strategy": "Site links to GitHub Release",
                "paid_cdn_required": False,
                "controlled_download_required": False,
            },
            "scope": {
                "signed_installer": False,
                "official_release": False,
                "release_created": False,
                "auto_update": False,
                "embedded_python_runtime": False,
                "runtime_capability_changed": False,
            },
            "deferred": {
                "p6_m8": ["signed installer"],
                "post_m3_core": ["future unstructured upgrade"],
            },
            "capability": {
                "pdf": False,
                "docx": False,
                "image_or_ocr": False,
                "html": False,
                "cloud": False,
                "pro": False,
                "license_or_entitlement": False,
                "payment_or_billing": False,
            },
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
    assert report["summary"]["p6_m3_final_status"] == "sealed"
    assert report["summary"]["go"] is True
