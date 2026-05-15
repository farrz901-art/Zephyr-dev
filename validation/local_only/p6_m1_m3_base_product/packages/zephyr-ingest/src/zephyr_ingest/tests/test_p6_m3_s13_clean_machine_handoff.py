from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_s13_clean_machine_handoff.py"
    spec = importlib.util.spec_from_file_location("p6_m3_s13_clean_machine_handoff", module_path)
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
            "m3_s13_status": "conditional",
            "active_blockers": 0,
            "major_gaps": 1,
        },
        "proof_pack": {
            "pack_built": True,
            "zip_exists": True,
            "relocation_audit_pass": True,
            "local_unzip_simulation_pass": True,
            "external_clean_machine_proof_imported": False,
            "external_clean_machine_proof_pass": False,
        },
        "clean_machine_runtime": {
            "proof_level": "L1",
            "requires_python_on_clean_machine": True,
            "requires_network_for_bootstrap": True,
            "requires_git": False,
            "requires_node": False,
            "requires_rust": False,
            "requires_zephyr_dev": False,
            "requires_zephyr_base_repo": False,
            "clean_machine_runtime_proven": False,
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
            "requires_network_at_runtime": False,
            "requires_p45_substrate": False,
        },
        "scope": {
            "installer_built": False,
            "release_created": False,
            "signed_installer": False,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "offline_runtime_install_proven": False,
        },
        "boundary": {
            "commercial_terms_blocked": 0,
            "network_runtime_calls_blocked": 0,
        },
        "issues": [],
        "non_blocking_notes": [],
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3-S13 Clean-machine runtime proof handoff" in rendered
    assert "## Proof pack" in rendered
    assert "## Clean-machine runtime" in rendered
    assert "## Runtime evidence" in rendered


def test_build_report_is_conditional_without_external_clean_machine_proof(tmp_path: Path) -> None:
    module = _load_module()
    base_root = tmp_path / "Zephyr-base"
    (base_root / ".tmp/clean_machine_proof_pack").mkdir(parents=True)
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    simulation_root = tmp_path / "simulation" / "ZephyrBase"
    proof_root = simulation_root / "proof"
    proof_root.mkdir(parents=True)

    proof_payload = {
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
    }
    (proof_root / "clean_machine_runtime_proof.json").write_text(
        json.dumps(proof_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    reports = {
        ".tmp/no_bom_check.json": {"summary": {"pass": True}},
        ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        ".tmp/install_layout_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/runtime_packaging_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/base_install_layout_build.json": {"summary": {"pass": True}},
        ".tmp/base_install_layout_audit.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_proof_pack/clean_machine_proof_pack_manifest.json": {
            "zip_path": str(
                base_root / ".tmp/clean_machine_proof_pack/ZephyrBase-clean-machine-proof.zip"
            ),
            "proof_level": "L1",
            "requires_python_on_clean_machine": True,
            "requires_network_for_bootstrap": True,
            "requires_git": False,
            "requires_node": False,
            "requires_rust": False,
            "requires_zephyr_dev": False,
            "requires_zephyr_base_repo": False,
            "installer_built": False,
            "release_created": False,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
        },
        ".tmp/clean_machine_pack_relocation_audit.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_proof_pack_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_pack_local_simulation_check.json": {
            "summary": {
                "pass": True,
                "local_simulation_pass": True,
            },
            "simulation_root": str(simulation_root),
        },
        ".tmp/ui_build_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_path_check.json": {"summary": {"pass": True}},
        ".tmp/managed_runtime_flow_check.json": {"summary": {"pass": True}},
        ".tmp/s13_repo_status.json": {
            "previous_head_sha": module.PREVIOUS_HEAD_SHA,
            "current_head_sha": "46a5fb5c0ffee",
            "pushed": True,
        },
    }
    for rel, payload in reports.items():
        path = base_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (base_root / ".tmp/clean_machine_proof_pack/ZephyrBase-clean-machine-proof.zip").write_text(
        "", encoding="utf-8"
    )

    report = module.build_report(root=tmp_path, base_root=base_root)
    summary = report["summary"]
    assert summary["overall"] == "conditional"
    assert summary["m3_s13_status"] == "conditional"
    assert summary["major_gaps"] == 1
    assert report["proof_pack"]["external_clean_machine_proof_imported"] is False
    assert report["clean_machine_runtime"]["clean_machine_runtime_proven"] is False
