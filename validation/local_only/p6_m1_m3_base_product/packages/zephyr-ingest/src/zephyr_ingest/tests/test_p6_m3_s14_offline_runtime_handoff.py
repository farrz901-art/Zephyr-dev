from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "tools" / "p6_m3_s14_offline_runtime_handoff.py"
    spec = importlib.util.spec_from_file_location("p6_m3_s14_offline_runtime_handoff", module_path)
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
            "m3_s14_status": "sealed",
            "active_blockers": 0,
            "major_gaps": 0,
        },
        "wheelhouse": {
            "wheelhouse_build_attempted": True,
            "wheelhouse_built": True,
            "wheels_count": 10,
            "missing_requirements": [],
            "wheelhouse_committed": False,
            "venv_committed": False,
        },
        "offline_install": {
            "offline_install_pass": True,
            "uses_no_index": True,
            "uses_find_links": True,
            "requires_network_for_dependency_install": False,
            "requires_network_at_runtime": False,
            "selected_python_is_wheelhouse_venv": True,
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
            "requires_p45_substrate": False,
        },
        "offline_proof_pack": {
            "offline_pack_built": True,
            "offline_pack_local_simulation_pass": True,
            "external_offline_proof_imported": False,
            "external_offline_proof_pass": False,
        },
        "scope": {
            "installer_built": False,
            "release_created": False,
            "signed_installer": False,
            "embedded_python_runtime": False,
            "wheelhouse_bundled_in_repo": False,
            "offline_runtime_install_proven_locally": True,
            "external_clean_machine_offline_proven": False,
        },
        "boundary": {
            "commercial_terms_blocked": 0,
            "network_runtime_calls_blocked": 0,
        },
        "issues": [],
        "non_blocking_notes": [],
    }
    rendered = module.render_markdown(report)
    assert "# P6-M3-S14 Offline wheelhouse runtime handoff" in rendered
    assert "## Wheelhouse" in rendered
    assert "## Offline install" in rendered
    assert "## Offline proof pack" in rendered


def test_build_report_is_pass_when_required_s14_artifacts_exist(tmp_path: Path) -> None:
    module = _load_module()
    base_root = tmp_path / "Zephyr-base"
    (base_root / ".tmp/clean_machine_offline_proof_pack").mkdir(parents=True)

    reports = {
        ".tmp/no_bom_check.json": {"summary": {"pass": True}},
        ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        ".tmp/offline_runtime_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/base_runtime_wheelhouse_manifest.json": {
            "download_pass": True,
            "wheels_count": 5,
            "missing_requirements": [],
        },
        ".tmp/wheelhouse_runtime_install_check.json": {
            "summary": {
                "pass": True,
                "offline_install_pass": True,
                "uses_no_index": True,
                "uses_find_links": True,
                "requires_network_for_dependency_install": False,
                "requires_network_at_runtime": False,
            },
            "selected_python_is_wheelhouse_venv": True,
            "text_flow": {
                "pass": True,
                "marker_found": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_p45_substrate": False,
            },
            "file_flow": {
                "pass": True,
                "marker_found": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_p45_substrate": False,
            },
        },
        ".tmp/offline_install_network_guard.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_offline_proof_pack/offline_proof_pack_manifest.json": {
            "installer_built": False,
            "release_created": False,
            "embedded_python_runtime": False,
        },
        ".tmp/offline_proof_pack_local_simulation_check.json": {
            "summary": {
                "pass": True,
                "local_simulation_pass": True,
                "offline_runtime_install_proven_locally": True,
            }
        },
        ".tmp/runtime_packaging_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/install_layout_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_proof_pack_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/ui_build_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_path_check.json": {"summary": {"pass": True}},
        ".tmp/managed_runtime_flow_check.json": {"summary": {"pass": True}},
        ".tmp/s14_repo_status.json": {
            "previous_head_sha": module.PREVIOUS_HEAD_SHA,
            "current_head_sha": "76bbfe0deadbeef",
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
    assert summary["m3_s14_status"] == "sealed"
    assert report["wheelhouse"]["wheelhouse_built"] is True
    assert report["offline_install"]["offline_install_pass"] is True
    assert any("langdetect sdist build path" in note for note in report["non_blocking_notes"])


def test_build_report_marks_external_offline_proof_as_passed_when_import_exists(
    tmp_path: Path,
) -> None:
    module = _load_module()
    base_root = tmp_path / "Zephyr-base"
    (base_root / ".tmp/clean_machine_offline_proof_pack").mkdir(parents=True)

    reports = {
        ".tmp/no_bom_check.json": {"summary": {"pass": True}},
        ".tmp/base_boundary_check.json": {"summary": {"pass": True}},
        ".tmp/offline_runtime_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/base_runtime_wheelhouse_manifest.json": {
            "download_pass": True,
            "wheels_count": 5,
            "missing_requirements": [],
        },
        ".tmp/wheelhouse_runtime_install_check.json": {
            "summary": {
                "pass": True,
                "offline_install_pass": True,
                "uses_no_index": True,
                "uses_find_links": True,
                "requires_network_for_dependency_install": False,
                "requires_network_at_runtime": False,
            },
            "selected_python_is_wheelhouse_venv": True,
            "text_flow": {
                "pass": True,
                "marker_found": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_p45_substrate": False,
            },
            "file_flow": {
                "pass": True,
                "marker_found": True,
                "billing_semantics": False,
                "bundled_runtime_used": True,
                "fixture_runner_used": False,
                "zephyr_dev_working_tree_required": False,
                "requires_p45_substrate": False,
            },
        },
        ".tmp/offline_install_network_guard.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_offline_proof_pack/offline_proof_pack_manifest.json": {
            "installer_built": False,
            "release_created": False,
            "embedded_python_runtime": False,
        },
        ".tmp/offline_proof_pack_local_simulation_check.json": {
            "summary": {
                "pass": True,
                "local_simulation_pass": True,
                "offline_runtime_install_proven_locally": True,
            }
        },
        ".tmp/runtime_packaging_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/install_layout_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/clean_machine_proof_pack_baseline_check.json": {"summary": {"pass": True}},
        ".tmp/ui_build_check.json": {"summary": {"pass": True}},
        ".tmp/tauri_app_path_check.json": {"summary": {"pass": True}},
        ".tmp/managed_runtime_flow_check.json": {"summary": {"pass": True}},
        ".tmp/s14_repo_status.json": {
            "previous_head_sha": module.PREVIOUS_HEAD_SHA,
            "current_head_sha": "76bbfe0deadbeef",
            "pushed": True,
        },
        ".tmp/imported_offline_runtime_proof_report.json": {
            "summary": {
                "pass": True,
                "external_offline_proof_imported": True,
                "external_offline_proof_pass": True,
            }
        },
    }
    for rel, payload in reports.items():
        path = base_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = module.build_report(root=tmp_path, base_root=base_root)
    assert report["offline_proof_pack"]["external_offline_proof_imported"] is True
    assert report["offline_proof_pack"]["external_offline_proof_pass"] is True
    assert report["scope"]["external_clean_machine_offline_proven"] is True
