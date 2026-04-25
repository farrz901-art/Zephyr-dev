from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s5_report import main as p5_m5_s5_main

from zephyr_ingest.testing.p5_m5_s5 import (
    P5_M5_S5_BUNDLE_LAYOUT_PATH,
    P5_M5_S5_COMPLETENESS_CHECKS_PATH,
    P5_M5_S5_HANDOFF_CONTRACT_PATH,
    P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH,
    P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH,
    P5_M5_S5_RELEASE_MANIFEST_PATH,
    build_p5_m5_s5_bundle_layout,
    build_p5_m5_s5_completeness_checks,
    build_p5_m5_s5_handoff_contract,
    build_p5_m5_s5_release_bundle_inventory,
    build_p5_m5_s5_release_manifest,
    format_p5_m5_s5_results,
    validate_p5_m5_s5_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s5_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH) == (
        build_p5_m5_s5_release_bundle_inventory()
    )
    assert _load_json_object(P5_M5_S5_RELEASE_MANIFEST_PATH) == build_p5_m5_s5_release_manifest()
    assert _load_json_object(P5_M5_S5_BUNDLE_LAYOUT_PATH) == build_p5_m5_s5_bundle_layout()
    assert _load_json_object(P5_M5_S5_HANDOFF_CONTRACT_PATH) == build_p5_m5_s5_handoff_contract()
    assert _load_json_object(P5_M5_S5_COMPLETENESS_CHECKS_PATH) == (
        build_p5_m5_s5_completeness_checks()
    )
    assert P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s5_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s5_artifacts()

    assert all(check.ok for check in checks), format_p5_m5_s5_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s5_inventory_covers_release_consumable_groups() -> None:
    inventory = _load_json_object(P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH)
    inventory_groups = cast(dict[str, object], inventory["inventory_groups"])
    helper_group = cast(dict[str, object], inventory_groups["helper_report_canonical_entrypoints"])

    assert set(inventory_groups) == {
        "runtime_bounded_benchmark",
        "recovery_governance_manual_action",
        "usage_source_contract_provenance",
        "capability_domain_product_cut",
        "m4_m5_stage_truths",
        "helper_report_canonical_entrypoints",
    }
    assert len(cast(list[object], helper_group["surfaces"])) == 16


@pytest.mark.auth_contract
def test_p5_m5_s5_manifest_freezes_reference_bundle_layout_and_helper_mapping() -> None:
    manifest = _load_json_object(P5_M5_S5_RELEASE_MANIFEST_PATH)
    bundle_root = cast(dict[str, object], manifest["bundle_root"])
    helper_mapping = cast(dict[str, object], manifest["canonical_helper_report_mapping"])

    assert bundle_root["logical_root"] == "validation"
    assert bundle_root["helper_entrypoint_root"] == "tools"
    assert bundle_root["manifest_kind"] == "reference_bundle_not_archive"
    assert bundle_root["installer_or_deployment_bundle"] is False
    assert "m5_s5_report" in helper_mapping
    assert "governance_audit_report" in helper_mapping


@pytest.mark.auth_contract
def test_p5_m5_s5_handoff_contract_and_completeness_checks_prohibit_source_code_dependency() -> (
    None
):
    handoff = _load_json_object(P5_M5_S5_HANDOFF_CONTRACT_PATH)
    completeness = _load_json_object(P5_M5_S5_COMPLETENESS_CHECKS_PATH)

    assert handoff["source_code_reading_allowed"] is False
    assert "source code reading is a required discovery step" in cast(
        list[object], handoff["must_not_infer"]
    )
    assert "discovering bundle contents by reading helper source code" in cast(
        list[object], completeness["forbidden_consumption_patterns"]
    )
    assert len(cast(list[object], completeness["checks"])) == 8


@pytest.mark.auth_contract
def test_p5_m5_s5_report_preserves_non_installer_non_cloud_scope() -> None:
    report = P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH.read_text(encoding="utf-8")

    assert "S5 hardens a release-consumable truth bundle, not installer implementation." in report
    assert "S5 does not repeat S3 stability work or S4 cold-operator proof." in report
    assert (
        "Outer layers discover canonical truth through the bundle artifacts and helper/report "
        "entrypoints, not source code." in report
    )
    assert (
        "Capability domains, product cuts, source truth, governance, and fanout remain technical "
        "truth surfaces, not commercial ones." in report
    )


@pytest.mark.auth_contract
def test_p5_m5_s5_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s5_main(["--print-inventory-path"])
    inventory_output = capsys.readouterr()
    assert exit_code == 0
    printed_inventory = Path(inventory_output.out.strip())
    assert printed_inventory.parts[-2:] == (
        "validation",
        "p5_m5_s5_release_bundle_inventory.json",
    )

    exit_code = p5_m5_s5_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M5_S5_RELEASE_BUNDLE.md")

    exit_code = p5_m5_s5_main(["--print-manifest-path"])
    manifest_output = capsys.readouterr()
    assert exit_code == 0
    printed_manifest = Path(manifest_output.out.strip())
    assert printed_manifest.parts[-2:] == ("validation", "p5_m5_s5_release_manifest.json")

    exit_code = p5_m5_s5_main(["--print-layout-path"])
    layout_output = capsys.readouterr()
    assert exit_code == 0
    printed_layout = Path(layout_output.out.strip())
    assert printed_layout.parts[-2:] == ("validation", "p5_m5_s5_bundle_layout.json")

    exit_code = p5_m5_s5_main(["--print-handoff-path"])
    handoff_output = capsys.readouterr()
    assert exit_code == 0
    printed_handoff = Path(handoff_output.out.strip())
    assert printed_handoff.parts[-2:] == ("validation", "p5_m5_s5_handoff_contract.json")

    exit_code = p5_m5_s5_main(["--print-completeness-path"])
    completeness_output = capsys.readouterr()
    assert exit_code == 0
    printed_completeness = Path(completeness_output.out.strip())
    assert printed_completeness.parts[-2:] == (
        "validation",
        "p5_m5_s5_completeness_checks.json",
    )

    exit_code = p5_m5_s5_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s5_release_bundle_inventory_matches_helper -> ok" in check_output.out
    assert "m5_s5_handoff_prohibits_source_code_dependency -> ok" in check_output.out

    exit_code = p5_m5_s5_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "release_bundle_inventory" in rendered
    assert "release_manifest" in rendered
    assert "bundle_layout" in rendered
    assert "handoff_contract" in rendered
    assert "completeness_checks" in rendered

    exit_code = p5_m5_s5_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "source-code reading is explicitly prohibited" in summary_output.out
    assert "not S3 stability repetition" in summary_output.out
