from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s7_report import main as p5_m5_s7_main

from zephyr_ingest.testing.p5_m5_s7 import (
    P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH,
    P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH,
    P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH,
    P5_M5_S7_WORKFLOW_SCENARIOS_PATH,
    P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH,
    P5_M5_S7_WORKFLOW_VIEWS_PATH,
    build_p5_m5_s7_workflow_evidence_map,
    build_p5_m5_s7_workflow_failure_matrix,
    build_p5_m5_s7_workflow_scenarios,
    build_p5_m5_s7_workflow_success_criteria,
    build_p5_m5_s7_workflow_views,
    format_p5_m5_s7_results,
    validate_p5_m5_s7_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s7_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S7_WORKFLOW_SCENARIOS_PATH) == (
        build_p5_m5_s7_workflow_scenarios()
    )
    assert _load_json_object(P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH) == (
        build_p5_m5_s7_workflow_evidence_map()
    )
    assert _load_json_object(P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH) == (
        build_p5_m5_s7_workflow_success_criteria()
    )
    assert _load_json_object(P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH) == (
        build_p5_m5_s7_workflow_failure_matrix()
    )
    assert _load_json_object(P5_M5_S7_WORKFLOW_VIEWS_PATH) == build_p5_m5_s7_workflow_views()
    assert P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s7_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s7_artifacts()
    assert all(check.ok for check in checks), format_p5_m5_s7_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s7_workflow_scenarios_stay_bounded_and_exact() -> None:
    scenarios_obj = _load_json_object(P5_M5_S7_WORKFLOW_SCENARIOS_PATH)
    scenarios = cast(list[dict[str, object]], scenarios_obj["scenarios"])

    assert scenarios_obj["scenario_count"] == 4
    assert [cast(str, scenario["scenario_id"]) for scenario in scenarios] == [
        "s7_uns_happy_01",
        "s7_it_happy_01",
        "s7_failure_01",
        "s7_fanout_01",
    ]
    assert scenarios_obj["intentional_file_level_carriers"] == []
    for scenario in scenarios:
        for carrier in cast(list[dict[str, object]], scenario["carriers"]):
            assert carrier["carrier_granularity"] == "exact_test_id"


@pytest.mark.auth_contract
def test_p5_m5_s7_evidence_and_views_do_not_require_source_code() -> None:
    evidence = _load_json_object(P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH)
    views = _load_json_object(P5_M5_S7_WORKFLOW_VIEWS_PATH)
    scenarios = cast(list[dict[str, object]], evidence["scenarios"])

    assert evidence["scenario_count"] == 4
    assert views["source_code_reading_required"] is False
    for scenario in scenarios:
        chain = cast(dict[str, object], scenario["evidence_chain"])
        assert chain["missing_link"] == "none"
        for key in (
            "source_evidence",
            "task_run_evidence",
            "delivery_output_evidence",
            "usage_evidence",
            "provenance_evidence",
            "governance_recovery_evidence",
        ):
            assert cast(list[object], chain[key])


@pytest.mark.auth_contract
def test_p5_m5_s7_success_and_failure_models_stay_explicit() -> None:
    success = _load_json_object(P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH)
    failure = _load_json_object(P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH)
    platform_side = cast(dict[str, object], success["platform_side_corroboration"])

    assert success["scenario_count"] == 4
    assert failure["scenario_count"] == 4
    assert success["source_code_reading_required"] is False
    assert platform_side["role"] == "secondary_not_primary"
    failure_scenarios = cast(list[dict[str, object]], failure["scenarios"])
    fanout = next(
        scenario for scenario in failure_scenarios if scenario["scenario_id"] == "s7_fanout_01"
    )
    assert fanout["status_classification"] == "inspect_only"


@pytest.mark.auth_contract
def test_p5_m5_s7_report_preserves_scope_and_fanout_boundaries() -> None:
    report = P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH.read_text(encoding="utf-8")

    assert "S7 freezes real workflow chain truth, not load testing." in report
    assert (
        "S7 does not repeat S3 stability work, S4 cold-operator proof, "
        "S5 release bundle hardening, or S6 operational envelope profiling." in report
    )
    assert (
        "S7 does not start installer implementation, deployment packaging, or cloud validation."
        in report
    )
    assert (
        "Exact test ids are preferred; any remaining file-level carriers are intentional and "
        "explicitly bounded." in report
    )
    assert "Workflow truth is consumable without source-code reading as a requirement." in report
    assert "fanout remains composition/orchestration, not a sink connector." in report


@pytest.mark.auth_contract
def test_p5_m5_s7_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s7_main(["--print-scenarios-path"])
    scenarios_output = capsys.readouterr()
    assert exit_code == 0
    printed_scenarios = Path(scenarios_output.out.strip())
    assert printed_scenarios.parts[-2:] == (
        "validation",
        "p5_m5_s7_workflow_scenarios.json",
    )

    exit_code = p5_m5_s7_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M5_S7_REAL_WORKFLOWS.md")

    exit_code = p5_m5_s7_main(["--print-evidence-map-path"])
    evidence_output = capsys.readouterr()
    assert exit_code == 0
    printed_evidence = Path(evidence_output.out.strip())
    assert printed_evidence.parts[-2:] == (
        "validation",
        "p5_m5_s7_workflow_evidence_map.json",
    )

    exit_code = p5_m5_s7_main(["--print-success-criteria-path"])
    success_output = capsys.readouterr()
    assert exit_code == 0
    printed_success = Path(success_output.out.strip())
    assert printed_success.parts[-2:] == (
        "validation",
        "p5_m5_s7_workflow_success_criteria.json",
    )

    exit_code = p5_m5_s7_main(["--print-failure-matrix-path"])
    failure_output = capsys.readouterr()
    assert exit_code == 0
    printed_failure = Path(failure_output.out.strip())
    assert printed_failure.parts[-2:] == (
        "validation",
        "p5_m5_s7_workflow_failure_matrix.json",
    )

    exit_code = p5_m5_s7_main(["--print-views-path"])
    views_output = capsys.readouterr()
    assert exit_code == 0
    printed_views = Path(views_output.out.strip())
    assert printed_views.parts[-2:] == ("validation", "p5_m5_s7_workflow_views.json")

    exit_code = p5_m5_s7_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s7_workflow_scenarios_match_helper -> ok" in check_output.out
    assert "m5_s7_exact_carriers_or_explicit_caveats -> ok" in check_output.out

    exit_code = p5_m5_s7_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "workflow_scenarios" in rendered
    assert "workflow_evidence_map" in rendered
    assert "workflow_success_criteria" in rendered
    assert "workflow_failure_matrix" in rendered
    assert "workflow_views" in rendered

    exit_code = p5_m5_s7_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "all carriers remain exact" in summary_output.out
    assert "platform-side corroboration is secondary" in summary_output.out
