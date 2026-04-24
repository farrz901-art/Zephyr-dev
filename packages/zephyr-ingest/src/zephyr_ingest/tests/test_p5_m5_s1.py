from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s1_report import main as p5_m5_s1_main

from zephyr_ingest.testing.p5_m5_s1 import (
    P5_M5_S1_BASELINE_MATRIX_REPORT_PATH,
    P5_M5_S1_CLEANUP_RULES_PATH,
    P5_M5_S1_EXACT_CARRIERS_PATH,
    P5_M5_S1_EXECUTION_PLAN_PATH,
    build_p5_m5_s1_cleanup_rules,
    build_p5_m5_s1_exact_carriers,
    build_p5_m5_s1_execution_plan,
    format_p5_m5_s1_results,
    validate_p5_m5_s1_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s1_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S1_EXECUTION_PLAN_PATH) == build_p5_m5_s1_execution_plan()
    assert _load_json_object(P5_M5_S1_EXACT_CARRIERS_PATH) == build_p5_m5_s1_exact_carriers()
    assert _load_json_object(P5_M5_S1_CLEANUP_RULES_PATH) == build_p5_m5_s1_cleanup_rules()
    assert P5_M5_S1_BASELINE_MATRIX_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s1_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s1_artifacts()

    assert all(check.ok for check in checks), format_p5_m5_s1_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s1_exact_carriers_freeze_all_retained_sources_destinations_and_fanout() -> None:
    carriers = _load_json_object(P5_M5_S1_EXACT_CARRIERS_PATH)
    retained_sources = cast(dict[str, object], carriers["retained_source_carriers"])
    uns_sources = cast(list[dict[str, object]], retained_sources["uns_stream"])
    it_sources = cast(list[dict[str, object]], retained_sources["it_stream"])
    destinations = cast(list[dict[str, object]], carriers["retained_destination_success_carriers"])
    fanout = cast(dict[str, object], carriers["fanout"])

    assert len(uns_sources) == 5
    assert len(it_sources) == 5
    assert len(destinations) == 10
    assert all(
        entry["success_carrier"] == entry["abnormal_carrier"] for entry in uns_sources + it_sources
    )
    assert fanout["surface_type"] == "destination_composition_orchestration"
    assert fanout["single_sink_connector"] is False
    assert fanout["success_carrier"] == fanout["partial_or_failure_carrier"]


@pytest.mark.auth_contract
def test_p5_m5_s1_execution_plan_stays_baseline_sized_and_light_visibility() -> None:
    plan = _load_json_object(P5_M5_S1_EXECUTION_PLAN_PATH)
    size = cast(dict[str, object], plan["execution_size"])
    scope = cast(dict[str, object], plan["scope"])
    deferred = cast(dict[str, object], plan["deferred_boundaries"])
    coverage = cast(dict[str, object], plan["covered_chains"])

    assert plan["status"] == "baseline_layer_only"
    assert size["baseline_execution_units_total"] == 32
    assert size["light_visibility_support_units"] == 2
    assert scope["governance_visibility"] == "light"
    assert scope["deep_validation"] == "deferred_to_s2"
    assert scope["repeatability_soak_cleanup_execution"] == "deferred_to_s3"
    light_visibility = cast(dict[str, object], coverage["light_governance_visibility"])
    assert "full replay/requeue/verify walkthrough per retained source" in cast(
        str, light_visibility["not"]
    )
    assert cast(list[object], deferred["s2_not_run_here"])
    assert cast(list[object], deferred["s3_not_run_here"])


@pytest.mark.auth_contract
def test_p5_m5_s1_cleanup_rules_are_machine_readable_and_non_residual() -> None:
    cleanup = _load_json_object(P5_M5_S1_CLEANUP_RULES_PATH)
    rules = cast(dict[str, object], cleanup["rules"])
    not_allowed = cast(list[object], cleanup["not_allowed"])

    for rule_name in (
        "filesystem_outputs",
        "opensearch_targets",
        "sqlite_queue",
        "spool_queue",
        "replay_db",
        "governance_receipts",
        "fanout_outputs",
        "saas_and_service_live",
    ):
        assert rule_name in rules
        assert cast(dict[str, object], rules[rule_name])["rule"]

    assert "reuse dirty residual state" in not_allowed
    assert "implicit remote cleanup assumptions" in not_allowed


@pytest.mark.auth_contract
def test_p5_m5_s1_report_preserves_scope_and_fanout_truth() -> None:
    report = P5_M5_S1_BASELINE_MATRIX_REPORT_PATH.read_text(encoding="utf-8")

    assert "S1 runs layer 1 baseline coverage only" in report
    assert "fanout is composition/orchestration, not a sink connector" in report
    assert "light governance visibility is preserved" in report
    assert "S2 representative deep validation is deferred" in report
    assert "S3 repeatability/soak/cleanup execution is deferred" in report


@pytest.mark.auth_contract
def test_p5_m5_s1_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s1_main(["--print-plan-path"])
    plan_output = capsys.readouterr()
    assert exit_code == 0
    printed_plan = Path(plan_output.out.strip())
    assert printed_plan.parts[-2:] == ("validation", "p5_m5_s1_execution_plan.json")

    exit_code = p5_m5_s1_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M5_S1_BASELINE_MATRIX.md")

    exit_code = p5_m5_s1_main(["--print-carriers-path"])
    carriers_output = capsys.readouterr()
    assert exit_code == 0
    printed_carriers = Path(carriers_output.out.strip())
    assert printed_carriers.parts[-2:] == ("validation", "p5_m5_s1_exact_carriers.json")

    exit_code = p5_m5_s1_main(["--print-cleanup-path"])
    cleanup_output = capsys.readouterr()
    assert exit_code == 0
    printed_cleanup = Path(cleanup_output.out.strip())
    assert printed_cleanup.parts[-2:] == ("validation", "p5_m5_s1_cleanup_rules.json")

    exit_code = p5_m5_s1_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s1_execution_plan_matches_helper -> ok" in check_output.out
    assert "s1_governance_visibility_stays_light -> ok" in check_output.out

    exit_code = p5_m5_s1_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "execution_plan" in rendered
    assert "exact_carriers" in rendered
    assert "cleanup_rules" in rendered

    exit_code = p5_m5_s1_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "layer 1 baseline coverage only" in summary_output.out
    assert "fanout is composition/orchestration" in summary_output.out
