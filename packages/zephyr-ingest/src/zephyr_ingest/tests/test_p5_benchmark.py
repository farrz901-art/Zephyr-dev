from __future__ import annotations

import json
from typing import cast

import pytest
from tools.p5_benchmark_report import main as p5_benchmark_main

from zephyr_ingest.testing.p5_benchmark import (
    P5_BENCHMARK_BOUNDED_CAVEATS,
    P5_BENCHMARK_REGISTRY_PATH,
    P5_BENCHMARK_REPORT_PATH,
    P5_BENCHMARK_REQUIRED_CASES,
    build_p5_benchmark_scenario_registry,
    format_p5_benchmark_result,
    get_p5_benchmark_case,
    run_p5_benchmark_case,
    validate_p5_benchmark_artifacts,
)


def _as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


@pytest.mark.auth_contract
def test_p5_benchmark_registry_artifact_matches_helper_cases() -> None:
    artifact = cast(
        dict[str, object],
        json.loads(P5_BENCHMARK_REGISTRY_PATH.read_text(encoding="utf-8")),
    )
    built = build_p5_benchmark_scenario_registry()
    artifact_cases = cast(list[dict[str, object]], artifact["first_wave_cases"])
    built_cases = cast(list[dict[str, object]], built["first_wave_cases"])

    assert artifact["phase"] == built["phase"]
    assert artifact["runtime_boundary"] == "bounded local same-host/shared-storage"
    assert {case["case_id"] for case in artifact_cases} == set(P5_BENCHMARK_REQUIRED_CASES)
    assert {case["case_id"] for case in artifact_cases} == {case["case_id"] for case in built_cases}
    assert artifact["sli_candidate_fields"] == built["sli_candidate_fields"]


@pytest.mark.auth_contract
def test_p5_benchmark_registry_keeps_benchmark_layers_separate() -> None:
    registry = build_p5_benchmark_scenario_registry()
    cases = cast(list[dict[str, object]], registry["first_wave_cases"])
    layers_by_case = {str(case["case_id"]): str(case["layer"]) for case in cases}

    assert layers_by_case["uns_reacquisition_replay_representative"] == (
        "source_flow_representative"
    )
    assert layers_by_case["it_resume_replay_representative"] == "source_flow_representative"
    assert layers_by_case["filesystem_lightweight_delivery"] == "destination_representative"
    assert layers_by_case["opensearch_heavier_delivery"] == "destination_representative"
    assert layers_by_case["replay_after_failed_delivery_recovery_overhead"] == "recovery_overhead"
    for case in cases:
        invalid = cast(list[str], case["invalid_comparisons"])
        assert invalid


@pytest.mark.auth_contract
def test_p5_benchmark_report_declares_units_caveats_and_no_single_headline() -> None:
    report_text = P5_BENCHMARK_REPORT_PATH.read_text(encoding="utf-8")

    assert "bounded local same-host/shared-storage" in report_text
    assert "These cases must not be collapsed into one headline result." in report_text
    assert "`delivery_payload`" in report_text
    assert "`inspect_decide_act_verify_workflow`" in report_text
    assert "not a full retained-surface validation matrix" in report_text


@pytest.mark.auth_contract
def test_p5_benchmark_harness_result_has_explicit_unit_and_sli_candidates() -> None:
    result = run_p5_benchmark_case("filesystem_lightweight_delivery")
    case = _as_dict(result["case"])
    measured = _as_dict(result["measured"])
    validity = _as_dict(result["validity"])
    sli_candidates = _as_dict(result["sli_candidates"])
    redaction = _as_dict(result["redaction"])

    assert case["case_id"] == "filesystem_lightweight_delivery"
    assert measured["measurement_unit"] == "delivery_payload"
    assert measured["unit_count"] == 0
    assert sli_candidates["throughput_units_per_second"] is None
    assert validity["status"] == "harness_validation_only"
    assert validity["not_full_benchmark_execution"] is True
    assert P5_BENCHMARK_BOUNDED_CAVEATS[0] in cast(list[str], validity["caveats"])
    assert redaction["contains_secret_values"] is False
    assert "token-1" not in json.dumps(result)


@pytest.mark.auth_contract
def test_p5_benchmark_recovery_case_frames_workflow_not_action_only() -> None:
    result = run_p5_benchmark_case("replay_after_failed_delivery_recovery_overhead")
    case = _as_dict(result["case"])
    recovery_summary = _as_dict(result["recovery_summary"])

    assert case["measurement_unit"] == "inspect_decide_act_verify_workflow"
    assert recovery_summary["recovery_overhead_scope"] == "inspect_decide_act_verify_workflow"
    assert "action-only" in " ".join(cast(list[str], case["partial_surfaces"]))


@pytest.mark.auth_contract
def test_p5_benchmark_artifact_validation_reports_green() -> None:
    checks = validate_p5_benchmark_artifacts()

    assert all(check.ok for check in checks), checks


@pytest.mark.auth_contract
def test_p5_benchmark_cli_lists_cases_and_runs_json_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    list_exit = p5_benchmark_main(["--list-cases"])
    list_output = capsys.readouterr()
    assert list_exit == 0
    assert "filesystem_lightweight_delivery" in list_output.out

    run_exit = p5_benchmark_main(["--case", "filesystem_lightweight_delivery", "--json"])
    run_output = capsys.readouterr()
    payload = cast(dict[str, object], json.loads(run_output.out))
    case = _as_dict(payload["case"])

    assert run_exit == 0
    assert case["case_id"] == "filesystem_lightweight_delivery"
    assert payload["execution_mode"] == "harness_dry_run"


@pytest.mark.auth_contract
def test_p5_benchmark_cli_check_artifacts(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = p5_benchmark_main(["--check-artifacts"])
    output = capsys.readouterr()

    assert exit_code == 0
    assert "registry_cases_match_helper -> ok" in output.out


@pytest.mark.auth_contract
def test_get_p5_benchmark_case_rejects_unknown_case() -> None:
    with pytest.raises(ValueError, match="Unknown P5-M4 benchmark case"):
        get_p5_benchmark_case("unknown")


@pytest.mark.auth_contract
def test_format_p5_benchmark_result_is_human_readable() -> None:
    result = run_p5_benchmark_case("filesystem_lightweight_delivery")
    rendered = format_p5_benchmark_result(result)

    assert "P5-M4 benchmark harness result:" in rendered
    assert "filesystem_lightweight_delivery" in rendered
    assert "harness validation output" in rendered
