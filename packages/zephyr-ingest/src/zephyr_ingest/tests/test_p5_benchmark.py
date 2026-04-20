from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_benchmark_report import main as p5_benchmark_main

from zephyr_ingest.testing.p5_benchmark import (
    P5_BENCHMARK_REGISTRY_PATH,
    P5_BENCHMARK_REPORT_PATH,
    P5_BENCHMARK_REQUIRED_CASES,
    benchmark_field_maturity_map,
    build_p5_benchmark_resource_name,
    build_p5_benchmark_scenario_registry,
    format_p5_benchmark_result,
    get_p5_benchmark_case,
    record_p5_benchmark_result,
    summarize_p5_benchmark_results,
    validate_p5_benchmark_artifacts,
    validate_p5_benchmark_case_contract,
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
    assert artifact["field_maturity"] == built["field_maturity"]


@pytest.mark.auth_contract
def test_p5_benchmark_case_carrier_contracts_are_explicit() -> None:
    registry = build_p5_benchmark_scenario_registry()
    cases = cast(list[dict[str, object]], registry["first_wave_cases"])

    for case in cases:
        carrier = _as_dict(case["carrier"])
        assert carrier["kind"] == "pytest"
        assert carrier["command"]
        assert carrier["pytest_target"]
        assert carrier["pytest_selector"]
        assert carrier["expected_artifacts"]
        assert carrier["isolation_strategy"]

    opensearch = next(case for case in cases if case["case_id"] == "opensearch_heavier_delivery")
    opensearch_carrier = _as_dict(opensearch["carrier"])
    assert opensearch_carrier["isolation_strategy"] == "unique_opensearch_index_per_run"
    assert opensearch_carrier["env_overrides"] == [
        "ZEPHYR_P45_BENCHMARK_OPENSEARCH_INDEX={unique_index_per_run}"
    ]


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
def test_p5_benchmark_field_maturity_classifies_usable_partial_and_not_ready() -> None:
    maturity = benchmark_field_maturity_map()

    assert maturity["wall_clock_ms"]["maturity"] == "usable_now"
    assert maturity["measurement_unit"]["headline_ready"] is True
    assert maturity["throughput_units_per_second"]["maturity"] == "partial"
    assert maturity["resource_usage_snapshot"]["maturity"] == "partial"
    assert maturity["prometheus_metrics_surface"]["maturity"] == "not_ready_for_headline"
    assert maturity["requeue_count"]["headline_ready"] is False


@pytest.mark.auth_contract
def test_p5_benchmark_report_declares_s4_guardrails_and_no_single_headline() -> None:
    report_text = P5_BENCHMARK_REPORT_PATH.read_text(encoding="utf-8")

    assert "bounded local same-host/shared-storage" in report_text
    assert "These cases must not be collapsed into one headline result." in report_text
    assert "unique index per run" in report_text
    assert "Canonical result ingestion" in report_text
    assert "2-run usable baseline" in report_text
    assert "targeted later `r3`" in report_text


@pytest.mark.auth_contract
def test_p5_benchmark_resource_name_is_unique_and_opensearch_safe() -> None:
    first = build_p5_benchmark_resource_name(
        case_id="opensearch_heavier_delivery",
        run_id="r1-20260420T010101",
        resource_kind="index",
    )
    second = build_p5_benchmark_resource_name(
        case_id="opensearch_heavier_delivery",
        run_id="r2-20260420T010102",
        resource_kind="index",
    )

    assert first != second
    assert first.startswith("zephyr-p5-m4-opensearch-heavier-delivery")
    assert first == first.lower()
    assert "_" not in first
    assert first.endswith("-index")


@pytest.mark.auth_contract
def test_p5_benchmark_contract_validation_is_not_a_fake_result() -> None:
    result = validate_p5_benchmark_case_contract("filesystem_lightweight_delivery")
    execution_surface = _as_dict(result["execution_surface"])
    guardrails = _as_dict(result["layer_guardrails"])

    assert result["execution_mode"] == "case_contract_validation"
    assert execution_surface["is_benchmark_result"] is False
    assert execution_surface["real_carrier_required"] is True
    assert guardrails["single_headline_across_layers_allowed"] is False


@pytest.mark.auth_contract
def test_p5_benchmark_record_result_ingests_real_carrier_observations() -> None:
    result = record_p5_benchmark_result(
        case_id="filesystem_lightweight_delivery",
        observed={
            "carrier_run_id": "filesystem-r1",
            "run_ordinal": 1,
            "wall_clock_ms": 100.0,
            "process_cpu_ms": 12.5,
            "unit_count": 4,
            "delivery_success_count": 4,
        },
    )
    measured = _as_dict(result["measured"])
    sli_candidates = _as_dict(result["sli_candidates"])
    validity = _as_dict(result["validity"])
    baseline_policy = _as_dict(result["baseline_run_policy"])
    guardrails = _as_dict(result["layer_guardrails"])

    assert result["execution_mode"] == "real_carrier_result_ingested"
    assert measured["measurement_unit"] == "delivery_payload"
    assert measured["throughput_units_per_second"] == 40.0
    assert sli_candidates["delivery_success_count"] == 4
    assert validity["status"] == "real_result_ingested"
    assert baseline_policy["current_required_runs"] == 2
    assert baseline_policy["global_r3_required"] is False
    assert guardrails["single_headline_across_layers_allowed"] is False


@pytest.mark.auth_contract
def test_p5_benchmark_summary_blocks_mixed_layer_headline() -> None:
    filesystem_result = record_p5_benchmark_result(
        case_id="filesystem_lightweight_delivery",
        observed={"wall_clock_ms": 50.0, "unit_count": 1},
    )
    recovery_result = record_p5_benchmark_result(
        case_id="replay_after_failed_delivery_recovery_overhead",
        observed={"wall_clock_ms": 75.0, "unit_count": 1, "replay_attempt_count": 1},
    )

    summary = summarize_p5_benchmark_results([filesystem_result, recovery_result])
    guardrails = _as_dict(summary["guardrails"])

    assert guardrails["mixed_layers"] is True
    assert guardrails["single_headline_allowed"] is False
    assert (
        guardrails["headline_reason"] == "mixed benchmark layers cannot produce one headline result"
    )


@pytest.mark.auth_contract
def test_p5_benchmark_artifact_validation_reports_green() -> None:
    checks = validate_p5_benchmark_artifacts()

    assert all(check.ok for check in checks), checks


@pytest.mark.auth_contract
def test_p5_benchmark_cli_lists_cases_validates_contract_and_records_result(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    list_exit = p5_benchmark_main(["--list-cases"])
    list_output = capsys.readouterr()
    assert list_exit == 0
    assert "filesystem_lightweight_delivery" in list_output.out

    validate_exit = p5_benchmark_main(
        ["--validate-case", "filesystem_lightweight_delivery", "--json"]
    )
    validate_output = capsys.readouterr()
    contract_payload = cast(dict[str, object], json.loads(validate_output.out))
    assert validate_exit == 0
    assert contract_payload["execution_mode"] == "case_contract_validation"

    observed_path = tmp_path / "observed.json"
    observed_path.write_text(
        json.dumps(
            {
                "carrier_run_id": "filesystem-r1",
                "run_ordinal": 1,
                "wall_clock_ms": 100.0,
                "unit_count": 2,
                "delivery_success_count": 2,
            }
        ),
        encoding="utf-8",
    )
    record_exit = p5_benchmark_main(
        [
            "--record-result",
            "filesystem_lightweight_delivery",
            "--result-json",
            str(observed_path),
            "--json",
        ]
    )
    record_output = capsys.readouterr()
    record_payload = cast(dict[str, object], json.loads(record_output.out))

    assert record_exit == 0
    assert record_payload["execution_mode"] == "real_carrier_result_ingested"


@pytest.mark.auth_contract
def test_p5_benchmark_cli_check_artifacts(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = p5_benchmark_main(["--check-artifacts"])
    output = capsys.readouterr()

    assert exit_code == 0
    assert "registry_cases_match_helper -> ok" in output.out
    assert "registry_declares_real_carriers -> ok" in output.out


@pytest.mark.auth_contract
def test_get_p5_benchmark_case_rejects_unknown_case() -> None:
    with pytest.raises(ValueError, match="Unknown P5-M4 benchmark case"):
        get_p5_benchmark_case("unknown")


@pytest.mark.auth_contract
def test_format_p5_benchmark_result_is_human_readable() -> None:
    result = record_p5_benchmark_result(
        case_id="filesystem_lightweight_delivery",
        observed={"wall_clock_ms": 100.0, "unit_count": 1},
    )
    rendered = format_p5_benchmark_result(result)

    assert "P5-M4 benchmark result:" in rendered
    assert "filesystem_lightweight_delivery" in rendered
    assert "anti-mix-layer" in rendered
    assert "2-run usable baseline" in rendered
