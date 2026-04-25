from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s6_report import main as p5_m5_s6_main

from zephyr_ingest.testing.p5_m5_s6 import (
    P5_M5_S6_FAMILY_EXPECTATIONS_PATH,
    P5_M5_S6_LOAD_CLASSES_PATH,
    P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH,
    P5_M5_S6_OPERATIONAL_PROFILES_PATH,
    P5_M5_S6_OPERATIONAL_SIGNALS_PATH,
    P5_M5_S6_SUCCESS_CRITERIA_PATH,
    build_p5_m5_s6_family_expectations,
    build_p5_m5_s6_load_classes,
    build_p5_m5_s6_operational_profiles,
    build_p5_m5_s6_operational_signals,
    build_p5_m5_s6_success_criteria,
    format_p5_m5_s6_results,
    validate_p5_m5_s6_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s6_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S6_OPERATIONAL_PROFILES_PATH) == (
        build_p5_m5_s6_operational_profiles()
    )
    assert _load_json_object(P5_M5_S6_LOAD_CLASSES_PATH) == build_p5_m5_s6_load_classes()
    assert _load_json_object(P5_M5_S6_OPERATIONAL_SIGNALS_PATH) == (
        build_p5_m5_s6_operational_signals()
    )
    assert _load_json_object(P5_M5_S6_FAMILY_EXPECTATIONS_PATH) == (
        build_p5_m5_s6_family_expectations()
    )
    assert _load_json_object(P5_M5_S6_SUCCESS_CRITERIA_PATH) == build_p5_m5_s6_success_criteria()
    assert P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s6_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s6_artifacts()

    assert all(check.ok for check in checks), format_p5_m5_s6_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s6_operational_profiles_cover_all_required_families() -> None:
    profiles_obj = _load_json_object(P5_M5_S6_OPERATIONAL_PROFILES_PATH)
    profiles = cast(list[dict[str, object]], profiles_obj["profiles"])

    assert profiles_obj["profile_count"] == 5
    assert [cast(str, profile["family_id"]) for profile in profiles] == [
        "delivery",
        "queue_governance",
        "source_deep",
        "fanout",
        "filesystem_control",
    ]


@pytest.mark.auth_contract
def test_p5_m5_s6_load_classes_stay_bounded_and_non_cloud() -> None:
    load_classes = _load_json_object(P5_M5_S6_LOAD_CLASSES_PATH)
    classes = cast(list[dict[str, object]], load_classes["classes"])

    assert load_classes["scope_kind"] == "bounded_same_host_only"
    assert [cast(str, item["class_id"]) for item in classes] == ["A", "B", "C"]
    assert "real load testing" in cast(list[object], load_classes["not_claimed"])


@pytest.mark.auth_contract
def test_p5_m5_s6_operational_signals_are_unified_and_bounded() -> None:
    signals = _load_json_object(P5_M5_S6_OPERATIONAL_SIGNALS_PATH)
    signal_list = cast(list[dict[str, object]], signals["signals"])

    assert signals["signal_count"] == 8
    assert [cast(str, signal["signal_id"]) for signal in signal_list] == [
        "elapsed_time_latency_band",
        "artifact_count_growth",
        "queue_state_growth",
        "governance_receipt_growth",
        "usage_record_growth",
        "fanout_child_aggregate_output_growth",
        "helper_report_artifact_check_stability",
        "cleanup_overhead",
    ]


@pytest.mark.auth_contract
def test_p5_m5_s6_family_expectations_and_success_criteria_stay_explicit() -> None:
    expectations = _load_json_object(P5_M5_S6_FAMILY_EXPECTATIONS_PATH)
    success = _load_json_object(P5_M5_S6_SUCCESS_CRITERIA_PATH)
    families = cast(list[dict[str, object]], expectations["families"])
    platform_side = cast(dict[str, object], success["platform_side_corroboration"])

    assert expectations["family_count"] == 5
    assert len(families) == 5
    assert success["helper_report_green_required"] is True
    assert success["no_dirty_state_dependence_required"] is True
    assert success["no_phase_boundary_violation_required"] is True
    assert success["source_code_reading_required"] is False
    assert platform_side["role"] == "secondary_not_primary"


@pytest.mark.auth_contract
def test_p5_m5_s6_report_preserves_scope_boundaries_and_fanout_truth() -> None:
    report = P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH.read_text(encoding="utf-8")

    assert "S6 freezes bounded same-host operational envelope truth, not load testing." in report
    assert (
        "S6 does not repeat S3 stability work, S4 cold-operator proof, or "
        "S5 release bundle hardening." in report
    )
    assert (
        "S6 does not start installer implementation, deployment packaging, or cloud validation."
        in report
    )
    assert (
        "Outer-layer or platform-side checking is secondary corroboration, not primary truth."
        in report
    )
    assert "fanout remains composition/orchestration, not a sink connector." in report


@pytest.mark.auth_contract
def test_p5_m5_s6_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s6_main(["--print-profiles-path"])
    profiles_output = capsys.readouterr()
    assert exit_code == 0
    printed_profiles = Path(profiles_output.out.strip())
    assert printed_profiles.parts[-2:] == (
        "validation",
        "p5_m5_s6_operational_profiles.json",
    )

    exit_code = p5_m5_s6_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M5_S6_OPERATIONAL_ENVELOPE.md")

    exit_code = p5_m5_s6_main(["--print-load-classes-path"])
    load_output = capsys.readouterr()
    assert exit_code == 0
    printed_load = Path(load_output.out.strip())
    assert printed_load.parts[-2:] == ("validation", "p5_m5_s6_load_classes.json")

    exit_code = p5_m5_s6_main(["--print-signals-path"])
    signals_output = capsys.readouterr()
    assert exit_code == 0
    printed_signals = Path(signals_output.out.strip())
    assert printed_signals.parts[-2:] == ("validation", "p5_m5_s6_operational_signals.json")

    exit_code = p5_m5_s6_main(["--print-expectations-path"])
    expectations_output = capsys.readouterr()
    assert exit_code == 0
    printed_expectations = Path(expectations_output.out.strip())
    assert printed_expectations.parts[-2:] == (
        "validation",
        "p5_m5_s6_family_expectations.json",
    )

    exit_code = p5_m5_s6_main(["--print-success-criteria-path"])
    success_output = capsys.readouterr()
    assert exit_code == 0
    printed_success = Path(success_output.out.strip())
    assert printed_success.parts[-2:] == (
        "validation",
        "p5_m5_s6_success_criteria.json",
    )

    exit_code = p5_m5_s6_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s6_operational_profiles_match_helper -> ok" in check_output.out
    assert "m5_s6_platform_side_corroboration_stays_secondary -> ok" in check_output.out

    exit_code = p5_m5_s6_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "operational_profiles" in rendered
    assert "load_classes" in rendered
    assert "operational_signals" in rendered
    assert "family_expectations" in rendered
    assert "success_criteria" in rendered

    exit_code = p5_m5_s6_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "same-host and bounded only" in summary_output.out
    assert "platform-side corroboration is secondary" in summary_output.out
