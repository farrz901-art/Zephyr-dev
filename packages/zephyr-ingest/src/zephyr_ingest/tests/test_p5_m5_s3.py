from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s3_report import main as p5_m5_s3_main

from zephyr_ingest.testing.p5_m5_s3 import (
    P5_M5_S3_CLEANUP_REPETITION_RULES_PATH,
    P5_M5_S3_EXECUTION_PLAN_PATH,
    P5_M5_S3_REPEATABILITY_CARRIERS_PATH,
    P5_M5_S3_STABILITY_MATRIX_REPORT_PATH,
    P5_M5_S3_STABILITY_SIGNALS_PATH,
    build_p5_m5_s3_cleanup_repetition_rules,
    build_p5_m5_s3_execution_plan,
    build_p5_m5_s3_repeatability_carriers,
    build_p5_m5_s3_stability_signals,
    format_p5_m5_s3_results,
    validate_p5_m5_s3_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s3_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S3_EXECUTION_PLAN_PATH) == build_p5_m5_s3_execution_plan()
    assert _load_json_object(P5_M5_S3_REPEATABILITY_CARRIERS_PATH) == (
        build_p5_m5_s3_repeatability_carriers()
    )
    assert _load_json_object(P5_M5_S3_CLEANUP_REPETITION_RULES_PATH) == (
        build_p5_m5_s3_cleanup_repetition_rules()
    )
    assert _load_json_object(P5_M5_S3_STABILITY_SIGNALS_PATH) == (
        build_p5_m5_s3_stability_signals()
    )
    assert P5_M5_S3_STABILITY_MATRIX_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s3_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s3_artifacts()

    assert all(check.ok for check in checks), format_p5_m5_s3_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s3_repeatability_carriers_freeze_four_repeated_closures() -> None:
    repeatability = _load_json_object(P5_M5_S3_REPEATABILITY_CARRIERS_PATH)
    families = cast(list[dict[str, object]], repeatability["repeatability_families"])

    assert len(families) == 4
    family_names = [cast(str, family["family_name"]) for family in families]
    assert family_names == [
        "delivery_replay_verify",
        "queue_governance",
        "source_deep",
        "fanout_composition",
    ]
    for family in families:
        rounds = cast(dict[str, object], family["rounds"])
        assert "run_1" in rounds
        assert "run_2" in rounds
        assert "optional_run_3" in rounds
        assert cast(dict[str, object], rounds["optional_run_3"])["required"] is False


@pytest.mark.auth_contract
def test_p5_m5_s3_execution_plan_stays_stability_sized_and_non_load_test() -> None:
    plan = _load_json_object(P5_M5_S3_EXECUTION_PLAN_PATH)
    size = cast(dict[str, object], plan["execution_size"])
    rounds = cast(dict[str, object], plan["round_counts"])
    fanout_truth = cast(dict[str, object], plan["fanout_truth"])

    assert plan["status"] == "stability_layer_only"
    assert plan["strategy"] == "repeat_representative_closures_not_new_anchor_discovery"
    assert size["stability_sized_total_rough"] == 20
    assert rounds["mandatory_rounds_per_family"] == 2
    assert rounds["optional_rounds_per_family"] == 1
    assert fanout_truth["single_sink_connector"] is False
    assert "distributed runtime" in cast(list[object], plan["out_of_scope"])


@pytest.mark.auth_contract
def test_p5_m5_s3_cleanup_repetition_rules_are_machine_readable() -> None:
    cleanup = _load_json_object(P5_M5_S3_CLEANUP_REPETITION_RULES_PATH)
    anchor_rules = cast(dict[str, object], cleanup["anchor_rules"])

    for anchor in (
        "webhook",
        "kafka",
        "opensearch",
        "spool_queue",
        "sqlite_queue",
        "replay_db",
        "governance_receipts",
        "google_drive_document_v1",
        "confluence_document_v1",
        "kafka_partition_offset_v1",
        "fanout",
        "filesystem",
    ):
        assert anchor in anchor_rules
        rule = cast(dict[str, object], anchor_rules[anchor])
        assert "run_carrier_bundle" in rule
        assert "cleanup_step" in rule
        assert "rerun_carrier_bundle" in rule
        assert "compare_artifacts" in rule
        assert "dirty_state_pollution" in rule


@pytest.mark.auth_contract
def test_p5_m5_s3_stability_signals_are_unified_and_explicit() -> None:
    signals_obj = _load_json_object(P5_M5_S3_STABILITY_SIGNALS_PATH)
    signals = cast(dict[str, object], signals_obj["signals"])
    false_stability_controls = cast(list[object], signals_obj["false_stability_controls"])

    assert set(signals) == {
        "artifact_path_stability",
        "usage_linkage_stability",
        "provenance_linkage_stability",
        "governance_receipt_stability",
        "queue_state_stability",
        "source_contract_id_stability",
        "fanout_aggregate_shared_summary_stability",
        "helper_report_artifact_check_stability",
    }
    assert "fresh replay.db per rerun" in false_stability_controls
    assert "fresh spool/sqlite roots per rerun" in false_stability_controls


@pytest.mark.auth_contract
def test_p5_m5_s3_report_preserves_scope_false_stability_control_and_fanout_truth() -> None:
    report = P5_M5_S3_STABILITY_MATRIX_REPORT_PATH.read_text(encoding="utf-8")

    assert "S3 stability-validates repeated representative closures only" in report
    assert "cleanup then rerun is explicit" in report
    assert "bounded same-host soak remains non-load-test" in report
    assert "fanout remains composition/orchestration, not a sink connector" in report
    assert "false-stability risk is controlled through fresh scope per rerun" in report


@pytest.mark.auth_contract
def test_p5_m5_s3_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s3_main(["--print-plan-path"])
    plan_output = capsys.readouterr()
    assert exit_code == 0
    printed_plan = Path(plan_output.out.strip())
    assert printed_plan.parts[-2:] == ("validation", "p5_m5_s3_execution_plan.json")

    exit_code = p5_m5_s3_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M5_S3_STABILITY_MATRIX.md")

    exit_code = p5_m5_s3_main(["--print-repeatability-path"])
    repeatability_output = capsys.readouterr()
    assert exit_code == 0
    printed_repeatability = Path(repeatability_output.out.strip())
    assert printed_repeatability.parts[-2:] == (
        "validation",
        "p5_m5_s3_repeatability_carriers.json",
    )

    exit_code = p5_m5_s3_main(["--print-cleanup-path"])
    cleanup_output = capsys.readouterr()
    assert exit_code == 0
    printed_cleanup = Path(cleanup_output.out.strip())
    assert printed_cleanup.parts[-2:] == (
        "validation",
        "p5_m5_s3_cleanup_repetition_rules.json",
    )

    exit_code = p5_m5_s3_main(["--print-signals-path"])
    signals_output = capsys.readouterr()
    assert exit_code == 0
    printed_signals = Path(signals_output.out.strip())
    assert printed_signals.parts[-2:] == ("validation", "p5_m5_s3_stability_signals.json")

    exit_code = p5_m5_s3_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s3_execution_plan_matches_helper -> ok" in check_output.out
    assert "s3_stability_signals_are_unified" in check_output.out

    exit_code = p5_m5_s3_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "execution_plan" in rendered
    assert "repeatability_carriers" in rendered
    assert "cleanup_repetition_rules" in rendered
    assert "stability_signals" in rendered

    exit_code = p5_m5_s3_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "repeated representative closures only" in summary_output.out
    assert "fanout remains composition/orchestration" in summary_output.out
