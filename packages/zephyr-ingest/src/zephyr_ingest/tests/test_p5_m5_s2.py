from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s2_report import main as p5_m5_s2_main

from zephyr_ingest.testing.p5_m5_s2 import (
    P5_M5_S2_CLEANUP_RULES_PATH,
    P5_M5_S2_DEEP_CLOSURES_PATH,
    P5_M5_S2_DEEP_MATRIX_REPORT_PATH,
    P5_M5_S2_EXACT_DEEP_CARRIERS_PATH,
    P5_M5_S2_EXECUTION_PLAN_PATH,
    build_p5_m5_s2_cleanup_rules,
    build_p5_m5_s2_deep_closures,
    build_p5_m5_s2_exact_deep_carriers,
    build_p5_m5_s2_execution_plan,
    format_p5_m5_s2_results,
    validate_p5_m5_s2_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s2_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S2_EXECUTION_PLAN_PATH) == build_p5_m5_s2_execution_plan()
    assert _load_json_object(P5_M5_S2_EXACT_DEEP_CARRIERS_PATH) == (
        build_p5_m5_s2_exact_deep_carriers()
    )
    assert _load_json_object(P5_M5_S2_CLEANUP_RULES_PATH) == build_p5_m5_s2_cleanup_rules()
    assert _load_json_object(P5_M5_S2_DEEP_CLOSURES_PATH) == build_p5_m5_s2_deep_closures()
    assert P5_M5_S2_DEEP_MATRIX_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s2_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s2_artifacts()

    assert all(check.ok for check in checks), format_p5_m5_s2_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s2_exact_deep_carriers_freeze_representative_anchors_and_support() -> None:
    carriers = _load_json_object(P5_M5_S2_EXACT_DEEP_CARRIERS_PATH)
    delivery = cast(dict[str, object], carriers["delivery_failure_replay_verify"])
    queues = cast(dict[str, object], carriers["queue_governance"])
    sources = cast(dict[str, object], carriers["representative_sources"])
    fanout = cast(dict[str, object], carriers["fanout_deep"])
    support = cast(dict[str, object], carriers["shared_support_carriers"])

    assert set(delivery) == {"webhook", "kafka", "opensearch"}
    assert set(queues) == {"spool_queue", "sqlite_queue"}
    assert set(sources) == {
        "google_drive_document_v1",
        "confluence_document_v1",
        "kafka_partition_offset_v1",
    }
    assert fanout["surface_type"] == "destination_composition_orchestration"
    assert fanout["single_sink_connector"] is False
    assert "replay" in support
    assert "verify" in support
    assert "governance" in support
    assert "usage_and_provenance" in support


@pytest.mark.auth_contract
def test_p5_m5_s2_deep_closures_make_grouped_truth_explicit() -> None:
    closures_obj = _load_json_object(P5_M5_S2_DEEP_CLOSURES_PATH)
    closures = cast(dict[str, object], closures_obj["closures"])

    assert set(closures) == {
        "failure_replay_verify_governance_receipt",
        "poison_orphan_requeue_inspect_verify",
        "source_contract_id_usage_governance_provenance",
        "fanout_partial_failure_shared_summary_governance_usage",
    }

    failure = cast(dict[str, object], closures["failure_replay_verify_governance_receipt"])
    queue = cast(dict[str, object], closures["poison_orphan_requeue_inspect_verify"])
    source = cast(dict[str, object], closures["source_contract_id_usage_governance_provenance"])
    fanout = cast(
        dict[str, object], closures["fanout_partial_failure_shared_summary_governance_usage"]
    )

    assert failure["grouped_command_name"] == "delivery_failure_replay_verify_deep"
    assert queue["grouped_command_name"] == "queue_requeue_inspect_verify_deep"
    assert source["grouped_command_name"] == "source_usage_governance_provenance_deep"
    assert fanout["grouped_command_name"] == "fanout_composition_deep"
    assert cast(dict[str, object], queue["support_difference"])["spool_queue"] == (
        "persisted_in_history"
    )
    assert cast(dict[str, object], queue["support_difference"])["sqlite_queue"] == "result_only"


@pytest.mark.auth_contract
def test_p5_m5_s2_execution_plan_stays_representative_not_all_pairs() -> None:
    plan = _load_json_object(P5_M5_S2_EXECUTION_PLAN_PATH)
    size = cast(dict[str, object], plan["execution_size"])
    anchors = cast(dict[str, object], plan["included_representative_anchors"])
    fanout_truth = cast(dict[str, object], plan["fanout_truth"])
    commands = cast(list[dict[str, object]], plan["grouped_execution_commands"])

    assert plan["status"] == "representative_deep_layer_only"
    assert plan["strategy"] == "representative_deep_not_all_pairs"
    assert size["representative_deep_units_total_rough"] == 31
    assert anchors["low_cost_control"] == ["filesystem"]
    assert fanout_truth["single_sink_connector"] is False
    assert [command["name"] for command in commands] == [
        "delivery_failure_replay_verify_deep",
        "queue_requeue_inspect_verify_deep",
        "source_usage_governance_provenance_deep",
        "fanout_composition_deep",
        "s2_artifact_drift",
    ]


@pytest.mark.auth_contract
def test_p5_m5_s2_cleanup_rules_are_machine_readable_and_case_scoped() -> None:
    cleanup = _load_json_object(P5_M5_S2_CLEANUP_RULES_PATH)
    rules = cast(dict[str, object], cleanup["rules"])
    not_allowed = cast(list[object], cleanup["not_allowed"])

    for rule_name in (
        "webhook_deep_runs",
        "kafka_deep_runs",
        "opensearch_deep_runs",
        "spool_queue_deep_runs",
        "sqlite_queue_deep_runs",
        "replay_db",
        "verify_receipts",
        "backfill_outputs",
        "google_drive_and_confluence",
        "kafka_partition_offset",
        "fanout_deep_runs",
        "filesystem_control_anchor",
    ):
        assert rule_name in rules
        assert cast(dict[str, object], rules[rule_name])["rule"]

    assert "reuse dirty residual state" in not_allowed
    assert "shared replay.db across independent deep anchors" in not_allowed


@pytest.mark.auth_contract
def test_p5_m5_s2_report_preserves_representative_scope_and_fanout_truth() -> None:
    report = P5_M5_S2_DEEP_MATRIX_REPORT_PATH.read_text(encoding="utf-8")

    assert "S2 deeply validates representative anchors only" in report
    assert "grouped deep closures are explicit" in report
    assert "fanout remains composition/orchestration, not a sink connector" in report
    assert "S1 was baseline coverage; S2 is representative deep proof" in report
    assert "S3 repeatability/soak/cleanup execution is deferred" in report


@pytest.mark.auth_contract
def test_p5_m5_s2_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s2_main(["--print-plan-path"])
    plan_output = capsys.readouterr()
    assert exit_code == 0
    printed_plan = Path(plan_output.out.strip())
    assert printed_plan.parts[-2:] == ("validation", "p5_m5_s2_execution_plan.json")

    exit_code = p5_m5_s2_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M5_S2_DEEP_MATRIX.md")

    exit_code = p5_m5_s2_main(["--print-carriers-path"])
    carriers_output = capsys.readouterr()
    assert exit_code == 0
    printed_carriers = Path(carriers_output.out.strip())
    assert printed_carriers.parts[-2:] == ("validation", "p5_m5_s2_exact_deep_carriers.json")

    exit_code = p5_m5_s2_main(["--print-cleanup-path"])
    cleanup_output = capsys.readouterr()
    assert exit_code == 0
    printed_cleanup = Path(cleanup_output.out.strip())
    assert printed_cleanup.parts[-2:] == ("validation", "p5_m5_s2_cleanup_rules.json")

    exit_code = p5_m5_s2_main(["--print-closures-path"])
    closures_output = capsys.readouterr()
    assert exit_code == 0
    printed_closures = Path(closures_output.out.strip())
    assert printed_closures.parts[-2:] == ("validation", "p5_m5_s2_deep_closures.json")

    exit_code = p5_m5_s2_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s2_execution_plan_matches_helper -> ok" in check_output.out
    assert "s2_strategy_stays_representative_not_all_pairs -> ok" in check_output.out

    exit_code = p5_m5_s2_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "execution_plan" in rendered
    assert "exact_deep_carriers" in rendered
    assert "cleanup_rules" in rendered
    assert "deep_closures" in rendered

    exit_code = p5_m5_s2_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "representative deep anchors only" in summary_output.out
    assert "fanout remains composition/orchestration" in summary_output.out
