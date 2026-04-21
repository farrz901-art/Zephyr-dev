from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_bounded_concurrency_report import main as p5_concurrency_main

from zephyr_ingest.testing.p5_bounded_concurrency import (
    P5_BOUNDED_CONCURRENCY_MATRIX_PATH,
    P5_BOUNDED_CONCURRENCY_REPORT_PATH,
    build_p5_bounded_concurrency_matrix,
    format_p5_bounded_concurrency_results,
    validate_p5_bounded_concurrency_artifacts,
)


@pytest.mark.auth_contract
def test_p5_bounded_concurrency_matrix_artifact_matches_helper_shape() -> None:
    artifact = json.loads(P5_BOUNDED_CONCURRENCY_MATRIX_PATH.read_text(encoding="utf-8"))
    built = build_p5_bounded_concurrency_matrix()
    built_scenarios = cast(list[dict[str, object]], built["scenarios"])

    assert artifact["phase"] == built["phase"]
    assert artifact["execution_chain"] == "TaskV1 -> FlowProcessor -> Delivery"
    assert artifact["claim_boundary"] == built["claim_boundary"]
    assert {scenario["name"] for scenario in artifact["scenarios"]} == {
        scenario["name"] for scenario in built_scenarios
    }
    assert P5_BOUNDED_CONCURRENCY_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_bounded_concurrency_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_bounded_concurrency_artifacts()

    assert all(check.ok for check in checks), format_p5_bounded_concurrency_results(checks)


@pytest.mark.auth_contract
def test_p5_bounded_concurrency_matrix_keeps_claim_bounded() -> None:
    artifact = json.loads(P5_BOUNDED_CONCURRENCY_MATRIX_PATH.read_text(encoding="utf-8"))
    claim_boundary = cast(dict[str, object], artifact["claim_boundary"])
    scenarios = cast(list[dict[str, object]], artifact["scenarios"])
    not_claimed_obj = claim_boundary["not_claimed"]
    assert isinstance(not_claimed_obj, list)
    not_claimed_values = cast(list[object], not_claimed_obj)
    not_claimed = [item for item in not_claimed_values if isinstance(item, str)]

    assert claim_boundary["runtime_model"] == "same-host/shared-storage"
    assert claim_boundary["worker_runtime_scope"] == "single_process_local_polling"
    assert claim_boundary["maximum_direct_worker_count_evidenced"] == 4
    assert "distributed lease choreography" in not_claimed
    assert "broker-backed queue abstraction" in not_claimed
    assert {scenario["name"] for scenario in scenarios} == {
        "3_to_4_worker_same_pending_contention",
        "backlog_drain_under_multi_worker",
        "stale_lock_plus_contention_same_host",
        "poison_requeue_under_contention",
        "replay_and_queue_governance_coexistence_under_contention",
    }


@pytest.mark.auth_contract
def test_p5_bounded_concurrency_report_cli_prints_paths_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_concurrency_main(["--print-matrix-path"])
    matrix_output = capsys.readouterr()
    assert exit_code == 0
    printed_matrix = Path(matrix_output.out.strip())
    assert printed_matrix.parts[-2:] == ("validation", "p5_bounded_concurrency_matrix.json")

    exit_code = p5_concurrency_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M4_S8_BOUNDED_CONCURRENCY.md")

    exit_code = p5_concurrency_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "bounded_concurrency_matrix_exists -> ok" in check_output.out

    exit_code = p5_concurrency_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "P5-M4-S8 bounded concurrency:" in summary_output.out
