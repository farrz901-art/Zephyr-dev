from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_runtime_boundary_report import main as p5_boundary_main

from zephyr_ingest.testing.p5_runtime_boundary import (
    P5_RUNTIME_BOUNDARY_MATRIX_PATH,
    P5_RUNTIME_BOUNDARY_REPORT_PATH,
    build_p5_runtime_boundary_matrix,
    format_p5_runtime_boundary_results,
    validate_p5_runtime_boundary_artifacts,
)


@pytest.mark.auth_contract
def test_p5_runtime_boundary_matrix_artifact_matches_helper_shape() -> None:
    artifact = json.loads(P5_RUNTIME_BOUNDARY_MATRIX_PATH.read_text(encoding="utf-8"))
    built = build_p5_runtime_boundary_matrix()
    built_scenarios = cast(list[dict[str, object]], built["scenarios"])

    assert artifact["phase"] == built["phase"]
    assert artifact["execution_chain"] == "TaskV1 -> FlowProcessor -> Delivery"
    assert artifact["queue_backend_boundary"] == built["queue_backend_boundary"]
    assert artifact["lock_backend_boundary"] == built["lock_backend_boundary"]
    assert {scenario["name"] for scenario in artifact["scenarios"]} == {
        scenario["name"] for scenario in built_scenarios
    }
    assert P5_RUNTIME_BOUNDARY_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_runtime_boundary_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_runtime_boundary_artifacts()

    assert all(check.ok for check in checks), format_p5_runtime_boundary_results(checks)


@pytest.mark.auth_contract
def test_p5_runtime_boundary_matrix_keeps_noise_and_network_topics_explicit() -> None:
    artifact = json.loads(P5_RUNTIME_BOUNDARY_MATRIX_PATH.read_text(encoding="utf-8"))

    known_noise = cast(list[str], artifact["known_environment_noise"])
    out_of_scope = cast(list[str], artifact["external_network_out_of_scope"])
    scenario_names = {scenario["name"] for scenario in artifact["scenarios"]}

    assert "runtime_drain_stop_inflight_behavior" in scenario_names
    assert "two_worker_contention_shared_pending_surface" in scenario_names
    assert any("Windows .tmp access-denied noise" in item for item in known_noise)
    assert any("Google Drive" in item for item in out_of_scope)


@pytest.mark.auth_contract
def test_p5_runtime_boundary_report_cli_prints_paths_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_boundary_main(["--print-matrix-path"])
    matrix_output = capsys.readouterr()
    assert exit_code == 0
    printed_matrix = Path(matrix_output.out.strip())
    assert printed_matrix.parts[-2:] == ("validation", "p5_runtime_boundary_matrix.json")

    exit_code = p5_boundary_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M2_RUNTIME_BOUNDARY.md")

    exit_code = p5_boundary_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "boundary_matrix_exists -> ok" in check_output.out

    exit_code = p5_boundary_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "P5-M2 runtime boundary:" in summary_output.out
