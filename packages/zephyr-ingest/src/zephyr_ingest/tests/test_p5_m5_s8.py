from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s8_report import main as p5_m5_s8_main

from zephyr_ingest.testing.p5_m5_s8 import (
    P5_M5_S8_CLOSEOUT_CHECKS_PATH,
    P5_M5_S8_CLOSEOUT_REPORT_PATH,
    P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH,
    P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH,
    P5_M5_S8_STAGE_TRUTH_INDEX_PATH,
    P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH,
    P5_M5_S8_USER_VALIDATION_PLAN_PATH,
    P5_USER_VALIDATION_PLAYBOOK_PATH,
    build_p5_m5_s8_closeout_checks,
    build_p5_m5_s8_cross_stage_consistency,
    build_p5_m5_s8_non_claims_and_deferred_scope,
    build_p5_m5_s8_stage_truth_index,
    build_p5_m5_s8_user_feedback_template,
    build_p5_m5_s8_user_validation_plan,
    format_p5_m5_s8_results,
    validate_p5_m5_s8_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s8_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S8_STAGE_TRUTH_INDEX_PATH) == build_p5_m5_s8_stage_truth_index()
    assert _load_json_object(P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH) == (
        build_p5_m5_s8_cross_stage_consistency()
    )
    assert _load_json_object(P5_M5_S8_CLOSEOUT_CHECKS_PATH) == build_p5_m5_s8_closeout_checks()
    assert _load_json_object(P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH) == (
        build_p5_m5_s8_non_claims_and_deferred_scope()
    )
    assert _load_json_object(P5_M5_S8_USER_VALIDATION_PLAN_PATH) == (
        build_p5_m5_s8_user_validation_plan()
    )
    assert _load_json_object(P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH) == (
        build_p5_m5_s8_user_feedback_template()
    )
    assert P5_M5_S8_CLOSEOUT_REPORT_PATH.exists()
    assert P5_USER_VALIDATION_PLAYBOOK_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s8_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s8_artifacts()
    assert all(check.ok for check in checks), format_p5_m5_s8_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s8_stage_truth_index_stays_complete_and_bounded() -> None:
    stage_index = _load_json_object(P5_M5_S8_STAGE_TRUTH_INDEX_PATH)
    stages = cast(list[dict[str, object]], stage_index["stages"])

    assert stage_index["stage_count"] == 8
    assert [cast(str, stage["stage_id"]) for stage in stages] == [
        "S0",
        "S1",
        "S2",
        "S3",
        "S4",
        "S5",
        "S6",
        "S7",
    ]
    assert all(stage["status"] == "complete" for stage in stages)
    assert all(stage["source_code_reading_required"] is False for stage in stages)


@pytest.mark.auth_contract
def test_p5_m5_s8_cross_stage_and_closeout_checks_stay_explicit() -> None:
    consistency = _load_json_object(P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH)
    closeout = _load_json_object(P5_M5_S8_CLOSEOUT_CHECKS_PATH)
    checks = cast(list[dict[str, object]], consistency["checks"])
    closeout_checks = cast(list[dict[str, object]], closeout["checks"])

    assert consistency["check_count"] == 6
    assert all(check["machine_checkable_status"] == "pass" for check in checks)
    assert closeout["check_count"] == 10
    assert all(check["current_status"] == "pass" for check in closeout_checks)


@pytest.mark.auth_contract
def test_p5_m5_s8_non_claims_and_user_validation_prep_stay_bounded() -> None:
    non_claims = _load_json_object(P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH)
    user_plan = _load_json_object(P5_M5_S8_USER_VALIDATION_PLAN_PATH)
    feedback = _load_json_object(P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH)

    assert non_claims["non_claim_count"] == 13
    assert user_plan["execution_status"] == "prep_only_not_executed"
    assert user_plan["source_code_reading_required"] is False
    assert user_plan["platform_side_visual_checking_role"] == "secondary_corroboration_only"
    assert len(cast(list[object], user_plan["tasks"])) == 5
    assert feedback["severity_enum"] == ["blocker", "major", "minor", "note"]


@pytest.mark.auth_contract
def test_p5_m5_s8_closeout_report_and_playbook_required_phrases() -> None:
    report = P5_M5_S8_CLOSEOUT_REPORT_PATH.read_text(encoding="utf-8")
    playbook = P5_USER_VALIDATION_PLAYBOOK_PATH.read_text(encoding="utf-8")

    assert "S8 freezes M5 final readiness and closeout truth; it does not redo S0-S7." in report
    assert "S8 does not run final user validation yet; it prepares it." in report
    assert "Platform-side corroboration remains secondary, not primary truth." in report
    assert (
        "The external runtime-home p45/env surface remains authoritative and is not replaced."
        in report
    )
    assert (
        "M5 can close only when stage truth, consistency, closeout checks, and "
        "non-claims remain green." in report
    )
    assert (
        "P5 final user-style validation can begin after S8, but P6 product shell is not claimed."
        in report
    )

    assert "This is a P5 final user validation prep package, not the execution itself." in playbook
    assert "This playbook does not claim P6 product shell is done." in playbook
    assert "Source-code reading is not required." in playbook
    assert "Platform-side visual checking is secondary corroboration." in playbook
    assert (
        "User feedback must be classified instead of turning into vague scope expansion."
        in playbook
    )


@pytest.mark.auth_contract
def test_p5_m5_s8_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s8_main(["--print-stage-truth-index-path"])
    stage_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(stage_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_m5_s8_stage_truth_index.json",
    )

    exit_code = p5_m5_s8_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(report_output.out.strip()).parts[-2:] == (
        "validation",
        "P5_M5_S8_CLOSEOUT.md",
    )

    exit_code = p5_m5_s8_main(["--print-cross-stage-consistency-path"])
    cross_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(cross_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_m5_s8_cross_stage_consistency.json",
    )

    exit_code = p5_m5_s8_main(["--print-closeout-checks-path"])
    closeout_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(closeout_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_m5_s8_closeout_checks.json",
    )

    exit_code = p5_m5_s8_main(["--print-non-claims-path"])
    non_claims_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(non_claims_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_m5_s8_non_claims_and_deferred_scope.json",
    )

    exit_code = p5_m5_s8_main(["--print-user-validation-plan-path"])
    plan_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(plan_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_m5_s8_user_validation_plan.json",
    )

    exit_code = p5_m5_s8_main(["--print-user-feedback-template-path"])
    template_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(template_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_m5_s8_user_feedback_template.json",
    )

    exit_code = p5_m5_s8_main(["--print-user-validation-playbook-path"])
    playbook_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(playbook_output.out.strip()).parts[-2:] == (
        "validation",
        "P5_USER_VALIDATION_PLAYBOOK.md",
    )

    exit_code = p5_m5_s8_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s8_stage_truth_index_matches_helper -> ok" in check_output.out
    assert "m5_s8_stage_count_and_statuses_complete -> ok" in check_output.out

    exit_code = p5_m5_s8_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "stage_truth_index" in rendered
    assert "cross_stage_consistency" in rendered
    assert "closeout_checks" in rendered
    assert "non_claims_and_deferred_scope" in rendered
    assert "user_validation_plan" in rendered
    assert "user_feedback_template" in rendered

    exit_code = p5_m5_s8_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "does not run final user validation yet" in summary_output.out
    assert "platform-side corroboration remains secondary" in summary_output.out
