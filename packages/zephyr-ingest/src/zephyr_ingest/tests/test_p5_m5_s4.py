from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_s4_report import main as p5_m5_s4_main

from zephyr_ingest.testing.p5_m5_s4 import (
    P5_M5_S4_COLD_OPERATOR_REPORT_PATH,
    P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH,
    P5_M5_S4_OPERATOR_QUESTIONS_PATH,
    P5_M5_S4_OPERATOR_SCENARIOS_PATH,
    P5_M5_S4_SCORING_RULES_PATH,
    build_p5_m5_s4_operator_evidence_surfaces,
    build_p5_m5_s4_operator_questions,
    build_p5_m5_s4_operator_scenarios,
    build_p5_m5_s4_scoring_rules,
    format_p5_m5_s4_results,
    validate_p5_m5_s4_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_s4_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_S4_OPERATOR_SCENARIOS_PATH) == (
        build_p5_m5_s4_operator_scenarios()
    )
    assert _load_json_object(P5_M5_S4_OPERATOR_QUESTIONS_PATH) == (
        build_p5_m5_s4_operator_questions()
    )
    assert _load_json_object(P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH) == (
        build_p5_m5_s4_operator_evidence_surfaces()
    )
    assert _load_json_object(P5_M5_S4_SCORING_RULES_PATH) == build_p5_m5_s4_scoring_rules()
    assert P5_M5_S4_COLD_OPERATOR_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_s4_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_s4_artifacts()

    assert all(check.ok for check in checks), format_p5_m5_s4_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_s4_scenarios_stay_representative_and_operator_sized() -> None:
    scenarios_obj = _load_json_object(P5_M5_S4_OPERATOR_SCENARIOS_PATH)
    scenarios = cast(list[dict[str, object]], scenarios_obj["scenarios"])
    package_size = cast(dict[str, object], scenarios_obj["package_size"])
    family_ids = [cast(str, scenario["family_id"]) for scenario in scenarios]

    assert len(scenarios) == 8
    assert family_ids == [
        "delivery",
        "queue_governance",
        "queue_governance",
        "source_deep",
        "source_deep",
        "fanout",
        "fanout",
        "delivery",
    ]
    assert package_size["total_fixed_questions"] == 28
    assert package_size["rough_total_execution_units"] == 22


@pytest.mark.auth_contract
def test_p5_m5_s4_questions_are_machine_readable_and_source_code_free() -> None:
    questions_obj = _load_json_object(P5_M5_S4_OPERATOR_QUESTIONS_PATH)
    families = cast(dict[str, object], questions_obj["families"])

    assert questions_obj["source_code_allowed"] is False
    assert questions_obj["total_questions"] == 28
    assert set(families) == {"delivery", "queue_governance", "source_deep", "fanout"}
    assert cast(dict[str, object], families["delivery"])["question_count"] == 7
    assert cast(dict[str, object], families["queue_governance"])["question_count"] == 8
    assert cast(dict[str, object], families["source_deep"])["question_count"] == 7
    assert cast(dict[str, object], families["fanout"])["question_count"] == 6


@pytest.mark.auth_contract
def test_p5_m5_s4_evidence_surfaces_freeze_allowed_operator_inputs() -> None:
    evidence = _load_json_object(P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH)
    families = cast(dict[str, object], evidence["families"])
    fanout = cast(dict[str, object], families["fanout"])
    composition_truth = cast(dict[str, object], fanout["composition_truth"])

    assert evidence["source_code_allowed"] is False
    assert evidence["author_explanation_required"] is False
    assert "helper_report_cli" in cast(list[object], evidence["allowed_evidence_kinds"])
    assert "raw_artifact_file" in cast(list[object], evidence["allowed_evidence_kinds"])
    assert "source code" in cast(list[object], evidence["prohibited_sources"])
    assert composition_truth["single_sink_connector"] is False


@pytest.mark.auth_contract
def test_p5_m5_s4_scoring_rules_keep_cold_operator_judgment_explicit() -> None:
    scoring = _load_json_object(P5_M5_S4_SCORING_RULES_PATH)
    thresholds = cast(dict[str, object], scoring["thresholds"])

    assert cast(dict[str, object], thresholds["pass"])["minimum_percent"] == 90
    assert cast(dict[str, object], thresholds["partial"])["minimum_percent"] == 70
    assert cast(dict[str, object], thresholds["partial"])["maximum_percent"] == 89
    assert cast(dict[str, object], thresholds["fail"])["or_any_key_mandatory_unanswerable"] is True
    assert scoring["helper_report_only_allowed"] is True
    assert scoring["raw_artifact_inspection_allowed"] is True
    assert scoring["source_code_reading_allowed"] is False


@pytest.mark.auth_contract
def test_p5_m5_s4_report_preserves_scope_and_non_author_judgment_truth() -> None:
    report = P5_M5_S4_COLD_OPERATOR_REPORT_PATH.read_text(encoding="utf-8")

    assert "S4 proves cold-operator readability, not S3 stability repetition." in report
    assert "S4 does not start S5 release-consumable hardening." in report
    assert (
        "Non-authors answer from helper/report surfaces and raw artifacts, not source code."
        in report
    )
    assert "fanout remains composition/orchestration, not a sink connector." in report


@pytest.mark.auth_contract
def test_p5_m5_s4_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_s4_main(["--print-scenarios-path"])
    scenarios_output = capsys.readouterr()
    assert exit_code == 0
    printed_scenarios = Path(scenarios_output.out.strip())
    assert printed_scenarios.parts[-2:] == ("validation", "p5_m5_s4_operator_scenarios.json")

    exit_code = p5_m5_s4_main(["--print-report-path"])
    report_output = capsys.readouterr()
    assert exit_code == 0
    printed_report = Path(report_output.out.strip())
    assert printed_report.parts[-2:] == ("validation", "P5_M5_S4_COLD_OPERATOR.md")

    exit_code = p5_m5_s4_main(["--print-questions-path"])
    questions_output = capsys.readouterr()
    assert exit_code == 0
    printed_questions = Path(questions_output.out.strip())
    assert printed_questions.parts[-2:] == ("validation", "p5_m5_s4_operator_questions.json")

    exit_code = p5_m5_s4_main(["--print-evidence-path"])
    evidence_output = capsys.readouterr()
    assert exit_code == 0
    printed_evidence = Path(evidence_output.out.strip())
    assert printed_evidence.parts[-2:] == (
        "validation",
        "p5_m5_s4_operator_evidence_surfaces.json",
    )

    exit_code = p5_m5_s4_main(["--print-scoring-path"])
    scoring_output = capsys.readouterr()
    assert exit_code == 0
    printed_scoring = Path(scoring_output.out.strip())
    assert printed_scoring.parts[-2:] == ("validation", "p5_m5_s4_scoring_rules.json")

    exit_code = p5_m5_s4_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_s4_operator_scenarios_match_helper -> ok" in check_output.out
    assert "m5_s4_scoring_prohibits_source_code_reading -> ok" in check_output.out

    exit_code = p5_m5_s4_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "operator_scenarios" in rendered
    assert "operator_questions" in rendered
    assert "operator_evidence_surfaces" in rendered
    assert "scoring_rules" in rendered

    exit_code = p5_m5_s4_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "source code is explicitly prohibited for judgment" in summary_output.out
    assert "not S5 release hardening" in summary_output.out
