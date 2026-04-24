from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

M5S4Severity = Literal["error", "warning", "info"]

P5_M5_S4_OPERATOR_SCENARIOS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s4_operator_scenarios.json"
)
P5_M5_S4_COLD_OPERATOR_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_S4_COLD_OPERATOR.md"
)
P5_M5_S4_OPERATOR_QUESTIONS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s4_operator_questions.json"
)
P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s4_operator_evidence_surfaces.json"
)
P5_M5_S4_SCORING_RULES_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s4_scoring_rules.json"
)


def _scenario(
    *,
    scenario_id: str,
    family_id: str,
    name: str,
    carriers: tuple[str, ...],
    happened: str,
    why_operator_relevant: str,
    difficulty: str,
) -> dict[str, object]:
    return {
        "scenario_id": scenario_id,
        "family_id": family_id,
        "name": name,
        "exact_carriers": list(carriers),
        "what_happened": happened,
        "why_operator_relevant": why_operator_relevant,
        "difficulty": difficulty,
        "judgeable_without_source_code": True,
    }


_OPERATOR_SCENARIOS: Final[tuple[dict[str, object], ...]] = (
    _scenario(
        scenario_id="delivery_replay_verify_receipt",
        family_id="delivery",
        name="Replay failed delivery and confirm governance receipt",
        carriers=(
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
        ),
        happened=(
            "A retained delivery failure is replayed, verify is surfaced, and the resulting "
            "governance receipt becomes the operator-readable closure record."
        ),
        why_operator_relevant=(
            "This is the common retained failure -> replay -> verify -> receipt judgment path."
        ),
        difficulty="medium",
    ),
    _scenario(
        scenario_id="queue_requeue_provenance_explicit",
        family_id="queue_governance",
        name="Inspect poison task, requeue it, then verify provenance stays explicit",
        carriers=(
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py::"
            "test_p5_recovery_requeue_keeps_requeue_provenance_distinct",
        ),
        happened=(
            "A poison-path operator action is inspected and requeued, while requeue provenance "
            "remains separate from prior queue or delivery provenance."
        ),
        why_operator_relevant=(
            "It proves a non-author can tell when state changed, why it changed, and where that "
            "action was recorded."
        ),
        difficulty="medium",
    ),
    _scenario(
        scenario_id="queue_summary_first_look",
        family_id="queue_governance",
        name="Summarize queue poison + DLQ + provenance histories from operator view",
        carriers=(
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py::"
            "test_p5_recovery_summary_combines_queue_delivery_and_provenance",
        ),
        happened=(
            "Queue poison state, delivery failure history, and provenance facts are summarized "
            "into one first-look operator reading."
        ),
        why_operator_relevant=(
            "This is the closest retained scenario to an operator runbook first-pass summary."
        ),
        difficulty="easy",
    ),
    _scenario(
        scenario_id="source_usage_governance_alignment",
        family_id="source_deep",
        name="Read source-linked usage/governance truth for retained source",
        carriers=(
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
        ),
        happened=(
            "Usage, provenance, and governance surfaces are read together to confirm the retained "
            "source contract id remains aligned across operator-visible artifacts."
        ),
        why_operator_relevant=(
            "It answers which source contract id was used and whether usage/provenance/governance "
            "still agree without author explanation."
        ),
        difficulty="medium",
    ),
    _scenario(
        scenario_id="source_base_not_pro_confusion",
        family_id="source_deep",
        name=(
            "Google Drive / Confluence source remains operator-readable without naive Pro confusion"
        ),
        carriers=(
            "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py",
            "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
        ),
        happened=(
            "Retained Google Drive and Confluence source evidence is read together with usage "
            "linkage so the operator can see these retained paths stay Base and are not misread "
            "as Pro or Extra."
        ),
        why_operator_relevant=(
            "It prevents product/domain confusion when reading retained source evidence."
        ),
        difficulty="medium",
    ),
    _scenario(
        scenario_id="fanout_partial_failure_shared_summary",
        family_id="fanout",
        name="Fanout partial failure with shared summary",
        carriers=(
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
            "test_fanout_any_fail",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
            "test_fanout_surfaces_shared_failure_summary_when_failed_children_agree",
        ),
        happened=(
            "A composition run has failed children, surfaces partial failure, and emits a shared "
            "failure summary that must be interpreted as aggregate composition truth."
        ),
        why_operator_relevant=(
            "An operator must distinguish aggregate fanout state from child sink state."
        ),
        difficulty="medium",
    ),
    _scenario(
        scenario_id="fanout_all_ok_composition",
        family_id="fanout",
        name="Fanout all-ok aggregate remains composition, not sink",
        carriers=(
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
            "test_fanout_all_ok",
        ),
        happened=(
            "A fully successful fanout run emits aggregate success while staying a composition "
            "surface rather than becoming a sink-owned result."
        ),
        why_operator_relevant=(
            "It teaches the retained interpretation boundary between aggregate and child outputs."
        ),
        difficulty="easy",
    ),
    _scenario(
        scenario_id="delivery_replayed_webhook_receipt_read",
        family_id="delivery",
        name="Read governance receipt for replayed webhook delivery",
        carriers=(
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
            "test_replay_delivery_writes_governance_action_receipt_with_run_linkage",
        ),
        happened=(
            "A replayed webhook delivery produces a governance receipt whose fields answer task, "
            "run, destination, recovery kind, and result without source-code reading."
        ),
        why_operator_relevant=(
            "It is the direct retained receipt-reading scenario for replayed delivery judgment."
        ),
        difficulty="easy",
    ),
)


def _question(
    *,
    question_id: str,
    text: str,
    mandatory: bool,
    answerable_from: tuple[str, ...],
) -> dict[str, object]:
    return {
        "question_id": question_id,
        "text": text,
        "mandatory": mandatory,
        "answerable_from": list(answerable_from),
        "requires_source_code": False,
    }


_DELIVERY_QUESTIONS: Final[tuple[dict[str, object], ...]] = (
    _question(
        question_id="delivery_failure_type",
        text="What failure type happened?",
        mandatory=True,
        answerable_from=("delivery_receipt.json", "run_meta.json", "p5_recovery_operator_report"),
    ),
    _question(
        question_id="delivery_replay_happened",
        text="Did replay happen?",
        mandatory=True,
        answerable_from=("_governance/actions/<action_id>.json", "p5_recovery_operator_report"),
    ),
    _question(
        question_id="delivery_verify_happened",
        text="Did verify happen?",
        mandatory=True,
        answerable_from=("_governance/actions/<action_id>.json", "p5_recovery_operator_report"),
    ),
    _question(
        question_id="delivery_governance_receipt_location",
        text="Where is the governance receipt?",
        mandatory=True,
        answerable_from=("_governance/actions/<action_id>.json", "p5_governance_audit_report"),
    ),
    _question(
        question_id="delivery_usage_provenance_source_alignment",
        text="Are usage, provenance, and source linkage aligned?",
        mandatory=True,
        answerable_from=("usage_record.json", "run_meta.json", "p5_usage_report"),
    ),
    _question(
        question_id="delivery_destination_identity",
        text="What destination was involved?",
        mandatory=True,
        answerable_from=("delivery_receipt.json", "_governance/actions/<action_id>.json"),
    ),
    _question(
        question_id="delivery_evidence_sufficient",
        text="Is current evidence sufficient?",
        mandatory=False,
        answerable_from=("p5_recovery_operator_report", "p5_governance_audit_report"),
    ),
)

_QUEUE_QUESTIONS: Final[tuple[dict[str, object], ...]] = (
    _question(
        question_id="queue_bucket_or_state",
        text="What bucket or state is the task in?",
        mandatory=True,
        answerable_from=("queue inspect result", "spool bucket files", "queue.sqlite3"),
    ),
    _question(
        question_id="queue_poison_or_orphan",
        text="Is it poison or orphan/stale inflight?",
        mandatory=True,
        answerable_from=("queue inspect result", "p5_recovery_operator_report"),
    ),
    _question(
        question_id="queue_inspect_read_only",
        text="Is inspect read-only?",
        mandatory=True,
        answerable_from=("queue inspect result", "p5_recovery_operator_report"),
    ),
    _question(
        question_id="queue_spool_sqlite_audit_difference",
        text="What is the spool vs sqlite audit-support difference?",
        mandatory=True,
        answerable_from=("_governance/actions/<action_id>.json", "p5_governance_audit_report"),
    ),
    _question(
        question_id="queue_operator_next_action",
        text="Should the operator requeue, verify, or only audit?",
        mandatory=True,
        answerable_from=("p5_recovery_operator_report", "queue inspect result"),
    ),
    _question(
        question_id="queue_governance_receipt_location",
        text="Where is the governance receipt?",
        mandatory=True,
        answerable_from=("_governance/actions/<action_id>.json", "p5_governance_audit_report"),
    ),
    _question(
        question_id="queue_provenance_explicit",
        text="Is provenance explicit?",
        mandatory=True,
        answerable_from=("run_meta.json", "_governance/actions/<action_id>.json"),
    ),
    _question(
        question_id="queue_evidence_sufficient",
        text="Is current evidence sufficient?",
        mandatory=False,
        answerable_from=("p5_recovery_operator_report", "p5_governance_audit_report"),
    ),
)

_SOURCE_QUESTIONS: Final[tuple[dict[str, object], ...]] = (
    _question(
        question_id="source_contract_id",
        text="What is the source contract id?",
        mandatory=True,
        answerable_from=("usage_record.json", "p5_source_contract_report", "p5_usage_report"),
    ),
    _question(
        question_id="source_usage_provenance_governance_alignment",
        text="Are usage, provenance, and governance aligned?",
        mandatory=True,
        answerable_from=(
            "usage_record.json",
            "run_meta.json",
            "_governance/actions/<action_id>.json",
        ),
    ),
    _question(
        question_id="source_progress_isolated",
        text="Is checkpoint or progress isolated?",
        mandatory=True,
        answerable_from=("run_meta.json", "p5_usage_report", "p5_source_contract_report"),
    ),
    _question(
        question_id="source_abnormal_or_failure_type",
        text="What source abnormal or failure type happened?",
        mandatory=True,
        answerable_from=("run_meta.json", "p5_usage_report", "p5_source_contract_report"),
    ),
    _question(
        question_id="source_evidence_sufficient",
        text="Is current evidence sufficient?",
        mandatory=False,
        answerable_from=("p5_usage_report", "p5_governance_audit_report"),
    ),
    _question(
        question_id="source_id_misread_risk",
        text="Is there source-id misread risk?",
        mandatory=True,
        answerable_from=("p5_source_contract_report", "usage_record.json"),
    ),
    _question(
        question_id="source_governance_visibility",
        text="Where is governance visibility?",
        mandatory=True,
        answerable_from=("_governance/actions/<action_id>.json", "p5_governance_audit_report"),
    ),
)

_FANOUT_QUESTIONS: Final[tuple[dict[str, object], ...]] = (
    _question(
        question_id="fanout_is_composition",
        text="Is this composition or orchestration?",
        mandatory=True,
        answerable_from=("batch_report.json", "shared summary", "child outputs"),
    ),
    _question(
        question_id="fanout_mode_differences",
        text="How do all-ok, partial-failure, and shared-summary differ?",
        mandatory=True,
        answerable_from=("batch_report.json", "shared summary", "p5_usage_report"),
    ),
    _question(
        question_id="fanout_child_aggregate_outputs",
        text="Where are child and aggregate outputs?",
        mandatory=True,
        answerable_from=("child outputs", "aggregate outputs", "batch_report.json"),
    ),
    _question(
        question_id="fanout_usage_governance_visibility",
        text="Where is usage and governance visibility?",
        mandatory=True,
        answerable_from=("usage_record.json", "_governance/actions/<action_id>.json"),
    ),
    _question(
        question_id="fanout_scope_mixing_risk",
        text="Is there scope-mixing risk?",
        mandatory=True,
        answerable_from=("batch_report.json", "child outputs", "aggregate outputs"),
    ),
    _question(
        question_id="fanout_evidence_sufficient",
        text="Is current evidence sufficient?",
        mandatory=False,
        answerable_from=("batch_report.json", "p5_governance_audit_report"),
    ),
)


def build_p5_m5_s4_operator_scenarios() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S4 cold operator scenarios",
        "status": "cold_operator_proof_only",
        "scope_boundaries": {
            "repeats_s3_stability_work": False,
            "starts_s5_release_hardening": False,
            "adds_new_retained_anchors": False,
            "distributed_or_cloud_scope": False,
        },
        "package_size": {
            "total_scenarios": 8,
            "total_fixed_questions": 28,
            "helper_report_surface_reuse_count": 14,
            "rough_total_execution_units": 22,
        },
        "families": [
            "delivery",
            "queue_governance",
            "source_deep",
            "fanout",
        ],
        "scenarios": list(_OPERATOR_SCENARIOS),
        "out_of_scope": [
            "distributed runtime",
            "cloud or Kubernetes packaging",
            "billing pricing entitlement",
            "RBAC approval workflow",
            "enterprise connector implementation",
            "S3 repeatability reruns",
            "S5 release-consumable hardening",
        ],
    }


def build_p5_m5_s4_operator_questions() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S4 cold operator questions",
        "source_code_allowed": False,
        "question_family_count": 4,
        "total_questions": 28,
        "families": {
            "delivery": {
                "question_count": len(_DELIVERY_QUESTIONS),
                "questions": list(_DELIVERY_QUESTIONS),
            },
            "queue_governance": {
                "question_count": len(_QUEUE_QUESTIONS),
                "questions": list(_QUEUE_QUESTIONS),
            },
            "source_deep": {
                "question_count": len(_SOURCE_QUESTIONS),
                "questions": list(_SOURCE_QUESTIONS),
            },
            "fanout": {
                "question_count": len(_FANOUT_QUESTIONS),
                "questions": list(_FANOUT_QUESTIONS),
            },
        },
    }


def build_p5_m5_s4_operator_evidence_surfaces() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S4 operator evidence surfaces",
        "allowed_evidence_kinds": [
            "helper_report_cli",
            "governance_receipt",
            "usage_record",
            "run_meta",
            "delivery_receipt",
            "queue_inspect_result",
            "queue_backend_storage",
            "source_contract_report",
            "batch_report",
            "shared_summary",
            "child_output",
            "aggregate_output",
            "raw_artifact_file",
        ],
        "source_code_allowed": False,
        "author_explanation_required": False,
        "families": {
            "delivery": {
                "helper_report_clis": [
                    "tools/p5_recovery_operator_report.py",
                    "tools/p5_governance_audit_report.py",
                    "tools/p5_usage_report.py",
                ],
                "raw_artifacts": [
                    "delivery_receipt.json",
                    "run_meta.json",
                    "usage_record.json",
                    "_governance/actions/<action_id>.json",
                ],
            },
            "queue_governance": {
                "helper_report_clis": [
                    "tools/p5_recovery_operator_report.py",
                    "tools/p5_governance_audit_report.py",
                ],
                "raw_artifacts": [
                    "queue inspect result",
                    "spool queue bucket files",
                    "queue.sqlite3",
                    "_governance/actions/<action_id>.json",
                    "replay/requeue/verify result surfaces",
                ],
            },
            "source_deep": {
                "helper_report_clis": [
                    "tools/p5_source_contract_report.py",
                    "tools/p5_usage_report.py",
                    "tools/p5_governance_audit_report.py",
                ],
                "raw_artifacts": [
                    "usage_record.json",
                    "run_meta.json",
                    "_governance/actions/<action_id>.json",
                    "source contract report output",
                ],
            },
            "fanout": {
                "helper_report_clis": [
                    "tools/p5_usage_report.py",
                    "tools/p5_governance_audit_report.py",
                ],
                "raw_artifacts": [
                    "batch_report.json",
                    "shared summary",
                    "child outputs",
                    "aggregate outputs",
                    "usage_record.json",
                    "_governance/actions/<action_id>.json",
                ],
                "composition_truth": {
                    "surface_type": "destination_composition_orchestration",
                    "single_sink_connector": False,
                },
            },
        },
        "prohibited_sources": [
            "source code",
            "author memory",
            "hidden internal paths not named in artifacts",
        ],
    }


def build_p5_m5_s4_scoring_rules() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S4 scoring rules",
        "thresholds": {
            "pass": {
                "minimum_percent": 90,
                "requires_all_mandatory_correct": True,
            },
            "partial": {
                "minimum_percent": 70,
                "maximum_percent": 89,
                "requires_most_mandatory_correct": True,
            },
            "fail": {
                "below_percent": 70,
                "or_any_key_mandatory_unanswerable": True,
            },
        },
        "mandatory_logic": (
            "Identity, state, receipt, linkage, read-only-or-not, and composition-or-not "
            "questions are mandatory for their family."
        ),
        "helper_report_only_allowed": True,
        "raw_artifact_inspection_allowed": True,
        "source_code_reading_allowed": False,
        "judgeable_without_author_explanation": True,
        "family_mandatory_focus": {
            "delivery": [
                "failure_type",
                "replay_happened",
                "verify_happened",
                "governance_receipt_location",
                "usage_provenance_source_alignment",
                "destination_identity",
            ],
            "queue_governance": [
                "bucket_or_state",
                "poison_or_orphan",
                "inspect_read_only",
                "spool_sqlite_audit_difference",
                "operator_next_action",
                "governance_receipt_location",
                "provenance_explicit",
            ],
            "source_deep": [
                "source_contract_id",
                "usage_provenance_governance_alignment",
                "progress_isolated",
                "abnormal_or_failure_type",
                "source_id_misread_risk",
                "governance_visibility",
            ],
            "fanout": [
                "is_composition",
                "mode_differences",
                "child_aggregate_outputs",
                "usage_governance_visibility",
                "scope_mixing_risk",
            ],
        },
    }


@dataclass(frozen=True, slots=True)
class P5M5S4Check:
    name: str
    ok: bool
    detail: str
    severity: M5S4Severity = "error"


def _read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded_obj, dict):
        return None
    return cast(dict[str, object], loaded_obj)


def _json_artifact_check(
    *,
    name: str,
    path: Path,
    expected: dict[str, object],
) -> P5M5S4Check:
    observed = _read_json_object(path)
    return P5M5S4Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _scenario_carrier_files_exist() -> P5M5S4Check:
    scenario_obj = build_p5_m5_s4_operator_scenarios()
    scenario_list = cast(list[dict[str, object]], scenario_obj["scenarios"])
    carrier_files: list[str] = []
    for scenario in scenario_list:
        carriers = cast(list[str], scenario["exact_carriers"])
        for carrier in carriers:
            carrier_files.append(carrier.split("::", 1)[0])
    missing = [path for path in dict.fromkeys(carrier_files) if not (repo_root() / path).exists()]
    return P5M5S4Check(
        name="m5_s4_scenario_carrier_files_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s4_artifacts() -> list[P5M5S4Check]:
    checks = [
        _json_artifact_check(
            name="m5_s4_operator_scenarios_match_helper",
            path=P5_M5_S4_OPERATOR_SCENARIOS_PATH,
            expected=build_p5_m5_s4_operator_scenarios(),
        ),
        _json_artifact_check(
            name="m5_s4_operator_questions_match_helper",
            path=P5_M5_S4_OPERATOR_QUESTIONS_PATH,
            expected=build_p5_m5_s4_operator_questions(),
        ),
        _json_artifact_check(
            name="m5_s4_operator_evidence_surfaces_match_helper",
            path=P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH,
            expected=build_p5_m5_s4_operator_evidence_surfaces(),
        ),
        _json_artifact_check(
            name="m5_s4_scoring_rules_match_helper",
            path=P5_M5_S4_SCORING_RULES_PATH,
            expected=build_p5_m5_s4_scoring_rules(),
        ),
        _scenario_carrier_files_exist(),
    ]

    scenarios = build_p5_m5_s4_operator_scenarios()
    package_size = cast(dict[str, object], scenarios["package_size"])
    checks.append(
        P5M5S4Check(
            name="m5_s4_operator_package_stays_sized",
            ok=package_size["total_scenarios"] == 8
            and package_size["total_fixed_questions"] == 28
            and package_size["rough_total_execution_units"] == 22,
            detail=str(package_size),
        )
    )

    evidence = build_p5_m5_s4_operator_evidence_surfaces()
    fanout = cast(dict[str, object], cast(dict[str, object], evidence["families"])["fanout"])
    composition_truth = cast(dict[str, object], fanout["composition_truth"])
    checks.append(
        P5M5S4Check(
            name="fanout_stays_composition_not_sink_in_s4",
            ok=composition_truth["single_sink_connector"] is False,
            detail=str(composition_truth),
        )
    )

    scoring = build_p5_m5_s4_scoring_rules()
    checks.append(
        P5M5S4Check(
            name="m5_s4_scoring_prohibits_source_code_reading",
            ok=scoring["source_code_reading_allowed"] is False,
            detail=str(scoring["source_code_reading_allowed"]),
        )
    )

    if P5_M5_S4_COLD_OPERATOR_REPORT_PATH.exists():
        report_text = P5_M5_S4_COLD_OPERATOR_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S4 proves cold-operator readability, not S3 stability repetition.",
            "S4 does not start S5 release-consumable hardening.",
            "Non-authors answer from helper/report surfaces and raw artifacts, not source code.",
            "fanout remains composition/orchestration, not a sink connector.",
            (
                "Distributed runtime, cloud/Kubernetes, billing/RBAC, and enterprise "
                "connectors remain out of scope."
            ),
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S4Check(
                name="m5_s4_report_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )
    else:
        checks.append(
            P5M5S4Check(
                name="m5_s4_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S4_COLD_OPERATOR_REPORT_PATH),
            )
        )

    return checks


def format_p5_m5_s4_results(results: list[P5M5S4Check]) -> str:
    lines = ["P5-M5-S4 cold-operator checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s4_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S4 cold-operator proof:",
            "- scope: representative operator scenarios, evidence surfaces, questions, and scoring",
            "- operator can use helper/report surfaces and raw artifacts",
            "- source code is explicitly prohibited for judgment",
            "- fanout remains composition/orchestration, not a sink connector",
            "- this is not S3 stability repetition and not S5 release hardening",
            "- no distributed, cloud, billing, RBAC, or enterprise scope",
        )
    )


def render_p5_m5_s4_json() -> str:
    bundle = {
        "operator_scenarios": build_p5_m5_s4_operator_scenarios(),
        "operator_questions": build_p5_m5_s4_operator_questions(),
        "operator_evidence_surfaces": build_p5_m5_s4_operator_evidence_surfaces(),
        "scoring_rules": build_p5_m5_s4_scoring_rules(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
