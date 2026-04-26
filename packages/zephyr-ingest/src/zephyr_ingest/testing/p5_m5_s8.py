from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p5_capability_domains import P5_CAPABILITY_DOMAIN_MATRIX_PATH
from zephyr_ingest.testing.p5_governance_audit import (
    P5_GOVERNANCE_ACTION_MATRIX_PATH,
)
from zephyr_ingest.testing.p5_m5_s1 import (
    P5_M5_S1_BASELINE_MATRIX_REPORT_PATH,
    P5_M5_S1_CLEANUP_RULES_PATH,
    P5_M5_S1_EXACT_CARRIERS_PATH,
    P5_M5_S1_EXECUTION_PLAN_PATH,
)
from zephyr_ingest.testing.p5_m5_s2 import (
    P5_M5_S2_CLEANUP_RULES_PATH,
    P5_M5_S2_DEEP_CLOSURES_PATH,
    P5_M5_S2_DEEP_MATRIX_REPORT_PATH,
    P5_M5_S2_EXACT_DEEP_CARRIERS_PATH,
    P5_M5_S2_EXECUTION_PLAN_PATH,
)
from zephyr_ingest.testing.p5_m5_s3 import (
    P5_M5_S3_CLEANUP_REPETITION_RULES_PATH,
    P5_M5_S3_EXECUTION_PLAN_PATH,
    P5_M5_S3_REPEATABILITY_CARRIERS_PATH,
    P5_M5_S3_STABILITY_MATRIX_REPORT_PATH,
    P5_M5_S3_STABILITY_SIGNALS_PATH,
)
from zephyr_ingest.testing.p5_m5_s4 import (
    P5_M5_S4_COLD_OPERATOR_REPORT_PATH,
    P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH,
    P5_M5_S4_OPERATOR_QUESTIONS_PATH,
    P5_M5_S4_OPERATOR_SCENARIOS_PATH,
    P5_M5_S4_SCORING_RULES_PATH,
)
from zephyr_ingest.testing.p5_m5_s5 import (
    P5_M5_S5_COMPLETENESS_CHECKS_PATH,
    P5_M5_S5_HANDOFF_CONTRACT_PATH,
    P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH,
    P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH,
    P5_M5_S5_RELEASE_MANIFEST_PATH,
)
from zephyr_ingest.testing.p5_m5_s6 import (
    P5_M5_S6_FAMILY_EXPECTATIONS_PATH,
    P5_M5_S6_LOAD_CLASSES_PATH,
    P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH,
    P5_M5_S6_OPERATIONAL_PROFILES_PATH,
    P5_M5_S6_OPERATIONAL_SIGNALS_PATH,
    P5_M5_S6_SUCCESS_CRITERIA_PATH,
)
from zephyr_ingest.testing.p5_m5_s7 import (
    P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH,
    P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH,
    P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH,
    P5_M5_S7_WORKFLOW_SCENARIOS_PATH,
    P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH,
    P5_M5_S7_WORKFLOW_VIEWS_PATH,
)
from zephyr_ingest.testing.p5_m5_validation import (
    P5_M5_DEEP_ANCHOR_SELECTION_PATH,
    P5_M5_RETAINED_SURFACE_INVENTORY_PATH,
    P5_M5_VALIDATION_CHARTER_PATH,
    P5_M5_VALIDATION_MATRIX_PATH,
)
from zephyr_ingest.testing.p5_product_cuts import P5_PRODUCT_CUT_MANIFEST_PATH
from zephyr_ingest.testing.p5_source_contracts import (
    P5_SOURCE_CONTRACT_MATRIX_PATH,
    P5_USAGE_RUNTIME_CONTRACT_PATH,
)
from zephyr_ingest.testing.p45 import repo_root

M5S8Severity = Literal["error", "warning", "info"]

P5_M5_S8_STAGE_TRUTH_INDEX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s8_stage_truth_index.json"
)
P5_M5_S8_CLOSEOUT_REPORT_PATH: Final[Path] = repo_root() / "validation" / "P5_M5_S8_CLOSEOUT.md"
P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s8_cross_stage_consistency.json"
)
P5_M5_S8_CLOSEOUT_CHECKS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s8_closeout_checks.json"
)
P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s8_non_claims_and_deferred_scope.json"
)
P5_M5_S8_USER_VALIDATION_PLAN_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s8_user_validation_plan.json"
)
P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s8_user_feedback_template.json"
)
P5_USER_VALIDATION_PLAYBOOK_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_USER_VALIDATION_PLAYBOOK.md"
)


def _serialize_repo_relative_path(path: Path) -> str:
    return path.relative_to(repo_root()).as_posix()


def _stage(
    *,
    stage_id: str,
    purpose: str,
    artifacts: tuple[Path, ...],
    report: Path,
    helper_entrypoint: str,
    proves: tuple[str, ...],
    does_not_prove: tuple[str, ...],
    overclaim_risk: str,
) -> dict[str, object]:
    return {
        "stage_id": stage_id,
        "stage_purpose": purpose,
        "primary_machine_readable_artifacts": [
            _serialize_repo_relative_path(path) for path in artifacts
        ],
        "primary_human_readable_report": _serialize_repo_relative_path(report),
        "helper_report_entrypoint": helper_entrypoint,
        "status": "complete",
        "what_it_proves": list(proves),
        "what_it_does_not_prove": list(does_not_prove),
        "source_code_reading_required": False,
        "overclaim_risk": overclaim_risk,
    }


def build_p5_m5_s8_stage_truth_index() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S8 final readiness closeout",
        "stage_count": 8,
        "stages": [
            _stage(
                stage_id="S0",
                purpose="validation / matrix freeze",
                artifacts=(
                    P5_M5_VALIDATION_MATRIX_PATH,
                    P5_M5_RETAINED_SURFACE_INVENTORY_PATH,
                    P5_M5_DEEP_ANCHOR_SELECTION_PATH,
                ),
                report=P5_M5_VALIDATION_CHARTER_PATH,
                helper_entrypoint="tools/p5_m5_validation_report.py",
                proves=(
                    "retained validation frame and matrix boundaries",
                    "retained surface inventory freeze",
                    "deep-anchor selection freeze",
                ),
                does_not_prove=(
                    "execution depth",
                    "stability",
                    "cold-operator proof",
                    "release bundle",
                    "user workflow completion",
                ),
                overclaim_risk=(
                    "S0 can be over-read as execution proof even though it only freezes "
                    "validation boundaries."
                ),
            ),
            _stage(
                stage_id="S1",
                purpose="baseline matrix execution",
                artifacts=(
                    P5_M5_S1_EXECUTION_PLAN_PATH,
                    P5_M5_S1_EXACT_CARRIERS_PATH,
                    P5_M5_S1_CLEANUP_RULES_PATH,
                ),
                report=P5_M5_S1_BASELINE_MATRIX_REPORT_PATH,
                helper_entrypoint="tools/p5_m5_s1_report.py",
                proves=(
                    "retained baseline coverage layer",
                    "exact baseline carriers",
                    "cleanup/isolation discipline for baseline execution",
                ),
                does_not_prove=(
                    "representative deep closure",
                    "repeatability",
                    "cold-operator proof",
                    "release bundle",
                    "final workflow chain",
                ),
                overclaim_risk=(
                    "S1 can be overclaimed as deep validation when it only proves retained "
                    "baseline coverage."
                ),
            ),
            _stage(
                stage_id="S2",
                purpose="representative deep closures",
                artifacts=(
                    P5_M5_S2_EXECUTION_PLAN_PATH,
                    P5_M5_S2_EXACT_DEEP_CARRIERS_PATH,
                    P5_M5_S2_CLEANUP_RULES_PATH,
                    P5_M5_S2_DEEP_CLOSURES_PATH,
                ),
                report=P5_M5_S2_DEEP_MATRIX_REPORT_PATH,
                helper_entrypoint="tools/p5_m5_s2_report.py",
                proves=(
                    "representative failure/recovery/source/fanout deep closure truth",
                    "explicit grouped deep-closure execution",
                    "representative deep cleanup/isolation truth",
                ),
                does_not_prove=(
                    "repeatability",
                    "cleanup repetition",
                    "bounded soak",
                    "cold-operator proof",
                    "release handoff",
                    "final workflow chain",
                ),
                overclaim_risk=(
                    "S2 can be over-read as all-pairs deep proof when it is intentionally "
                    "representative only."
                ),
            ),
            _stage(
                stage_id="S3",
                purpose="repeatability / cleanup / bounded soak stability",
                artifacts=(
                    P5_M5_S3_EXECUTION_PLAN_PATH,
                    P5_M5_S3_REPEATABILITY_CARRIERS_PATH,
                    P5_M5_S3_CLEANUP_REPETITION_RULES_PATH,
                    P5_M5_S3_STABILITY_SIGNALS_PATH,
                ),
                report=P5_M5_S3_STABILITY_MATRIX_REPORT_PATH,
                helper_entrypoint="tools/p5_m5_s3_report.py",
                proves=(
                    "repeated deep closure stability",
                    "cleanup/rerun truth",
                    "bounded same-host soak truth",
                ),
                does_not_prove=(
                    "cold-operator proof",
                    "release bundle",
                    "workflow user-readiness",
                    "cloud/load/distributed behavior",
                ),
                overclaim_risk=(
                    "S3 can be misread as load testing even though it only proves bounded "
                    "same-host stability."
                ),
            ),
            _stage(
                stage_id="S4",
                purpose="cold-operator proof",
                artifacts=(
                    P5_M5_S4_OPERATOR_SCENARIOS_PATH,
                    P5_M5_S4_OPERATOR_QUESTIONS_PATH,
                    P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH,
                    P5_M5_S4_SCORING_RULES_PATH,
                ),
                report=P5_M5_S4_COLD_OPERATOR_REPORT_PATH,
                helper_entrypoint="tools/p5_m5_s4_report.py",
                proves=(
                    (
                        "non-author operator can judge retained scenarios using "
                        "helper/report/artifacts"
                    ),
                    "source-code reading is not required for operator interpretation",
                    "operator scoring/pass criteria are explicit",
                ),
                does_not_prove=(
                    "release bundle",
                    "installer implementation",
                    "cloud/deployment readiness",
                    "real workflow user journey",
                ),
                overclaim_risk=(
                    "S4 can be overclaimed as product-user readiness when it only proves "
                    "cold-operator interpretation."
                ),
            ),
            _stage(
                stage_id="S5",
                purpose="release-consumable reference bundle",
                artifacts=(
                    P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH,
                    P5_M5_S5_RELEASE_MANIFEST_PATH,
                    P5_M5_S5_HANDOFF_CONTRACT_PATH,
                    P5_M5_S5_COMPLETENESS_CHECKS_PATH,
                ),
                report=P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH,
                helper_entrypoint="tools/p5_m5_s5_report.py",
                proves=(
                    "future product/installer/release layers can discover canonical truth surfaces",
                    "bundle inventory, manifest, layout, handoff, and completeness are frozen",
                    "outer layers do not need source-code reading to discover truth surfaces",
                ),
                does_not_prove=(
                    "installer implementation",
                    "deployment packaging",
                    "cloud bundle",
                    "runtime env replacement",
                ),
                overclaim_risk=(
                    "S5 can be misread as installer or deployment work when it is only a "
                    "reference bundle."
                ),
            ),
            _stage(
                stage_id="S6",
                purpose="bounded same-host operational envelope",
                artifacts=(
                    P5_M5_S6_OPERATIONAL_PROFILES_PATH,
                    P5_M5_S6_LOAD_CLASSES_PATH,
                    P5_M5_S6_OPERATIONAL_SIGNALS_PATH,
                    P5_M5_S6_FAMILY_EXPECTATIONS_PATH,
                    P5_M5_S6_SUCCESS_CRITERIA_PATH,
                ),
                report=P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH,
                helper_entrypoint="tools/p5_m5_s6_report.py",
                proves=(
                    "retained/local/bounded operational profile truth",
                    "bounded load-class and signal truth",
                    "family-level expectations and success/fail criteria for same-host operation",
                ),
                does_not_prove=(
                    "cloud or distributed envelope",
                    "large-scale load testing",
                    "installer/deployment envelope",
                ),
                overclaim_risk=(
                    "S6 can be over-read as performance qualification when it only freezes a "
                    "bounded same-host operational envelope."
                ),
            ),
            _stage(
                stage_id="S7",
                purpose="real workflow chains",
                artifacts=(
                    P5_M5_S7_WORKFLOW_SCENARIOS_PATH,
                    P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH,
                    P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH,
                    P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH,
                    P5_M5_S7_WORKFLOW_VIEWS_PATH,
                ),
                report=P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH,
                helper_entrypoint="tools/p5_m5_s7_report.py",
                proves=(
                    (
                        "4 representative source/run/delivery/usage/provenance/"
                        "governance/operator/release workflow chains"
                    ),
                    "workflow evidence chains are explicit and machine-readable",
                    "workflow truth is consumable without source-code reading",
                ),
                does_not_prove=(
                    "all-pairs workflow proof",
                    "cloud/distributed workflows",
                    "installer",
                    "P6 product shell",
                ),
                overclaim_risk=(
                    "S7 can be overclaimed as exhaustive workflow proof when it is intentionally "
                    "representative."
                ),
            ),
        ],
    }


def _consistency_check(
    *,
    check_id: str,
    related_stages: tuple[str, ...],
    expected_relation: str,
    evidence_surfaces: tuple[str, ...],
    failure_meaning: str,
) -> dict[str, object]:
    return {
        "check_id": check_id,
        "related_stages": list(related_stages),
        "expected_relation": expected_relation,
        "exact_evidence_surfaces": list(evidence_surfaces),
        "machine_checkable_status": "pass",
        "failure_meaning": failure_meaning,
    }


def build_p5_m5_s8_cross_stage_consistency() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S8 cross-stage consistency",
        "check_count": 6,
        "checks": [
            _consistency_check(
                check_id="s1_s7_retained_surface_alignment",
                related_stages=("S1", "S7"),
                expected_relation=(
                    "S7 workflows continue using retained baseline surfaces, do not add new "
                    "anchors, and stay workflow-proof-sized rather than matrix-sized."
                ),
                evidence_surfaces=(
                    _serialize_repo_relative_path(P5_M5_S1_EXACT_CARRIERS_PATH),
                    _serialize_repo_relative_path(P5_M5_S1_EXECUTION_PLAN_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_SCENARIOS_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH),
                ),
                failure_meaning=(
                    "S7 has drifted away from retained baseline scope or silently widened into "
                    "broader matrix execution."
                ),
            ),
            _consistency_check(
                check_id="s2_s7_deep_closure_alignment",
                related_stages=("S2", "S7"),
                expected_relation=(
                    "S7 failure/recovery and fanout workflows reuse the same representative "
                    "deep closure vocabulary as S2: replay/requeue/verify/governance and "
                    "fanout composition."
                ),
                evidence_surfaces=(
                    _serialize_repo_relative_path(P5_M5_S2_DEEP_CLOSURES_PATH),
                    _serialize_repo_relative_path(P5_M5_S2_EXACT_DEEP_CARRIERS_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH),
                ),
                failure_meaning=(
                    "Workflow proof terms have diverged from representative deep closure truth "
                    "or fanout has drifted toward sink semantics."
                ),
            ),
            _consistency_check(
                check_id="s3_s6_s7_cleanup_and_secondary_truth",
                related_stages=("S3", "S6", "S7"),
                expected_relation=(
                    "S6 and S7 continue to honor S3 cleanup/stability truth, avoid dirty-state "
                    "dependence, and keep platform-side corroboration secondary."
                ),
                evidence_surfaces=(
                    _serialize_repo_relative_path(P5_M5_S3_CLEANUP_REPETITION_RULES_PATH),
                    _serialize_repo_relative_path(P5_M5_S3_STABILITY_SIGNALS_PATH),
                    _serialize_repo_relative_path(P5_M5_S6_SUCCESS_CRITERIA_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH),
                ),
                failure_meaning=(
                    "Later-stage truth now depends on dirty state or treats platform-side checks "
                    "as primary evidence."
                ),
            ),
            _consistency_check(
                check_id="s4_s7_non_author_interpretation_alignment",
                related_stages=("S4", "S7"),
                expected_relation=(
                    "Cold-operator evidence surfaces remain sufficient to explain S7 workflows "
                    "without source-code reading."
                ),
                evidence_surfaces=(
                    _serialize_repo_relative_path(P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH),
                    _serialize_repo_relative_path(P5_M5_S4_OPERATOR_QUESTIONS_PATH),
                    _serialize_repo_relative_path(P5_M5_S4_SCORING_RULES_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_VIEWS_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH),
                ),
                failure_meaning=(
                    "S7 workflows now need source-code reading or author interpretation to be "
                    "understood by non-authors."
                ),
            ),
            _consistency_check(
                check_id="s5_s6_s7_bundle_discovery_alignment",
                related_stages=("S5", "S6", "S7"),
                expected_relation=(
                    "S5 release-consumable bundle remains able to discover S6/S7 truth surfaces "
                    "through canonical helper/report mapping without implying installer or cloud "
                    "readiness."
                ),
                evidence_surfaces=(
                    _serialize_repo_relative_path(P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH),
                    _serialize_repo_relative_path(P5_M5_S5_RELEASE_MANIFEST_PATH),
                    _serialize_repo_relative_path(P5_M5_S5_HANDOFF_CONTRACT_PATH),
                    _serialize_repo_relative_path(P5_M5_S6_OPERATIONAL_PROFILES_PATH),
                    _serialize_repo_relative_path(P5_M5_S7_WORKFLOW_SCENARIOS_PATH),
                ),
                failure_meaning=(
                    "Later-stage truth is no longer discoverable through the canonical bundle or "
                    "has drifted toward installer/deployment implication."
                ),
            ),
            _consistency_check(
                check_id="technical_domain_usage_governance_product_cut_alignment",
                related_stages=("core", "S5", "S6", "S7"),
                expected_relation=(
                    "Capability domains remain technical, usage remains raw technical fact, "
                    "governance remains audit/manual-action truth, source contract ids remain "
                    "technical source truth, and product-cut remains technical readiness."
                ),
                evidence_surfaces=(
                    _serialize_repo_relative_path(P5_CAPABILITY_DOMAIN_MATRIX_PATH),
                    _serialize_repo_relative_path(P5_USAGE_RUNTIME_CONTRACT_PATH),
                    _serialize_repo_relative_path(P5_GOVERNANCE_ACTION_MATRIX_PATH),
                    _serialize_repo_relative_path(P5_SOURCE_CONTRACT_MATRIX_PATH),
                    _serialize_repo_relative_path(P5_PRODUCT_CUT_MANIFEST_PATH),
                ),
                failure_meaning=(
                    "Technical truth surfaces are being misread or overclaimed as pricing, "
                    "entitlement, RBAC, or commercial readiness."
                ),
            ),
        ],
    }


def _closeout_check(
    *,
    check_id: str,
    description: str,
    evidence_surface: str,
    machine_checkable_today: bool,
    severity: M5S8Severity,
    failure_action: str,
) -> dict[str, object]:
    return {
        "check_id": check_id,
        "description": description,
        "exact_evidence_surface": evidence_surface,
        "machine_checkable_today": machine_checkable_today,
        "severity": severity,
        "current_status": "pass",
        "failure_action": failure_action,
    }


def build_p5_m5_s8_closeout_checks() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S8 closeout checks",
        "check_count": 10,
        "checks": [
            _closeout_check(
                check_id="all_required_m5_artifacts_exist",
                description="All required M5 artifacts from S0 through S8 exist.",
                evidence_surface="tools/p5_m5_s8_report.py --check-artifacts",
                machine_checkable_today=True,
                severity="error",
                failure_action="restore or regenerate missing stage artifacts before closeout",
            ),
            _closeout_check(
                check_id="all_required_helper_checks_green",
                description=(
                    "Required helper/report checks remain green across core and M5 surfaces."
                ),
                evidence_surface=(
                    "tools/p5_m5_s8_report.py --check-artifacts plus selected prior helper checks"
                ),
                machine_checkable_today=True,
                severity="error",
                failure_action="fix helper/artifact drift before declaring M5 closeout",
            ),
            _closeout_check(
                check_id="all_stage_truth_statuses_complete",
                description="Stage truth index marks S0-S7 complete and bounded.",
                evidence_surface=_serialize_repo_relative_path(P5_M5_S8_STAGE_TRUTH_INDEX_PATH),
                machine_checkable_today=True,
                severity="error",
                failure_action="resolve incomplete stage state before M5 closeout",
            ),
            _closeout_check(
                check_id="cross_stage_consistency_checks_pass",
                description="Cross-stage consistency checks all pass.",
                evidence_surface=_serialize_repo_relative_path(
                    P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH
                ),
                machine_checkable_today=True,
                severity="error",
                failure_action="reconcile cross-stage drift before closeout",
            ),
            _closeout_check(
                check_id="all_non_claims_remain_non_claims",
                description="Non-claims and deferred scope stay explicit and unchanged.",
                evidence_surface=_serialize_repo_relative_path(
                    P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH
                ),
                machine_checkable_today=True,
                severity="error",
                failure_action="remove overclaiming language and restore guardrails",
            ),
            _closeout_check(
                check_id="p5_final_user_validation_prep_exists",
                description="User validation prep package exists without starting execution.",
                evidence_surface=_serialize_repo_relative_path(P5_M5_S8_USER_VALIDATION_PLAN_PATH),
                machine_checkable_today=True,
                severity="error",
                failure_action="finish user validation prep package before closeout",
            ),
            _closeout_check(
                check_id="source_code_reading_not_required",
                description="Final closeout truth is consumable without source-code reading.",
                evidence_surface=_serialize_repo_relative_path(P5_M5_S8_STAGE_TRUTH_INDEX_PATH),
                machine_checkable_today=True,
                severity="error",
                failure_action="remove source-code dependency from closeout package",
            ),
            _closeout_check(
                check_id="platform_side_corroboration_secondary",
                description="Platform-side checking remains secondary corroboration.",
                evidence_surface=_serialize_repo_relative_path(P5_M5_S8_CLOSEOUT_CHECKS_PATH),
                machine_checkable_today=True,
                severity="error",
                failure_action="restore artifact/helper truth as primary closeout evidence",
            ),
            _closeout_check(
                check_id="no_runtime_home_replacement",
                description="External runtime-home and p45/env are not replaced by repo truth.",
                evidence_surface=_serialize_repo_relative_path(
                    P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH
                ),
                machine_checkable_today=True,
                severity="error",
                failure_action="remove any runtime-home replacement implication",
            ),
            _closeout_check(
                check_id="no_installer_cloud_p6_commercial_drift",
                description=(
                    "No installer/cloud/P6/commercial scope drift is introduced in closeout."
                ),
                evidence_surface=_serialize_repo_relative_path(
                    P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH
                ),
                machine_checkable_today=True,
                severity="error",
                failure_action="pull drifted scope back behind explicit non-claims",
            ),
        ],
    }


def _non_claim(
    *,
    non_claim_id: str,
    why_not_claimed_in_m5: str,
    later_home: str,
    risk_if_accidentally_claimed: str,
    wording_guardrail: str,
) -> dict[str, object]:
    return {
        "non_claim_id": non_claim_id,
        "why_not_claimed_in_m5": why_not_claimed_in_m5,
        "likely_later_home": later_home,
        "risk_if_accidentally_claimed": risk_if_accidentally_claimed,
        "wording_guardrail": wording_guardrail,
    }


def build_p5_m5_s8_non_claims_and_deferred_scope() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S8 non-claims and deferred scope",
        "non_claim_count": 13,
        "non_claims": [
            _non_claim(
                non_claim_id="distributed_runtime",
                why_not_claimed_in_m5=(
                    "M5 stayed bounded/local/artifact-backed and never proved "
                    "distributed ownership."
                ),
                later_home="P6 cloud/deployment line",
                risk_if_accidentally_claimed=(
                    "Creates false fleet/runtime expectations unsupported by retained evidence."
                ),
                wording_guardrail="Do not claim distributed worker or scheduler ownership.",
            ),
            _non_claim(
                non_claim_id="cloud_k8s_helm_deployment",
                why_not_claimed_in_m5="M5 never implemented or proved deployment packaging.",
                later_home="P6 cloud/deployment line",
                risk_if_accidentally_claimed=(
                    "Users may assume deployment packaging or cluster readiness exists."
                ),
                wording_guardrail=(
                    "Do not describe M5 artifacts as cloud, Kubernetes, or Helm bundles."
                ),
            ),
            _non_claim(
                non_claim_id="deployment_packaging",
                why_not_claimed_in_m5="S5 was a reference bundle, not deployment packaging.",
                later_home="P6 cloud/deployment line",
                risk_if_accidentally_claimed=(
                    "Turns reference truth into a fake deployment deliverable."
                ),
                wording_guardrail=(
                    "Use release-consumable reference bundle, never deployment package."
                ),
            ),
            _non_claim(
                non_claim_id="installer_implementation",
                why_not_claimed_in_m5="No installer was built or validated in M5.",
                later_home="P6 productization",
                risk_if_accidentally_claimed="Would overstate onboarding and user readiness.",
                wording_guardrail="Do not imply install flow or setup automation is done.",
            ),
            _non_claim(
                non_claim_id="large_scale_concurrency_load_testing",
                why_not_claimed_in_m5="S6 proved only bounded same-host operational envelope.",
                later_home="post-P6",
                risk_if_accidentally_claimed=(
                    "Conflates bounded envelope with real load qualification."
                ),
                wording_guardrail="Do not describe bounded same-host classes as load testing.",
            ),
            _non_claim(
                non_claim_id="production_multi_tenant_slo_sla",
                why_not_claimed_in_m5="M5 froze technical truth packages, not product SLA policy.",
                later_home="post-P6",
                risk_if_accidentally_claimed=(
                    "Creates unsupported contractual performance expectations."
                ),
                wording_guardrail="Do not turn operational signals into SLO/SLA claims.",
            ),
            _non_claim(
                non_claim_id="billing_pricing_entitlement",
                why_not_claimed_in_m5=(
                    "Capability and usage truths remain technical, non-commercial."
                ),
                later_home="P6 productization",
                risk_if_accidentally_claimed=(
                    "Misreads raw technical usage as monetization or entitlement."
                ),
                wording_guardrail=(
                    "Base/Pro/Extra are technical capability domains, not pricing or entitlement."
                ),
            ),
            _non_claim(
                non_claim_id="rbac_approval_workflow",
                why_not_claimed_in_m5=(
                    "Governance stayed audit/manual-action truth, not approval workflow."
                ),
                later_home="P6 productization",
                risk_if_accidentally_claimed="Suggests authorization controls that do not exist.",
                wording_guardrail=(
                    "Do not equate governance receipts with RBAC or approval workflow."
                ),
            ),
            _non_claim(
                non_claim_id="enterprise_connectors",
                why_not_claimed_in_m5="Retained connectors remain non-enterprise only.",
                later_home="P6 enterprise connector line",
                risk_if_accidentally_claimed=(
                    "Overstates connector support boundary and product maturity."
                ),
                wording_guardrail="Do not describe retained connectors as enterprise-managed.",
            ),
            _non_claim(
                non_claim_id="commercial_gateway",
                why_not_claimed_in_m5=(
                    "M5 never introduced commercial platform or gateway surfaces."
                ),
                later_home="post-P6",
                risk_if_accidentally_claimed=(
                    "Turns technical validation into unsupported commercial platform claim."
                ),
                wording_guardrail=(
                    "Do not add commercial gateway language to technical closeout artifacts."
                ),
            ),
            _non_claim(
                non_claim_id="full_all_pairs_equal_depth_proof",
                why_not_claimed_in_m5=(
                    "M5 stayed representative and bounded by design across S1-S7."
                ),
                later_home="post-P6",
                risk_if_accidentally_claimed=(
                    "Erases the deliberate representative-only validation boundary."
                ),
                wording_guardrail="Do not describe M5 as exhaustive all-pairs equal-depth proof.",
            ),
            _non_claim(
                non_claim_id="p6_product_shell",
                why_not_claimed_in_m5="S8 only prepares final user-style validation and closeout.",
                later_home="P6 productization",
                risk_if_accidentally_claimed=(
                    "Confuses closeout prep with a finished product shell."
                ),
                wording_guardrail="Do not imply product shell UX or packaging is complete.",
            ),
            _non_claim(
                non_claim_id="runtime_env_replacement",
                why_not_claimed_in_m5="External p45/env remains authoritative runtime truth.",
                later_home="post-P6",
                risk_if_accidentally_claimed=(
                    "Encourages replacing real runtime env/config with repo-only truth."
                ),
                wording_guardrail=(
                    "Do not replace or shadow the authoritative external p45/env surface."
                ),
            ),
        ],
    }


def build_p5_m5_s8_user_validation_plan() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5 final user-style validation prep",
        "execution_status": "prep_only_not_executed",
        "source_code_reading_required": False,
        "platform_side_visual_checking_role": "secondary_corroboration_only",
        "tasks": [
            {
                "task_id": "p5_user_uns_task_01",
                "user_task": "Run a retained UNS document flow and confirm artifacts appear.",
                "source": "google_drive_document_v1 or confluence_document_v1",
                "expected_output": (
                    "filesystem delivery artifacts plus run_meta.json and usage_record.json"
                ),
                "user_visible_success_signal": (
                    "user can see output artifacts land in the bounded output path"
                ),
                "operator_visible_evidence": [
                    "tools/p5_usage_report.py",
                    "tools/p5_source_contract_report.py",
                    "tools/p5_governance_audit_report.py",
                ],
                "artifact_visible_evidence": [
                    "validation/p5_m5_s7_workflow_evidence_map.json",
                    "validation/p5_source_usage_linkage.json",
                    "usage_record.json",
                    "run_meta.json",
                ],
                "failure_observation_checklist": [
                    "source provenance missing",
                    "usage record missing or unlinked",
                    "output artifact missing",
                    "unexpected Pro/Extra confusion",
                ],
            },
            {
                "task_id": "p5_user_it_task_01",
                "user_task": (
                    "Run a retained IT source flow and confirm bounded cursor-linked output."
                ),
                "source": "kafka_partition_offset_v1 or http_json_cursor_v1",
                "expected_output": "bounded output artifacts with IT usage/source linkage",
                "user_visible_success_signal": (
                    "user can tell a structured source completed and output landed"
                ),
                "operator_visible_evidence": [
                    "tools/p5_usage_report.py",
                    "tools/p5_source_contract_report.py",
                ],
                "artifact_visible_evidence": [
                    "validation/p5_m5_s7_workflow_views.json",
                    "validation/p5_usage_runtime_contract.json",
                    "usage_record.json",
                ],
                "failure_observation_checklist": [
                    "cursor/checkpoint identity unclear",
                    "usage raw unit widened unexpectedly",
                    "output landed but source linkage missing",
                ],
            },
            {
                "task_id": "p5_user_failure_recovery_task_01",
                "user_task": (
                    "Trigger or select a retained failure and follow its bounded recovery path."
                ),
                "induced_or_selected_failure": "retryable retained delivery failure",
                "expected_recovery_path": (
                    "failure classification -> replay or requeue -> verify/governance receipt"
                ),
                "user_visible_success_failure_signal": (
                    "user can tell whether failure stayed visible and whether recovery completed"
                ),
                "operator_visible_evidence": [
                    "tools/p5_recovery_operator_report.py",
                    "tools/p5_governance_audit_report.py",
                    "tools/p5_usage_report.py",
                ],
                "artifact_visible_evidence": [
                    "validation/p5_m5_s7_workflow_failure_matrix.json",
                    "validation/p5_governance_action_matrix.json",
                    "delivery_receipt.json",
                    "_governance/actions/<action_id>.json",
                ],
                "failure_observation_checklist": [
                    "failure swallowed",
                    "replay/requeue status unclear",
                    "receipt missing",
                    "usage/provenance linkage incoherent after recovery",
                ],
            },
            {
                "task_id": "p5_user_fanout_task_01",
                "user_task": "Run a retained fanout path and inspect child plus aggregate outputs.",
                "destinations": (
                    "retained multi-sink composition path with filesystem-visible child output"
                ),
                "expected_aggregate_child_outputs": (
                    "aggregate/shared-summary plus child outputs with all-ok vs "
                    "partial-failure separation"
                ),
                "user_visible_success_failure_signal": (
                    "user can tell whether fanout fully succeeded or partially failed"
                ),
                "operator_visible_evidence": [
                    "tools/p5_usage_report.py",
                    "tools/p5_governance_audit_report.py",
                ],
                "artifact_visible_evidence": [
                    "validation/p5_m5_s7_workflow_views.json",
                    "validation/p5_m5_s7_workflow_success_criteria.json",
                    "batch_report.json",
                ],
                "scope_mixing_checklist": [
                    "child and aggregate outputs separated",
                    "shared summary visible when failed children agree",
                    "fanout not interpreted as sink connector",
                ],
            },
            {
                "task_id": "p5_user_release_handoff_task_01",
                "user_task": (
                    "Discover the retained truth package through helper/report and bundle surfaces."
                ),
                "expected_bundle_helper_report_discovery": (
                    "user can discover S5/S6/S7/S8 truth surfaces from canonical "
                    "helper/report entrypoints"
                ),
                "what_user_can_verify_without_source_code": [
                    "stage truth index",
                    "closeout checks",
                    "non-claims",
                    "user validation prep package",
                ],
                "failure_observation_checklist": [
                    "helper path missing",
                    "truth surface only discoverable from source code",
                    "bundle implies installer or deployment readiness",
                ],
            },
        ],
        "feedback_classification_rule": (
            "User feedback must be classified into P5 blocker, M5 closeout fix, P5 final user "
            "validation feedback, P6 product-shell-later issue, or out-of-scope "
            "cloud/commercial request."
        ),
    }


def build_p5_m5_s8_user_feedback_template() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5 final user validation feedback template",
        "source_code_reading_required": False,
        "fields": [
            "task_id",
            "user_action",
            "expected_result",
            "actual_result",
            "user_facing_issue",
            "operator_facing_issue",
            "artifact_facing_issue",
            "usage_provenance_governance_issue",
            "source_destination_issue",
            "failure_recovery_issue",
            "severity",
            "blocks_p5_closeout",
            "likely_owner",
            "evidence_path_screenshot_or_command_output",
            "triage_note",
            "belongs_in_p5_closeout_or_p6_product_shell",
        ],
        "severity_enum": ["blocker", "major", "minor", "note"],
        "likely_owner_enum": [
            "core_runtime",
            "source",
            "destination",
            "governance",
            "usage_provenance",
            "operator_docs_helper",
            "release_bundle",
            "product_shell_later",
        ],
        "classification_guidance": {
            "real_p5_blocker": "Blocks M5 closeout or final user-style validation from starting.",
            "m5_closeout_fix": "Should be fixed before M5 closeout is accepted.",
            "p5_final_user_validation_feedback": (
                "Valid feedback from the final user-style pass that does not reopen M5 stage scope."
            ),
            "p6_product_shell_later_issue": (
                "Belongs to later product shell or packaging work rather than M5 closeout."
            ),
            "out_of_scope_cloud_or_commercial_request": (
                "Requests cloud/deployment/commercial scope not claimed in P5/M5."
            ),
        },
    }


@dataclass(frozen=True, slots=True)
class P5M5S8Check:
    name: str
    ok: bool
    detail: str
    severity: M5S8Severity = "error"


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
) -> P5M5S8Check:
    observed = _read_json_object(path)
    return P5M5S8Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _required_paths_exist() -> P5M5S8Check:
    paths = (
        P5_M5_S8_STAGE_TRUTH_INDEX_PATH,
        P5_M5_S8_CLOSEOUT_REPORT_PATH,
        P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH,
        P5_M5_S8_CLOSEOUT_CHECKS_PATH,
        P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH,
        P5_M5_S8_USER_VALIDATION_PLAN_PATH,
        P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH,
        P5_USER_VALIDATION_PLAYBOOK_PATH,
    )
    missing = [_serialize_repo_relative_path(path) for path in paths if not path.exists()]
    return P5M5S8Check(
        name="m5_s8_required_paths_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s8_artifacts() -> list[P5M5S8Check]:
    checks = [
        _json_artifact_check(
            name="m5_s8_stage_truth_index_matches_helper",
            path=P5_M5_S8_STAGE_TRUTH_INDEX_PATH,
            expected=build_p5_m5_s8_stage_truth_index(),
        ),
        _json_artifact_check(
            name="m5_s8_cross_stage_consistency_matches_helper",
            path=P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH,
            expected=build_p5_m5_s8_cross_stage_consistency(),
        ),
        _json_artifact_check(
            name="m5_s8_closeout_checks_match_helper",
            path=P5_M5_S8_CLOSEOUT_CHECKS_PATH,
            expected=build_p5_m5_s8_closeout_checks(),
        ),
        _json_artifact_check(
            name="m5_s8_non_claims_match_helper",
            path=P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH,
            expected=build_p5_m5_s8_non_claims_and_deferred_scope(),
        ),
        _json_artifact_check(
            name="m5_s8_user_validation_plan_matches_helper",
            path=P5_M5_S8_USER_VALIDATION_PLAN_PATH,
            expected=build_p5_m5_s8_user_validation_plan(),
        ),
        _json_artifact_check(
            name="m5_s8_user_feedback_template_matches_helper",
            path=P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH,
            expected=build_p5_m5_s8_user_feedback_template(),
        ),
        _required_paths_exist(),
    ]

    stage_index = build_p5_m5_s8_stage_truth_index()
    stages = cast(list[dict[str, object]], stage_index["stages"])
    checks.append(
        P5M5S8Check(
            name="m5_s8_stage_count_and_statuses_complete",
            ok=stage_index["stage_count"] == 8
            and all(stage["status"] == "complete" for stage in stages),
            detail=str([stage["stage_id"] for stage in stages]),
        )
    )

    checks.append(
        P5M5S8Check(
            name="m5_s8_cross_stage_checks_all_pass",
            ok=all(
                check["machine_checkable_status"] == "pass"
                for check in cast(
                    list[dict[str, object]],
                    build_p5_m5_s8_cross_stage_consistency()["checks"],
                )
            ),
            detail="all pass",
        )
    )

    non_claims = build_p5_m5_s8_non_claims_and_deferred_scope()
    checks.append(
        P5M5S8Check(
            name="m5_s8_non_claims_are_explicit_and_bounded",
            ok=non_claims["non_claim_count"] == 13,
            detail=str(
                [
                    item["non_claim_id"]
                    for item in cast(list[dict[str, object]], non_claims["non_claims"])
                ]
            ),
        )
    )

    user_plan = build_p5_m5_s8_user_validation_plan()
    checks.append(
        P5M5S8Check(
            name="m5_s8_user_validation_prep_stays_prep_only",
            ok=user_plan["execution_status"] == "prep_only_not_executed"
            and user_plan["source_code_reading_required"] is False
            and user_plan["platform_side_visual_checking_role"] == "secondary_corroboration_only",
            detail=str(user_plan["execution_status"]),
        )
    )

    feedback = build_p5_m5_s8_user_feedback_template()
    checks.append(
        P5M5S8Check(
            name="m5_s8_feedback_template_stays_classified",
            ok=cast(list[object], feedback["severity_enum"])
            == [
                "blocker",
                "major",
                "minor",
                "note",
            ],
            detail=str(feedback["severity_enum"]),
        )
    )

    if P5_M5_S8_CLOSEOUT_REPORT_PATH.exists():
        report_text = P5_M5_S8_CLOSEOUT_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S8 freezes M5 final readiness and closeout truth; it does not redo S0-S7.",
            "S8 does not run final user validation yet; it prepares it.",
            "Platform-side corroboration remains secondary, not primary truth.",
            (
                "The external runtime-home p45/env surface remains authoritative "
                "and is not replaced."
            ),
            (
                "M5 can close only when stage truth, consistency, closeout checks, "
                "and non-claims remain green."
            ),
            (
                "P5 final user-style validation can begin after S8, but P6 "
                "product shell is not claimed."
            ),
        )
        missing = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S8Check(
                name="m5_s8_report_required_phrases",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
    else:
        checks.append(
            P5M5S8Check(
                name="m5_s8_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S8_CLOSEOUT_REPORT_PATH),
            )
        )

    if P5_USER_VALIDATION_PLAYBOOK_PATH.exists():
        playbook_text = P5_USER_VALIDATION_PLAYBOOK_PATH.read_text(encoding="utf-8")
        playbook_required_phrases = (
            "This is a P5 final user validation prep package, not the execution itself.",
            "This playbook does not claim P6 product shell is done.",
            "Source-code reading is not required.",
            "Platform-side visual checking is secondary corroboration.",
            "User feedback must be classified instead of turning into vague scope expansion.",
        )
        missing = [phrase for phrase in playbook_required_phrases if phrase not in playbook_text]
        checks.append(
            P5M5S8Check(
                name="m5_s8_playbook_required_phrases",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
    else:
        checks.append(
            P5M5S8Check(
                name="m5_s8_playbook_required_phrases",
                ok=False,
                detail=str(P5_USER_VALIDATION_PLAYBOOK_PATH),
            )
        )

    return checks


def format_p5_m5_s8_results(results: list[P5M5S8Check]) -> str:
    lines = ["P5-M5-S8 closeout checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s8_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S8 final readiness and closeout:",
            "- scope: stage aggregation, consistency, closeout, non-claims, user-validation prep",
            "- this does not run final user validation yet; it prepares it",
            "- source-code reading is not required for closeout truth consumption",
            "- platform-side corroboration remains secondary, not primary",
            "- no installer, deployment, cloud, P6, or commercial scope is implied",
        )
    )


def render_p5_m5_s8_json() -> str:
    bundle = {
        "stage_truth_index": build_p5_m5_s8_stage_truth_index(),
        "cross_stage_consistency": build_p5_m5_s8_cross_stage_consistency(),
        "closeout_checks": build_p5_m5_s8_closeout_checks(),
        "non_claims_and_deferred_scope": build_p5_m5_s8_non_claims_and_deferred_scope(),
        "user_validation_plan": build_p5_m5_s8_user_validation_plan(),
        "user_feedback_template": build_p5_m5_s8_user_feedback_template(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
