from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p5_benchmark import (
    P5_BENCHMARK_REGISTRY_PATH,
    P5_BENCHMARK_REPORT_PATH,
)
from zephyr_ingest.testing.p5_bounded_concurrency import (
    P5_BOUNDED_CONCURRENCY_MATRIX_PATH,
    P5_BOUNDED_CONCURRENCY_REPORT_PATH,
)
from zephyr_ingest.testing.p5_capability_domains import (
    P5_BUILD_CUT_MANIFEST_PATH,
    P5_CAPABILITY_DOMAIN_MATRIX_PATH,
    P5_CAPABILITY_DOMAIN_REPORT_PATH,
    P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH,
)
from zephyr_ingest.testing.p5_governance_audit import (
    P5_GOVERNANCE_ACTION_MATRIX_PATH,
    P5_GOVERNANCE_AUDIT_REPORT_PATH,
    P5_GOVERNANCE_USAGE_LINKAGE_PATH,
    P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH,
    P5_SOURCE_SPEC_PARITY_PATH,
)
from zephyr_ingest.testing.p5_m4_s14_closeout import (
    P5_GOVERNANCE_VOCABULARY_MATRIX_PATH,
    P5_INSPECT_RECEIPT_CONTRACT_PATH,
    P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH,
    P5_M4_RESIDUAL_RISK_PATH,
    P5_SOURCE_LINKAGE_V2_PATH,
    P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH,
)
from zephyr_ingest.testing.p5_m5_s1 import P5_M5_S1_EXECUTION_PLAN_PATH
from zephyr_ingest.testing.p5_m5_s2 import P5_M5_S2_EXECUTION_PLAN_PATH
from zephyr_ingest.testing.p5_m5_s3 import P5_M5_S3_EXECUTION_PLAN_PATH
from zephyr_ingest.testing.p5_m5_s4 import (
    P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH,
    P5_M5_S4_OPERATOR_QUESTIONS_PATH,
    P5_M5_S4_OPERATOR_SCENARIOS_PATH,
    P5_M5_S4_SCORING_RULES_PATH,
)
from zephyr_ingest.testing.p5_m5_validation import (
    P5_M5_DEEP_ANCHOR_SELECTION_PATH,
    P5_M5_RETAINED_SURFACE_INVENTORY_PATH,
    P5_M5_VALIDATION_MATRIX_PATH,
)
from zephyr_ingest.testing.p5_product_cuts import (
    P5_DEPENDENCY_SLICE_MANIFEST_PATH,
    P5_DESTINATION_SURFACE_CLASSIFICATION_PATH,
    P5_PACKAGING_OUTPUT_MANIFEST_PATH,
    P5_PACKAGING_SKELETON_REPORT_PATH,
    P5_PRODUCT_CUT_MANIFEST_PATH,
)
from zephyr_ingest.testing.p5_recovery_operator import (
    P5_RECOVERY_MATRIX_PATH,
    P5_RECOVERY_RUNBOOK_PATH,
)
from zephyr_ingest.testing.p5_runtime_boundary import (
    P5_RUNTIME_BOUNDARY_MATRIX_PATH,
    P5_RUNTIME_BOUNDARY_REPORT_PATH,
)
from zephyr_ingest.testing.p5_source_contracts import (
    P5_SOURCE_CONTRACT_MATRIX_PATH,
    P5_SOURCE_CONTRACT_REPORT_PATH,
    P5_SOURCE_USAGE_LINKAGE_PATH,
    P5_USAGE_RUNTIME_CONTRACT_PATH,
    P5_USAGE_RUNTIME_REPORT_PATH,
)
from zephyr_ingest.testing.p5_usage_facts import (
    P5_USAGE_FACT_MANIFEST_PATH,
    P5_USAGE_FACT_REPORT_PATH,
    P5_USAGE_LINKAGE_CONTRACT_PATH,
)
from zephyr_ingest.testing.p45 import repo_root

M5S5Severity = Literal["error", "warning", "info"]

P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s5_release_bundle_inventory.json"
)
P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_S5_RELEASE_BUNDLE.md"
)
P5_M5_S5_RELEASE_MANIFEST_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s5_release_manifest.json"
)
P5_M5_S5_BUNDLE_LAYOUT_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s5_bundle_layout.json"
)
P5_M5_S5_HANDOFF_CONTRACT_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s5_handoff_contract.json"
)
P5_M5_S5_COMPLETENESS_CHECKS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s5_completeness_checks.json"
)


def _artifact_entry(
    *,
    surface_id: str,
    canonical_path: Path,
    readability: str,
    classification: str,
    future_consumers: tuple[str, ...],
    bundle_presence: str,
    ownership_role: str,
) -> dict[str, object]:
    return {
        "surface_id": surface_id,
        "canonical_path": str(canonical_path.relative_to(repo_root())),
        "readability": readability,
        "classification": classification,
        "future_consumers": list(future_consumers),
        "bundle_presence": bundle_presence,
        "ownership_role": ownership_role,
        "belongs_inside_release_consumable_bundle": True,
    }


def _helper_entry(
    *,
    entrypoint_id: str,
    tool_name: str,
    primary_truth_groups: tuple[str, ...],
    future_consumers: tuple[str, ...],
) -> dict[str, object]:
    return {
        "entrypoint_id": entrypoint_id,
        "canonical_path": f"tools/{tool_name}",
        "readability": "executable_helper_report",
        "classification": "required",
        "future_consumers": list(future_consumers),
        "bundle_presence": "referenced_by_manifest",
        "ownership_role": "canonical_helper_report_entrypoint",
        "primary_truth_groups": list(primary_truth_groups),
        "belongs_inside_release_consumable_bundle": True,
    }


_HELPER_ENTRYPOINTS: Final[tuple[dict[str, object], ...]] = (
    _helper_entry(
        entrypoint_id="runtime_boundary_report",
        tool_name="p5_runtime_boundary_report.py",
        primary_truth_groups=("runtime_bounded_benchmark",),
        future_consumers=("product_shell", "installer_release_system", "operator_support"),
    ),
    _helper_entry(
        entrypoint_id="benchmark_report",
        tool_name="p5_benchmark_report.py",
        primary_truth_groups=("runtime_bounded_benchmark",),
        future_consumers=("product_shell", "installer_release_system", "internal_validation_only"),
    ),
    _helper_entry(
        entrypoint_id="bounded_concurrency_report",
        tool_name="p5_bounded_concurrency_report.py",
        primary_truth_groups=("runtime_bounded_benchmark",),
        future_consumers=("operator_support", "internal_validation_only"),
    ),
    _helper_entry(
        entrypoint_id="capability_domain_report",
        tool_name="p5_capability_domain_report.py",
        primary_truth_groups=("capability_domain_product_cut",),
        future_consumers=("product_shell", "installer_release_system"),
    ),
    _helper_entry(
        entrypoint_id="product_cut_report",
        tool_name="p5_product_cut_report.py",
        primary_truth_groups=("capability_domain_product_cut",),
        future_consumers=("product_shell", "installer_release_system"),
    ),
    _helper_entry(
        entrypoint_id="recovery_operator_report",
        tool_name="p5_recovery_operator_report.py",
        primary_truth_groups=("recovery_governance_manual_action",),
        future_consumers=("operator_support", "product_shell"),
    ),
    _helper_entry(
        entrypoint_id="governance_audit_report",
        tool_name="p5_governance_audit_report.py",
        primary_truth_groups=("recovery_governance_manual_action",),
        future_consumers=("operator_support", "installer_release_system"),
    ),
    _helper_entry(
        entrypoint_id="usage_report",
        tool_name="p5_usage_report.py",
        primary_truth_groups=("usage_source_contract_provenance",),
        future_consumers=("operator_support", "product_shell"),
    ),
    _helper_entry(
        entrypoint_id="source_contract_report",
        tool_name="p5_source_contract_report.py",
        primary_truth_groups=("usage_source_contract_provenance",),
        future_consumers=("operator_support", "product_shell", "installer_release_system"),
    ),
    _helper_entry(
        entrypoint_id="m4_closeout_report",
        tool_name="p5_m4_closeout_report.py",
        primary_truth_groups=("m4_m5_stage_truths",),
        future_consumers=("installer_release_system", "internal_validation_only"),
    ),
    _helper_entry(
        entrypoint_id="m5_validation_report",
        tool_name="p5_m5_validation_report.py",
        primary_truth_groups=("m4_m5_stage_truths",),
        future_consumers=("installer_release_system", "internal_validation_only"),
    ),
    _helper_entry(
        entrypoint_id="m5_s1_report",
        tool_name="p5_m5_s1_report.py",
        primary_truth_groups=("m4_m5_stage_truths",),
        future_consumers=("internal_validation_only",),
    ),
    _helper_entry(
        entrypoint_id="m5_s2_report",
        tool_name="p5_m5_s2_report.py",
        primary_truth_groups=("m4_m5_stage_truths",),
        future_consumers=("internal_validation_only",),
    ),
    _helper_entry(
        entrypoint_id="m5_s3_report",
        tool_name="p5_m5_s3_report.py",
        primary_truth_groups=("m4_m5_stage_truths",),
        future_consumers=("internal_validation_only",),
    ),
    _helper_entry(
        entrypoint_id="m5_s4_report",
        tool_name="p5_m5_s4_report.py",
        primary_truth_groups=("m4_m5_stage_truths",),
        future_consumers=("operator_support", "internal_validation_only"),
    ),
    _helper_entry(
        entrypoint_id="m5_s5_report",
        tool_name="p5_m5_s5_report.py",
        primary_truth_groups=("release_consumable_bundle",),
        future_consumers=("product_shell", "installer_release_system", "operator_support"),
    ),
)


def build_p5_m5_s5_release_bundle_inventory() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S5 release-consumable hardening",
        "bundle_kind": "release_consumable_truth_package",
        "bundle_root_model": {
            "validation_root": "validation",
            "helper_entrypoint_root": "tools",
            "authoritative_runtime_env": "external_runtime_home_p45_env",
            "runtime_env_replaced_by_bundle": False,
        },
        "inventory_groups": {
            "runtime_bounded_benchmark": {
                "purpose": "bounded runtime, benchmark, and concurrency truth surfaces",
                "surfaces": [
                    _artifact_entry(
                        surface_id="runtime_boundary_matrix",
                        canonical_path=P5_RUNTIME_BOUNDARY_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="runtime_boundary_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="runtime_boundary_report",
                        canonical_path=P5_RUNTIME_BOUNDARY_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="runtime_boundary_explanatory_report",
                    ),
                    _artifact_entry(
                        surface_id="benchmark_registry",
                        canonical_path=P5_BENCHMARK_REGISTRY_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="benchmark_truth_registry",
                    ),
                    _artifact_entry(
                        surface_id="benchmark_report",
                        canonical_path=P5_BENCHMARK_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("product_shell", "operator_support"),
                        bundle_presence="bundled_directly",
                        ownership_role="benchmark_explanatory_report",
                    ),
                    _artifact_entry(
                        surface_id="bounded_concurrency_matrix",
                        canonical_path=P5_BOUNDED_CONCURRENCY_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="bounded_concurrency_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="bounded_concurrency_report",
                        canonical_path=P5_BOUNDED_CONCURRENCY_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("operator_support", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="bounded_concurrency_explanatory_report",
                    ),
                ],
            },
            "recovery_governance_manual_action": {
                "purpose": "recovery, governance, manual-action, and receipt truth surfaces",
                "surfaces": [
                    _artifact_entry(
                        surface_id="recovery_matrix",
                        canonical_path=P5_RECOVERY_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="recovery_operator_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="recovery_runbook",
                        canonical_path=P5_RECOVERY_RUNBOOK_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("operator_support",),
                        bundle_presence="bundled_directly",
                        ownership_role="recovery_operator_explanatory_report",
                    ),
                    _artifact_entry(
                        surface_id="governance_action_matrix",
                        canonical_path=P5_GOVERNANCE_ACTION_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="governance_action_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="manual_action_audit_manifest",
                        canonical_path=P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="governance_audit_manifest_truth",
                    ),
                    _artifact_entry(
                        surface_id="governance_usage_linkage",
                        canonical_path=P5_GOVERNANCE_USAGE_LINKAGE_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="governance_usage_linkage_truth",
                    ),
                    _artifact_entry(
                        surface_id="source_spec_parity",
                        canonical_path=P5_SOURCE_SPEC_PARITY_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="source_spec_parity_truth",
                    ),
                    _artifact_entry(
                        surface_id="verify_recovery_result_contract",
                        canonical_path=P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="verify_action_contract_truth",
                    ),
                    _artifact_entry(
                        surface_id="inspect_receipt_contract",
                        canonical_path=P5_INSPECT_RECEIPT_CONTRACT_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="inspect_receipt_contract_truth",
                    ),
                    _artifact_entry(
                        surface_id="governance_vocabulary_matrix",
                        canonical_path=P5_GOVERNANCE_VOCABULARY_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="governance_vocabulary_truth",
                    ),
                    _artifact_entry(
                        surface_id="governance_audit_report",
                        canonical_path=P5_GOVERNANCE_AUDIT_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="governance_explanatory_report",
                    ),
                ],
            },
            "usage_source_contract_provenance": {
                "purpose": "usage, source-contract, source-linkage, and provenance truth surfaces",
                "surfaces": [
                    _artifact_entry(
                        surface_id="usage_fact_manifest",
                        canonical_path=P5_USAGE_FACT_MANIFEST_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "operator_support"),
                        bundle_presence="bundled_directly",
                        ownership_role="usage_fact_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="usage_linkage_contract",
                        canonical_path=P5_USAGE_LINKAGE_CONTRACT_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="usage_linkage_truth",
                    ),
                    _artifact_entry(
                        surface_id="source_contract_matrix",
                        canonical_path=P5_SOURCE_CONTRACT_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="source_contract_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="usage_runtime_contract",
                        canonical_path=P5_USAGE_RUNTIME_CONTRACT_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "operator_support"),
                        bundle_presence="bundled_directly",
                        ownership_role="usage_runtime_truth",
                    ),
                    _artifact_entry(
                        surface_id="source_usage_linkage",
                        canonical_path=P5_SOURCE_USAGE_LINKAGE_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "operator_support"),
                        bundle_presence="bundled_directly",
                        ownership_role="source_usage_linkage_truth",
                    ),
                    _artifact_entry(
                        surface_id="source_linkage_v2",
                        canonical_path=P5_SOURCE_LINKAGE_V2_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="canonical_source_linkage_truth",
                    ),
                    _artifact_entry(
                        surface_id="usage_fact_report",
                        canonical_path=P5_USAGE_FACT_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("operator_support",),
                        bundle_presence="bundled_directly",
                        ownership_role="usage_fact_explanatory_report",
                    ),
                    _artifact_entry(
                        surface_id="source_contract_report",
                        canonical_path=P5_SOURCE_CONTRACT_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="source_contract_explanatory_report",
                    ),
                    _artifact_entry(
                        surface_id="usage_runtime_report",
                        canonical_path=P5_USAGE_RUNTIME_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="usage_runtime_explanatory_report",
                    ),
                ],
            },
            "capability_domain_product_cut": {
                "purpose": "capability-domain and product-cut technical truth surfaces",
                "surfaces": [
                    _artifact_entry(
                        surface_id="capability_domain_matrix",
                        canonical_path=P5_CAPABILITY_DOMAIN_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="capability_domain_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="dependency_boundary_manifest",
                        canonical_path=P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="dependency_boundary_truth",
                    ),
                    _artifact_entry(
                        surface_id="build_cut_manifest",
                        canonical_path=P5_BUILD_CUT_MANIFEST_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="build_cut_truth",
                    ),
                    _artifact_entry(
                        surface_id="product_cut_manifest",
                        canonical_path=P5_PRODUCT_CUT_MANIFEST_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="product_cut_truth_ssot",
                    ),
                    _artifact_entry(
                        surface_id="dependency_slice_manifest",
                        canonical_path=P5_DEPENDENCY_SLICE_MANIFEST_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system",),
                        bundle_presence="bundled_directly",
                        ownership_role="dependency_slice_truth",
                    ),
                    _artifact_entry(
                        surface_id="destination_surface_classification",
                        canonical_path=P5_DESTINATION_SURFACE_CLASSIFICATION_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="destination_surface_truth",
                    ),
                    _artifact_entry(
                        surface_id="packaging_output_manifest",
                        canonical_path=P5_PACKAGING_OUTPUT_MANIFEST_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="packaging_skeleton_truth",
                    ),
                    _artifact_entry(
                        surface_id="capability_domain_report",
                        canonical_path=P5_CAPABILITY_DOMAIN_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("product_shell", "operator_support"),
                        bundle_presence="bundled_directly",
                        ownership_role="capability_domain_explanatory_report",
                    ),
                    _artifact_entry(
                        surface_id="packaging_skeleton_report",
                        canonical_path=P5_PACKAGING_SKELETON_REPORT_PATH,
                        readability="human_readable",
                        classification="derived",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="product_cut_explanatory_report",
                    ),
                ],
            },
            "m4_m5_stage_truths": {
                "purpose": "M4 closeout and M5 stage truth packages needed for handoff",
                "surfaces": [
                    _artifact_entry(
                        surface_id="m4_closeout_truth_matrix",
                        canonical_path=P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="m4_closeout_gate_truth",
                    ),
                    _artifact_entry(
                        surface_id="m4_residual_risk",
                        canonical_path=P5_M4_RESIDUAL_RISK_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("product_shell", "installer_release_system"),
                        bundle_presence="bundled_directly",
                        ownership_role="post_m4_non_claim_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_validation_matrix",
                        canonical_path=P5_M5_VALIDATION_MATRIX_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_scope_and_matrix_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_retained_surface_inventory",
                        canonical_path=P5_M5_RETAINED_SURFACE_INVENTORY_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("installer_release_system", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_retained_surface_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_deep_anchor_selection",
                        canonical_path=P5_M5_DEEP_ANCHOR_SELECTION_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("internal_validation_only",),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_deep_anchor_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_s1_execution_plan",
                        canonical_path=P5_M5_S1_EXECUTION_PLAN_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("internal_validation_only",),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_s1_baseline_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_s2_execution_plan",
                        canonical_path=P5_M5_S2_EXECUTION_PLAN_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("internal_validation_only",),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_s2_deep_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_s3_execution_plan",
                        canonical_path=P5_M5_S3_EXECUTION_PLAN_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("internal_validation_only",),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_s3_stability_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_s4_operator_scenarios",
                        canonical_path=P5_M5_S4_OPERATOR_SCENARIOS_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_s4_scenario_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_s4_operator_questions",
                        canonical_path=P5_M5_S4_OPERATOR_QUESTIONS_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support",),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_s4_question_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_s4_operator_evidence_surfaces",
                        canonical_path=P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "product_shell"),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_s4_evidence_surface_truth",
                    ),
                    _artifact_entry(
                        surface_id="m5_s4_scoring_rules",
                        canonical_path=P5_M5_S4_SCORING_RULES_PATH,
                        readability="machine_readable",
                        classification="required",
                        future_consumers=("operator_support", "internal_validation_only"),
                        bundle_presence="bundled_directly",
                        ownership_role="m5_s4_scoring_truth",
                    ),
                ],
            },
            "helper_report_canonical_entrypoints": {
                "purpose": "canonical helper/report entrypoints for outer-layer discovery",
                "surfaces": list(_HELPER_ENTRYPOINTS),
            },
        },
    }


def build_p5_m5_s5_release_manifest() -> dict[str, object]:
    inventory = build_p5_m5_s5_release_bundle_inventory()
    inventory_groups = cast(dict[str, object], inventory["inventory_groups"])
    helper_group = cast(dict[str, object], inventory_groups["helper_report_canonical_entrypoints"])
    helper_entries = cast(
        list[dict[str, object]],
        helper_group["surfaces"],
    )
    return {
        "schema_version": 1,
        "phase": "P5-M5-S5 release manifest",
        "bundle_root": {
            "logical_root": "validation",
            "helper_entrypoint_root": "tools",
            "manifest_kind": "reference_bundle_not_archive",
            "installer_or_deployment_bundle": False,
        },
        "inventory_pointer": "validation/p5_m5_s5_release_bundle_inventory.json",
        "layout_pointer": "validation/p5_m5_s5_bundle_layout.json",
        "handoff_pointer": "validation/p5_m5_s5_handoff_contract.json",
        "completeness_pointer": "validation/p5_m5_s5_completeness_checks.json",
        "artifact_groups": [
            {
                "group_id": group_id,
                "copy_model": "bundled_directly"
                if group_id != "helper_report_canonical_entrypoints"
                else "referenced_by_manifest",
                "required": True,
            }
            for group_id in inventory_groups
        ],
        "section_split": {
            "required_sections": [
                "inventory_pointer",
                "layout_pointer",
                "handoff_pointer",
                "completeness_pointer",
                "canonical_helper_report_mapping",
            ],
            "optional_sections": [
                "human_readable_reports",
            ],
            "derived_sections": [
                "helper_rendered_json_or_summary_outputs",
            ],
        },
        "versioning": {
            "phase_anchor": "P5-M5-S5",
            "schema_version": 1,
            "depends_on_truth": [
                "P5-M4-S14 closeout",
                "P5-M5-S0 validation freeze",
                "P5-M5-S1 baseline execution",
                "P5-M5-S2 deep execution",
                "P5-M5-S3 stability package",
                "P5-M5-S4 cold operator package",
            ],
        },
        "canonical_helper_report_mapping": {
            cast(str, entry["entrypoint_id"]): {
                "path": cast(str, entry["canonical_path"]),
                "primary_truth_groups": cast(list[str], entry["primary_truth_groups"]),
            }
            for entry in helper_entries
        },
        "not_claimed": [
            "installer implementation",
            "deployment packaging",
            "cloud or Kubernetes release bundle",
            "runtime env replacement",
        ],
    }


def build_p5_m5_s5_bundle_layout() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S5 bundle layout",
        "layout_groups": [
            {
                "group_id": "machine_readable_truth",
                "root": "validation",
                "classification": "required",
                "contains": [
                    "inventory",
                    "manifest",
                    "layout",
                    "handoff_contract",
                    "completeness_checks",
                    "canonical_machine_readable_truth_surfaces",
                ],
                "ownership_expectation": "SSOT-backed validation artifacts",
                "completeness_checkable": True,
            },
            {
                "group_id": "human_readable_reports",
                "root": "validation",
                "classification": "derived",
                "contains": [
                    "release bundle report",
                    "group explanatory reports",
                ],
                "ownership_expectation": "derived reports from machine-readable truth",
                "completeness_checkable": True,
            },
            {
                "group_id": "canonical_helper_entrypoints",
                "root": "tools",
                "classification": "required",
                "contains": [
                    "family helper/report entrypoints",
                    "bundle discovery helper",
                ],
                "ownership_expectation": "stable outer-layer entrypoints referenced by manifest",
                "completeness_checkable": True,
            },
            {
                "group_id": "runtime_plane_non_bundle_boundary",
                "root": "external_runtime_home",
                "classification": "optional",
                "contains": [
                    "authoritative p45/env",
                    "real compose/runtime env files",
                ],
                "ownership_expectation": (
                    "runtime-plane truth is referenced, not copied into bundle"
                ),
                "completeness_checkable": False,
            },
        ],
        "machine_vs_human_split": {
            "machine_readable_root": "validation",
            "human_readable_root": "validation",
            "helper_entrypoint_root": "tools",
        },
        "what_belongs_together": {
            "inventory_manifest_layout_handoff_completeness": (
                "These five S5 artifacts define bundle discovery, content, structure, "
                "and drift checks."
            ),
            "family_truth_surfaces": (
                "Earlier P5 truth packages remain canonical family inputs consumed by "
                "the S5 bundle."
            ),
            "helper_entrypoints": (
                "Helper/report scripts remain stable discovery surfaces rather than "
                "hidden author tools."
            ),
        },
        "not_in_layout": [
            "installer payload structure",
            "deployment manifests",
            "cloud or Kubernetes packaging layout",
            "private temporary env replacement for p45/env",
        ],
    }


def build_p5_m5_s5_handoff_contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S5 handoff contract",
        "outer_layer_discovery": {
            "primary_helper_entrypoint": "tools/p5_m5_s5_report.py",
            "canonical_inventory_path": "validation/p5_m5_s5_release_bundle_inventory.json",
            "canonical_manifest_path": "validation/p5_m5_s5_release_manifest.json",
            "canonical_layout_path": "validation/p5_m5_s5_bundle_layout.json",
            "canonical_handoff_path": "validation/p5_m5_s5_handoff_contract.json",
            "canonical_completeness_path": "validation/p5_m5_s5_completeness_checks.json",
            "discovery_order": [
                "resolve primary helper or canonical inventory path",
                "read release manifest",
                "read bundle inventory",
                "resolve needed truth group or helper mapping",
                "run completeness checks before consumption",
            ],
        },
        "truth_group_lookup": {
            "runtime_bounded_benchmark": [
                "validation/p5_runtime_boundary_matrix.json",
                "validation/p5_benchmark_scenario_registry.json",
                "validation/p5_bounded_concurrency_matrix.json",
            ],
            "recovery_governance_manual_action": [
                "validation/p5_recovery_scenario_matrix.json",
                "validation/p5_governance_action_matrix.json",
                "validation/p5_manual_action_audit_manifest.json",
                "validation/p5_governance_usage_linkage.json",
            ],
            "usage_source_contract_provenance": [
                "validation/p5_usage_fact_manifest.json",
                "validation/p5_source_contract_matrix.json",
                "validation/p5_usage_runtime_contract.json",
                "validation/p5_source_linkage_v2.json",
            ],
            "capability_domain_product_cut": [
                "validation/p5_capability_domain_matrix.json",
                "validation/p5_product_cut_manifest.json",
            ],
            "m4_m5_stage_truths": [
                "validation/p5_m4_closeout_truth_matrix.json",
                "validation/p5_m5_validation_matrix.json",
                "validation/p5_m5_s1_execution_plan.json",
                "validation/p5_m5_s2_execution_plan.json",
                "validation/p5_m5_s3_execution_plan.json",
                "validation/p5_m5_s4_operator_scenarios.json",
            ],
        },
        "completeness_verification": {
            "helper_cli_check": "tools/p5_m5_s5_report.py --check-artifacts",
            "machine_readable_check_spec": "validation/p5_m5_s5_completeness_checks.json",
        },
        "must_not_infer": [
            "installer implementation is complete",
            "deployment packaging is complete",
            "distributed runtime or cloud readiness",
            "billing or entitlement meaning from technical truth surfaces",
            "RBAC or approval workflow semantics",
            "fanout is a sink connector",
            "source code reading is a required discovery step",
        ],
        "source_code_reading_allowed": False,
        "runtime_env_authority": (
            "p45/env under the external runtime-home remains the authoritative retained runtime "
            "environment surface and is not replaced by this repo bundle."
        ),
    }


def build_p5_m5_s5_completeness_checks() -> dict[str, object]:
    helper_paths = [cast(str, entry["canonical_path"]) for entry in _HELPER_ENTRYPOINTS]
    return {
        "schema_version": 1,
        "phase": "P5-M5-S5 completeness checks",
        "required_inventory_groups": [
            "runtime_bounded_benchmark",
            "recovery_governance_manual_action",
            "usage_source_contract_provenance",
            "capability_domain_product_cut",
            "m4_m5_stage_truths",
            "helper_report_canonical_entrypoints",
        ],
        "checks": [
            {
                "name": "inventory_group_coverage",
                "kind": "inventory_completeness",
                "required": True,
                "expectation": "all required inventory groups exist",
            },
            {
                "name": "manifest_pointer_completeness",
                "kind": "manifest_layout_completeness",
                "required": True,
                "expectation": "inventory/layout/handoff/completeness pointers are all present",
            },
            {
                "name": "required_canonical_paths_exist",
                "kind": "canonical_path_existence",
                "required": True,
                "expectation": "every required artifact and helper path exists",
            },
            {
                "name": "required_optional_derived_split_is_explicit",
                "kind": "classification_correctness",
                "required": True,
                "expectation": "required/optional/derived split is explicit in manifest and layout",
            },
            {
                "name": "helper_report_mapping_consistent",
                "kind": "helper_path_consistency",
                "required": True,
                "expectation": "manifest helper mapping matches inventory helper entrypoints",
            },
            {
                "name": "out_of_scope_clarity_explicit",
                "kind": "out_of_scope_clarity",
                "required": True,
                "expectation": "installer/cloud/commercial non-claims stay explicit",
            },
            {
                "name": "no_source_code_reading_dependency",
                "kind": "consumption_boundary",
                "required": True,
                "expectation": "outer layers can discover bundle truth without source-code reading",
            },
            {
                "name": "runtime_env_authority_not_replaced",
                "kind": "runtime_env_boundary",
                "required": True,
                "expectation": "external p45/env stays authoritative and is not silently replaced",
            },
        ],
        "required_helper_paths": helper_paths,
        "forbidden_consumption_patterns": [
            "discovering bundle contents by reading helper source code",
            "inventing hidden artifact paths outside inventory and manifest",
            "treating release-consumable bundle as deployment or cloud package",
            "replacing external runtime env truth with a private temporary env",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5M5S5Check:
    name: str
    ok: bool
    detail: str
    severity: M5S5Severity = "error"


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
) -> P5M5S5Check:
    observed = _read_json_object(path)
    return P5M5S5Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _required_inventory_paths_exist() -> P5M5S5Check:
    inventory = build_p5_m5_s5_release_bundle_inventory()
    inventory_groups = cast(dict[str, object], inventory["inventory_groups"])
    required_paths: list[str] = []
    for group in inventory_groups.values():
        surfaces = cast(list[dict[str, object]], cast(dict[str, object], group)["surfaces"])
        for surface in surfaces:
            if surface["classification"] == "required":
                required_paths.append(cast(str, surface["canonical_path"]))
    missing = [path for path in required_paths if not (repo_root() / path).exists()]
    return P5M5S5Check(
        name="m5_s5_required_paths_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s5_artifacts() -> list[P5M5S5Check]:
    checks = [
        _json_artifact_check(
            name="m5_s5_release_bundle_inventory_matches_helper",
            path=P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH,
            expected=build_p5_m5_s5_release_bundle_inventory(),
        ),
        _json_artifact_check(
            name="m5_s5_release_manifest_matches_helper",
            path=P5_M5_S5_RELEASE_MANIFEST_PATH,
            expected=build_p5_m5_s5_release_manifest(),
        ),
        _json_artifact_check(
            name="m5_s5_bundle_layout_matches_helper",
            path=P5_M5_S5_BUNDLE_LAYOUT_PATH,
            expected=build_p5_m5_s5_bundle_layout(),
        ),
        _json_artifact_check(
            name="m5_s5_handoff_contract_matches_helper",
            path=P5_M5_S5_HANDOFF_CONTRACT_PATH,
            expected=build_p5_m5_s5_handoff_contract(),
        ),
        _json_artifact_check(
            name="m5_s5_completeness_checks_match_helper",
            path=P5_M5_S5_COMPLETENESS_CHECKS_PATH,
            expected=build_p5_m5_s5_completeness_checks(),
        ),
        _required_inventory_paths_exist(),
    ]

    inventory = build_p5_m5_s5_release_bundle_inventory()
    inventory_groups = cast(dict[str, object], inventory["inventory_groups"])
    checks.append(
        P5M5S5Check(
            name="m5_s5_inventory_group_coverage",
            ok=set(inventory_groups)
            == {
                "runtime_bounded_benchmark",
                "recovery_governance_manual_action",
                "usage_source_contract_provenance",
                "capability_domain_product_cut",
                "m4_m5_stage_truths",
                "helper_report_canonical_entrypoints",
            },
            detail=str(list(inventory_groups.keys())),
        )
    )

    manifest = build_p5_m5_s5_release_manifest()
    manifest_root = cast(dict[str, object], manifest["bundle_root"])
    checks.append(
        P5M5S5Check(
            name="m5_s5_manifest_stays_release_consumable_not_installer",
            ok=manifest_root["manifest_kind"] == "reference_bundle_not_archive"
            and manifest_root["installer_or_deployment_bundle"] is False,
            detail=str(manifest_root),
        )
    )

    handoff = build_p5_m5_s5_handoff_contract()
    checks.append(
        P5M5S5Check(
            name="m5_s5_handoff_prohibits_source_code_dependency",
            ok=handoff["source_code_reading_allowed"] is False,
            detail=str(handoff["source_code_reading_allowed"]),
        )
    )

    completeness = build_p5_m5_s5_completeness_checks()
    checks.append(
        P5M5S5Check(
            name="m5_s5_completeness_checks_are_bounded_and_explicit",
            ok=len(cast(list[object], completeness["checks"])) == 8,
            detail=str(
                [check["name"] for check in cast(list[dict[str, object]], completeness["checks"])]
            ),
        )
    )

    if P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH.exists():
        report_text = P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S5 hardens a release-consumable truth bundle, not installer implementation.",
            "S5 does not repeat S3 stability work or S4 cold-operator proof.",
            (
                "Outer layers discover canonical truth through the bundle artifacts "
                "and helper/report entrypoints, not source code."
            ),
            (
                "The external runtime-home p45/env surface remains authoritative and "
                "is not replaced by this bundle."
            ),
            (
                "Capability domains, product cuts, source truth, governance, and "
                "fanout remain technical truth surfaces, not commercial ones."
            ),
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S5Check(
                name="m5_s5_report_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )
    else:
        checks.append(
            P5M5S5Check(
                name="m5_s5_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH),
            )
        )

    return checks


def format_p5_m5_s5_results(results: list[P5M5S5Check]) -> str:
    lines = ["P5-M5-S5 release-consumable checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s5_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S5 release-consumable hardening:",
            "- scope: bundle inventory, manifest, layout, handoff, completeness",
            (
                "- outer layers discover truth through canonical artifacts and "
                "helper/report entrypoints"
            ),
            "- source-code reading is explicitly prohibited as a consumption requirement",
            "- external runtime-home p45/env remains authoritative runtime env truth",
            "- this is not S3 stability repetition, S4 operator proof, or installer implementation",
            "- no distributed, cloud, billing, RBAC, or enterprise scope",
        )
    )


def render_p5_m5_s5_json() -> str:
    bundle = {
        "release_bundle_inventory": build_p5_m5_s5_release_bundle_inventory(),
        "release_manifest": build_p5_m5_s5_release_manifest(),
        "bundle_layout": build_p5_m5_s5_bundle_layout(),
        "handoff_contract": build_p5_m5_s5_handoff_contract(),
        "completeness_checks": build_p5_m5_s5_completeness_checks(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
