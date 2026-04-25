from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

M5S6Severity = Literal["error", "warning", "info"]

P5_M5_S6_OPERATIONAL_PROFILES_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s6_operational_profiles.json"
)
P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_S6_OPERATIONAL_ENVELOPE.md"
)
P5_M5_S6_LOAD_CLASSES_PATH: Final[Path] = repo_root() / "validation" / "p5_m5_s6_load_classes.json"
P5_M5_S6_OPERATIONAL_SIGNALS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s6_operational_signals.json"
)
P5_M5_S6_FAMILY_EXPECTATIONS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s6_family_expectations.json"
)
P5_M5_S6_SUCCESS_CRITERIA_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s6_success_criteria.json"
)

_DELIVERY_PROFILE_CARRIERS: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py::"
    "test_webhook_retries_timeout_failures_and_marks_retryable",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_kafka_destination.py::"
    "test_kafka_destination_timeout_exception_maps_to_timeout_failure_kind",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_opensearch_destination.py::"
    "test_opensearch_destination_maps_rate_limits_into_shared_failure_semantics",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py::"
    "test_replay_delivery_moves_dlq_to_done",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_kafka_sink.py::"
    "test_replay_to_kafka_sink",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
    "test_replay_delivery_writes_governance_action_receipt_with_run_linkage",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_usage_record_runtime_shape_keeps_recovery_paths_distinct",
)
_QUEUE_PROFILE_CARRIERS: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
    "test_requeue_poison_task_moves_to_pending_and_preserves_governance",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
    "test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py::"
    "test_p5_recovery_requeue_keeps_requeue_provenance_distinct",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py::"
    "test_p5_recovery_summary_combines_queue_delivery_and_provenance",
)
_SOURCE_PROFILE_CARRIERS: Final[tuple[str, ...]] = (
    "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py::"
    "test_process_file_fetches_google_drive_document_and_preserves_provenance",
    "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py::"
    "test_process_file_fetches_confluence_document_and_preserves_provenance",
    "packages/it-stream/src/it_stream/tests/test_kafka_source.py::"
    "test_kafka_source_builds_cursor_checkpoints_and_resume_provenance",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_usage_record_runtime_shape_keeps_uns_base_not_naive_pro",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_usage_record_runtime_shape_keeps_it_raw_unit_bounded",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
    "test_governance_action_receipt_helper_supports_read_only_inspect_and_usage_linkage",
)
_FANOUT_PROFILE_CARRIERS: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::test_fanout_all_ok",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
    "test_fanout_any_fail",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
    "test_fanout_surfaces_shared_failure_summary_when_failed_children_agree",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_batch_usage_record_preserves_batch_success_failure_symmetry",
)
_FILESYSTEM_CONTROL_CARRIERS: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
    "test_filesystem_destination_writes_run_meta",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
    "test_filesystem_destination_writes_success_artifacts",
)


def _profile(
    *,
    family_id: str,
    profile_name: str,
    carriers: tuple[str, ...],
    why_representative: str,
    signals_exposed: tuple[str, ...],
    major_measurement_risk: str,
) -> dict[str, object]:
    return {
        "family_id": family_id,
        "profile_name": profile_name,
        "exact_carriers": list(carriers),
        "why_representative": why_representative,
        "signals_exposed": list(signals_exposed),
        "major_measurement_risk": major_measurement_risk,
        "judgeable_without_source_code": True,
    }


def build_p5_m5_s6_operational_profiles() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S6 operational profiles",
        "status": "bounded_same_host_operational_envelope_only",
        "profile_count": 5,
        "profiles": [
            _profile(
                family_id="delivery",
                profile_name="bounded_delivery_replay_verify_envelope",
                carriers=_DELIVERY_PROFILE_CARRIERS,
                why_representative=(
                    "Combines retained destination failure semantics, replay, governance receipt, "
                    "and usage recovery distinction under one bounded same-host profile."
                ),
                signals_exposed=(
                    "elapsed_time_latency_band",
                    "governance_receipt_growth",
                    "usage_record_growth",
                    "helper_report_artifact_check_stability",
                ),
                major_measurement_risk=(
                    "delivery backends differ in local timing variance, so envelope judgment "
                    "must stay rough-band and same-host only."
                ),
            ),
            _profile(
                family_id="queue_governance",
                profile_name="bounded_queue_state_transition_envelope",
                carriers=_QUEUE_PROFILE_CARRIERS,
                why_representative=(
                    "Covers spool/sqlite queue state transitions, requeue, inspect, provenance, "
                    "and governance visibility without widening into distributed queue claims."
                ),
                signals_exposed=(
                    "queue_state_growth",
                    "governance_receipt_growth",
                    "helper_report_artifact_check_stability",
                    "cleanup_overhead",
                ),
                major_measurement_risk=(
                    "queue roots or receipt scopes can create false signal growth if case "
                    "isolation is not preserved."
                ),
            ),
            _profile(
                family_id="source_deep",
                profile_name="bounded_source_contract_usage_provenance_envelope",
                carriers=_SOURCE_PROFILE_CARRIERS,
                why_representative=(
                    "Combines retained uns and it source behavior with usage/provenance/governance "
                    "linkage and checkpoint isolation under bounded local execution."
                ),
                signals_exposed=(
                    "elapsed_time_latency_band",
                    "usage_record_growth",
                    "governance_receipt_growth",
                    "cleanup_overhead",
                ),
                major_measurement_risk=(
                    "source-local progress or remote selection residue can masquerade as stable "
                    "behavior if checkpoint/case identity isolation drifts."
                ),
            ),
            _profile(
                family_id="fanout",
                profile_name="bounded_fanout_composition_envelope",
                carriers=_FANOUT_PROFILE_CARRIERS,
                why_representative=(
                    "Captures child/aggregate output growth, partial-failure shared summary, and "
                    "composition visibility without flattening fanout into a sink."
                ),
                signals_exposed=(
                    "fanout_child_aggregate_output_growth",
                    "usage_record_growth",
                    "helper_report_artifact_check_stability",
                    "cleanup_overhead",
                ),
                major_measurement_risk=(
                    "child-output and aggregate-output scope mixing can create false growth or "
                    "false shared-summary stability."
                ),
            ),
            _profile(
                family_id="filesystem_control",
                profile_name="bounded_filesystem_control_envelope",
                carriers=_FILESYSTEM_CONTROL_CARRIERS,
                why_representative=(
                    "Provides the lowest-cost retained control anchor for artifact growth, output "
                    "shape, and case-scoped path stability."
                ),
                signals_exposed=(
                    "artifact_count_growth",
                    "elapsed_time_latency_band",
                    "cleanup_overhead",
                ),
                major_measurement_risk=(
                    "filesystem output reuse can hide dirty-state pollution if out_root scoping "
                    "is not refreshed per case."
                ),
            ),
        ],
        "not_claimed": [
            "distributed runtime operational envelope",
            "cloud or Kubernetes load envelope",
            "installer implementation",
            "new retained anchors",
        ],
    }


def build_p5_m5_s6_load_classes() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S6 load classes",
        "scope_kind": "bounded_same_host_only",
        "classes": [
            {
                "class_id": "A",
                "name": "single representative run",
                "intent": "minimal bounded single-case profile",
                "intended_scale": "one case-scoped representative run per selected family",
                "family_applicability": [
                    "delivery",
                    "queue_governance",
                    "source_deep",
                    "fanout",
                    "filesystem_control",
                ],
                "too_large_for_phase": [
                    "multi-host execution",
                    "distributed queue pressure",
                    "parallel fleet-scale fanout",
                ],
            },
            {
                "class_id": "B",
                "name": "short repeated batch",
                "intent": "small bounded repeat batch for selected family profiles",
                "intended_scale": "2 short case-scoped batch runs with fresh basetemp/out_root",
                "family_applicability": [
                    "delivery",
                    "queue_governance",
                    "source_deep",
                    "fanout",
                ],
                "too_large_for_phase": [
                    "long soak loops",
                    "sustained throughput pressure",
                    "background worker fleet burn-in",
                ],
            },
            {
                "class_id": "C",
                "name": "bounded soak burst",
                "intent": "short same-host burst for selected low-cost representative families",
                "intended_scale": (
                    "3 brief same-host bursts using low-cost profiles only, still bounded and "
                    "case-isolated"
                ),
                "family_applicability": [
                    "fanout",
                    "filesystem_control",
                ],
                "too_large_for_phase": [
                    "true load testing",
                    "distributed saturation",
                    "cloud autoscaling or installation packaging validation",
                ],
            },
        ],
        "not_claimed": [
            "real load testing",
            "distributed runtime scale",
            "cloud or Kubernetes capacity envelope",
        ],
    }


def _signal(
    *,
    signal_id: str,
    exact_sources: tuple[str, ...],
    machine_readable_evidence_status: str,
    helper_or_artifact_provider: str,
    acceptable_drift_band: str,
    failure_condition: str,
) -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "exact_sources": list(exact_sources),
        "machine_readable_evidence_status": machine_readable_evidence_status,
        "helper_or_artifact_provider": helper_or_artifact_provider,
        "acceptable_drift_band": acceptable_drift_band,
        "failure_condition": failure_condition,
    }


def build_p5_m5_s6_operational_signals() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S6 operational signals",
        "signal_count": 8,
        "signals": [
            _signal(
                signal_id="elapsed_time_latency_band",
                exact_sources=(
                    "validation/p5_benchmark_scenario_registry.json",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py::"
                    "test_webhook_retries_timeout_failures_and_marks_retryable",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
                    "test_filesystem_destination_writes_success_artifacts",
                ),
                machine_readable_evidence_status="existing_plus_derived_helper_check",
                helper_or_artifact_provider="tools/p5_benchmark_report.py",
                acceptable_drift_band="rough_same_host_latency_band_only_not_statistical",
                failure_condition=(
                    "unexpected order-of-magnitude latency jump or uncontrolled timing variance "
                    "without retained failure explanation"
                ),
            ),
            _signal(
                signal_id="artifact_count_growth",
                exact_sources=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
                    "test_filesystem_destination_writes_run_meta",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
                    "test_filesystem_destination_writes_success_artifacts",
                    "validation/p5_m5_s5_release_bundle_inventory.json",
                ),
                machine_readable_evidence_status="derived_via_artifact_outputs_and_bundle_inventory",
                helper_or_artifact_provider="tools/p5_m5_s5_report.py",
                acceptable_drift_band="bounded_case_scoped_artifact_growth_only",
                failure_condition=(
                    "artifact outputs grow outside case scope or require hidden cleanup "
                    "to stay bounded"
                ),
            ),
            _signal(
                signal_id="queue_state_growth",
                exact_sources=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
                    "test_requeue_poison_task_moves_to_pending_and_preserves_governance",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
                    "test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset",
                    "validation/p5_recovery_scenario_matrix.json",
                ),
                machine_readable_evidence_status="existing",
                helper_or_artifact_provider="tools/p5_recovery_operator_report.py",
                acceptable_drift_band="bounded_bucket_and_state_growth_with_case_isolation",
                failure_condition=(
                    "queue state accumulates across cases or inspectable state drifts away from "
                    "retained pending/inflight/poison/requeue expectations"
                ),
            ),
            _signal(
                signal_id="governance_receipt_growth",
                exact_sources=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
                    "test_replay_delivery_writes_governance_action_receipt_with_run_linkage",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
                    "test_governance_action_receipt_helper_supports_read_only_inspect_and_usage_linkage",
                    "validation/p5_manual_action_audit_manifest.json",
                ),
                machine_readable_evidence_status="existing",
                helper_or_artifact_provider="tools/p5_governance_audit_report.py",
                acceptable_drift_band="bounded_receipt_count_growth_with_stable_fields",
                failure_condition=(
                    "receipt growth becomes unclear, inconsistent by family, or depends on hidden "
                    "paths rather than canonical _governance/actions receipts"
                ),
            ),
            _signal(
                signal_id="usage_record_growth",
                exact_sources=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
                    "test_usage_record_runtime_shape_keeps_recovery_paths_distinct",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
                    "test_batch_usage_record_preserves_batch_success_failure_symmetry",
                    "validation/p5_usage_fact_manifest.json",
                ),
                machine_readable_evidence_status="existing",
                helper_or_artifact_provider="tools/p5_usage_report.py",
                acceptable_drift_band="bounded_usage_artifact_growth_with_family-linked_runtime_fields",
                failure_condition=(
                    "usage records stop reflecting retained source/recovery/fanout distinctions or "
                    "grow outside bounded runtime output expectations"
                ),
            ),
            _signal(
                signal_id="fanout_child_aggregate_output_growth",
                exact_sources=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
                    "test_fanout_all_ok",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
                    "test_fanout_any_fail",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
                    "test_fanout_surfaces_shared_failure_summary_when_failed_children_agree",
                ),
                machine_readable_evidence_status="derived_via_runtime_artifacts_and_usage_check",
                helper_or_artifact_provider="tools/p5_usage_report.py",
                acceptable_drift_band=(
                    "bounded_child_and_aggregate_output_growth_with_shared_summary_stability"
                ),
                failure_condition=(
                    "fanout outputs mix scope, collapse into sink semantics, or lose "
                    "shared-summary consistency "
                    "across bounded runs"
                ),
            ),
            _signal(
                signal_id="helper_report_artifact_check_stability",
                exact_sources=(
                    "tools/p5_benchmark_report.py --check-artifacts",
                    "tools/p5_bounded_concurrency_report.py --check-artifacts",
                    "tools/p5_governance_audit_report.py --check-artifacts",
                    "tools/p5_source_contract_report.py --check-artifacts",
                    "tools/p5_m5_s5_report.py --check-artifacts",
                ),
                machine_readable_evidence_status="existing",
                helper_or_artifact_provider="tools/p5_m5_s6_report.py",
                acceptable_drift_band="all referenced helper artifact checks remain green",
                failure_condition=(
                    "any referenced helper truth drifts or requires source-code "
                    "inspection to recover"
                ),
            ),
            _signal(
                signal_id="cleanup_overhead",
                exact_sources=(
                    "validation/p5_m5_s3_cleanup_repetition_rules.json",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py::"
                    "test_p5_recovery_requeue_keeps_requeue_provenance_distinct",
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
                    "test_filesystem_destination_writes_run_meta",
                ),
                machine_readable_evidence_status="existing_plus_derived_helper_check",
                helper_or_artifact_provider="tools/p5_m5_s3_report.py",
                acceptable_drift_band="cleanup stays bounded_case_scoped_and_local",
                failure_condition=(
                    "cleanup depends on hidden manual intervention or leaves dirty-state pollution "
                    "that invalidates bounded-envelope interpretation"
                ),
            ),
        ],
        "not_claimed": [
            "fleet-wide SLI instrumentation",
            "distributed autoscaling signal package",
            "cloud load metrics",
        ],
    }


def _family_expectation(
    *,
    family_id: str,
    expected_behavior: str,
    acceptable_patterns: tuple[str, ...],
    unacceptable_drift: tuple[str, ...],
    why_matters: str,
) -> dict[str, object]:
    return {
        "family_id": family_id,
        "expected_behavior": expected_behavior,
        "acceptable_bounded_patterns": list(acceptable_patterns),
        "unacceptable_drift": list(unacceptable_drift),
        "why_family_matters": why_matters,
    }


def build_p5_m5_s6_family_expectations() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S6 family expectations",
        "family_count": 5,
        "families": [
            _family_expectation(
                family_id="delivery",
                expected_behavior=(
                    "Retained destination delivery remains bounded under same-host execution, "
                    "with replay/verify/governance visibility and rough local latency bands."
                ),
                acceptable_patterns=(
                    "bounded receipt and usage growth per case",
                    "failure_kind and retryability stay within shared delivery vocabulary",
                    "rough latency stays in local-band rather than headline throughput claim",
                ),
                unacceptable_drift=(
                    "unclear receipt growth",
                    "delivery-origin linkage drift",
                    "silent widening into load-test semantics",
                ),
                why_matters=(
                    "Delivery is the retained operator-facing boundary between work execution and "
                    "destination-side failure/replay handling."
                ),
            ),
            _family_expectation(
                family_id="queue_governance",
                expected_behavior=(
                    "Queue state transitions remain inspectable and bounded, with spool/sqlite "
                    "differences explicit and provenance/governance growth staying case-scoped."
                ),
                acceptable_patterns=(
                    "bounded queue bucket growth",
                    "inspect stays read-only",
                    "requeue/provenance linkage remains explicit",
                ),
                unacceptable_drift=(
                    "queue root pollution",
                    "hidden state transitions",
                    "receipt or provenance ambiguity",
                ),
                why_matters=(
                    "Queue/governance defines the retained same-host recovery envelope and "
                    "operator trust surface."
                ),
            ),
            _family_expectation(
                family_id="source_deep",
                expected_behavior=(
                    "Retained source contract, usage, provenance, governance, and checkpoint "
                    "isolation remain aligned under bounded local execution."
                ),
                acceptable_patterns=(
                    "bounded usage and governance growth per source case",
                    "source contract ids remain canonical",
                    "checkpoint/progress stays isolated to case scope",
                ),
                unacceptable_drift=(
                    "source-id misread",
                    "checkpoint reuse across cases",
                    "governance linkage no longer explaining source behavior",
                ),
                why_matters=(
                    "Source-deep behavior is the retained proof that source identity and "
                    "progress semantics remain local, bounded, and inspectable."
                ),
            ),
            _family_expectation(
                family_id="fanout",
                expected_behavior=(
                    "Fanout remains a composition/orchestration surface with bounded child and "
                    "aggregate output growth plus stable shared-summary behavior."
                ),
                acceptable_patterns=(
                    "bounded child and aggregate artifact growth",
                    "shared summary remains stable when failed children agree",
                    "usage visibility preserves batch/fanout distinction",
                ),
                unacceptable_drift=(
                    "fanout treated as sink connector",
                    "child/aggregate scope mixing",
                    "shared-summary collapse or ambiguity",
                ),
                why_matters=(
                    "Fanout is the retained composition envelope and must stay operationally "
                    "clear without flattening into sink semantics."
                ),
            ),
            _family_expectation(
                family_id="filesystem_control",
                expected_behavior=(
                    "Filesystem remains the lowest-cost control anchor for bounded "
                    "artifact growth, output shape, and cleanup-overhead interpretation."
                ),
                acceptable_patterns=(
                    "bounded output count growth",
                    "case-scoped out_root stability",
                    "rough latency remains low-cost and local",
                ),
                unacceptable_drift=(
                    "artifact reuse across cases",
                    "unexpected path inflation",
                    "cleanup dependence on hidden manual actions",
                ),
                why_matters=(
                    "Filesystem provides the control profile for interpreting whether other family "
                    "growth is genuinely bounded or merely hidden by environment noise."
                ),
            ),
        ],
    }


def build_p5_m5_s6_success_criteria() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S6 success criteria",
        "required_family_coverage": [
            "delivery",
            "queue_governance",
            "source_deep",
            "fanout",
            "filesystem_control",
        ],
        "required_load_class_coverage": ["A", "B", "C"],
        "required_signal_coverage": [
            "elapsed_time_latency_band",
            "artifact_count_growth",
            "queue_state_growth",
            "governance_receipt_growth",
            "usage_record_growth",
            "fanout_child_aggregate_output_growth",
            "helper_report_artifact_check_stability",
            "cleanup_overhead",
        ],
        "success_requires": [
            "required families covered",
            "required load classes covered",
            "required signals collected",
            "helper/report checks remain green",
            "no dirty-state dependence",
            "no drift beyond accepted bounded band",
            "no phase-boundary violation",
        ],
        "fail_includes": [
            "helper/report truth drift",
            "unclear or unbounded artifact growth",
            "unclear or unbounded governance receipt growth",
            "queue or provenance linkage drift",
            "source linkage drift",
            "cross-run scope pollution",
            "silent widening into load testing",
            "installer or cloud scope inference",
        ],
        "helper_report_green_required": True,
        "no_dirty_state_dependence_required": True,
        "no_phase_boundary_violation_required": True,
        "platform_side_corroboration": {
            "allowed": True,
            "role": "secondary_not_primary",
        },
        "source_code_reading_required": False,
    }


@dataclass(frozen=True, slots=True)
class P5M5S6Check:
    name: str
    ok: bool
    detail: str
    severity: M5S6Severity = "error"


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
) -> P5M5S6Check:
    observed = _read_json_object(path)
    return P5M5S6Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _profile_carrier_files_exist() -> P5M5S6Check:
    profiles = build_p5_m5_s6_operational_profiles()
    carrier_files: list[str] = []
    for profile in cast(list[dict[str, object]], profiles["profiles"]):
        for carrier in cast(list[str], profile["exact_carriers"]):
            carrier_files.append(carrier.split("::", 1)[0])
    missing = [path for path in dict.fromkeys(carrier_files) if not (repo_root() / path).exists()]
    return P5M5S6Check(
        name="m5_s6_profile_carrier_files_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s6_artifacts() -> list[P5M5S6Check]:
    checks = [
        _json_artifact_check(
            name="m5_s6_operational_profiles_match_helper",
            path=P5_M5_S6_OPERATIONAL_PROFILES_PATH,
            expected=build_p5_m5_s6_operational_profiles(),
        ),
        _json_artifact_check(
            name="m5_s6_load_classes_match_helper",
            path=P5_M5_S6_LOAD_CLASSES_PATH,
            expected=build_p5_m5_s6_load_classes(),
        ),
        _json_artifact_check(
            name="m5_s6_operational_signals_match_helper",
            path=P5_M5_S6_OPERATIONAL_SIGNALS_PATH,
            expected=build_p5_m5_s6_operational_signals(),
        ),
        _json_artifact_check(
            name="m5_s6_family_expectations_match_helper",
            path=P5_M5_S6_FAMILY_EXPECTATIONS_PATH,
            expected=build_p5_m5_s6_family_expectations(),
        ),
        _json_artifact_check(
            name="m5_s6_success_criteria_match_helper",
            path=P5_M5_S6_SUCCESS_CRITERIA_PATH,
            expected=build_p5_m5_s6_success_criteria(),
        ),
        _profile_carrier_files_exist(),
    ]

    profiles = build_p5_m5_s6_operational_profiles()
    checks.append(
        P5M5S6Check(
            name="m5_s6_profile_coverage_stays_bounded",
            ok=profiles["profile_count"] == 5,
            detail=str(profiles["profile_count"]),
        )
    )

    load_classes = build_p5_m5_s6_load_classes()
    classes = cast(list[dict[str, object]], load_classes["classes"])
    checks.append(
        P5M5S6Check(
            name="m5_s6_load_classes_stay_non_load_test",
            ok=[cast(str, item["class_id"]) for item in classes] == ["A", "B", "C"],
            detail=str([cast(str, item["name"]) for item in classes]),
        )
    )

    signals = build_p5_m5_s6_operational_signals()
    checks.append(
        P5M5S6Check(
            name="m5_s6_operational_signals_are_unified",
            ok=signals["signal_count"] == 8,
            detail=str(
                [
                    signal["signal_id"]
                    for signal in cast(list[dict[str, object]], signals["signals"])
                ]
            ),
        )
    )

    success = build_p5_m5_s6_success_criteria()
    platform_side = cast(dict[str, object], success["platform_side_corroboration"])
    checks.append(
        P5M5S6Check(
            name="m5_s6_platform_side_corroboration_stays_secondary",
            ok=platform_side["role"] == "secondary_not_primary"
            and success["source_code_reading_required"] is False,
            detail=str(platform_side),
        )
    )

    if P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH.exists():
        report_text = P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S6 freezes bounded same-host operational envelope truth, not load testing.",
            (
                "S6 does not repeat S3 stability work, S4 cold-operator proof, or "
                "S5 release bundle hardening."
            ),
            (
                "S6 does not start installer implementation, deployment packaging, "
                "or cloud validation."
            ),
            (
                "Outer-layer or platform-side checking is secondary corroboration, "
                "not primary truth."
            ),
            (
                "The external runtime-home p45/env surface remains authoritative and "
                "is not replaced."
            ),
            "fanout remains composition/orchestration, not a sink connector.",
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S6Check(
                name="m5_s6_report_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )
    else:
        checks.append(
            P5M5S6Check(
                name="m5_s6_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH),
            )
        )

    return checks


def format_p5_m5_s6_results(results: list[P5M5S6Check]) -> str:
    lines = ["P5-M5-S6 operational-envelope checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s6_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S6 bounded operational-envelope proof:",
            (
                "- scope: representative profiles, bounded load classes, signals, "
                "expectations, success/fail"
            ),
            "- operational truth remains same-host and bounded only",
            "- platform-side corroboration is secondary, not primary",
            "- source-code reading is not required for operational-envelope discovery",
            "- fanout remains composition/orchestration, not a sink connector",
            (
                "- this is not S3 stability repetition, S4 operator proof, "
                "S5 bundle hardening, or installer/cloud work"
            ),
        )
    )


def render_p5_m5_s6_json() -> str:
    bundle = {
        "operational_profiles": build_p5_m5_s6_operational_profiles(),
        "load_classes": build_p5_m5_s6_load_classes(),
        "operational_signals": build_p5_m5_s6_operational_signals(),
        "family_expectations": build_p5_m5_s6_family_expectations(),
        "success_criteria": build_p5_m5_s6_success_criteria(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
