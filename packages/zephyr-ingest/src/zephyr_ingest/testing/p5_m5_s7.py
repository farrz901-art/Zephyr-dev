from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

M5S7Severity = Literal["error", "warning", "info"]

P5_M5_S7_WORKFLOW_SCENARIOS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s7_workflow_scenarios.json"
)
P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_S7_REAL_WORKFLOWS.md"
)
P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s7_workflow_evidence_map.json"
)
P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s7_workflow_success_criteria.json"
)
P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s7_workflow_failure_matrix.json"
)
P5_M5_S7_WORKFLOW_VIEWS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s7_workflow_views.json"
)

_S7_UNS_HAPPY_CARRIERS: Final[tuple[str, ...]] = (
    "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py::"
    "test_process_file_fetches_google_drive_document_and_preserves_provenance",
    "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py::"
    "test_process_file_fetches_confluence_document_and_preserves_provenance",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
    "test_filesystem_destination_writes_run_meta",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
    "test_filesystem_destination_writes_success_artifacts",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_runner_emits_usage_record_json_beside_run_meta_and_delivery_receipt",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_source_contracts.py::"
    "test_p5_source_usage_linkage_covers_sources_and_keeps_extra_non_default",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
    "test_governance_action_receipt_helper_supports_read_only_inspect_and_usage_linkage",
)
_S7_IT_HAPPY_CARRIERS: Final[tuple[str, ...]] = (
    "packages/it-stream/src/it_stream/tests/test_kafka_source.py::"
    "test_kafka_source_builds_cursor_checkpoints_and_resume_provenance",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
    "test_filesystem_destination_writes_run_meta",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
    "test_filesystem_destination_writes_success_artifacts",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_usage_record_runtime_shape_keeps_it_raw_unit_bounded",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_runner_emits_usage_record_json_beside_run_meta_and_delivery_receipt",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_source_contracts.py::"
    "test_p5_source_usage_linkage_covers_sources_and_keeps_extra_non_default",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
    "test_governance_action_receipt_helper_supports_read_only_inspect_and_usage_linkage",
)
_S7_FAILURE_CARRIERS: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py::"
    "test_webhook_retries_timeout_failures_and_marks_retryable",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_kafka_destination.py::"
    "test_kafka_destination_timeout_exception_maps_to_timeout_failure_kind",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_opensearch_destination.py::"
    "test_opensearch_destination_maps_rate_limits_into_shared_failure_semantics",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py::"
    "test_replay_delivery_moves_dlq_to_done",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py::"
    "test_replay_delivery_preserves_resume_checkpoint_provenance_while_marking_replay",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py::"
    "test_p5_recovery_requeue_keeps_requeue_provenance_distinct",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py::"
    "test_p5_recovery_summary_combines_queue_delivery_and_provenance",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
    "test_replay_delivery_writes_governance_action_receipt_with_run_linkage",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_usage_record_runtime_shape_covers_failure_and_delivery_failure",
)
_S7_FANOUT_CARRIERS: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::test_fanout_all_ok",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
    "test_fanout_any_fail",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
    "test_fanout_surfaces_shared_failure_summary_when_failed_children_agree",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py::"
    "test_filesystem_destination_writes_success_artifacts",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py::"
    "test_batch_usage_record_preserves_batch_success_failure_symmetry",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py::"
    "test_governance_action_receipt_helper_supports_read_only_inspect_and_usage_linkage",
)


def _carrier(
    *,
    carrier: str,
    role: str,
    bounded_claim: str,
    carrier_granularity: str = "exact_test_id",
    caveat: str | None = None,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "carrier": carrier,
        "role": role,
        "carrier_granularity": carrier_granularity,
        "bounded_claim": bounded_claim,
    }
    if caveat is not None:
        entry["caveat"] = caveat
    return entry


def _evidence_entry(
    *,
    surface: str,
    carrier: str,
    explanation: str,
    carrier_granularity: str = "exact_test_id",
    caveat: str | None = None,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "surface": surface,
        "carrier": carrier,
        "carrier_granularity": carrier_granularity,
        "explanation": explanation,
    }
    if caveat is not None:
        entry["caveat"] = caveat
    return entry


def _workflow_scenario(
    *,
    scenario_id: str,
    name: str,
    family: str,
    source_candidates: tuple[str, ...],
    processing_path: str,
    destination_candidates: tuple[str, ...],
    representative_reason: str,
    exact_carriers: tuple[str, ...],
    major_risk: str,
) -> dict[str, object]:
    return {
        "scenario_id": scenario_id,
        "name": name,
        "family": family,
        "source_candidates": list(source_candidates),
        "processing_path": processing_path,
        "destination_candidates": list(destination_candidates),
        "representative_reason": representative_reason,
        "carriers": [
            _carrier(
                carrier=carrier,
                role="workflow_proof",
                bounded_claim="representative workflow chain proof only",
            )
            for carrier in exact_carriers
        ],
        "major_risk": major_risk,
        "judgeable_without_source_code": True,
    }


def build_p5_m5_s7_workflow_scenarios() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S7 real workflow scenarios",
        "status": "real_workflow_chain_proof_only",
        "scenario_count": 4,
        "scenarios": [
            _workflow_scenario(
                scenario_id="s7_uns_happy_01",
                name="UNS happy workflow",
                family="UNS happy",
                source_candidates=(
                    "google_drive_document_v1",
                    "confluence_document_v1",
                ),
                processing_path=(
                    "uns-stream retained non-extra ingestion / normalization / delivery chain"
                ),
                destination_candidates=("filesystem", "s3"),
                representative_reason=(
                    "Proves retained UNS document workflow can acquire source content, "
                    "emit task/run identity, link usage/provenance, and land bounded output "
                    "artifacts."
                ),
                exact_carriers=_S7_UNS_HAPPY_CARRIERS,
                major_risk=(
                    "Source acquisition can look healthy while usage/governance linkage "
                    "quietly drifts if helper/artifact surfaces are not checked together."
                ),
            ),
            _workflow_scenario(
                scenario_id="s7_it_happy_01",
                name="IT happy workflow",
                family="IT happy",
                source_candidates=(
                    "kafka_partition_offset_v1",
                    "http_json_cursor_v1",
                ),
                processing_path=("it-stream retained incremental / cursor / delivery chain"),
                destination_candidates=("filesystem", "kafka", "sqlite"),
                representative_reason=(
                    "Proves retained IT workflow can preserve cursor identity, emit bounded "
                    "usage/provenance semantics, and land delivery artifacts without widening "
                    "into broader source-platform claims."
                ),
                exact_carriers=_S7_IT_HAPPY_CARRIERS,
                major_risk=(
                    "Checkpoint or cursor facts can become implicit unless source contract, "
                    "usage runtime, and delivery evidence stay linked."
                ),
            ),
            _workflow_scenario(
                scenario_id="s7_failure_01",
                name="Failure / recovery workflow",
                family="failure/recovery",
                source_candidates=("retained source context only",),
                processing_path=("failed delivery -> replay/requeue -> verify/governance receipt"),
                destination_candidates=("webhook", "kafka", "opensearch"),
                representative_reason=(
                    "Proves Zephyr is not happy-path-only: failure is classified, visible, "
                    "recoverable or inspectable, and linked to usage/provenance/governance."
                ),
                exact_carriers=_S7_FAILURE_CARRIERS,
                major_risk=(
                    "Failure can appear recovered while provenance or governance linkage is "
                    "incomplete unless replay/requeue and usage evidence are frozen together."
                ),
            ),
            _workflow_scenario(
                scenario_id="s7_fanout_01",
                name="Fanout workflow",
                family="fanout",
                source_candidates=("combined representative retained input context",),
                processing_path=(
                    "ingestion -> fanout -> child outputs / aggregate result / shared summary"
                ),
                destination_candidates=("multi-sink composition path",),
                representative_reason=(
                    "Proves fanout is a real composition/orchestration workflow with visible "
                    "child outputs, aggregate output, and interpretable partial failure."
                ),
                exact_carriers=_S7_FANOUT_CARRIERS,
                major_risk=(
                    "Child and aggregate scopes can mix or fanout can be misread as a sink "
                    "unless shared-summary and batch-usage evidence stay explicit."
                ),
            ),
        ],
        "intentional_file_level_carriers": [],
        "not_claimed": [
            "distributed runtime workflow proof",
            "cloud or Kubernetes workflow proof",
            "installer implementation",
            "new retained anchors",
        ],
    }


def _release_handoff_evidence() -> list[dict[str, object]]:
    return [
        {
            "surface": "release_manifest",
            "artifact": "validation/p5_m5_s5_release_manifest.json",
        },
        {
            "surface": "handoff_contract",
            "artifact": "validation/p5_m5_s5_handoff_contract.json",
        },
        {
            "surface": "release_bundle_helper",
            "artifact": "tools/p5_m5_s5_report.py --check-artifacts",
        },
    ]


def build_p5_m5_s7_workflow_evidence_map() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S7 workflow evidence map",
        "scenario_count": 4,
        "scenarios": [
            {
                "scenario_id": "s7_uns_happy_01",
                "evidence_chain": {
                    "source_evidence": [
                        _evidence_entry(
                            surface="google_drive_source_provenance",
                            carrier=_S7_UNS_HAPPY_CARRIERS[0],
                            explanation=(
                                "Shows retained Google Drive acquisition enters UNS with "
                                "preserved provenance facts."
                            ),
                        ),
                        _evidence_entry(
                            surface="confluence_source_provenance",
                            carrier=_S7_UNS_HAPPY_CARRIERS[1],
                            explanation=(
                                "Shows retained Confluence acquisition enters UNS with "
                                "preserved provenance facts."
                            ),
                        ),
                        _evidence_entry(
                            surface="source_contract_linkage",
                            carrier=_S7_UNS_HAPPY_CARRIERS[5],
                            explanation=(
                                "Binds retained source ids to canonical usage linkage without "
                                "requiring source-code reading."
                            ),
                        ),
                    ],
                    "task_run_evidence": [
                        _evidence_entry(
                            surface="task_run_artifacts",
                            carrier=_S7_UNS_HAPPY_CARRIERS[4],
                            explanation=(
                                "Confirms run_meta, delivery_receipt, and usage_record are "
                                "emitted together for the bounded workflow run."
                            ),
                        ),
                    ],
                    "delivery_output_evidence": [
                        _evidence_entry(
                            surface="filesystem_run_meta",
                            carrier=_S7_UNS_HAPPY_CARRIERS[2],
                            explanation="Shows filesystem delivery writes canonical run metadata.",
                        ),
                        _evidence_entry(
                            surface="filesystem_success_artifacts",
                            carrier=_S7_UNS_HAPPY_CARRIERS[3],
                            explanation="Shows bounded output artifacts land in the retained sink.",
                        ),
                    ],
                    "usage_evidence": [
                        _evidence_entry(
                            surface="usage_record_json",
                            carrier=_S7_UNS_HAPPY_CARRIERS[4],
                            explanation=(
                                "Shows canonical usage_record.json exists beside run and "
                                "delivery artifacts."
                            ),
                        ),
                    ],
                    "provenance_evidence": [
                        _evidence_entry(
                            surface="source_provenance_fields",
                            carrier=_S7_UNS_HAPPY_CARRIERS[0],
                            explanation=(
                                "Preserves source-linked provenance for retained UNS workflow."
                            ),
                        ),
                        _evidence_entry(
                            surface="source_provenance_fields",
                            carrier=_S7_UNS_HAPPY_CARRIERS[1],
                            explanation=(
                                "Preserves knowledge-space provenance for retained UNS workflow."
                            ),
                        ),
                    ],
                    "governance_recovery_evidence": [
                        _evidence_entry(
                            surface="governance_usage_linkage_helper",
                            carrier=_S7_UNS_HAPPY_CARRIERS[6],
                            explanation=(
                                "Shows governance receipt helper can explain usage linkage "
                                "without requiring recovery-specific widening."
                            ),
                        ),
                    ],
                    "operator_helper_evidence": [
                        {
                            "surface": "usage_report_helper",
                            "artifact": "tools/p5_usage_report.py",
                        },
                        {
                            "surface": "source_contract_report_helper",
                            "artifact": "tools/p5_source_contract_report.py",
                        },
                        {
                            "surface": "governance_audit_report_helper",
                            "artifact": "tools/p5_governance_audit_report.py",
                        },
                        {
                            "surface": "workflow_package_helper",
                            "artifact": "tools/p5_m5_s7_report.py",
                        },
                    ],
                    "release_handoff_evidence": _release_handoff_evidence(),
                    "missing_link": "none",
                },
            },
            {
                "scenario_id": "s7_it_happy_01",
                "evidence_chain": {
                    "source_evidence": [
                        _evidence_entry(
                            surface="kafka_source_checkpoint_provenance",
                            carrier=_S7_IT_HAPPY_CARRIERS[0],
                            explanation=(
                                "Shows retained IT source builds bounded checkpoint and resume "
                                "provenance facts."
                            ),
                        ),
                        _evidence_entry(
                            surface="source_contract_linkage",
                            carrier=_S7_IT_HAPPY_CARRIERS[5],
                            explanation=(
                                "Shows retained source ids remain canonical across the usage "
                                "linkage package."
                            ),
                        ),
                    ],
                    "task_run_evidence": [
                        _evidence_entry(
                            surface="task_run_artifacts",
                            carrier=_S7_IT_HAPPY_CARRIERS[4],
                            explanation=(
                                "Shows bounded workflow execution emits canonical runtime "
                                "artifacts for task/run interpretation."
                            ),
                        ),
                    ],
                    "delivery_output_evidence": [
                        _evidence_entry(
                            surface="filesystem_run_meta",
                            carrier=_S7_IT_HAPPY_CARRIERS[1],
                            explanation="Shows retained bounded delivery metadata is written.",
                        ),
                        _evidence_entry(
                            surface="filesystem_success_artifacts",
                            carrier=_S7_IT_HAPPY_CARRIERS[2],
                            explanation=(
                                "Shows bounded output landing without needing broader sink claims."
                            ),
                        ),
                    ],
                    "usage_evidence": [
                        _evidence_entry(
                            surface="it_usage_runtime_shape",
                            carrier=_S7_IT_HAPPY_CARRIERS[3],
                            explanation=(
                                "Shows IT raw-unit and source-contract semantics stay bounded "
                                "and technical."
                            ),
                        ),
                        _evidence_entry(
                            surface="usage_record_json",
                            carrier=_S7_IT_HAPPY_CARRIERS[4],
                            explanation=(
                                "Shows workflow emits usage_record.json beside run and "
                                "delivery evidence."
                            ),
                        ),
                    ],
                    "provenance_evidence": [
                        _evidence_entry(
                            surface="checkpoint_resume_provenance",
                            carrier=_S7_IT_HAPPY_CARRIERS[0],
                            explanation=(
                                "Shows resume-relevant checkpoint provenance remains explicit."
                            ),
                        ),
                    ],
                    "governance_recovery_evidence": [
                        _evidence_entry(
                            surface="governance_usage_linkage_helper",
                            carrier=_S7_IT_HAPPY_CARRIERS[6],
                            explanation=(
                                "Shows governance-side helper surfaces can interpret retained "
                                "usage linkage."
                            ),
                        ),
                    ],
                    "operator_helper_evidence": [
                        {
                            "surface": "usage_report_helper",
                            "artifact": "tools/p5_usage_report.py",
                        },
                        {
                            "surface": "source_contract_report_helper",
                            "artifact": "tools/p5_source_contract_report.py",
                        },
                        {
                            "surface": "governance_audit_report_helper",
                            "artifact": "tools/p5_governance_audit_report.py",
                        },
                        {
                            "surface": "workflow_package_helper",
                            "artifact": "tools/p5_m5_s7_report.py",
                        },
                    ],
                    "release_handoff_evidence": _release_handoff_evidence(),
                    "missing_link": "none",
                },
            },
            {
                "scenario_id": "s7_failure_01",
                "evidence_chain": {
                    "source_evidence": [
                        _evidence_entry(
                            surface="source_context_for_failure_linkage",
                            carrier=(
                                "packages/zephyr-ingest/src/zephyr_ingest/tests/"
                                "test_p5_source_contracts.py::"
                                "test_p5_source_usage_linkage_covers_sources_and_keeps_extra_"
                                "non_default"
                            ),
                            explanation=(
                                "Keeps retained source identity explicit while failure is "
                                "interpreted through shared usage/governance surfaces."
                            ),
                        ),
                    ],
                    "task_run_evidence": [
                        _evidence_entry(
                            surface="replay_task_run_linkage",
                            carrier=_S7_FAILURE_CARRIERS[4],
                            explanation=(
                                "Shows replay preserves resume-oriented task/run provenance "
                                "while marking delivery_origin=replay."
                            ),
                        ),
                        _evidence_entry(
                            surface="queue_recovery_summary",
                            carrier=_S7_FAILURE_CARRIERS[6],
                            explanation=(
                                "Shows recovery summary combines queue, delivery, and provenance "
                                "facts in one retained surface."
                            ),
                        ),
                    ],
                    "delivery_output_evidence": [
                        _evidence_entry(
                            surface="webhook_failure_classification",
                            carrier=_S7_FAILURE_CARRIERS[0],
                            explanation=(
                                "Shows retryable timeout classification is explicit for webhook."
                            ),
                        ),
                        _evidence_entry(
                            surface="kafka_failure_classification",
                            carrier=_S7_FAILURE_CARRIERS[1],
                            explanation=(
                                "Shows timeout failure_kind is explicit for Kafka destination."
                            ),
                        ),
                        _evidence_entry(
                            surface="opensearch_failure_classification",
                            carrier=_S7_FAILURE_CARRIERS[2],
                            explanation=(
                                "Shows rate-limit failure_kind remains inside shared delivery "
                                "semantics."
                            ),
                        ),
                        _evidence_entry(
                            surface="replay_delivery_done_transition",
                            carrier=_S7_FAILURE_CARRIERS[3],
                            explanation=(
                                "Shows failed delivery can move from DLQ to done through replay."
                            ),
                        ),
                    ],
                    "usage_evidence": [
                        _evidence_entry(
                            surface="usage_failure_shape",
                            carrier=_S7_FAILURE_CARRIERS[8],
                            explanation=(
                                "Shows run failure and delivery failure remain distinct in "
                                "usage evidence."
                            ),
                        ),
                    ],
                    "provenance_evidence": [
                        _evidence_entry(
                            surface="requeue_provenance_distinction",
                            carrier=_S7_FAILURE_CARRIERS[5],
                            explanation=(
                                "Shows requeue provenance is explicit rather than collapsed "
                                "into generic recovery."
                            ),
                        ),
                        _evidence_entry(
                            surface="replay_resume_provenance",
                            carrier=_S7_FAILURE_CARRIERS[4],
                            explanation=(
                                "Shows replay marks delivery replay while preserving resume "
                                "checkpoint identity."
                            ),
                        ),
                    ],
                    "governance_recovery_evidence": [
                        _evidence_entry(
                            surface="replay_governance_receipt",
                            carrier=_S7_FAILURE_CARRIERS[7],
                            explanation=(
                                "Shows governance receipt records replay action, run linkage, "
                                "and destination summary."
                            ),
                        ),
                        _evidence_entry(
                            surface="recovery_operator_summary",
                            carrier=_S7_FAILURE_CARRIERS[6],
                            explanation=(
                                "Shows operator-facing recovery summary can explain queue, "
                                "delivery, and provenance together."
                            ),
                        ),
                    ],
                    "operator_helper_evidence": [
                        {
                            "surface": "recovery_operator_report_helper",
                            "artifact": "tools/p5_recovery_operator_report.py",
                        },
                        {
                            "surface": "governance_audit_report_helper",
                            "artifact": "tools/p5_governance_audit_report.py",
                        },
                        {
                            "surface": "usage_report_helper",
                            "artifact": "tools/p5_usage_report.py",
                        },
                        {
                            "surface": "workflow_package_helper",
                            "artifact": "tools/p5_m5_s7_report.py",
                        },
                    ],
                    "release_handoff_evidence": _release_handoff_evidence(),
                    "missing_link": "none",
                },
            },
            {
                "scenario_id": "s7_fanout_01",
                "evidence_chain": {
                    "source_evidence": [
                        _evidence_entry(
                            surface="retained_source_context",
                            carrier=(
                                "packages/zephyr-ingest/src/zephyr_ingest/tests/"
                                "test_p5_source_contracts.py::"
                                "test_p5_source_usage_linkage_covers_sources_and_keeps_extra_"
                                "non_default"
                            ),
                            explanation=(
                                "Provides bounded retained source context without widening fanout "
                                "into new source validation."
                            ),
                        ),
                    ],
                    "task_run_evidence": [
                        _evidence_entry(
                            surface="batch_usage_runtime_shape",
                            carrier=_S7_FANOUT_CARRIERS[4],
                            explanation=(
                                "Shows batch-level workflow interpretation remains distinct for "
                                "partial or successful fanout runs."
                            ),
                        ),
                    ],
                    "delivery_output_evidence": [
                        _evidence_entry(
                            surface="fanout_all_ok",
                            carrier=_S7_FANOUT_CARRIERS[0],
                            explanation=(
                                "Shows all-ok aggregate interpretation for composition workflow."
                            ),
                        ),
                        _evidence_entry(
                            surface="fanout_partial_failure",
                            carrier=_S7_FANOUT_CARRIERS[1],
                            explanation=(
                                "Shows partial failure remains visible rather than swallowed."
                            ),
                        ),
                        _evidence_entry(
                            surface="fanout_shared_summary",
                            carrier=_S7_FANOUT_CARRIERS[2],
                            explanation=(
                                "Shows shared summary remains interpretable when failed "
                                "children agree."
                            ),
                        ),
                        _evidence_entry(
                            surface="child_output_artifacts",
                            carrier=_S7_FANOUT_CARRIERS[3],
                            explanation=(
                                "Provides bounded child-output evidence from retained "
                                "filesystem sink behavior."
                            ),
                        ),
                    ],
                    "usage_evidence": [
                        _evidence_entry(
                            surface="batch_usage_record",
                            carrier=_S7_FANOUT_CARRIERS[4],
                            explanation=(
                                "Shows fanout usage remains visible with batch success/failure "
                                "symmetry."
                            ),
                        ),
                    ],
                    "provenance_evidence": [
                        _evidence_entry(
                            surface="shared_summary_provenance_context",
                            carrier=_S7_FANOUT_CARRIERS[2],
                            explanation=(
                                "Shows aggregate/shared-summary interpretation remains aligned "
                                "with child failure agreement."
                            ),
                        ),
                    ],
                    "governance_recovery_evidence": [
                        _evidence_entry(
                            surface="governance_usage_linkage_helper",
                            carrier=_S7_FANOUT_CARRIERS[5],
                            explanation=(
                                "Shows operator-facing governance helper can still interpret "
                                "usage linkage for retained workflow evidence."
                            ),
                        ),
                    ],
                    "operator_helper_evidence": [
                        {
                            "surface": "usage_report_helper",
                            "artifact": "tools/p5_usage_report.py",
                        },
                        {
                            "surface": "governance_audit_report_helper",
                            "artifact": "tools/p5_governance_audit_report.py",
                        },
                        {
                            "surface": "workflow_package_helper",
                            "artifact": "tools/p5_m5_s7_report.py",
                        },
                    ],
                    "release_handoff_evidence": _release_handoff_evidence(),
                    "missing_link": "none",
                },
            },
        ],
    }


def build_p5_m5_s7_workflow_success_criteria() -> dict[str, object]:
    common = [
        "source truly entered or source identity is represented by exact source carrier",
        "processing path truly executed or is represented by exact run/task carrier",
        "delivery/output truly landed or failure/recovery behavior is represented by exact carrier",
        "usage emitted",
        "provenance linked",
        "governance/recovery visible where relevant",
        "helper/report remains readable",
        "no dirty-state dependence",
        "no phase-boundary violation",
    ]
    return {
        "schema_version": 1,
        "phase": "P5-M5-S7 workflow success criteria",
        "scenario_count": 4,
        "platform_side_corroboration": {
            "allowed": True,
            "role": "secondary_not_primary",
        },
        "source_code_reading_required": False,
        "scenarios": [
            {
                "scenario_id": "s7_uns_happy_01",
                "common_success_criteria": common,
                "scenario_specific_success_criteria": [
                    (
                        "retained UNS source provenance remains visible for Google Drive "
                        "and Confluence"
                    ),
                    (
                        "filesystem output artifacts land with canonical run_meta "
                        "and receipt surfaces"
                    ),
                    "source contract linkage remains technical and non-Pro/non-Extra by default",
                ],
            },
            {
                "scenario_id": "s7_it_happy_01",
                "common_success_criteria": common,
                "scenario_specific_success_criteria": [
                    "cursor/checkpoint identity remains explicit for retained IT source",
                    (
                        "IT raw unit remains bounded record_or_emitted_item rather "
                        "than widened platform unit"
                    ),
                    "bounded delivery artifacts land without needing broader sink claims",
                ],
            },
            {
                "scenario_id": "s7_failure_01",
                "common_success_criteria": common,
                "scenario_specific_success_criteria": [
                    "failure classification visible",
                    "failure not swallowed",
                    "replay/requeue/verify path visible",
                    "recovery/governance receipt visible",
                    "post-recovery usage/provenance linkage coherent",
                    "terminal / replayable / requeueable / inspect-only status explicit",
                ],
            },
            {
                "scenario_id": "s7_fanout_01",
                "common_success_criteria": common,
                "scenario_specific_success_criteria": [
                    "fanout remains composition/orchestration",
                    "child outputs visible",
                    "aggregate/shared-summary visible",
                    "all-ok and partial-failure interpretations separated",
                    "no child/aggregate scope mixing",
                ],
            },
        ],
    }


def build_p5_m5_s7_workflow_failure_matrix() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S7 workflow failure matrix",
        "scenario_count": 4,
        "scenarios": [
            {
                "scenario_id": "s7_uns_happy_01",
                "main_failure_mode": (
                    "source acquisition succeeds but usage/provenance linkage drifts"
                ),
                "evidence_of_failure": [
                    "usage_record.json no longer matches retained source contract linkage",
                    "helper/report surfaces stop explaining source-linked workflow evidence",
                ],
                "recovery_possibility": "inspect_only",
                "status_classification": "inspect_only",
                "proving_artifacts": [
                    _S7_UNS_HAPPY_CARRIERS[4],
                    _S7_UNS_HAPPY_CARRIERS[5],
                    _S7_UNS_HAPPY_CARRIERS[6],
                ],
                "biggest_ambiguity": (
                    "Happy-path delivery can hide linkage drift unless source, usage, and "
                    "governance evidence are read together."
                ),
            },
            {
                "scenario_id": "s7_it_happy_01",
                "main_failure_mode": (
                    "checkpoint/cursor semantics drift while delivery still looks healthy"
                ),
                "evidence_of_failure": [
                    "usage runtime no longer reports bounded IT raw-unit semantics",
                    "source contract linkage stops matching retained cursor identity",
                ],
                "recovery_possibility": "inspect_only",
                "status_classification": "inspect_only",
                "proving_artifacts": [
                    _S7_IT_HAPPY_CARRIERS[0],
                    _S7_IT_HAPPY_CARRIERS[3],
                    _S7_IT_HAPPY_CARRIERS[5],
                ],
                "biggest_ambiguity": (
                    "Bounded filesystem delivery can look normal even if cursor semantics have "
                    "quietly widened or blurred."
                ),
            },
            {
                "scenario_id": "s7_failure_01",
                "main_failure_mode": (
                    "delivery failure is classified but replay/requeue linkage becomes incoherent"
                ),
                "evidence_of_failure": [
                    "failure_kind visible without usable replay/requeue provenance",
                    "governance receipt exists but no longer explains run/task linkage",
                ],
                "recovery_possibility": "replayable_or_requeueable_depending_on_failure_surface",
                "status_classification": "replayable_requeueable_or_inspect_only_explicit",
                "proving_artifacts": list(_S7_FAILURE_CARRIERS),
                "biggest_ambiguity": (
                    "Different retained sinks use different failure surfaces, so S7 must stay "
                    "representative and not imply exhaustive sink failure proof."
                ),
            },
            {
                "scenario_id": "s7_fanout_01",
                "main_failure_mode": (
                    "aggregate result stays visible while child/summary scope mixes"
                ),
                "evidence_of_failure": [
                    "partial failure becomes indistinguishable from all-ok aggregate result",
                    "shared summary or child outputs stop matching batch usage interpretation",
                ],
                "recovery_possibility": "inspect_only",
                "status_classification": "inspect_only",
                "proving_artifacts": list(_S7_FANOUT_CARRIERS),
                "biggest_ambiguity": (
                    "Fanout can be misread as a sink if child and aggregate artifacts are not "
                    "kept explicitly separate."
                ),
            },
        ],
    }


def build_p5_m5_s7_workflow_views() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S7 workflow views",
        "scenario_count": 4,
        "source_code_reading_required": False,
        "scenarios": [
            {
                "scenario_id": "s7_uns_happy_01",
                "user_facing_view": {
                    "what_user_can_tell_happened": (
                        "A retained UNS document source entered the workflow, produced output "
                        "artifacts, and emitted usage/runtime evidence."
                    ),
                    "biggest_ambiguity": (
                        "Which retained UNS source variant supplied the clearest evidence may "
                        "still require reading helper surfaces rather than raw output alone."
                    ),
                },
                "operator_facing_view": {
                    "what_operator_can_inspect": (
                        "Source contract linkage, usage record, governance helper linkage, "
                        "run_meta, and filesystem output surfaces."
                    ),
                    "biggest_ambiguity": (
                        "Happy workflow governance visibility is lighter than recovery workflow "
                        "governance visibility."
                    ),
                },
                "artifact_facing_view": {
                    "canonical_machine_readable_evidence": [
                        "validation/p5_source_usage_linkage.json",
                        "validation/p5_usage_runtime_contract.json",
                        "validation/p5_m5_s7_workflow_evidence_map.json",
                    ],
                    "biggest_ambiguity": (
                        "Exact acquisition path detail stays source-local and is intentionally not "
                        "flattened into one universal artifact."
                    ),
                },
            },
            {
                "scenario_id": "s7_it_happy_01",
                "user_facing_view": {
                    "what_user_can_tell_happened": (
                        "A retained IT source entered with bounded cursor identity, emitted usage "
                        "evidence, and landed delivery artifacts."
                    ),
                    "biggest_ambiguity": (
                        "The retained IT workflow is representative rather than exhaustive across "
                        "all supported IT sources."
                    ),
                },
                "operator_facing_view": {
                    "what_operator_can_inspect": (
                        "Checkpoint-oriented source evidence, usage runtime facts, source "
                        "contract linkage, and bounded delivery artifacts."
                    ),
                    "biggest_ambiguity": (
                        "Cursor semantics remain stream-local, so S7 proves bounded workflow "
                        "truth rather than a generic source-runtime platform."
                    ),
                },
                "artifact_facing_view": {
                    "canonical_machine_readable_evidence": [
                        "validation/p5_source_contract_matrix.json",
                        "validation/p5_usage_runtime_contract.json",
                        "validation/p5_m5_s7_workflow_evidence_map.json",
                    ],
                    "biggest_ambiguity": (
                        "Destination output proof is bounded and low-cost, not a broader sink "
                        "performance claim."
                    ),
                },
            },
            {
                "scenario_id": "s7_failure_01",
                "user_facing_view": {
                    "what_user_can_tell_happened": (
                        "A retained workflow can fail visibly, be replayed or requeued where "
                        "appropriate, and leave coherent recovery evidence."
                    ),
                    "biggest_ambiguity": (
                        "Different retained sinks surface different failure shapes, so the proof "
                        "is representative rather than sink-exhaustive."
                    ),
                },
                "operator_facing_view": {
                    "what_operator_can_inspect": (
                        "Failure classification, replay/requeue summary, governance receipts, "
                        "usage failure facts, and recovery helper/report surfaces."
                    ),
                    "biggest_ambiguity": (
                        "Terminal vs replayable vs requeueable remains bounded to the explicit "
                        "retained evidence, not a universal failure platform."
                    ),
                },
                "artifact_facing_view": {
                    "canonical_machine_readable_evidence": [
                        "validation/p5_governance_action_matrix.json",
                        "validation/p5_governance_usage_linkage.json",
                        "validation/p5_m5_s7_workflow_failure_matrix.json",
                    ],
                    "biggest_ambiguity": (
                        "Some failure explanation still spans multiple canonical artifacts by "
                        "design, even though S7 now freezes that chain explicitly."
                    ),
                },
            },
            {
                "scenario_id": "s7_fanout_01",
                "user_facing_view": {
                    "what_user_can_tell_happened": (
                        "Fanout can succeed fully or partially fail while still exposing child "
                        "outputs, aggregate result, and shared summary."
                    ),
                    "biggest_ambiguity": (
                        "Without the frozen workflow package, fanout can be misread as a single "
                        "sink outcome instead of composition."
                    ),
                },
                "operator_facing_view": {
                    "what_operator_can_inspect": (
                        "Child output surfaces, aggregate/shared-summary evidence, batch usage "
                        "record, and governance helper linkage."
                    ),
                    "biggest_ambiguity": (
                        "Operator still needs the explicit S7 framing to avoid collapsing child "
                        "and aggregate scopes."
                    ),
                },
                "artifact_facing_view": {
                    "canonical_machine_readable_evidence": [
                        "validation/p5_m5_s7_workflow_evidence_map.json",
                        "validation/p5_m5_s7_workflow_success_criteria.json",
                        "validation/p5_m5_s7_workflow_failure_matrix.json",
                    ],
                    "biggest_ambiguity": (
                        "The retained bundle proves workflow interpretation, not a destination "
                        "matrix or performance profile."
                    ),
                },
            },
        ],
    }


@dataclass(frozen=True, slots=True)
class P5M5S7Check:
    name: str
    ok: bool
    detail: str
    severity: M5S7Severity = "error"


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
) -> P5M5S7Check:
    observed = _read_json_object(path)
    return P5M5S7Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _all_carrier_entries() -> list[dict[str, object]]:
    scenarios = build_p5_m5_s7_workflow_scenarios()
    entries: list[dict[str, object]] = []
    for scenario in cast(list[dict[str, object]], scenarios["scenarios"]):
        entries.extend(cast(list[dict[str, object]], scenario["carriers"]))

    evidence = build_p5_m5_s7_workflow_evidence_map()
    for scenario in cast(list[dict[str, object]], evidence["scenarios"]):
        chain = cast(dict[str, object], scenario["evidence_chain"])
        for key in (
            "source_evidence",
            "task_run_evidence",
            "delivery_output_evidence",
            "usage_evidence",
            "provenance_evidence",
            "governance_recovery_evidence",
        ):
            entries.extend(cast(list[dict[str, object]], chain[key]))
    return entries


def _carrier_files_exist() -> P5M5S7Check:
    carrier_files: list[str] = []
    for entry in _all_carrier_entries():
        carrier_files.append(cast(str, entry["carrier"]).split("::", 1)[0])
    missing = [path for path in dict.fromkeys(carrier_files) if not (repo_root() / path).exists()]
    return P5M5S7Check(
        name="m5_s7_carrier_files_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s7_artifacts() -> list[P5M5S7Check]:
    checks = [
        _json_artifact_check(
            name="m5_s7_workflow_scenarios_match_helper",
            path=P5_M5_S7_WORKFLOW_SCENARIOS_PATH,
            expected=build_p5_m5_s7_workflow_scenarios(),
        ),
        _json_artifact_check(
            name="m5_s7_workflow_evidence_map_match_helper",
            path=P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH,
            expected=build_p5_m5_s7_workflow_evidence_map(),
        ),
        _json_artifact_check(
            name="m5_s7_workflow_success_criteria_match_helper",
            path=P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH,
            expected=build_p5_m5_s7_workflow_success_criteria(),
        ),
        _json_artifact_check(
            name="m5_s7_workflow_failure_matrix_match_helper",
            path=P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH,
            expected=build_p5_m5_s7_workflow_failure_matrix(),
        ),
        _json_artifact_check(
            name="m5_s7_workflow_views_match_helper",
            path=P5_M5_S7_WORKFLOW_VIEWS_PATH,
            expected=build_p5_m5_s7_workflow_views(),
        ),
        _carrier_files_exist(),
    ]

    scenario_ids = [
        cast(str, scenario["scenario_id"])
        for scenario in cast(
            list[dict[str, object]],
            build_p5_m5_s7_workflow_scenarios()["scenarios"],
        )
    ]
    evidence_ids = [
        cast(str, scenario["scenario_id"])
        for scenario in cast(
            list[dict[str, object]],
            build_p5_m5_s7_workflow_evidence_map()["scenarios"],
        )
    ]
    success_ids = [
        cast(str, scenario["scenario_id"])
        for scenario in cast(
            list[dict[str, object]],
            build_p5_m5_s7_workflow_success_criteria()["scenarios"],
        )
    ]
    failure_ids = [
        cast(str, scenario["scenario_id"])
        for scenario in cast(
            list[dict[str, object]],
            build_p5_m5_s7_workflow_failure_matrix()["scenarios"],
        )
    ]
    view_ids = [
        cast(str, scenario["scenario_id"])
        for scenario in cast(
            list[dict[str, object]],
            build_p5_m5_s7_workflow_views()["scenarios"],
        )
    ]
    checks.append(
        P5M5S7Check(
            name="m5_s7_scenario_ids_align_across_artifacts",
            ok=scenario_ids == evidence_ids == success_ids == failure_ids == view_ids,
            detail=str(scenario_ids),
        )
    )

    carrier_entries = _all_carrier_entries()
    non_exact = [
        cast(str, entry["carrier"])
        for entry in carrier_entries
        if entry["carrier_granularity"] != "exact_test_id"
    ]
    checks.append(
        P5M5S7Check(
            name="m5_s7_exact_carriers_or_explicit_caveats",
            ok=not non_exact
            and build_p5_m5_s7_workflow_scenarios()["intentional_file_level_carriers"] == [],
            detail="non_exact=" + (", ".join(non_exact) if non_exact else "none"),
        )
    )

    checks.append(
        P5M5S7Check(
            name="m5_s7_workflow_scope_stays_bounded",
            ok=build_p5_m5_s7_workflow_scenarios()["scenario_count"] == 4,
            detail="scenario_count=4",
        )
    )

    views = build_p5_m5_s7_workflow_views()
    success = build_p5_m5_s7_workflow_success_criteria()
    platform_side = cast(dict[str, object], success["platform_side_corroboration"])
    checks.append(
        P5M5S7Check(
            name="m5_s7_source_code_reading_not_required",
            ok=views["source_code_reading_required"] is False
            and success["source_code_reading_required"] is False
            and platform_side["role"] == "secondary_not_primary",
            detail=str(platform_side),
        )
    )

    if P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH.exists():
        report_text = P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S7 freezes real workflow chain truth, not load testing.",
            (
                "S7 does not repeat S3 stability work, S4 cold-operator proof, "
                "S5 release bundle hardening, or S6 operational envelope profiling."
            ),
            (
                "S7 does not start installer implementation, deployment packaging, "
                "or cloud validation."
            ),
            (
                "Exact test ids are preferred; any remaining file-level carriers are "
                "intentional and explicitly bounded."
            ),
            "Workflow truth is consumable without source-code reading as a requirement.",
            "fanout remains composition/orchestration, not a sink connector.",
        )
        missing = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S7Check(
                name="m5_s7_report_required_phrases",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
    else:
        checks.append(
            P5M5S7Check(
                name="m5_s7_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH),
            )
        )

    return checks


def format_p5_m5_s7_results(results: list[P5M5S7Check]) -> str:
    lines = ["P5-M5-S7 real-workflow checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s7_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S7 real workflow chain proof:",
            (
                "- scope: 4 representative workflow scenarios, evidence chains, "
                "success/failure, views"
            ),
            "- exact test ids are preferred and currently all carriers remain exact",
            "- workflow truth is bounded and not a load-test, installer, or cloud package",
            "- source-code reading is not required for workflow discovery",
            "- fanout remains composition/orchestration, not a sink connector",
            "- platform-side corroboration is secondary, not primary",
        )
    )


def render_p5_m5_s7_json() -> str:
    bundle = {
        "workflow_scenarios": build_p5_m5_s7_workflow_scenarios(),
        "workflow_evidence_map": build_p5_m5_s7_workflow_evidence_map(),
        "workflow_success_criteria": build_p5_m5_s7_workflow_success_criteria(),
        "workflow_failure_matrix": build_p5_m5_s7_workflow_failure_matrix(),
        "workflow_views": build_p5_m5_s7_workflow_views(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
