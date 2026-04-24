from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

M5S2Severity = Literal["error", "warning", "info"]

P5_M5_S2_EXECUTION_PLAN_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s2_execution_plan.json"
)
P5_M5_S2_DEEP_MATRIX_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_S2_DEEP_MATRIX.md"
)
P5_M5_S2_EXACT_DEEP_CARRIERS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s2_exact_deep_carriers.json"
)
P5_M5_S2_CLEANUP_RULES_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s2_cleanup_rules.json"
)
P5_M5_S2_DEEP_CLOSURES_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s2_deep_closures.json"
)

_DELIVERY_FAILURE_ANCHORS: Final[dict[str, dict[str, object]]] = {
    "webhook": {
        "failure_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py"
        ),
        "replay_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py"
        ],
        "verify_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py"
        ),
        "governance_receipt_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
        "usage_provenance_linkage_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py",
        ],
    },
    "kafka": {
        "failure_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_kafka_destination.py"
        ),
        "replay_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_kafka_sink.py",
        ],
        "verify_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py"
        ),
        "governance_receipt_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
        "usage_provenance_linkage_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ],
    },
    "opensearch": {
        "failure_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_opensearch_destination.py"
        ),
        "replay_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py"
        ],
        "verify_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py"
        ),
        "governance_receipt_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
        "usage_provenance_linkage_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ],
    },
}

_QUEUE_GOVERNANCE_ANCHORS: Final[dict[str, dict[str, object]]] = {
    "spool_queue": {
        "poison_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_requeue_poison_task_moves_to_pending_and_preserves_governance"
        ),
        "orphan_or_inflight_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_recover_stale_inflight_moves_task_back_to_pending",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_recover_stale_inflight_transitions_to_poison_at_orphan_threshold",
        ],
        "requeue_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_requeue_poison_task_moves_to_pending_and_preserves_governance",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_requeue_inflight_task_moves_to_pending_and_preserves_governance",
        ],
        "inspect_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_inspect_queue_bucket_list_surfaces_governance_and_identity_metadata",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_inspect_queue_is_read_only",
        ],
        "verify_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py"
        ),
        "governance_receipt_backfill_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
        ],
        "audit_support_level": "persisted_in_history",
    },
    "sqlite_queue": {
        "poison_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
            "test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset"
        ),
        "orphan_or_inflight_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
            "test_sqlite_queue_backend_work_source_handles_lock_contention_and_stale_inflight"
        ],
        "requeue_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
            "test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset"
        ],
        "inspect_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
            "test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset"
        ],
        "verify_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py"
        ),
        "governance_receipt_backfill_carriers": [
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
        ],
        "audit_support_level": "result_only",
    },
}

_SOURCE_DEEP_ANCHORS: Final[dict[str, dict[str, object]]] = {
    "google_drive_document_v1": {
        "success_deep_carrier": (
            "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py"
        ),
        "abnormal_or_failure_deep_carrier": (
            "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py"
        ),
        "source_contract_id_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_usage_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_provenance_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_governance_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
    },
    "confluence_document_v1": {
        "success_deep_carrier": (
            "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py"
        ),
        "abnormal_or_failure_deep_carrier": (
            "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py"
        ),
        "source_contract_id_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_usage_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_provenance_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_governance_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
    },
    "kafka_partition_offset_v1": {
        "success_deep_carrier": ("packages/it-stream/src/it_stream/tests/test_kafka_source.py"),
        "abnormal_or_failure_deep_carrier": (
            "packages/it-stream/src/it_stream/tests/test_kafka_source.py"
        ),
        "source_contract_id_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_usage_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
        "source_to_provenance_carriers": [
            "packages/it-stream/src/it_stream/tests/test_kafka_source.py",
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
        ],
        "source_to_governance_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
    },
}

_FANOUT_DEEP: Final[dict[str, object]] = {
    "all_ok_aggregate_carrier": (
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
        "test_fanout_all_ok"
    ),
    "partial_failure_carrier": (
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
        "test_fanout_any_fail"
    ),
    "shared_failure_summary_carrier": (
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
        "test_fanout_surfaces_shared_failure_summary_when_failed_children_agree"
    ),
    "child_and_aggregate_visibility_carrier": (
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py"
    ),
    "usage_provenance_governance_visibility_carriers": [
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
    ],
    "surface_type": "destination_composition_orchestration",
    "single_sink_connector": False,
}

_LOW_COST_CONTROL: Final[dict[str, object]] = {
    "filesystem": {
        "control_carrier": (
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py"
        ),
        "role": "low_cost_output_control_anchor",
        "note": (
            "retained as a low-cost control anchor for output visibility and isolation "
            "discipline, not as a new deep-closure family"
        ),
    }
}

_SHARED_SUPPORT: Final[dict[str, list[str]]] = {
    "replay": [
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_kafka_sink.py",
    ],
    "verify": ["packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py"],
    "governance": ["packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"],
    "usage_and_provenance": [
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
    ],
}

_S2_EXECUTION_COMMANDS: Final[tuple[dict[str, object], ...]] = (
    {
        "name": "delivery_failure_replay_verify_deep",
        "closure": "failure_replay_verify_governance_receipt",
        "scope": (
            "webhook + kafka + opensearch failure anchors with shared "
            "replay/verify/governance/usage"
        ),
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_kafka_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_opensearch_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_kafka_sink.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
        ),
    },
    {
        "name": "queue_requeue_inspect_verify_deep",
        "closure": "poison_orphan_requeue_inspect_verify",
        "scope": (
            "spool_queue + sqlite_queue governance deep chain with "
            "explicit support-level differences"
        ),
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_requeue_poison_task_moves_to_pending_and_preserves_governance "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_recover_stale_inflight_moves_task_back_to_pending "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_recover_stale_inflight_transitions_to_poison_at_orphan_threshold "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_requeue_inflight_task_moves_to_pending_and_preserves_governance "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_inspect_queue_bucket_list_surfaces_governance_and_identity_metadata "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
            "test_inspect_queue_is_read_only "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
            "test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
            "test_sqlite_queue_backend_work_source_handles_lock_contention_and_stale_inflight "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
    },
    {
        "name": "source_usage_governance_provenance_deep",
        "closure": "source_contract_id_usage_governance_provenance",
        "scope": (
            "google_drive + confluence + kafka source deep anchors "
            "with shared usage/governance visibility"
        ),
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py "
            "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py "
            "packages/it-stream/src/it_stream/tests/test_kafka_source.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py"
        ),
    },
    {
        "name": "fanout_composition_deep",
        "closure": "fanout_partial_failure_shared_summary_governance_usage",
        "scope": (
            "fanout partial failure + shared summary + governance/usage "
            "visibility with filesystem control anchor"
        ),
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
            "test_fanout_all_ok "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
            "test_fanout_any_fail "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
            "test_fanout_surfaces_shared_failure_summary_when_failed_children_agree "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
    },
    {
        "name": "s2_artifact_drift",
        "closure": "artifact_and_report_consistency",
        "scope": "S2 artifact/helper drift protection",
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m5_s2.py"
        ),
    },
)


def build_p5_m5_s2_exact_deep_carriers() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S2 exact representative deep carriers",
        "delivery_failure_replay_verify": _DELIVERY_FAILURE_ANCHORS,
        "queue_governance": _QUEUE_GOVERNANCE_ANCHORS,
        "representative_sources": _SOURCE_DEEP_ANCHORS,
        "fanout_deep": _FANOUT_DEEP,
        "low_cost_control_anchor": _LOW_COST_CONTROL,
        "shared_support_carriers": _SHARED_SUPPORT,
    }


def build_p5_m5_s2_cleanup_rules() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S2 deep cleanup and isolation rules",
        "rules": {
            "webhook_deep_runs": {
                "rule": (
                    "independent out_root, independent _dlq/delivery and delivery_done, "
                    "independent governance receipt scope"
                )
            },
            "kafka_deep_runs": {
                "rule": (
                    "independent topic/run identity, replay.db, out_root, governance receipts; "
                    "do not rely on residual broker state"
                )
            },
            "opensearch_deep_runs": {
                "rule": (
                    "independent index/namespace/run-specific target with explicit cleanup or "
                    "unique isolation"
                )
            },
            "spool_queue_deep_runs": {
                "rule": "independent spool root; no bucket reuse across cases"
            },
            "sqlite_queue_deep_runs": {"rule": "independent queue.sqlite3 or queue root per case"},
            "replay_db": {"rule": "single-case replay.db; no reuse across deep anchors"},
            "verify_receipts": {"rule": "case-scoped _governance/actions/<action_id>.json"},
            "backfill_outputs": {
                "rule": ("case-scoped outputs; do not mix with replay/requeue/inspect receipt dirs")
            },
            "google_drive_and_confluence": {
                "rule": (
                    "independent case identity; no residual remote state; explicit cleanup "
                    "or unique naming if needed"
                )
            },
            "kafka_partition_offset": {
                "rule": (
                    "independent topic/run/checkpoint identity; do not reuse consumer progress "
                    "or checkpoint artifacts"
                )
            },
            "fanout_deep_runs": {
                "rule": (
                    "child outputs and aggregate outputs stay in the same case scope; "
                    "do not mix with direct-sink cases"
                )
            },
            "filesystem_control_anchor": {
                "rule": (
                    "independent low-cost output scope; used as a control anchor, not a deep "
                    "sink permutation expansion"
                )
            },
        },
        "not_allowed": [
            "reuse dirty residual state",
            "shared replay.db across independent deep anchors",
            "mix fanout child outputs with direct-sink cases",
            "implicit remote cleanup assumptions",
            "residual broker progress reuse",
        ],
    }


def build_p5_m5_s2_deep_closures() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S2 grouped deep closures",
        "closures": {
            "failure_replay_verify_governance_receipt": {
                "included_anchors": ["webhook", "kafka", "opensearch"],
                "carrier_sets": {
                    "failure_anchors": _DELIVERY_FAILURE_ANCHORS,
                    "shared_replay": _SHARED_SUPPORT["replay"],
                    "shared_verify": _SHARED_SUPPORT["verify"],
                    "shared_governance": _SHARED_SUPPORT["governance"],
                    "shared_usage_provenance": _SHARED_SUPPORT["usage_and_provenance"],
                },
                "grouped_command_name": "delivery_failure_replay_verify_deep",
                "proves": [
                    "destination failure anchors are explicitly linked to replay",
                    "verify step is explicit rather than inferred",
                    "governance receipt closure is explicit rather than implied",
                ],
            },
            "poison_orphan_requeue_inspect_verify": {
                "included_anchors": ["spool_queue", "sqlite_queue"],
                "carrier_sets": {
                    "queue_anchors": _QUEUE_GOVERNANCE_ANCHORS,
                    "shared_verify": _SHARED_SUPPORT["verify"],
                    "shared_governance": _SHARED_SUPPORT["governance"],
                },
                "grouped_command_name": "queue_requeue_inspect_verify_deep",
                "support_difference": {
                    "spool_queue": "persisted_in_history",
                    "sqlite_queue": "result_only",
                },
            },
            "source_contract_id_usage_governance_provenance": {
                "included_anchors": [
                    "google_drive_document_v1",
                    "confluence_document_v1",
                    "kafka_partition_offset_v1",
                ],
                "carrier_sets": {
                    "source_anchors": _SOURCE_DEEP_ANCHORS,
                    "shared_usage_provenance": _SHARED_SUPPORT["usage_and_provenance"],
                    "shared_governance": _SHARED_SUPPORT["governance"],
                    "shared_replay_context": [
                        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py"
                    ],
                },
                "grouped_command_name": "source_usage_governance_provenance_deep",
            },
            "fanout_partial_failure_shared_summary_governance_usage": {
                "included_anchors": ["fanout", "filesystem"],
                "carrier_sets": {
                    "fanout": _FANOUT_DEEP,
                    "filesystem_control": _LOW_COST_CONTROL,
                    "shared_usage_provenance": _SHARED_SUPPORT["usage_and_provenance"],
                    "shared_governance": _SHARED_SUPPORT["governance"],
                },
                "grouped_command_name": "fanout_composition_deep",
                "fanout_truth": {
                    "surface_type": "destination_composition_orchestration",
                    "single_sink_connector": False,
                },
            },
        },
    }


@dataclass(frozen=True, slots=True)
class P5M5S2Check:
    name: str
    ok: bool
    detail: str
    severity: M5S2Severity = "error"


def build_p5_m5_s2_execution_plan() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S2 representative deep validation",
        "status": "representative_deep_layer_only",
        "strategy": "representative_deep_not_all_pairs",
        "included_representative_anchors": {
            "delivery_failure": ["webhook", "kafka", "opensearch"],
            "queue_governance": ["spool_queue", "sqlite_queue"],
            "source_deep": [
                "google_drive_document_v1",
                "confluence_document_v1",
                "kafka_partition_offset_v1",
            ],
            "composition": ["fanout"],
            "low_cost_control": ["filesystem"],
        },
        "execution_size": {
            "delivery_replay_verify_deep_units": 9,
            "queue_governance_deep_units": 10,
            "source_deep_units": 9,
            "fanout_deep_units": 3,
            "representative_deep_units_total_rough": 31,
            "filesystem_control_anchor_included": True,
            "note": (
                "filesystem is included as a low-cost control anchor without creating an "
                "additional deep-closure family"
            ),
        },
        "exact_deep_carrier_artifact": "validation/p5_m5_s2_exact_deep_carriers.json",
        "deep_cleanup_rules_artifact": "validation/p5_m5_s2_cleanup_rules.json",
        "deep_closures_artifact": "validation/p5_m5_s2_deep_closures.json",
        "grouped_execution_commands": list(_S2_EXECUTION_COMMANDS),
        "shared_support_is_explicit": True,
        "deferred_to_s3": [
            "repeatability execution",
            "soak execution",
            "cleanup repetition proof",
        ],
        "out_of_scope": [
            "flat all-pairs deep validation",
            "distributed runtime",
            "cloud or Kubernetes packaging",
            "billing pricing entitlement",
            "RBAC approval workflow",
            "enterprise connector implementation",
        ],
        "fanout_truth": {
            "surface_type": "destination_composition_orchestration",
            "single_sink_connector": False,
        },
    }


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
) -> P5M5S2Check:
    observed = _read_json_object(path)
    return P5M5S2Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _deep_carrier_files_exist() -> P5M5S2Check:
    carriers = build_p5_m5_s2_exact_deep_carriers()
    paths: list[str] = []

    def _collect(obj: object) -> None:
        if isinstance(obj, str) and obj.endswith(".py"):
            paths.append(obj)
        elif isinstance(obj, str) and ".py::" in obj:
            paths.append(obj.split("::", 1)[0])
        elif isinstance(obj, dict):
            for value in cast(dict[object, object], obj).values():
                _collect(value)
        elif isinstance(obj, list):
            for value in cast(list[object], obj):
                _collect(value)

    _collect(carriers)
    missing = [path for path in dict.fromkeys(paths) if not (repo_root() / path).exists()]
    return P5M5S2Check(
        name="s2_exact_deep_carrier_files_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s2_artifacts() -> list[P5M5S2Check]:
    checks = [
        _json_artifact_check(
            name="m5_s2_execution_plan_matches_helper",
            path=P5_M5_S2_EXECUTION_PLAN_PATH,
            expected=build_p5_m5_s2_execution_plan(),
        ),
        _json_artifact_check(
            name="m5_s2_exact_deep_carriers_match_helper",
            path=P5_M5_S2_EXACT_DEEP_CARRIERS_PATH,
            expected=build_p5_m5_s2_exact_deep_carriers(),
        ),
        _json_artifact_check(
            name="m5_s2_cleanup_rules_match_helper",
            path=P5_M5_S2_CLEANUP_RULES_PATH,
            expected=build_p5_m5_s2_cleanup_rules(),
        ),
        _json_artifact_check(
            name="m5_s2_deep_closures_match_helper",
            path=P5_M5_S2_DEEP_CLOSURES_PATH,
            expected=build_p5_m5_s2_deep_closures(),
        ),
        _deep_carrier_files_exist(),
    ]

    plan = build_p5_m5_s2_execution_plan()
    size = cast(dict[str, object], plan["execution_size"])
    checks.append(
        P5M5S2Check(
            name="s2_representative_size_stays_roughly_31_units",
            ok=size["representative_deep_units_total_rough"] == 31,
            detail=str(size),
        )
    )
    checks.append(
        P5M5S2Check(
            name="s2_strategy_stays_representative_not_all_pairs",
            ok=plan["strategy"] == "representative_deep_not_all_pairs",
            detail=cast(str, plan["strategy"]),
        )
    )
    checks.append(
        P5M5S2Check(
            name="fanout_stays_composition_not_sink_in_s2",
            ok=cast(dict[str, object], plan["fanout_truth"])["single_sink_connector"] is False,
            detail=str(plan["fanout_truth"]),
        )
    )
    closures = build_p5_m5_s2_deep_closures()
    checks.append(
        P5M5S2Check(
            name="s2_deep_closure_count_is_explicit",
            ok=len(cast(dict[str, object], closures["closures"])) == 4,
            detail=str(list(cast(dict[str, object], closures["closures"]).keys())),
        )
    )
    queue_anchors = cast(
        dict[str, object], build_p5_m5_s2_exact_deep_carriers()["queue_governance"]
    )
    spool_support = cast(dict[str, object], queue_anchors["spool_queue"])["audit_support_level"]
    sqlite_support = cast(dict[str, object], queue_anchors["sqlite_queue"])["audit_support_level"]
    checks.append(
        P5M5S2Check(
            name="s2_spool_sqlite_support_difference_remains_explicit",
            ok=spool_support == "persisted_in_history" and sqlite_support == "result_only",
            detail=f"spool={spool_support!r}; sqlite={sqlite_support!r}",
        )
    )

    if P5_M5_S2_DEEP_MATRIX_REPORT_PATH.exists():
        report_text = P5_M5_S2_DEEP_MATRIX_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S2 deeply validates representative anchors only",
            "grouped deep closures are explicit",
            "fanout remains composition/orchestration, not a sink connector",
            "S1 was baseline coverage; S2 is representative deep proof",
            "S3 repeatability/soak/cleanup execution is deferred",
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S2Check(
                name="m5_s2_report_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )
    else:
        checks.append(
            P5M5S2Check(
                name="m5_s2_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S2_DEEP_MATRIX_REPORT_PATH),
            )
        )

    return checks


def format_p5_m5_s2_results(results: list[P5M5S2Check]) -> str:
    lines = ["P5-M5-S2 representative deep checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s2_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S2 representative deep validation:",
            "- scope: representative deep anchors only, not all-pairs",
            (
                "- closures: failure/replay/verify, queue requeue/inspect/verify, "
                "source usage/governance/provenance, fanout partial-failure summary"
            ),
            "- shared support carriers are explicit rather than hidden in author reasoning",
            "- spool and sqlite governance support differences remain explicit",
            "- fanout remains composition/orchestration, not a sink connector",
            "- S3 repeatability/soak/cleanup execution remains deferred",
        )
    )


def render_p5_m5_s2_json() -> str:
    bundle = {
        "execution_plan": build_p5_m5_s2_execution_plan(),
        "exact_deep_carriers": build_p5_m5_s2_exact_deep_carriers(),
        "cleanup_rules": build_p5_m5_s2_cleanup_rules(),
        "deep_closures": build_p5_m5_s2_deep_closures(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
