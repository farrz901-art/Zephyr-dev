from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.spec.registry import list_spec_ids
from zephyr_ingest.testing.p5_source_contracts import (
    P5_SOURCE_CONTRACT_MATRIX_PATH,
    P5_SOURCE_USAGE_LINKAGE_PATH,
)
from zephyr_ingest.testing.p45 import repo_root

GovernanceAuditSeverity = Literal["error", "warning", "info"]

P5_GOVERNANCE_ACTION_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_governance_action_matrix.json"
)
P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_manual_action_audit_manifest.json"
)
P5_GOVERNANCE_USAGE_LINKAGE_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_governance_usage_linkage.json"
)
P5_SOURCE_SPEC_PARITY_PATH: Final[Path] = repo_root() / "validation" / "p5_source_spec_parity.json"
P5_GOVERNANCE_AUDIT_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S13_GOVERNANCE_AUDIT.md"
)
P5_SOURCE_SPEC_PARITY_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S13_SOURCE_SPEC_PARITY.md"
)


def build_p5_governance_action_matrix() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S13 governance/manual-action audit hardening",
        "scope": "bounded_local_operator_actions_not_rbac_or_approval_platform",
        "actions": [
            {
                "action_kind": "requeue",
                "category": "state_changing",
                "runtime_behavior": "real",
                "cli": "zephyr-ingest queue requeue",
                "python_entry": "requeue_local_task",
                "receipt_support": "automatic_persisted_receipt",
                "receipt_root": "queue_root/_governance/actions/<action_id>.json",
                "underlying_audit_reality": {
                    "spool": "persisted_in_history_plus_receipt",
                    "sqlite": "result_only_plus_receipt",
                },
                "provenance_linkage": "run_origin=requeue",
                "usage_linkage": (
                    "receipt can reference usage_record when caller has usage evidence"
                ),
            },
            {
                "action_kind": "replay_delivery",
                "category": "state_changing",
                "runtime_behavior": "real",
                "cli": "zephyr-ingest replay-delivery",
                "python_entry": "replay_delivery_dlq",
                "receipt_support": "automatic_persisted_receipt",
                "receipt_root": "out_root/_governance/actions/<action_id>.json",
                "underlying_audit_reality": "DLQ/delivery_done movement plus receipt",
                "provenance_linkage": "delivery_origin=replay",
                "usage_linkage": (
                    "receipt can reference usage_record when caller has usage evidence"
                ),
            },
            {
                "action_kind": "inspect_queue",
                "category": "read_only",
                "runtime_behavior": "real",
                "cli": "zephyr-ingest queue inspect",
                "python_entry": "inspect_local_queue",
                "receipt_support": "receipt_supported_by_helper_not_auto_written",
                "receipt_root": (
                    "caller_selected_artifact_root/_governance/actions/<action_id>.json"
                ),
                "underlying_audit_reality": "read_only_result_surface",
                "provenance_linkage": "task identity and queue state only when tasks are listed",
                "usage_linkage": "bounded; inspect does not create usage facts",
            },
            {
                "action_kind": "verify_recovery_result",
                "category": "verification",
                "runtime_behavior": "not_first_class_runtime_action",
                "cli": None,
                "python_entry": None,
                "receipt_support": "contract_modeled_not_runtime_first_class",
                "receipt_root": None,
                "underlying_audit_reality": (
                    "manual/helper verification remains external to runtime"
                ),
                "provenance_linkage": "bounded by inspected run_meta/delivery/queue evidence",
                "usage_linkage": "bounded by inspected usage_record evidence",
            },
        ],
        "not_claimed": [
            "RBAC",
            "approval workflow",
            "multi-operator collaboration platform",
            "billing or entitlement action",
            "full parity across requeue/replay/inspect/verify persistence",
        ],
    }


def build_p5_manual_action_audit_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S13 manual action audit manifest",
        "receipt_record_kind": "governance_action_receipt_v1",
        "logical_artifact": "governance_action.json",
        "physical_layout": "_governance/actions/<action_id>.json",
        "required_fields": [
            "action_id",
            "action_kind",
            "action_category",
            "recorded_at_utc",
            "status",
            "audit_support",
            "recovery_kind",
            "linkage.task_id",
            "linkage.run_id",
            "linkage.task_identity_key",
            "linkage.checkpoint_identity_key",
            "linkage.usage_record_ref",
            "linkage.source_contract_id",
            "evidence_refs",
            "result_summary",
            "not_claimed",
        ],
        "event_taxonomy": {
            "runtime_events": [
                "governance_action_start",
                "governance_action_result",
                "audit_linkage_written",
            ],
            "reserved_or_helper_events": ["manual_verify_result"],
            "not_claimed": "full governance event platform",
        },
        "redaction_boundary": {
            "must_not_include": ["secret values", "tokens", "credentials", "billing price"],
        },
    }


def build_p5_governance_usage_linkage() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S13 governance to usage/provenance linkage",
        "linkage_fields": {
            "task": ["task_id", "task_identity_key"],
            "run": ["run_id", "checkpoint_identity_key"],
            "usage": ["usage_record_ref"],
            "source": ["source_contract_id"],
            "evidence": ["evidence_refs", "result_summary"],
        },
        "action_linkage": {
            "requeue": {
                "changes_state": True,
                "recovery_kind": "requeue",
                "provenance_marker": "run_origin=requeue",
                "usage_linkage_status": "receipt_field_available_when_usage_record_known",
            },
            "replay_delivery": {
                "changes_state": True,
                "recovery_kind": "replay",
                "provenance_marker": "delivery_origin=replay",
                "usage_linkage_status": "receipt_field_available_when_usage_record_known",
            },
            "inspect_queue": {
                "changes_state": False,
                "recovery_kind": None,
                "usage_linkage_status": "read_only_no_usage_fact_created",
            },
            "verify_recovery_result": {
                "changes_state": False,
                "recovery_kind": "manual_verification",
                "usage_linkage_status": "not_first_class_runtime_action",
            },
        },
        "not_claimed": [
            "perfect linkage for every historical action",
            "billing action ledger",
            "RBAC or approval audit system",
        ],
    }


def _source_spec_mapping() -> list[dict[str, object]]:
    mappings: list[dict[str, object]] = []
    source_pairs = (
        ("http_document_v1", "source.uns.http_document.v1", "uns"),
        ("s3_document_v1", "source.uns.s3_document.v1", "uns"),
        ("git_document_v1", "source.uns.git_document.v1", "uns"),
        ("google_drive_document_v1", "source.uns.google_drive_document.v1", "uns"),
        ("confluence_document_v1", "source.uns.confluence_document.v1", "uns"),
        ("http_json_cursor_v1", "source.it.http_json_cursor.v1", "it"),
        ("postgresql_incremental_v1", "source.it.postgresql_incremental.v1", "it"),
        ("clickhouse_incremental_v1", "source.it.clickhouse_incremental.v1", "it"),
        ("kafka_partition_offset_v1", "source.it.kafka_partition_offset.v1", "it"),
        ("mongodb_incremental_v1", "source.it.mongodb_incremental.v1", "it"),
    )
    for source_contract_id, proposed_spec_id, flow_family in source_pairs:
        mappings.append(
            {
                "source_contract_id": source_contract_id,
                "proposed_source_spec_id": proposed_spec_id,
                "flow_family": flow_family,
                "status": "contracted_not_in_spec_registry",
            }
        )
    return mappings


def build_p5_source_spec_parity() -> dict[str, object]:
    spec_ids = list_spec_ids()
    source_spec_ids = [spec_id for spec_id in spec_ids if spec_id.startswith("source.")]
    return {
        "schema_version": 1,
        "phase": "P5-M4-S13 source spec parity bounded reduction",
        "full_source_spec_registry_parity_today": False,
        "current_source_spec_ids_in_registry": source_spec_ids,
        "source_contract_truth_source": str(P5_SOURCE_CONTRACT_MATRIX_PATH),
        "source_usage_linkage_truth_source": str(P5_SOURCE_USAGE_LINKAGE_PATH),
        "canonical_source_spec_direction": _source_spec_mapping(),
        "canonical_runtime_linkage": {
            "normalizer": "zephyr_ingest.usage_record.normalize_source_contract_id",
            "status": "canonical mapping helper now explicit; fallback remains flow_family_only",
        },
        "asymmetry_preserved": {
            "uns": "document acquisition source ids",
            "it": "structured progress source ids",
        },
        "not_claimed": [
            "source specs are fully registered",
            "uns and it sources are symmetric",
            "all future source additions join usage linkage automatically",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5GovernanceAuditCheck:
    name: str
    ok: bool
    detail: str
    severity: GovernanceAuditSeverity = "error"


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
) -> P5GovernanceAuditCheck:
    observed = _read_json_object(path)
    return P5GovernanceAuditCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_governance_audit_artifacts() -> list[P5GovernanceAuditCheck]:
    checks = [
        _json_artifact_check(
            name="governance_action_matrix_matches_helper",
            path=P5_GOVERNANCE_ACTION_MATRIX_PATH,
            expected=build_p5_governance_action_matrix(),
        ),
        _json_artifact_check(
            name="manual_action_audit_manifest_matches_helper",
            path=P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH,
            expected=build_p5_manual_action_audit_manifest(),
        ),
        _json_artifact_check(
            name="governance_usage_linkage_matches_helper",
            path=P5_GOVERNANCE_USAGE_LINKAGE_PATH,
            expected=build_p5_governance_usage_linkage(),
        ),
        _json_artifact_check(
            name="source_spec_parity_matches_helper",
            path=P5_SOURCE_SPEC_PARITY_PATH,
            expected=build_p5_source_spec_parity(),
        ),
    ]
    matrix = build_p5_governance_action_matrix()
    actions = cast(list[dict[str, object]], matrix["actions"])
    action_kinds = {cast(str, action["action_kind"]) for action in actions}
    checks.append(
        P5GovernanceAuditCheck(
            name="governance_matrix_covers_core_actions",
            ok=action_kinds
            == {"requeue", "replay_delivery", "inspect_queue", "verify_recovery_result"},
            detail=str(sorted(action_kinds)),
        )
    )
    verify = next(action for action in actions if action["action_kind"] == "verify_recovery_result")
    inspect = next(action for action in actions if action["action_kind"] == "inspect_queue")
    checks.append(
        P5GovernanceAuditCheck(
            name="governance_matrix_preserves_bounded_action_differences",
            ok=verify["runtime_behavior"] == "not_first_class_runtime_action"
            and inspect["category"] == "read_only",
            detail=f"verify={verify['runtime_behavior']} inspect={inspect['category']}",
        )
    )
    audit_manifest = build_p5_manual_action_audit_manifest()
    event_taxonomy = cast(dict[str, object], audit_manifest["event_taxonomy"])
    runtime_events = cast(list[object], event_taxonomy["runtime_events"])
    checks.append(
        P5GovernanceAuditCheck(
            name="governance_event_taxonomy_declared",
            ok={
                "governance_action_start",
                "governance_action_result",
                "audit_linkage_written",
            }.issubset(set(runtime_events))
            and "manual_verify_result"
            in cast(list[object], event_taxonomy["reserved_or_helper_events"]),
            detail=str(event_taxonomy),
        )
    )
    source_parity = build_p5_source_spec_parity()
    checks.append(
        P5GovernanceAuditCheck(
            name="source_spec_parity_is_bounded_not_full",
            ok=source_parity["full_source_spec_registry_parity_today"] is False
            and len(cast(list[object], source_parity["canonical_source_spec_direction"])) == 10,
            detail=f"registry={source_parity['current_source_spec_ids_in_registry']}",
        )
    )

    for report_path, required_phrases in (
        (
            P5_GOVERNANCE_AUDIT_REPORT_PATH,
            (
                "governance/manual-action truth is bounded",
                "governance_action_receipt_v1",
                "not RBAC",
                "verify is not first-class",
            ),
        ),
        (
            P5_SOURCE_SPEC_PARITY_REPORT_PATH,
            (
                "source spec parity remains partial",
                "canonical source spec direction",
                "flow_family_only fallback remains honest",
            ),
        ),
    ):
        if report_path.exists():
            report_text = report_path.read_text(encoding="utf-8")
            missing = [phrase for phrase in required_phrases if phrase not in report_text]
            checks.append(
                P5GovernanceAuditCheck(
                    name=f"{report_path.name}_required_phrases",
                    ok=not missing,
                    detail="missing=" + (", ".join(missing) if missing else "none"),
                )
            )
        else:
            checks.append(
                P5GovernanceAuditCheck(
                    name=f"{report_path.name}_required_phrases",
                    ok=False,
                    detail=str(report_path),
                )
            )
    return checks


def format_p5_governance_audit_results(results: list[P5GovernanceAuditCheck]) -> str:
    lines = ["P5-M4-S13 governance audit checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_governance_audit_summary() -> str:
    return "\n".join(
        (
            "P5-M4-S13 governance/manual-action truth:",
            "- governance/manual-action truth is bounded and non-RBAC",
            "- requeue and replay_delivery now have persisted governance action receipts",
            "- inspect_queue remains read-only; verify is not first-class runtime action",
            "- governance receipts link task/run/provenance/usage/source fields when available",
            "- source spec parity remains partial with canonical source spec direction captured",
        )
    )


def render_p5_governance_audit_json() -> str:
    bundle = {
        "governance_action_matrix": build_p5_governance_action_matrix(),
        "manual_action_audit_manifest": build_p5_manual_action_audit_manifest(),
        "governance_usage_linkage": build_p5_governance_usage_linkage(),
        "source_spec_parity": build_p5_source_spec_parity(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
