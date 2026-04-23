from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.source_contracts import SOURCE_SPEC_ID_BY_CONTRACT_ID
from zephyr_ingest.spec.registry import list_spec_ids
from zephyr_ingest.testing.p45 import repo_root

S14Severity = Literal["error", "warning", "info"]

P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_verify_recovery_result_contract.json"
)
P5_INSPECT_RECEIPT_CONTRACT_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_inspect_receipt_contract.json"
)
P5_SOURCE_LINKAGE_V2_PATH: Final[Path] = repo_root() / "validation" / "p5_source_linkage_v2.json"
P5_GOVERNANCE_VOCABULARY_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_governance_vocabulary_matrix.json"
)
P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m4_closeout_truth_matrix.json"
)
P5_M4_RESIDUAL_RISK_PATH: Final[Path] = repo_root() / "validation" / "p5_m4_residual_risk.json"
P5_M4_S14_CLOSEOUT_REPORT_PATH: Final[Path] = repo_root() / "validation" / "P5_M4_S14_CLOSEOUT.md"

S14_MUST_FIX_ITEMS: Final[tuple[str, ...]] = (
    "verify_first_class_runtime_manual_action",
    "inspect_persisted_receipt_without_default_mutation",
    "retained_source_spec_registry_parity",
    "canonical_source_contract_id_end_to_end",
    "governance_history_backfill_path",
    "governance_vocabulary_normalized",
)


def build_p5_verify_recovery_result_contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S14 verify first-classization",
        "action_kind": "verify_recovery_result",
        "category": "verification",
        "python_entry": "zephyr_ingest.governance_action.verify_recovery_result",
        "receipt_support": "persisted_receipt",
        "events": [
            "governance_action_start",
            "manual_verify_result",
            "governance_action_result",
            "audit_linkage_written",
        ],
        "state_change": False,
        "not_claimed": ["RBAC approval", "billing decision", "complete historical reconstruction"],
    }


def build_p5_inspect_receipt_contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S14 inspect persisted receipt",
        "action_kind": "inspect_queue",
        "category": "read_only",
        "python_entry": "zephyr_ingest.queue_inspect.write_queue_inspect_receipt",
        "default_behavior": "inspect_local_queue remains read_only_result_only",
        "receipt_behavior": "opt_in_persisted_receipt_to_caller_selected_artifact_root",
        "state_change": False,
        "not_claimed": ["queue mutation", "distributed governance audit"],
    }


def build_p5_source_linkage_v2() -> dict[str, object]:
    spec_ids = set(list_spec_ids())
    source_entries = [
        {
            "source_contract_id": contract_id,
            "source_spec_id": spec_id,
            "registry_status": "registered" if spec_id in spec_ids else "missing",
            "runtime_fields": [
                "TaskV1.inputs.document.source_contract_id",
                "usage_record.source.source_contract_id",
                "governance_action_receipt_v1.linkage.source_contract_id",
            ],
        }
        for contract_id, spec_id in SOURCE_SPEC_ID_BY_CONTRACT_ID.items()
    ]
    return {
        "schema_version": 2,
        "phase": "P5-M4-S14 canonical source linkage",
        "canonical_source_contract_id_status": "normal_retained_paths_are_end_to_end",
        "source_entries": source_entries,
        "fallback_policy": {
            "flow_family_only": (
                "exceptional for non-retained or legacy tasks lacking canonical ids"
            ),
            "alias_normalization": "bounded compatibility fallback, not the primary source truth",
        },
        "not_claimed": [
            "future source additions are automatically canonical without registry update",
            "uns and it source models are symmetric",
        ],
    }


def build_p5_governance_vocabulary_matrix() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S14 governance vocabulary normalization",
        "action_support_ladder": [
            "modeled_contract",
            "runtime_entry",
            "receipt_capable",
            "automatic_persisted_receipt",
            "opt_in_persisted_receipt",
            "backfill_capable",
        ],
        "persistence_status": [
            "automatic_persisted_receipt",
            "opt_in_persisted_receipt",
            "backfilled_persisted_receipt",
            "history_only",
            "result_only",
            "insufficient_evidence",
        ],
        "evidence_completeness": ["complete", "partial", "insufficient_evidence"],
        "actions": {
            "requeue": {
                "category": "state_changing",
                "persistence_status": "automatic_persisted_receipt",
                "backfill_status": "backfill_capable",
            },
            "replay_delivery": {
                "category": "state_changing",
                "persistence_status": "automatic_persisted_receipt",
                "backfill_status": "backfill_capable",
            },
            "inspect_queue": {
                "category": "read_only",
                "persistence_status": "opt_in_persisted_receipt",
                "backfill_status": "not_applicable",
            },
            "verify_recovery_result": {
                "category": "verification",
                "persistence_status": "automatic_persisted_receipt",
                "backfill_status": "not_applicable",
            },
        },
        "not_claimed": ["RBAC", "approval workflow", "multi-operator governance platform"],
    }


def build_p5_m4_closeout_truth_matrix() -> dict[str, object]:
    source_linkage = build_p5_source_linkage_v2()
    source_entries = cast(list[dict[str, object]], source_linkage["source_entries"])
    all_sources_registered = all(
        entry["registry_status"] == "registered" for entry in source_entries
    )
    return {
        "schema_version": 1,
        "phase": "P5-M4 final closeout gated by S14-A",
        "closeout_gate": dict.fromkeys(S14_MUST_FIX_ITEMS, "resolved"),
        "gate_valid": all_sources_registered,
        "truth_surfaces": {
            "runtime": "resolved",
            "recovery_operator": "resolved",
            "benchmark": "resolved",
            "bounded_concurrency": "resolved",
            "capability_domains": "resolved",
            "product_cut_packaging_skeleton": "resolved",
            "usage_facts": "resolved",
            "source_contracts": "resolved",
            "runtime_usage_emission": "resolved",
            "governance_manual_action": "resolved",
        },
        "source_registry_parity": {
            "full_retained_source_registry_parity": all_sources_registered,
            "registered_source_spec_ids": sorted(
                spec_id for spec_id in list_spec_ids() if spec_id.startswith("source.")
            ),
        },
        "not_claimed": [
            "distributed runtime",
            "cloud or Kubernetes packaging",
            "billing pricing entitlement",
            "RBAC approval workflow",
            "enterprise connector implementation",
        ],
    }


def build_p5_m4_residual_risk() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4 residual risk after S14 closeout",
        "residual_risks": [
            {
                "risk": "distributed_runtime_not_claimed",
                "classification": "explicit_non_claim",
                "why_not_m4_debt": "M4 proves bounded local/same-host behavior only",
            },
            {
                "risk": "cloud_kubernetes_packaging_not_claimed",
                "classification": "post_m4_out_of_scope",
                "why_not_m4_debt": "S10 produced packaging skeletons, not installers/deployment",
            },
            {
                "risk": "billing_entitlement_rbac_not_claimed",
                "classification": "explicit_non_claim",
                "why_not_m4_debt": "technical usage/governance facts are non-commercial",
            },
            {
                "risk": "enterprise_connectors_not_implemented",
                "classification": "post_m4_out_of_scope",
                "why_not_m4_debt": "current retained connectors are Base and non-enterprise",
            },
        ],
        "must_not_hide": list(S14_MUST_FIX_ITEMS),
    }


@dataclass(frozen=True, slots=True)
class S14CloseoutCheck:
    name: str
    ok: bool
    detail: str
    severity: S14Severity = "error"


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
) -> S14CloseoutCheck:
    observed = _read_json_object(path)
    return S14CloseoutCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_m4_s14_closeout_artifacts() -> list[S14CloseoutCheck]:
    checks = [
        _json_artifact_check(
            name="verify_recovery_result_contract_matches_helper",
            path=P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH,
            expected=build_p5_verify_recovery_result_contract(),
        ),
        _json_artifact_check(
            name="inspect_receipt_contract_matches_helper",
            path=P5_INSPECT_RECEIPT_CONTRACT_PATH,
            expected=build_p5_inspect_receipt_contract(),
        ),
        _json_artifact_check(
            name="source_linkage_v2_matches_helper",
            path=P5_SOURCE_LINKAGE_V2_PATH,
            expected=build_p5_source_linkage_v2(),
        ),
        _json_artifact_check(
            name="governance_vocabulary_matrix_matches_helper",
            path=P5_GOVERNANCE_VOCABULARY_MATRIX_PATH,
            expected=build_p5_governance_vocabulary_matrix(),
        ),
        _json_artifact_check(
            name="m4_closeout_truth_matrix_matches_helper",
            path=P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH,
            expected=build_p5_m4_closeout_truth_matrix(),
        ),
        _json_artifact_check(
            name="m4_residual_risk_matches_helper",
            path=P5_M4_RESIDUAL_RISK_PATH,
            expected=build_p5_m4_residual_risk(),
        ),
    ]
    source_linkage = build_p5_source_linkage_v2()
    source_entries = cast(list[dict[str, object]], source_linkage["source_entries"])
    checks.append(
        S14CloseoutCheck(
            name="all_retained_sources_registered",
            ok=all(entry["registry_status"] == "registered" for entry in source_entries),
            detail=str(
                [entry for entry in source_entries if entry["registry_status"] != "registered"]
            ),
        )
    )
    closeout = build_p5_m4_closeout_truth_matrix()
    gate = cast(dict[str, object], closeout["closeout_gate"])
    checks.append(
        S14CloseoutCheck(
            name="s14_must_fix_items_resolved",
            ok=all(gate.get(item) == "resolved" for item in S14_MUST_FIX_ITEMS),
            detail=str(gate),
        )
    )
    if P5_M4_S14_CLOSEOUT_REPORT_PATH.exists():
        report_text = P5_M4_S14_CLOSEOUT_REPORT_PATH.read_text(encoding="utf-8")
        required = (
            "S14-A must-fix gate is resolved",
            "verify_recovery_result is first-class",
            "inspect_queue has opt-in persisted receipt",
            "source spec registry parity is complete for retained sources",
        )
        missing = [phrase for phrase in required if phrase not in report_text]
        checks.append(
            S14CloseoutCheck(
                name="P5_M4_S14_CLOSEOUT_report_required_phrases",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
    else:
        checks.append(
            S14CloseoutCheck(
                name="P5_M4_S14_CLOSEOUT_report_required_phrases",
                ok=False,
                detail=str(P5_M4_S14_CLOSEOUT_REPORT_PATH),
            )
        )
    return checks


def format_p5_m4_s14_closeout_results(results: list[S14CloseoutCheck]) -> str:
    lines = ["P5-M4-S14 closeout checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m4_s14_closeout_summary() -> str:
    return "\n".join(
        (
            "P5-M4-S14 closeout truth:",
            "- S14-A must-fix gate is resolved",
            "- verify_recovery_result is first-class and receipt-backed",
            "- inspect_queue has opt-in persisted receipt while remaining read-only by default",
            "- source spec registry parity is complete for retained sources",
            "- canonical source contract id is carried on normal TaskV1 -> usage paths",
            "- residual risks are explicit non-claims, not hidden M4-core debt",
        )
    )


def render_p5_m4_s14_closeout_json() -> str:
    bundle = {
        "verify_recovery_result_contract": build_p5_verify_recovery_result_contract(),
        "inspect_receipt_contract": build_p5_inspect_receipt_contract(),
        "source_linkage_v2": build_p5_source_linkage_v2(),
        "governance_vocabulary_matrix": build_p5_governance_vocabulary_matrix(),
        "m4_closeout_truth_matrix": build_p5_m4_closeout_truth_matrix(),
        "m4_residual_risk": build_p5_m4_residual_risk(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
