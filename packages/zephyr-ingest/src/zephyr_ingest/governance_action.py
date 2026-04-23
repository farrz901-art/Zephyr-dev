from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict, cast
from uuid import uuid4

from zephyr_ingest.obs.events import log_event

GovernanceActionKind = Literal[
    "requeue",
    "replay_delivery",
    "inspect_queue",
    "verify_recovery_result",
]
GovernanceActionCategory = Literal["state_changing", "read_only", "verification"]
GovernanceActionAuditSupport = Literal[
    "persisted_receipt",
    "persisted_in_history",
    "result_only",
    "not_first_class",
]
GovernanceActionStatus = Literal["succeeded", "failed", "observed", "not_first_class"]


class GovernanceActionLinkageV1(TypedDict):
    task_id: str | None
    run_id: str | None
    task_identity_key: str | None
    checkpoint_identity_key: str | None
    usage_record_ref: str | None
    source_contract_id: str | None


class GovernanceEvidenceRefV1(TypedDict):
    kind: str
    ref: str


class GovernanceActionReceiptDict(TypedDict):
    schema_version: Literal[1]
    record_kind: Literal["governance_action_receipt_v1"]
    action_id: str
    action_kind: GovernanceActionKind
    action_category: GovernanceActionCategory
    recorded_at_utc: str
    status: GovernanceActionStatus
    audit_support: GovernanceActionAuditSupport
    persistence_scope: Literal["local_artifact"]
    recovery_kind: str | None
    linkage: GovernanceActionLinkageV1
    evidence_refs: list[GovernanceEvidenceRefV1]
    result_summary: dict[str, object]
    not_claimed: list[str]


@dataclass(frozen=True, slots=True)
class GovernanceActionReceiptV1:
    action_id: str
    action_kind: GovernanceActionKind
    action_category: GovernanceActionCategory
    recorded_at_utc: str
    status: GovernanceActionStatus
    audit_support: GovernanceActionAuditSupport
    recovery_kind: str | None
    linkage: GovernanceActionLinkageV1
    evidence_refs: tuple[GovernanceEvidenceRefV1, ...]
    result_summary: dict[str, object]

    def to_dict(self) -> GovernanceActionReceiptDict:
        return {
            "schema_version": 1,
            "record_kind": "governance_action_receipt_v1",
            "action_id": self.action_id,
            "action_kind": self.action_kind,
            "action_category": self.action_category,
            "recorded_at_utc": self.recorded_at_utc,
            "status": self.status,
            "audit_support": self.audit_support,
            "persistence_scope": "local_artifact",
            "recovery_kind": self.recovery_kind,
            "linkage": self.linkage,
            "evidence_refs": list(self.evidence_refs),
            "result_summary": self.result_summary,
            "not_claimed": [
                "billing",
                "pricing",
                "license entitlement",
                "RBAC approval workflow",
                "multi-operator collaboration platform",
                "distributed governance platform",
            ],
        }


def governance_action_receipt_dir(*, artifact_root: Path) -> Path:
    return artifact_root.expanduser().resolve() / "_governance" / "actions"


def now_utc_isoformat() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def build_governance_action_receipt_v1(
    *,
    action_kind: GovernanceActionKind,
    action_category: GovernanceActionCategory,
    status: GovernanceActionStatus,
    audit_support: GovernanceActionAuditSupport,
    result_summary: dict[str, object],
    recovery_kind: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    task_identity_key: str | None = None,
    checkpoint_identity_key: str | None = None,
    usage_record_ref: str | None = None,
    source_contract_id: str | None = None,
    evidence_refs: tuple[GovernanceEvidenceRefV1, ...] = (),
    action_id: str | None = None,
    recorded_at_utc: str | None = None,
) -> GovernanceActionReceiptV1:
    resolved_action_id = action_id or f"gov-{action_kind}-{uuid4().hex}"
    return GovernanceActionReceiptV1(
        action_id=resolved_action_id,
        action_kind=action_kind,
        action_category=action_category,
        recorded_at_utc=recorded_at_utc or now_utc_isoformat(),
        status=status,
        audit_support=audit_support,
        recovery_kind=recovery_kind,
        linkage={
            "task_id": task_id,
            "run_id": run_id,
            "task_identity_key": task_identity_key,
            "checkpoint_identity_key": checkpoint_identity_key,
            "usage_record_ref": usage_record_ref,
            "source_contract_id": source_contract_id,
        },
        evidence_refs=evidence_refs,
        result_summary=dict(result_summary),
    )


def write_governance_action_receipt_v1(
    *,
    artifact_root: Path,
    receipt: GovernanceActionReceiptV1,
    logger: logging.Logger | None = None,
) -> Path:
    receipt_dir = governance_action_receipt_dir(artifact_root=artifact_root)
    receipt_dir.mkdir(parents=True, exist_ok=True)
    path = receipt_dir / f"{receipt.action_id}.json"
    path.write_text(
        json.dumps(cast(dict[str, object], receipt.to_dict()), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if logger is not None:
        log_event(
            logger,
            level=logging.INFO,
            event="audit_linkage_written",
            action_id=receipt.action_id,
            action_kind=receipt.action_kind,
            receipt_path=path,
        )
    return path
