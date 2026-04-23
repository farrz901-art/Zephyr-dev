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
GovernanceBackfillStatus = Literal["receipt_written", "insufficient_evidence"]


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


class GovernanceBackfillEntryDict(TypedDict):
    evidence_ref: str
    backfill_status: GovernanceBackfillStatus
    action_kind: GovernanceActionKind | None
    receipt_path: str | None
    reason: str | None


class GovernanceBackfillResultDict(TypedDict):
    record_kind: Literal["governance_history_backfill_result_v1"]
    artifact_root: str
    entries: list[GovernanceBackfillEntryDict]
    written_receipts: int
    insufficient_evidence: int


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


@dataclass(frozen=True, slots=True)
class GovernanceVerificationResultV1:
    ok: bool
    receipt_path: Path
    missing_evidence: tuple[str, ...]
    action_id: str


@dataclass(frozen=True, slots=True)
class GovernanceBackfillResultV1:
    artifact_root: str
    entries: tuple[GovernanceBackfillEntryDict, ...]

    @property
    def written_receipts(self) -> int:
        return sum(1 for entry in self.entries if entry["backfill_status"] == "receipt_written")

    @property
    def insufficient_evidence(self) -> int:
        return sum(
            1 for entry in self.entries if entry["backfill_status"] == "insufficient_evidence"
        )

    def to_dict(self) -> GovernanceBackfillResultDict:
        return {
            "record_kind": "governance_history_backfill_result_v1",
            "artifact_root": self.artifact_root,
            "entries": list(self.entries),
            "written_receipts": self.written_receipts,
            "insufficient_evidence": self.insufficient_evidence,
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


def verify_recovery_result(
    *,
    artifact_root: Path,
    evidence_paths: tuple[Path, ...],
    task_id: str | None = None,
    run_id: str | None = None,
    usage_record_ref: str | None = None,
    source_contract_id: str | None = None,
    logger: logging.Logger | None = None,
) -> GovernanceVerificationResultV1:
    resolved_root = artifact_root.expanduser().resolve()
    resolved_evidence = tuple(path.expanduser().resolve() for path in evidence_paths)
    missing = tuple(str(path) for path in resolved_evidence if not path.exists())
    ok = not missing
    if logger is not None:
        log_event(
            logger,
            level=logging.INFO,
            event="governance_action_start",
            action_kind="verify_recovery_result",
            artifact_root=resolved_root,
            evidence_count=len(resolved_evidence),
        )
    receipt = build_governance_action_receipt_v1(
        action_kind="verify_recovery_result",
        action_category="verification",
        status="succeeded" if ok else "failed",
        audit_support="persisted_receipt",
        recovery_kind="manual_verification",
        task_id=task_id,
        run_id=run_id,
        usage_record_ref=usage_record_ref,
        source_contract_id=source_contract_id,
        result_summary={
            "verification_result": "passed" if ok else "failed",
            "changed_state": False,
            "checked_evidence_count": len(resolved_evidence),
            "missing_evidence": list(missing),
        },
        evidence_refs=tuple(
            {"kind": "verification_evidence_path", "ref": str(path)} for path in resolved_evidence
        ),
    )
    receipt_path = write_governance_action_receipt_v1(
        artifact_root=resolved_root,
        receipt=receipt,
        logger=logger,
    )
    if logger is not None:
        log_event(
            logger,
            level=logging.INFO if ok else logging.WARNING,
            event="manual_verify_result",
            action_kind="verify_recovery_result",
            action_id=receipt.action_id,
            ok=ok,
            missing_evidence=len(missing),
        )
        log_event(
            logger,
            level=logging.INFO if ok else logging.WARNING,
            event="governance_action_result",
            action_kind="verify_recovery_result",
            action_id=receipt.action_id,
            ok=ok,
            receipt_path=receipt_path,
        )
    return GovernanceVerificationResultV1(
        ok=ok,
        receipt_path=receipt_path,
        missing_evidence=missing,
        action_id=receipt.action_id,
    )


def backfill_governance_history_from_evidence(
    *,
    artifact_root: Path,
    evidence_paths: tuple[Path, ...],
    logger: logging.Logger | None = None,
) -> GovernanceBackfillResultV1:
    resolved_root = artifact_root.expanduser().resolve()
    entries: list[GovernanceBackfillEntryDict] = []
    for evidence_path in evidence_paths:
        resolved_evidence = evidence_path.expanduser().resolve()
        entry = _backfill_one_evidence(
            artifact_root=resolved_root,
            evidence_path=resolved_evidence,
            logger=logger,
        )
        entries.append(entry)
    return GovernanceBackfillResultV1(artifact_root=str(resolved_root), entries=tuple(entries))


def _backfill_one_evidence(
    *,
    artifact_root: Path,
    evidence_path: Path,
    logger: logging.Logger | None,
) -> GovernanceBackfillEntryDict:
    evidence_ref = str(evidence_path)
    loaded = _read_json_object(evidence_path)
    if loaded is None:
        return {
            "evidence_ref": evidence_ref,
            "backfill_status": "insufficient_evidence",
            "action_kind": None,
            "receipt_path": None,
            "reason": "missing_or_non_object_json",
        }

    receipt = _receipt_from_historical_evidence(evidence_path=evidence_path, evidence=loaded)
    if receipt is None:
        return {
            "evidence_ref": evidence_ref,
            "backfill_status": "insufficient_evidence",
            "action_kind": None,
            "receipt_path": None,
            "reason": "no_supported_requeue_or_replay_evidence",
        }
    receipt_path = write_governance_action_receipt_v1(
        artifact_root=artifact_root,
        receipt=receipt,
        logger=logger,
    )
    return {
        "evidence_ref": evidence_ref,
        "backfill_status": "receipt_written",
        "action_kind": receipt.action_kind,
        "receipt_path": str(receipt_path),
        "reason": None,
    }


def _read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded_obj, dict):
        return None
    return cast(dict[str, object], loaded_obj)


def _receipt_from_historical_evidence(
    *, evidence_path: Path, evidence: dict[str, object]
) -> GovernanceActionReceiptV1 | None:
    action = evidence.get("action")
    task_id_obj = evidence.get("task_id")
    if action == "requeue" and isinstance(task_id_obj, str):
        source_contract_id_obj = evidence.get("source_contract_id")
        task_identity_obj = evidence.get("task_identity_key")
        return build_governance_action_receipt_v1(
            action_kind="requeue",
            action_category="state_changing",
            status="observed",
            audit_support="persisted_receipt",
            recovery_kind="requeue",
            task_id=task_id_obj,
            task_identity_key=task_identity_obj if isinstance(task_identity_obj, str) else None,
            source_contract_id=source_contract_id_obj
            if isinstance(source_contract_id_obj, str)
            else None,
            result_summary={
                "backfill_source": "historical_requeue_evidence",
                "changed_state": "historical_action_observed_not_replayed",
            },
            evidence_refs=({"kind": "historical_requeue_evidence", "ref": str(evidence_path)},),
        )

    run_meta_obj = evidence.get("run_meta")
    if isinstance(run_meta_obj, dict):
        run_meta = cast(dict[str, object], run_meta_obj)
        provenance_obj = run_meta.get("provenance")
        provenance = (
            cast(dict[str, object], provenance_obj) if isinstance(provenance_obj, dict) else {}
        )
        delivery_origin = provenance.get("delivery_origin")
        task_id = provenance.get("task_id")
        run_id = evidence.get("run_id")
        if delivery_origin == "replay" and isinstance(run_id, str):
            return build_governance_action_receipt_v1(
                action_kind="replay_delivery",
                action_category="state_changing",
                status="observed",
                audit_support="persisted_receipt",
                recovery_kind="replay",
                task_id=task_id if isinstance(task_id, str) else None,
                run_id=run_id,
                result_summary={
                    "backfill_source": "historical_replay_evidence",
                    "changed_state": "historical_action_observed_not_replayed",
                },
                evidence_refs=({"kind": "historical_replay_evidence", "ref": str(evidence_path)},),
            )
    return None
