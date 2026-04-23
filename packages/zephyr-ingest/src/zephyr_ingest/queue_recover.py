from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypedDict, cast

from zephyr_core.contracts.v1.run_meta import RunProvenanceV1
from zephyr_ingest.governance_action import (
    build_governance_action_receipt_v1,
    write_governance_action_receipt_v1,
)
from zephyr_ingest.obs.events import log_event
from zephyr_ingest.queue_backend_factory import LocalQueueBackendKind
from zephyr_ingest.spool_queue import (
    QueueRecoveryProvenanceV1,
    load_spool_record,
    now_utc_isoformat,
    spool_bucket_dir,
    write_spool_record,
)
from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
from zephyr_ingest.task_v1 import TaskV1

logger = logging.getLogger(__name__)

RecoverableSpoolBucket = Literal["poison", "inflight"]
QueueGovernanceActionKind = Literal["requeue"]
QueueGovernanceActionAuditSupport = Literal["persisted_in_history", "result_only"]
QueueGovernanceActionRedriveSemantics = Literal["not_modeled"]
QueueRecoverySupportStatus = Literal["supported"]
QueueRecoveryGovernanceResult = Literal["moved_to_pending"]


class QueueRecoveryResultDict(TypedDict):
    action: Literal["requeue"]
    support_status: QueueRecoverySupportStatus
    governance_result: QueueRecoveryGovernanceResult
    redrive_support: QueueGovernanceActionRedriveSemantics
    audit_support: QueueGovernanceActionAuditSupport
    root: str
    task_id: str
    kind: str
    source_bucket: RecoverableSpoolBucket
    target_bucket: Literal["pending"]
    source_path: str
    target_path: str
    failure_count: int
    orphan_count: int
    recorded_at_utc: str


class QueueRecoveryError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class QueueGovernanceActionV1:
    action: QueueGovernanceActionKind
    source_state: RecoverableSpoolBucket
    target_state: Literal["pending"]
    recorded_at_utc: str
    audit_support: QueueGovernanceActionAuditSupport
    redrive_semantics: QueueGovernanceActionRedriveSemantics = "not_modeled"


@dataclass(frozen=True, slots=True)
class QueueRecoveryResultV1:
    root: str
    task_id: str
    kind: str
    source_bucket: RecoverableSpoolBucket
    source_path: str
    target_path: str
    failure_count: int
    orphan_count: int
    recorded_at_utc: str
    task_identity_key: str | None = None
    audit_support: QueueGovernanceActionAuditSupport = "result_only"

    def to_dict(self) -> QueueRecoveryResultDict:
        return {
            "action": "requeue",
            "support_status": "supported",
            "governance_result": "moved_to_pending",
            "redrive_support": "not_modeled",
            "audit_support": self.audit_support,
            "root": self.root,
            "task_id": self.task_id,
            "kind": self.kind,
            "source_bucket": self.source_bucket,
            "target_bucket": "pending",
            "source_path": self.source_path,
            "target_path": self.target_path,
            "failure_count": self.failure_count,
            "orphan_count": self.orphan_count,
            "recorded_at_utc": self.recorded_at_utc,
        }

    def to_run_provenance(self) -> RunProvenanceV1:
        return RunProvenanceV1(
            run_origin="requeue",
            delivery_origin="primary",
            execution_mode="worker",
            task_id=self.task_id,
            task_identity_key=self.task_identity_key,
        )

    @property
    def governance_action(self) -> QueueGovernanceActionV1:
        return QueueGovernanceActionV1(
            action="requeue",
            source_state=self.source_bucket,
            target_state="pending",
            recorded_at_utc=self.recorded_at_utc,
            audit_support=self.audit_support,
        )

    @property
    def support_status(self) -> QueueRecoverySupportStatus:
        return "supported"

    @property
    def governance_result(self) -> QueueRecoveryGovernanceResult:
        return "moved_to_pending"


def requeue_local_task(
    *,
    root: Path,
    source_bucket: RecoverableSpoolBucket,
    task_id: str,
    backend_kind: LocalQueueBackendKind = "spool",
) -> QueueRecoveryResultV1:
    log_event(
        logger,
        level=logging.INFO,
        event="governance_action_start",
        action_kind="requeue",
        root=root,
        source_bucket=source_bucket,
        task_id=task_id,
        backend_kind=backend_kind,
    )
    if backend_kind == "spool":
        result = requeue_local_spool_task(
            root=root,
            source_bucket=source_bucket,
            task_id=task_id,
        )
    else:
        result = requeue_local_sqlite_task(
            root=root,
            source_bucket=source_bucket,
            task_id=task_id,
        )
    receipt = build_governance_action_receipt_v1(
        action_kind="requeue",
        action_category="state_changing",
        status="succeeded",
        audit_support="persisted_receipt",
        recovery_kind="requeue",
        task_id=result.task_id,
        task_identity_key=result.task_identity_key,
        result_summary={
            "governance_result": result.governance_result,
            "source_bucket": result.source_bucket,
            "target_bucket": "pending",
            "backend_kind": backend_kind,
            "underlying_audit_support": result.audit_support,
        },
        evidence_refs=(
            {"kind": "source_task_ref", "ref": result.source_path},
            {"kind": "target_task_ref", "ref": result.target_path},
        ),
    )
    receipt_path = write_governance_action_receipt_v1(
        artifact_root=Path(result.root),
        receipt=receipt,
        logger=logger,
    )
    log_event(
        logger,
        level=logging.INFO,
        event="governance_action_result",
        action_kind="requeue",
        action_id=receipt.action_id,
        task_id=result.task_id,
        ok=True,
        receipt_path=receipt_path,
    )
    return result


def requeue_local_spool_task(
    *,
    root: Path,
    source_bucket: RecoverableSpoolBucket,
    task_id: str,
) -> QueueRecoveryResultV1:
    resolved_root = root.expanduser().resolve()
    source_path = spool_bucket_dir(root=resolved_root, bucket=source_bucket) / f"{task_id}.json"
    target_path = spool_bucket_dir(root=resolved_root, bucket="pending") / f"{task_id}.json"

    if not source_path.exists():
        raise QueueRecoveryError(f"Task not found in {source_bucket}: {task_id}")
    if target_path.exists():
        raise QueueRecoveryError(f"Task already exists in pending: {task_id}")

    record = load_spool_record(source_path)
    if record.task.task_id != task_id:
        raise QueueRecoveryError(
            "Task record id mismatch for "
            f"{source_bucket}: expected {task_id}, got {record.task.task_id}"
        )

    recorded_at_utc = now_utc_isoformat()
    updated_record = record.__class__(
        task=record.task,
        governance=record.governance,
        provenance=(
            *record.provenance,
            QueueRecoveryProvenanceV1(
                action="requeue",
                source_bucket=source_bucket,
                target_bucket="pending",
                recorded_at_utc=recorded_at_utc,
            ),
        ),
    )
    write_spool_record(path=target_path, record=updated_record)
    source_path.unlink(missing_ok=True)
    task_identity_key = None
    if record.task.identity is not None:
        task_identity_key = normalize_task_idempotency_key(record.task)
    return QueueRecoveryResultV1(
        root=str(resolved_root),
        task_id=record.task.task_id,
        kind=record.task.kind,
        source_bucket=source_bucket,
        source_path=str(source_path),
        target_path=str(target_path),
        failure_count=record.governance.failure_count,
        orphan_count=record.governance.orphan_count,
        recorded_at_utc=recorded_at_utc,
        task_identity_key=task_identity_key,
        audit_support="persisted_in_history",
    )


def requeue_local_sqlite_task(
    *,
    root: Path,
    source_bucket: RecoverableSpoolBucket,
    task_id: str,
) -> QueueRecoveryResultV1:
    resolved_root = root.expanduser().resolve()
    db_path = resolved_root / "queue.sqlite3"
    if not db_path.exists():
        raise QueueRecoveryError(f"Task not found in {source_bucket}: {task_id}")

    with _connect_sqlite_queue(db_path) as conn:
        row = conn.execute(
            """
            SELECT task_id, task_json, failure_count, orphan_count
            FROM queue_tasks
            WHERE task_id = ?
              AND bucket = ?
            """,
            (task_id, source_bucket),
        ).fetchone()
        if row is None:
            raise QueueRecoveryError(f"Task not found in {source_bucket}: {task_id}")

        now_epoch_s = _recorded_epoch_s()
        updated = conn.execute(
            """
            UPDATE queue_tasks
            SET bucket = 'pending',
                claimed_at = NULL,
                updated_at = ?
            WHERE task_id = ?
              AND bucket = ?
            """,
            (now_epoch_s, task_id, source_bucket),
        ).rowcount
        if updated == 0:
            raise QueueRecoveryError(f"Task not found in {source_bucket}: {task_id}")

    task = _load_sqlite_task(payload=_read_sqlite_str(row=row, key="task_json"))
    task_identity_key = None
    if task.identity is not None:
        task_identity_key = normalize_task_idempotency_key(task)
    recorded_at_utc = now_utc_isoformat()
    return QueueRecoveryResultV1(
        root=str(resolved_root),
        task_id=task.task_id,
        kind=task.kind,
        source_bucket=source_bucket,
        source_path=_sqlite_queue_locator(
            db_path=db_path,
            bucket=source_bucket,
            task_id=task_id,
        ),
        target_path=_sqlite_queue_locator(
            db_path=db_path,
            bucket="pending",
            task_id=task_id,
        ),
        failure_count=_read_sqlite_int(row=row, key="failure_count"),
        orphan_count=_read_sqlite_int(row=row, key="orphan_count"),
        recorded_at_utc=recorded_at_utc,
        task_identity_key=task_identity_key,
        audit_support="result_only",
    )


def _connect_sqlite_queue(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _load_sqlite_task(*, payload: str) -> TaskV1:
    obj = json.loads(payload)
    if not isinstance(obj, dict):
        raise ValueError("sqlite queue task payload must be a JSON object")
    return TaskV1.from_dict(cast("dict[str, object]", obj))


def _read_sqlite_str(*, row: sqlite3.Row, key: str) -> str:
    value = row[key]
    if not isinstance(value, str):
        raise TypeError(f"sqlite queue field '{key}' must be a string")
    return value


def _read_sqlite_int(*, row: sqlite3.Row, key: str) -> int:
    value = row[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"sqlite queue field '{key}' must be an integer")
    return value


def _sqlite_queue_locator(*, db_path: Path, bucket: str, task_id: str) -> str:
    return f"{db_path.resolve()}#bucket={bucket},task_id={task_id}"


def _recorded_epoch_s() -> float:
    return time.time()
