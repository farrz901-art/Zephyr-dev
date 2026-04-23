from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict, cast

from zephyr_ingest.governance_action import (
    GovernanceEvidenceRefV1,
    build_governance_action_receipt_v1,
    write_governance_action_receipt_v1,
)
from zephyr_ingest.queue_backend_factory import LocalQueueBackendKind
from zephyr_ingest.queue_recover import QueueGovernanceActionAuditSupport, QueueGovernanceActionV1
from zephyr_ingest.source_contracts import normalize_source_contract_id
from zephyr_ingest.spool_queue import (
    QueueRecoveryProvenanceV1Dict,
    SpoolBucket,
    list_spool_bucket_paths,
    load_spool_record,
)
from zephyr_ingest.task_v1 import TaskIdentityV1Dict, TaskV1

SPOOL_BUCKETS: tuple[SpoolBucket, ...] = ("pending", "inflight", "done", "failed", "poison")
QueueTaskState = Literal["pending", "inflight", "done", "failed", "poison"]
QueueGovernanceLabel = Literal["orphaned", "requeued"]
QueuePoisonKind = Literal["attempts_exhausted", "orphaned", "not_poison"]
QueueHandlingExpectation = Literal["requeue_supported", "none"]
QueueGovernanceProblem = Literal["none", "orphaned", "poison_attempts_exhausted", "poison_orphaned"]
QueueInspectSummaryAuditSupport = Literal["persisted_in_history", "result_only", "none"]
QueueStateExplanation = Literal[
    "ready_for_local_worker_claim",
    "claimed_by_local_worker_or_recoverable_if_stale",
    "completed_by_local_worker",
    "reserved_terminal_failure_bucket_not_primary_retry_path",
    "operator_requeue_supported_after_attempt_or_orphan_threshold",
]


class QueueInspectSummaryDict(TypedDict):
    pending: int
    inflight: int
    done: int
    failed: int
    poison: int
    listed_tasks: int
    governance_problem_tasks: int
    visible_requeue_history_tasks: int
    recovery_audit_support: QueueInspectSummaryAuditSupport


class QueueInspectTaskDict(TypedDict):
    bucket: SpoolBucket
    state: QueueTaskState
    governance_labels: list[QueueGovernanceLabel]
    governance_problem: QueueGovernanceProblem
    state_explanation: QueueStateExplanation
    poison_kind: QueuePoisonKind
    handling_expectation: QueueHandlingExpectation
    recovery_audit_support: QueueGovernanceActionAuditSupport
    task_id: str
    kind: str
    record_path: str
    updated_at_utc: str
    uri: str
    source: str
    filename: str
    discovered_at_utc: str
    identity: TaskIdentityV1Dict | None
    failure_count: int
    orphan_count: int
    latest_recovery: QueueRecoveryProvenanceV1Dict | None


class QueueInspectResultDict(TypedDict):
    root: str
    summary: QueueInspectSummaryDict
    bucket: SpoolBucket | None
    tasks: list[QueueInspectTaskDict]


@dataclass(frozen=True, slots=True)
class QueueInspectSummaryV1:
    pending: int
    inflight: int
    done: int
    failed: int
    poison: int
    listed_tasks: int = 0
    governance_problem_tasks: int = 0
    visible_requeue_history_tasks: int = 0
    recovery_audit_support: QueueInspectSummaryAuditSupport = "none"

    def to_dict(self) -> QueueInspectSummaryDict:
        return {
            "pending": self.pending,
            "inflight": self.inflight,
            "done": self.done,
            "failed": self.failed,
            "poison": self.poison,
            "listed_tasks": self.listed_tasks,
            "governance_problem_tasks": self.governance_problem_tasks,
            "visible_requeue_history_tasks": self.visible_requeue_history_tasks,
            "recovery_audit_support": self.recovery_audit_support,
        }


@dataclass(frozen=True, slots=True)
class QueueInspectTaskV1:
    bucket: SpoolBucket
    state: QueueTaskState
    governance_labels: tuple[QueueGovernanceLabel, ...]
    poison_kind: QueuePoisonKind
    handling_expectation: QueueHandlingExpectation
    recovery_audit_support: QueueGovernanceActionAuditSupport
    task_id: str
    kind: str
    record_path: str
    updated_at_utc: str
    uri: str
    source: str
    filename: str
    discovered_at_utc: str
    identity: TaskIdentityV1Dict | None
    failure_count: int
    orphan_count: int
    latest_recovery: QueueRecoveryProvenanceV1Dict | None

    @property
    def governance_action_audit_support(self) -> QueueGovernanceActionAuditSupport:
        if self.latest_recovery is None:
            return "result_only"
        return "persisted_in_history"

    @property
    def governance_problem(self) -> QueueGovernanceProblem:
        if self.poison_kind == "attempts_exhausted":
            return "poison_attempts_exhausted"
        if self.poison_kind == "orphaned" and self.state == "poison":
            return "poison_orphaned"
        if self.orphan_count > 0:
            return "orphaned"
        return "none"

    @property
    def state_explanation(self) -> QueueStateExplanation:
        if self.state == "pending":
            return "ready_for_local_worker_claim"
        if self.state == "inflight":
            return "claimed_by_local_worker_or_recoverable_if_stale"
        if self.state == "done":
            return "completed_by_local_worker"
        if self.state == "failed":
            return "reserved_terminal_failure_bucket_not_primary_retry_path"
        return "operator_requeue_supported_after_attempt_or_orphan_threshold"

    @property
    def latest_governance_action(self) -> QueueGovernanceActionV1 | None:
        if self.latest_recovery is None:
            return None
        return QueueGovernanceActionV1(
            action="requeue",
            source_state=self.latest_recovery["source_bucket"],
            target_state="pending",
            recorded_at_utc=self.latest_recovery["recorded_at_utc"],
            audit_support="persisted_in_history",
        )

    def to_dict(self) -> QueueInspectTaskDict:
        return {
            "bucket": self.bucket,
            "state": self.state,
            "governance_labels": list(self.governance_labels),
            "governance_problem": self.governance_problem,
            "state_explanation": self.state_explanation,
            "poison_kind": self.poison_kind,
            "handling_expectation": self.handling_expectation,
            "recovery_audit_support": self.recovery_audit_support,
            "task_id": self.task_id,
            "kind": self.kind,
            "record_path": self.record_path,
            "updated_at_utc": self.updated_at_utc,
            "uri": self.uri,
            "source": self.source,
            "filename": self.filename,
            "discovered_at_utc": self.discovered_at_utc,
            "identity": self.identity,
            "failure_count": self.failure_count,
            "orphan_count": self.orphan_count,
            "latest_recovery": self.latest_recovery,
        }


@dataclass(frozen=True, slots=True)
class QueueInspectResultV1:
    root: str
    summary: QueueInspectSummaryV1
    bucket: SpoolBucket | None
    tasks: tuple[QueueInspectTaskV1, ...] = ()

    def to_dict(self) -> QueueInspectResultDict:
        return {
            "root": self.root,
            "summary": self.summary.to_dict(),
            "bucket": self.bucket,
            "tasks": [task.to_dict() for task in self.tasks],
        }


def inspect_local_queue(
    *,
    root: Path,
    bucket: SpoolBucket | None = None,
    limit: int | None = None,
    backend_kind: LocalQueueBackendKind = "spool",
) -> QueueInspectResultV1:
    if backend_kind == "spool":
        return inspect_local_spool_queue(root=root, bucket=bucket, limit=limit)
    return inspect_local_sqlite_queue(root=root, bucket=bucket, limit=limit)


def write_queue_inspect_receipt(
    *,
    artifact_root: Path,
    inspect_result: QueueInspectResultV1,
) -> Path:
    first_task = inspect_result.tasks[0] if inspect_result.tasks else None
    source_contract_id = None
    if first_task is not None:
        source_contract_id = normalize_source_contract_id(
            task_kind="uns" if first_task.kind == "uns" else "it",
            task_document_source=first_task.source,
        )
    evidence_refs: list[GovernanceEvidenceRefV1] = [
        {"kind": "queue_inspect_result", "ref": inspect_result.root}
    ]
    evidence_refs.extend(
        {"kind": "queue_task_ref", "ref": task.record_path} for task in inspect_result.tasks[:5]
    )
    receipt = build_governance_action_receipt_v1(
        action_kind="inspect_queue",
        action_category="read_only",
        status="observed",
        audit_support="persisted_receipt",
        recovery_kind=None,
        task_id=None if first_task is None else first_task.task_id,
        source_contract_id=source_contract_id,
        result_summary={
            "changed_state": False,
            "queue_root": inspect_result.root,
            "bucket": inspect_result.bucket,
            "listed_tasks": inspect_result.summary.listed_tasks,
            "governance_problem_tasks": inspect_result.summary.governance_problem_tasks,
        },
        evidence_refs=tuple(evidence_refs),
    )
    return write_governance_action_receipt_v1(artifact_root=artifact_root, receipt=receipt)


def inspect_local_spool_queue(
    *,
    root: Path,
    bucket: SpoolBucket | None = None,
    limit: int | None = None,
) -> QueueInspectResultV1:
    if limit is not None and limit <= 0:
        raise ValueError("limit must be > 0")

    resolved_root = root.expanduser().resolve()
    summary = QueueInspectSummaryV1(
        pending=len(list_spool_bucket_paths(root=resolved_root, bucket="pending")),
        inflight=len(list_spool_bucket_paths(root=resolved_root, bucket="inflight")),
        done=len(list_spool_bucket_paths(root=resolved_root, bucket="done")),
        failed=len(list_spool_bucket_paths(root=resolved_root, bucket="failed")),
        poison=len(list_spool_bucket_paths(root=resolved_root, bucket="poison")),
    )

    tasks: list[QueueInspectTaskV1] = []
    if bucket is not None:
        selected_paths = list_spool_bucket_paths(root=resolved_root, bucket=bucket)
        if limit is not None:
            selected_paths = selected_paths[:limit]
        tasks = [_build_task_inspection(bucket=bucket, path=path) for path in selected_paths]
        summary = _summary_with_task_view(summary=summary, tasks=tasks)

    return QueueInspectResultV1(
        root=str(resolved_root),
        summary=summary,
        bucket=bucket,
        tasks=tuple(tasks),
    )


def _build_task_inspection(*, bucket: SpoolBucket, path: Path) -> QueueInspectTaskV1:
    record = load_spool_record(path)
    task = record.task
    doc = task.inputs.document
    identity = None if task.identity is None else task.identity.to_dict()
    latest_recovery = None if not record.provenance else record.provenance[-1].to_dict()
    return QueueInspectTaskV1(
        bucket=bucket,
        state=_queue_task_state(bucket=bucket),
        governance_labels=_queue_governance_labels(
            orphan_count=record.governance.orphan_count,
            latest_recovery=latest_recovery,
        ),
        poison_kind=_queue_poison_kind(
            bucket=bucket,
            failure_count=record.governance.failure_count,
            orphan_count=record.governance.orphan_count,
        ),
        handling_expectation=_queue_handling_expectation(bucket=bucket),
        recovery_audit_support="persisted_in_history",
        task_id=task.task_id,
        kind=task.kind,
        record_path=str(path.resolve()),
        updated_at_utc=_mtime_to_utc(path),
        uri=doc.uri,
        source=doc.source,
        filename=doc.filename,
        discovered_at_utc=doc.discovered_at_utc,
        identity=identity,
        failure_count=record.governance.failure_count,
        orphan_count=record.governance.orphan_count,
        latest_recovery=latest_recovery,
    )


def inspect_local_sqlite_queue(
    *,
    root: Path,
    bucket: SpoolBucket | None = None,
    limit: int | None = None,
) -> QueueInspectResultV1:
    if limit is not None and limit <= 0:
        raise ValueError("limit must be > 0")

    resolved_root = root.expanduser().resolve()
    db_path = resolved_root / "queue.sqlite3"
    counts = dict.fromkeys(SPOOL_BUCKETS, 0)

    if db_path.exists():
        with _connect_sqlite_queue(db_path) as conn:
            rows = conn.execute(
                """
                SELECT bucket, COUNT(*) AS count
                FROM queue_tasks
                GROUP BY bucket
                """
            ).fetchall()
            for row in rows:
                bucket_name = row["bucket"]
                normalized_bucket: SpoolBucket
                if bucket_name == "pending":
                    normalized_bucket = "pending"
                elif bucket_name == "inflight":
                    normalized_bucket = "inflight"
                elif bucket_name == "done":
                    normalized_bucket = "done"
                elif bucket_name == "failed":
                    normalized_bucket = "failed"
                elif bucket_name == "poison":
                    normalized_bucket = "poison"
                else:
                    continue
                counts[normalized_bucket] = _read_sqlite_int(row=row, key="count")

    tasks: list[QueueInspectTaskV1] = []
    if bucket is not None and db_path.exists():
        tasks = _list_sqlite_bucket_tasks(db_path=db_path, bucket=bucket, limit=limit)

    summary = QueueInspectSummaryV1(
        pending=counts["pending"],
        inflight=counts["inflight"],
        done=counts["done"],
        failed=counts["failed"],
        poison=counts["poison"],
    )
    if bucket is not None:
        summary = _summary_with_task_view(summary=summary, tasks=tasks)

    return QueueInspectResultV1(
        root=str(resolved_root),
        summary=summary,
        bucket=bucket,
        tasks=tuple(tasks),
    )


def _list_sqlite_bucket_tasks(
    *,
    db_path: Path,
    bucket: SpoolBucket,
    limit: int | None,
) -> list[QueueInspectTaskV1]:
    query = """
        SELECT task_id, task_json, failure_count, orphan_count, updated_at
        FROM queue_tasks
        WHERE bucket = ?
        ORDER BY updated_at, task_id
    """
    params: tuple[object, ...]
    if limit is None:
        params = (bucket,)
    else:
        query += " LIMIT ?"
        params = (bucket, limit)

    with _connect_sqlite_queue(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    return [_build_sqlite_task_inspection(db_path=db_path, bucket=bucket, row=row) for row in rows]


def _build_sqlite_task_inspection(
    *,
    db_path: Path,
    bucket: SpoolBucket,
    row: sqlite3.Row,
) -> QueueInspectTaskV1:
    task = _load_sqlite_task(payload=_read_sqlite_str(row=row, key="task_json"))
    doc = task.inputs.document
    identity = None if task.identity is None else task.identity.to_dict()
    return QueueInspectTaskV1(
        bucket=bucket,
        state=_queue_task_state(bucket=bucket),
        governance_labels=_queue_governance_labels(
            orphan_count=_read_sqlite_int(row=row, key="orphan_count"),
            latest_recovery=None,
        ),
        poison_kind=_queue_poison_kind(
            bucket=bucket,
            failure_count=_read_sqlite_int(row=row, key="failure_count"),
            orphan_count=_read_sqlite_int(row=row, key="orphan_count"),
        ),
        handling_expectation=_queue_handling_expectation(bucket=bucket),
        recovery_audit_support="result_only",
        task_id=task.task_id,
        kind=task.kind,
        record_path=_sqlite_queue_locator(
            db_path=db_path,
            bucket=bucket,
            task_id=task.task_id,
        ),
        updated_at_utc=_epoch_to_utc(_read_sqlite_float(row=row, key="updated_at")),
        uri=doc.uri,
        source=doc.source,
        filename=doc.filename,
        discovered_at_utc=doc.discovered_at_utc,
        identity=identity,
        failure_count=_read_sqlite_int(row=row, key="failure_count"),
        orphan_count=_read_sqlite_int(row=row, key="orphan_count"),
        latest_recovery=None,
    )


def _queue_task_state(*, bucket: SpoolBucket) -> QueueTaskState:
    return bucket


def _queue_governance_labels(
    *,
    orphan_count: int,
    latest_recovery: QueueRecoveryProvenanceV1Dict | None,
) -> tuple[QueueGovernanceLabel, ...]:
    labels: list[QueueGovernanceLabel] = []
    if orphan_count > 0:
        labels.append("orphaned")
    if latest_recovery is not None and latest_recovery.get("action") == "requeue":
        labels.append("requeued")
    return tuple(labels)


def _queue_poison_kind(
    *,
    bucket: SpoolBucket,
    failure_count: int,
    orphan_count: int,
) -> QueuePoisonKind:
    if bucket != "poison":
        return "not_poison"
    if orphan_count > 0:
        return "orphaned"
    if failure_count > 0:
        return "attempts_exhausted"
    return "attempts_exhausted"


def _queue_handling_expectation(*, bucket: SpoolBucket) -> QueueHandlingExpectation:
    if bucket in ("poison", "inflight"):
        return "requeue_supported"
    return "none"


def _summary_with_task_view(
    *,
    summary: QueueInspectSummaryV1,
    tasks: list[QueueInspectTaskV1],
) -> QueueInspectSummaryV1:
    visible_requeue_history_tasks = sum(1 for task in tasks if task.latest_recovery is not None)
    if not tasks:
        recovery_audit_support: QueueInspectSummaryAuditSupport = "none"
    elif visible_requeue_history_tasks > 0:
        recovery_audit_support = "persisted_in_history"
    else:
        recovery_audit_support = tasks[0].recovery_audit_support

    return QueueInspectSummaryV1(
        pending=summary.pending,
        inflight=summary.inflight,
        done=summary.done,
        failed=summary.failed,
        poison=summary.poison,
        listed_tasks=len(tasks),
        governance_problem_tasks=sum(1 for task in tasks if task.governance_problem != "none"),
        visible_requeue_history_tasks=visible_requeue_history_tasks,
        recovery_audit_support=recovery_audit_support,
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


def _read_sqlite_float(*, row: sqlite3.Row, key: str) -> float:
    value = row[key]
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        raise TypeError(f"sqlite queue field '{key}' must be numeric")
    return float(value)


def _sqlite_queue_locator(*, db_path: Path, bucket: str, task_id: str) -> str:
    return f"{db_path.resolve()}#bucket={bucket},task_id={task_id}"


def _epoch_to_utc(epoch_s: float) -> str:
    return datetime.fromtimestamp(epoch_s, tz=UTC).isoformat().replace("+00:00", "Z")


def _mtime_to_utc(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat().replace("+00:00", "Z")
