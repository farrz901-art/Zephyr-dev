from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, TypedDict, cast

from zephyr_core import RunMetaV1
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.task_v1 import TaskKind, TaskV1

USAGE_RECORD_SCHEMA_VERSION: Literal[1] = 1
USAGE_RECORD_FILENAME = "usage_record.json"

UsageRecordStatus = Literal["succeeded", "failed", "skipped", "unknown"]
BatchUsageRecordStatus = Literal["succeeded", "failed", "partial", "empty"]
UsageRecoveryKind = Literal["primary", "resume", "replay", "requeue", "redrive"]


class CapabilityDomainsUsedV1(TypedDict):
    base: bool
    pro: bool
    extra: bool
    evidence: list[str]


class RawUnitV1(TypedDict):
    flow_kind: TaskKind
    primary_raw_unit: Literal["document", "record_or_emitted_item"]
    maturity: Literal["stable_current", "bounded_not_fully_first_class"]
    derivative_metrics: list[str]
    not_primary_units: list[str]


class UsageSourceLinkageV1(TypedDict):
    task_document_source: str
    source_contract_id: str | None
    source_contract_status: Literal["linked", "flow_family_only"]
    raw_unit_relation: Literal["document_selection", "structured_emitted_item"]


class UsageResultV1(TypedDict):
    status: UsageRecordStatus
    run_outcome: str | None
    delivery_outcome: str | None
    failure_stage: Literal["none", "run", "delivery"]
    error_code: str | None
    failure_retryability: str | None
    failure_kind: str | None


class UsageRecoveryV1(TypedDict):
    recovery_kind: UsageRecoveryKind
    run_origin: str | None
    delivery_origin: str | None
    execution_mode: str | None


class UsageLinkageV1(TypedDict):
    task_id: str
    run_id: str
    pipeline_version: str
    task_identity_key: str | None
    checkpoint_identity_key: str | None


class UsageRecordV1(TypedDict):
    schema_version: Literal[1]
    record_kind: Literal["raw_technical_usage_fact_v1"]
    non_billing: bool
    linkage: UsageLinkageV1
    capability_domains_used: CapabilityDomainsUsedV1
    raw_unit: RawUnitV1
    source: UsageSourceLinkageV1
    result: UsageResultV1
    recovery: UsageRecoveryV1
    not_claimed: list[str]


class BatchUsageCountsV1(TypedDict):
    docs_total: int
    docs_success: int
    docs_failed: int
    docs_skipped: int
    delivery_total: int
    delivery_ok: int
    delivery_failed: int


class BatchUsageResultV1(TypedDict):
    status: BatchUsageRecordStatus
    failure_stage: Literal["none", "batch"]


class BatchUsageRecordV1(TypedDict):
    schema_version: Literal[1]
    record_kind: Literal["raw_technical_usage_batch_summary_v1"]
    non_billing: bool
    linkage: dict[str, str]
    capability_domains_used: CapabilityDomainsUsedV1
    aggregate_counts: BatchUsageCountsV1
    result: BatchUsageResultV1
    source_linkage_scope: Literal["aggregate_from_task_records_and_batch_report"]
    not_claimed: list[str]


_UNS_SOURCE_CONTRACT_BY_DOCUMENT_SOURCE: dict[str, str] = {
    "http": "http_document_v1",
    "http_document": "http_document_v1",
    "http_document_v1": "http_document_v1",
    "url": "http_document_v1",
    "s3": "s3_document_v1",
    "s3_document": "s3_document_v1",
    "s3_document_v1": "s3_document_v1",
    "git": "git_document_v1",
    "git_document": "git_document_v1",
    "git_document_v1": "git_document_v1",
    "google_drive": "google_drive_document_v1",
    "google_drive_document": "google_drive_document_v1",
    "google_drive_document_v1": "google_drive_document_v1",
    "confluence": "confluence_document_v1",
    "confluence_document": "confluence_document_v1",
    "confluence_document_v1": "confluence_document_v1",
}

_IT_SOURCE_CONTRACT_BY_DOCUMENT_SOURCE: dict[str, str] = {
    "http_json_cursor": "http_json_cursor_v1",
    "http_json_cursor_v1": "http_json_cursor_v1",
    "postgresql": "postgresql_incremental_v1",
    "postgresql_incremental": "postgresql_incremental_v1",
    "postgresql_incremental_v1": "postgresql_incremental_v1",
    "clickhouse": "clickhouse_incremental_v1",
    "clickhouse_incremental": "clickhouse_incremental_v1",
    "clickhouse_incremental_v1": "clickhouse_incremental_v1",
    "kafka": "kafka_partition_offset_v1",
    "kafka_partition_offset": "kafka_partition_offset_v1",
    "kafka_partition_offset_v1": "kafka_partition_offset_v1",
    "mongodb": "mongodb_incremental_v1",
    "mongodb_incremental": "mongodb_incremental_v1",
    "mongodb_incremental_v1": "mongodb_incremental_v1",
}


def normalize_source_contract_id(*, task_kind: TaskKind, task_document_source: str) -> str | None:
    source = task_document_source.strip().lower()
    if task_kind == "uns":
        return _UNS_SOURCE_CONTRACT_BY_DOCUMENT_SOURCE.get(source)
    return _IT_SOURCE_CONTRACT_BY_DOCUMENT_SOURCE.get(source)


def _raw_unit(*, task_kind: TaskKind) -> RawUnitV1:
    if task_kind == "uns":
        return {
            "flow_kind": "uns",
            "primary_raw_unit": "document",
            "maturity": "stable_current",
            "derivative_metrics": ["elements_count", "normalized_text_len"],
            "not_primary_units": ["page", "delivery_success"],
        }
    return {
        "flow_kind": "it",
        "primary_raw_unit": "record_or_emitted_item",
        "maturity": "bounded_not_fully_first_class",
        "derivative_metrics": ["duration_ms", "attempts"],
        "not_primary_units": ["delivery_success", "checkpoint"],
    }


def _capability_domains_used(
    *, task: TaskV1, source_contract_id: str | None
) -> CapabilityDomainsUsedV1:
    evidence = [f"task.kind={task.kind}", "current retained source/destination surface=base"]
    if source_contract_id is not None:
        evidence.append(f"source_contract_id={source_contract_id}")
    return {"base": True, "pro": False, "extra": False, "evidence": evidence}


def _status_from_meta_and_receipt(
    *,
    meta: RunMetaV1,
    receipt: DeliveryReceipt | None,
) -> UsageRecordStatus:
    outcome = None if meta.outcome is None else str(meta.outcome)
    if outcome == "success" and (receipt is None or receipt.ok):
        return "succeeded"
    if outcome in {"failed", "skipped_unsupported"}:
        return "failed" if outcome == "failed" else "skipped"
    if receipt is not None and not receipt.ok:
        return "failed"
    return "unknown"


def _result(*, meta: RunMetaV1, receipt: DeliveryReceipt | None) -> UsageResultV1:
    run_outcome = None if meta.outcome is None else str(meta.outcome)
    delivery_outcome = None
    failure_retryability = None
    failure_kind = None
    delivery_error_code = None
    if receipt is not None:
        delivery_outcome = "delivered" if receipt.ok else "failed"
        failure_retryability = receipt.failure_retryability
        failure_kind = receipt.shared_failure_kind
        delivery_error_code = receipt.shared_error_code

    failure_stage: Literal["none", "run", "delivery"] = "none"
    error_code = None
    if meta.error is not None:
        failure_stage = "run"
        error_code = meta.error.code
    if receipt is not None and not receipt.ok:
        failure_stage = "delivery"
        error_code = delivery_error_code

    return {
        "status": _status_from_meta_and_receipt(meta=meta, receipt=receipt),
        "run_outcome": run_outcome,
        "delivery_outcome": delivery_outcome,
        "failure_stage": failure_stage,
        "error_code": error_code,
        "failure_retryability": failure_retryability,
        "failure_kind": failure_kind,
    }


def _recovery(*, meta: RunMetaV1) -> UsageRecoveryV1:
    provenance = meta.provenance
    run_origin = None if provenance is None else provenance.run_origin
    delivery_origin = None if provenance is None else provenance.delivery_origin
    execution_mode = None if provenance is None else provenance.execution_mode

    recovery_kind: UsageRecoveryKind = "primary"
    if run_origin == "resume":
        recovery_kind = "resume"
    elif run_origin == "requeue":
        recovery_kind = "requeue"
    elif run_origin == "redrive":
        recovery_kind = "redrive"
    elif delivery_origin == "replay":
        recovery_kind = "replay"

    return {
        "recovery_kind": recovery_kind,
        "run_origin": run_origin,
        "delivery_origin": delivery_origin,
        "execution_mode": execution_mode,
    }


def _linkage(*, task: TaskV1, meta: RunMetaV1) -> UsageLinkageV1:
    provenance = meta.provenance
    return {
        "task_id": task.task_id,
        "run_id": meta.run_id,
        "pipeline_version": meta.pipeline_version,
        "task_identity_key": None if provenance is None else provenance.task_identity_key,
        "checkpoint_identity_key": None
        if provenance is None
        else provenance.checkpoint_identity_key,
    }


def build_usage_record_v1(
    *,
    task: TaskV1,
    meta: RunMetaV1,
    receipt: DeliveryReceipt | None,
) -> UsageRecordV1:
    source_contract_id = normalize_source_contract_id(
        task_kind=task.kind,
        task_document_source=task.inputs.document.source,
    )
    source_relation: Literal["document_selection", "structured_emitted_item"]
    source_relation = "document_selection" if task.kind == "uns" else "structured_emitted_item"
    source_status: Literal["linked", "flow_family_only"]
    source_status = "linked" if source_contract_id is not None else "flow_family_only"

    return {
        "schema_version": USAGE_RECORD_SCHEMA_VERSION,
        "record_kind": "raw_technical_usage_fact_v1",
        "non_billing": True,
        "linkage": _linkage(task=task, meta=meta),
        "capability_domains_used": _capability_domains_used(
            task=task,
            source_contract_id=source_contract_id,
        ),
        "raw_unit": _raw_unit(task_kind=task.kind),
        "source": {
            "task_document_source": task.inputs.document.source,
            "source_contract_id": source_contract_id,
            "source_contract_status": source_status,
            "raw_unit_relation": source_relation,
        },
        "result": _result(meta=meta, receipt=receipt),
        "recovery": _recovery(meta=meta),
        "not_claimed": [
            "billing",
            "pricing",
            "license entitlement",
            "subscription allowance",
            "quota decision",
            "page is a universal raw unit",
            "distributed runtime evidence",
        ],
    }


def write_usage_record_v1(
    *,
    out_dir: Path,
    task: TaskV1,
    meta: RunMetaV1,
    receipt: DeliveryReceipt | None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / USAGE_RECORD_FILENAME
    record = build_usage_record_v1(task=task, meta=meta, receipt=receipt)
    path.write_text(
        json.dumps(cast(dict[str, object], record), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return path


def _batch_status(
    *,
    docs_total: int,
    docs_failed: int,
    delivery_failed: int,
) -> BatchUsageResultV1:
    if docs_total == 0:
        return {"status": "empty", "failure_stage": "none"}
    if docs_failed > 0 or delivery_failed > 0:
        return {"status": "partial", "failure_stage": "batch"}
    return {"status": "succeeded", "failure_stage": "none"}


def build_batch_usage_record_v1(
    *,
    run_id: str,
    pipeline_version: str,
    docs_total: int,
    docs_success: int,
    docs_failed: int,
    docs_skipped: int,
    delivery_total: int,
    delivery_ok: int,
    delivery_failed: int,
) -> BatchUsageRecordV1:
    return {
        "schema_version": USAGE_RECORD_SCHEMA_VERSION,
        "record_kind": "raw_technical_usage_batch_summary_v1",
        "non_billing": True,
        "linkage": {"run_id": run_id, "pipeline_version": pipeline_version},
        "capability_domains_used": {
            "base": True,
            "pro": False,
            "extra": False,
            "evidence": ["batch aggregate over current retained source/destination surface"],
        },
        "aggregate_counts": {
            "docs_total": docs_total,
            "docs_success": docs_success,
            "docs_failed": docs_failed,
            "docs_skipped": docs_skipped,
            "delivery_total": delivery_total,
            "delivery_ok": delivery_ok,
            "delivery_failed": delivery_failed,
        },
        "result": _batch_status(
            docs_total=docs_total,
            docs_failed=docs_failed,
            delivery_failed=delivery_failed,
        ),
        "source_linkage_scope": "aggregate_from_task_records_and_batch_report",
        "not_claimed": [
            "billing",
            "pricing",
            "license entitlement",
            "subscription allowance",
            "quota decision",
            "distributed runtime evidence",
        ],
    }


def write_batch_usage_record_v1(
    *,
    out_root: Path,
    run_id: str,
    pipeline_version: str,
    docs_total: int,
    docs_success: int,
    docs_failed: int,
    docs_skipped: int,
    delivery_total: int,
    delivery_ok: int,
    delivery_failed: int,
) -> Path:
    out_root.mkdir(parents=True, exist_ok=True)
    path = out_root / USAGE_RECORD_FILENAME
    record = build_batch_usage_record_v1(
        run_id=run_id,
        pipeline_version=pipeline_version,
        docs_total=docs_total,
        docs_success=docs_success,
        docs_failed=docs_failed,
        docs_skipped=docs_skipped,
        delivery_total=delivery_total,
        delivery_ok=delivery_ok,
        delivery_failed=delivery_failed,
    )
    path.write_text(
        json.dumps(cast(dict[str, object], record), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return path
