from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2
from typing import Any, Protocol, cast

import httpx

from zephyr_core import ErrorCode
from zephyr_core.contracts.v1.run_meta import (
    DeliveryOriginV1,
    ExecutionModeV1,
    RunOriginV1,
    RunProvenanceV1,
)
from zephyr_ingest._internal.delivery_payload import (
    DeliveryPayloadV1,
    build_delivery_payload_v1_from_run_meta_dict,
)
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
    normalize_weaviate_delivery_object_id,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.clickhouse import send_delivery_payload_v1_to_clickhouse
from zephyr_ingest.destinations.kafka import ProducerProtocol, send_delivery_payload_v1_to_kafka
from zephyr_ingest.destinations.loki import send_delivery_payload_v1_to_loki
from zephyr_ingest.destinations.mongodb import (
    MongoCollectionProtocol,
    send_delivery_payload_v1_to_mongodb,
)
from zephyr_ingest.destinations.opensearch import send_delivery_payload_v1_to_opensearch
from zephyr_ingest.destinations.s3 import S3ObjectWriterProtocol, send_delivery_payload_v1_to_s3
from zephyr_ingest.destinations.sqlite import send_delivery_payload_v1_to_sqlite
from zephyr_ingest.destinations.weaviate import (
    WeaviateCollectionProtocol,
    classify_weaviate_failed_objects,
    weaviate_exception_failure_kind,
    weaviate_exception_retryable,
    weaviate_failed_object_status,
)
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.governance_action import (
    GovernanceActionStatus,
    build_governance_action_receipt_v1,
    write_governance_action_receipt_v1,
)
from zephyr_ingest.obs.events import log_event

logger = logging.getLogger(__name__)


def _run_meta_with_replay_provenance(*, run_meta: dict[str, Any]) -> dict[str, Any]:
    payload = dict(run_meta)
    provenance_obj = payload.get("provenance")
    if isinstance(provenance_obj, dict):
        provenance_payload = cast(dict[str, Any], provenance_obj)
    else:
        provenance_payload = {}
    run_origin_obj = provenance_payload.get("run_origin")
    checkpoint_identity_key_obj = provenance_payload.get("checkpoint_identity_key")
    task_identity_key_obj = provenance_payload.get("task_identity_key")
    execution_mode_obj = provenance_payload.get("execution_mode")
    task_id_obj = provenance_payload.get("task_id")
    run_origin: RunOriginV1 | None = None
    if run_origin_obj == "intake":
        run_origin = "intake"
    elif run_origin_obj == "resume":
        run_origin = "resume"
    elif run_origin_obj == "redrive":
        run_origin = "redrive"
    elif run_origin_obj == "requeue":
        run_origin = "requeue"
    execution_mode: ExecutionModeV1 | None = None
    if execution_mode_obj == "batch":
        execution_mode = "batch"
    elif execution_mode_obj == "worker":
        execution_mode = "worker"
    delivery_origin: DeliveryOriginV1 = "replay"
    replay_provenance = RunProvenanceV1(
        run_origin=run_origin,
        delivery_origin=delivery_origin,
        execution_mode=execution_mode,
        task_id=task_id_obj if isinstance(task_id_obj, str) else None,
        checkpoint_identity_key=checkpoint_identity_key_obj
        if isinstance(checkpoint_identity_key_obj, str)
        else None,
        task_identity_key=task_identity_key_obj if isinstance(task_identity_key_obj, str) else None,
    )
    payload["provenance"] = replay_provenance.to_dict()
    return payload


def _load_replay_normalized_text_and_elements_count(
    *,
    artifacts: dict[str, Any],
) -> tuple[str, int] | DeliveryReceipt:
    normalized_path = Path(artifacts["normalized_path"])
    elements_path = Path(artifacts["elements_path"])

    try:
        normalized_text = normalized_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        records_path_raw = artifacts.get("records_path")
        if not isinstance(records_path_raw, str):
            return DeliveryReceipt(
                destination="weaviate",
                ok=False,
                details={
                    "retryable": False,
                    "reason": "missing_normalized_txt",
                    "normalized_path": str(normalized_path),
                    "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
                },
            )

        records_path = Path(records_path_raw)
        try:
            lines = [
                line
                for line in records_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except FileNotFoundError:
            return DeliveryReceipt(
                destination="weaviate",
                ok=False,
                details={
                    "retryable": False,
                    "reason": "missing_records_jsonl",
                    "records_path": str(records_path),
                    "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
                },
            )

        try:
            texts: list[str] = []
            for line in lines:
                raw_row = json.loads(line)
                if not isinstance(raw_row, dict):
                    raise ValueError("records.jsonl row is not an object")
                row = cast("dict[str, object]", raw_row)
                data_obj = row.get("data")
                if not isinstance(data_obj, dict):
                    raise ValueError("records.jsonl row field 'data' is not an object")
                data = cast("dict[str, object]", data_obj)
                texts.append(json.dumps(data, ensure_ascii=False, sort_keys=True))
            return "\n".join(texts), len(lines)
        except Exception as exc:
            return DeliveryReceipt(
                destination="weaviate",
                ok=False,
                details={
                    "retryable": False,
                    "reason": "invalid_records_jsonl",
                    "records_path": str(records_path),
                    "exc_type": type(exc).__name__,
                    "exc": str(exc),
                    "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
                },
            )

    try:
        raw_obj = json.loads(elements_path.read_text(encoding="utf-8"))
        if not isinstance(raw_obj, list):
            raise ValueError("elements.json is not a list")
        return normalized_text, len(cast(list[Any], raw_obj))
    except FileNotFoundError:
        return DeliveryReceipt(
            destination="weaviate",
            ok=False,
            details={
                "retryable": False,
                "reason": "missing_elements_json",
                "elements_path": str(elements_path),
                "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
            },
        )
    except Exception as exc:
        return DeliveryReceipt(
            destination="weaviate",
            ok=False,
            details={
                "retryable": False,
                "reason": "invalid_elements_json",
                "elements_path": str(elements_path),
                "exc_type": type(exc).__name__,
                "exc": str(exc),
                "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
            },
        )


class ReplaySink(Protocol):
    @property
    def name(self) -> str: ...

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt: ...


@dataclass(frozen=True, slots=True)
class WebhookReplaySink:
    url: str
    timeout_s: float = 10.0
    transport: httpx.BaseTransport | None = None

    @property
    def name(self) -> str:
        return "webhook"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        dest = WebhookDestination(url=self.url, timeout_s=self.timeout_s, transport=self.transport)
        return dest.post_payload(payload=payload, idempotency_key=idempotency_key)


@dataclass(frozen=True, slots=True)
class KafkaReplaySink:
    topic: str
    producer: ProducerProtocol
    flush_timeout_s: float = 10.0

    @property
    def name(self) -> str:
        return "kafka"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        return send_delivery_payload_v1_to_kafka(
            producer=self.producer,
            topic=self.topic,
            payload=payload,
            key_str=idempotency_key,
            flush_timeout_s=self.flush_timeout_s,
        )


@dataclass(frozen=True, slots=True)
class SqliteReplaySink:
    db_path: Path
    table_name: str = "zephyr_delivery_records"
    timeout_s: float = 5.0

    @property
    def name(self) -> str:
        return "sqlite"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        return send_delivery_payload_v1_to_sqlite(
            db_path=self.db_path,
            table_name=self.table_name,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
        )


@dataclass(frozen=True, slots=True)
class FilesystemReplaySink:
    out_root: Path

    @property
    def name(self) -> str:
        return "filesystem"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        del idempotency_key
        artifacts = payload["artifacts"]
        resolved_out_root = self.out_root.expanduser().resolve()
        out_dir = resolved_out_root / payload["sha256"]
        details: dict[str, object] = {"out_dir": str(out_dir)}

        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "run_meta.json").write_text(
                json.dumps(payload["run_meta"], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            for key in (
                "elements_path",
                "normalized_path",
                "records_path",
                "state_path",
                "logs_path",
            ):
                raw_source = artifacts.get(key)
                if raw_source is None:
                    if key in {"records_path", "state_path", "logs_path"}:
                        continue
                    return DeliveryReceipt(
                        destination="filesystem",
                        ok=False,
                        details={
                            **details,
                            "retryable": False,
                            "failure_kind": "operational",
                            "reason": f"missing_{key}",
                            "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
                        },
                    )
                if not isinstance(raw_source, str):
                    return DeliveryReceipt(
                        destination="filesystem",
                        ok=False,
                        details={
                            **details,
                            "retryable": False,
                            "failure_kind": "operational",
                            "reason": f"invalid_{key}",
                            "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
                        },
                    )
                source_path = Path(raw_source)
                copy2(source_path, out_dir / source_path.name)
        except FileNotFoundError as exc:
            return DeliveryReceipt(
                destination="filesystem",
                ok=False,
                details={
                    **details,
                    "retryable": False,
                    "failure_kind": "operational",
                    "exc_type": type(exc).__name__,
                    "exc": str(exc),
                    "error_code": str(ErrorCode.DELIVERY_INVALID_PAYLOAD),
                },
            )
        except OSError as exc:
            return DeliveryReceipt(
                destination="filesystem",
                ok=False,
                details={
                    **details,
                    "retryable": False,
                    "failure_kind": "operational",
                    "exc_type": type(exc).__name__,
                    "exc": str(exc),
                    "error_code": str(ErrorCode.DELIVERY_FAILED),
                },
            )
        return DeliveryReceipt(destination="filesystem", ok=True, details=details)


@dataclass(frozen=True, slots=True)
class WeaviateReplaySink:
    collection_name: str
    collection: WeaviateCollectionProtocol
    max_batch_errors: int = 0

    @property
    def name(self) -> str:
        return "weaviate"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        sha256 = payload["sha256"]
        obj_uuid = normalize_weaviate_delivery_object_id(sha256=sha256)

        artifacts = cast(dict[str, Any], payload["artifacts"])
        replay_material = _load_replay_normalized_text_and_elements_count(artifacts=artifacts)
        if isinstance(replay_material, DeliveryReceipt):
            return replay_material
        normalized_text, elements_count = replay_material

        run_meta = payload.get("run_meta") or {}
        run_id = run_meta.get("run_id")
        pipeline_version = run_meta.get("pipeline_version")
        timestamp_utc = run_meta.get("timestamp_utc")

        props: dict[str, Any] = {
            "sha256": sha256,
            "uuid": obj_uuid,
            "normalized_text": normalized_text,
            "elements_count": elements_count,
        }
        if isinstance(run_id, str):
            props["run_id"] = run_id
        if isinstance(pipeline_version, str):
            props["pipeline_version"] = pipeline_version
        if isinstance(timestamp_utc, str):
            props["timestamp_utc"] = timestamp_utc
        run_meta_obj = payload.get("run_meta", {})
        provenance_obj = run_meta_obj.get("provenance")
        if isinstance(provenance_obj, dict):
            typed_provenance = cast(dict[object, object], provenance_obj)
            delivery_origin = typed_provenance.get("delivery_origin")
            execution_mode = typed_provenance.get("execution_mode")
            if isinstance(delivery_origin, str):
                props["delivery_origin"] = delivery_origin
            if isinstance(execution_mode, str):
                props["execution_mode"] = execution_mode

        details: dict[str, Any] = {
            "collection": self.collection_name,
            "uuid": obj_uuid,
            "attempts": 1,
        }

        try:
            with self.collection.batch.dynamic() as batch:
                batch.add_object(properties=props, uuid=obj_uuid)
                batch_errors = batch.number_errors

            failed_objects = self.collection.batch.failed_objects
            failed_count = len(failed_objects)

            details["batch_errors"] = batch_errors
            details["failed_objects"] = failed_count

            if failed_count > 0:
                retryable, failure_kind = classify_weaviate_failed_objects(
                    failed_objects=failed_objects
                )
                details["retryable"] = retryable
                details["failure_kind"] = failure_kind
                backend_statuses = [
                    status
                    for item in failed_objects
                    if (status := weaviate_failed_object_status(item)) is not None
                ]
                if backend_statuses:
                    details["backend_statuses"] = backend_statuses
                details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
                return DeliveryReceipt(destination="weaviate", ok=False, details=details)
            if batch_errors > self.max_batch_errors:
                details["retryable"] = True
                details["failure_kind"] = "operational"
                details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
                return DeliveryReceipt(destination="weaviate", ok=False, details=details)
            details["retryable"] = False
            return DeliveryReceipt(destination="weaviate", ok=True, details=details)
        except Exception as exc:
            details["exc_type"] = type(exc).__name__
            details["exc"] = str(exc)
            details["retryable"] = weaviate_exception_retryable(exc=exc)
            details["failure_kind"] = weaviate_exception_failure_kind(exc=exc)
            details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
            return DeliveryReceipt(destination="weaviate", ok=False, details=details)


@dataclass(frozen=True, slots=True)
class S3ReplaySink:
    bucket: str
    client: S3ObjectWriterProtocol
    prefix: str = ""
    write_mode: str = "overwrite"

    @property
    def name(self) -> str:
        return "s3"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        return send_delivery_payload_v1_to_s3(
            client=self.client,
            bucket=self.bucket,
            prefix=self.prefix,
            payload=payload,
            idempotency_key=idempotency_key,
            write_mode=self.write_mode,
        )


@dataclass(frozen=True, slots=True)
class OpenSearchReplaySink:
    url: str
    index: str
    timeout_s: float = 10.0
    verify_tls: bool = True
    username: str | None = None
    password: str | None = None
    transport: httpx.BaseTransport | None = None

    @property
    def name(self) -> str:
        return "opensearch"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        return send_delivery_payload_v1_to_opensearch(
            url=self.url,
            index=self.index,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
            verify_tls=self.verify_tls,
            username=self.username,
            password=self.password,
            transport=self.transport,
        )


@dataclass(frozen=True, slots=True)
class ClickHouseReplaySink:
    url: str
    table: str
    timeout_s: float = 10.0
    database: str | None = None
    username: str | None = None
    password: str | None = None
    transport: httpx.BaseTransport | None = None

    @property
    def name(self) -> str:
        return "clickhouse"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        return send_delivery_payload_v1_to_clickhouse(
            url=self.url,
            table=self.table,
            database=self.database,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
            username=self.username,
            password=self.password,
            transport=self.transport,
        )


@dataclass(frozen=True, slots=True)
class MongoDBReplaySink:
    database: str
    collection: str
    collection_obj: MongoCollectionProtocol
    write_mode: str = "replace_upsert"

    @property
    def name(self) -> str:
        return "mongodb"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        return send_delivery_payload_v1_to_mongodb(
            collection_obj=self.collection_obj,
            database=self.database,
            collection=self.collection,
            payload=payload,
            idempotency_key=idempotency_key,
            write_mode=self.write_mode,
        )


@dataclass(frozen=True, slots=True)
class LokiReplaySink:
    url: str
    stream: str
    timeout_s: float = 10.0
    tenant_id: str | None = None
    transport: httpx.BaseTransport | None = None

    @property
    def name(self) -> str:
        return "loki"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        return send_delivery_payload_v1_to_loki(
            url=self.url,
            stream=self.stream,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
            tenant_id=self.tenant_id,
            transport=self.transport,
        )


@dataclass(frozen=True, slots=True)
class FanoutReplaySink:
    sinks: tuple[ReplaySink, ...]

    @property
    def name(self) -> str:
        children = ",".join(s.name for s in self.sinks)
        return f"fanout({children})"

    def send(self, *, payload: DeliveryPayloadV1, idempotency_key: str) -> DeliveryReceipt:
        details: dict[str, Any] = {"children": []}
        ok = True
        for s in self.sinks:
            r = s.send(payload=payload, idempotency_key=idempotency_key)
            details["children"].append(
                {"destination": r.destination, "ok": r.ok, "details": r.details}
            )
            ok = ok and r.ok
        return DeliveryReceipt(destination="fanout", ok=ok, details=details)


@dataclass(frozen=True, slots=True)
class ReplayStats:
    total: int
    attempted: int
    succeeded: int
    failed: int
    moved_to_done: int


def _iter_dlq_files(out_root: Path) -> list[Path]:
    dlq_dir = out_root / "_dlq" / "delivery"
    if not dlq_dir.exists():
        return []
    return sorted([p for p in dlq_dir.glob("*.json") if p.is_file()])


def _load_json(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("DLQ record is not a JSON object")
    return cast("dict[str, Any]", obj)


def replay_delivery_dlq(
    *,
    out_root: Path,
    webhook_url: str | None = None,
    timeout_s: float = 10.0,
    transport: httpx.BaseTransport | None = None,
    sink: ReplaySink | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    move_done: bool = True,
) -> ReplayStats:
    out_root = out_root.expanduser().resolve()
    files = _iter_dlq_files(out_root)
    if limit is not None:
        files = files[:limit]

    if sink is None:
        if webhook_url is None:
            raise ValueError("Either sink or webhook_url must be provided")
        sink = WebhookReplaySink(url=webhook_url, timeout_s=timeout_s, transport=transport)

    log_event(
        logger,
        level=logging.INFO,
        event="governance_action_start",
        action_kind="replay_delivery",
        out_root=out_root,
        destination=sink.name,
        total=len(files),
        dry_run=dry_run,
        move_done=move_done,
    )

    log_event(
        logger,
        level=logging.INFO,
        event="replay_start",
        out_root=out_root,
        destination=sink.name,
        total=len(files),
        limit=limit,
        dry_run=dry_run,
        move_done=move_done,
    )

    done_dir = out_root / "_dlq" / "delivery_done"
    if move_done:
        done_dir.mkdir(parents=True, exist_ok=True)

    attempted = succeeded = failed = moved = 0
    first_task_id: str | None = None
    first_run_id: str | None = None

    for fp in files:
        attempted += 1
        rec = _load_json(fp)

        sha256 = rec.get("sha256")
        run_id = rec.get("run_id")
        run_meta_raw = rec.get("run_meta")

        if (
            not isinstance(sha256, str)
            or not isinstance(run_id, str)
            or not isinstance(run_meta_raw, dict)
        ):
            # invalid record, keep it (treat as failed)
            log_event(
                logger,
                level=logging.WARNING,
                event="replay_result",
                destination=sink.name,
                dlq_file=fp.name,
                ok=False,
                invalid_record=True,
            )
            failed += 1
            continue

        run_meta = _run_meta_with_replay_provenance(run_meta=cast("dict[str, Any]", run_meta_raw))
        if first_run_id is None:
            first_run_id = run_id
        provenance_obj = run_meta.get("provenance")
        if first_task_id is None and isinstance(provenance_obj, dict):
            provenance = cast("dict[str, object]", provenance_obj)
            task_id_obj = provenance.get("task_id")
            if isinstance(task_id_obj, str):
                first_task_id = task_id_obj

        log_event(
            logger,
            level=logging.INFO,
            event="replay_attempt",
            destination=sink.name,
            dlq_file=fp.name,
            sha256=sha256,
            run_id=run_id,
        )

        payload: DeliveryPayloadV1 = build_delivery_payload_v1_from_run_meta_dict(
            out_root=out_root,
            sha256=sha256,
            run_meta=run_meta,
        )

        if dry_run:
            log_event(
                logger,
                level=logging.INFO,
                event="replay_result",
                destination=sink.name,
                dlq_file=fp.name,
                sha256=sha256,
                run_id=run_id,
                dry_run=True,
                ok=None,
            )
            continue

        idem = normalize_delivery_idempotency_key(
            identity=DeliveryIdentityV1(sha256=sha256, run_id=run_id)
        )
        receipt = sink.send(payload=payload, idempotency_key=idem)

        if receipt.ok:
            succeeded += 1
            if move_done:
                target = done_dir / fp.name
                fp.replace(target)
                moved += 1

            log_event(
                logger,
                level=logging.INFO,
                event="replay_result",
                destination=sink.name,
                dlq_file=fp.name,
                sha256=sha256,
                run_id=run_id,
                ok=True,
                moved=move_done,
            )
        else:
            failed += 1
            retryable = None
            if isinstance(receipt.details, dict):
                r = receipt.details.get("retryable")
                if isinstance(r, bool):
                    retryable = r
            log_event(
                logger,
                level=logging.WARNING,
                event="replay_result",
                destination=sink.name,
                dlq_file=fp.name,
                sha256=sha256,
                run_id=run_id,
                ok=False,
                retryable=retryable,
            )

    log_event(
        logger,
        level=logging.INFO,
        event="replay_done",
        out_root=out_root,
        destination=sink.name,
        total=len(files),
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        moved_to_done=moved,
    )

    stats = ReplayStats(
        total=len(files),
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        moved_to_done=moved,
    )
    if dry_run:
        governance_receipt_status: GovernanceActionStatus = "observed"
    elif failed == 0:
        governance_receipt_status = "succeeded"
    else:
        governance_receipt_status = "failed"
    governance_receipt = build_governance_action_receipt_v1(
        action_kind="replay_delivery",
        action_category="state_changing",
        status=governance_receipt_status,
        audit_support="persisted_receipt",
        recovery_kind="replay",
        task_id=first_task_id,
        run_id=first_run_id,
        result_summary={
            "total": stats.total,
            "attempted": stats.attempted,
            "succeeded": stats.succeeded,
            "failed": stats.failed,
            "moved_to_done": stats.moved_to_done,
            "destination": sink.name,
            "dry_run": dry_run,
            "move_done": move_done,
        },
        evidence_refs=(
            {"kind": "delivery_dlq_dir", "ref": str((out_root / "_dlq" / "delivery").resolve())},
            {
                "kind": "delivery_done_dir",
                "ref": str((out_root / "_dlq" / "delivery_done").resolve()),
            },
        ),
    )
    receipt_path = write_governance_action_receipt_v1(
        artifact_root=out_root,
        receipt=governance_receipt,
        logger=logger,
    )
    log_event(
        logger,
        level=logging.INFO,
        event="governance_action_result",
        action_kind="replay_delivery",
        action_id=governance_receipt.action_id,
        ok=failed == 0,
        receipt_path=receipt_path,
    )
    return stats
