from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

import httpx

from zephyr_core import ErrorCode
from zephyr_core.contracts.v1.run_meta import RunOriginV1, RunProvenanceV1
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
from zephyr_ingest.destinations.kafka import ProducerProtocol, send_delivery_payload_v1_to_kafka
from zephyr_ingest.destinations.weaviate import WeaviateCollectionProtocol
from zephyr_ingest.destinations.webhook import WebhookDestination
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
    run_origin: RunOriginV1 | None = None
    if run_origin_obj == "intake":
        run_origin = "intake"
    elif run_origin_obj == "resume":
        run_origin = "resume"
    elif run_origin_obj == "redrive":
        run_origin = "redrive"
    elif run_origin_obj == "requeue":
        run_origin = "requeue"
    replay_provenance = RunProvenanceV1(
        run_origin=run_origin,
        delivery_origin="replay",
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

            retryable = (batch_errors > self.max_batch_errors) or (failed_count > 0)
            details["retryable"] = retryable

            if retryable:
                return DeliveryReceipt(destination="weaviate", ok=False, details=details)
            return DeliveryReceipt(destination="weaviate", ok=True, details=details)
        except Exception as exc:
            details["exc_type"] = type(exc).__name__
            details["exc"] = str(exc)
            details["retryable"] = True
            details["error_code"] = str(ErrorCode.DELIVERY_WEAVIATE_FAILED)
            return DeliveryReceipt(destination="weaviate", ok=False, details=details)


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

    return ReplayStats(
        total=len(files),
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        moved_to_done=moved,
    )
