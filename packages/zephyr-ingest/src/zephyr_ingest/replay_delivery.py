from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

import httpx

from zephyr_ingest._internal.delivery_payload import (
    DeliveryPayloadV1,
    build_delivery_payload_v1_from_run_meta_dict,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.kafka import ProducerProtocol, send_delivery_payload_v1_to_kafka
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.obs.events import log_event

logger = logging.getLogger(__name__)


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

        run_meta = cast("dict[str, Any]", run_meta_raw)

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

        idem = f"{sha256}:{run_id}"
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
