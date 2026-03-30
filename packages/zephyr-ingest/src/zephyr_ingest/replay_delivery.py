from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import httpx

from zephyr_ingest._internal.delivery_payload import (
    DeliveryPayloadV1,
    build_delivery_payload_v1_from_run_meta_dict,
)
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.obs.events import log_event

logger = logging.getLogger(__name__)


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
    webhook_url: str,
    timeout_s: float = 10.0,
    transport: httpx.BaseTransport | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    move_done: bool = True,
) -> ReplayStats:
    out_root = out_root.expanduser().resolve()
    files = _iter_dlq_files(out_root)
    if limit is not None:
        files = files[:limit]

    log_event(
        logger,
        level=logging.INFO,
        event="replay_start",
        out_root=out_root,
        destination="webhook",
        total=len(files),
        limit=limit,
        dry_run=dry_run,
        move_done=move_done,
    )

    done_dir = out_root / "_dlq" / "delivery_done"
    if move_done:
        done_dir.mkdir(parents=True, exist_ok=True)

    dest = WebhookDestination(url=webhook_url, timeout_s=timeout_s, transport=transport)

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
                destination="webhook",
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
            destination="webhook",
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
                destination="webhook",
                dlq_file=fp.name,
                sha256=sha256,
                run_id=run_id,
                dry_run=True,
                ok=None,
            )
            continue

        receipt = dest.post_payload(payload=payload, idempotency_key=f"{sha256}:{run_id}")

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
                destination="webhook",
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
                destination="webhook",
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
        destination="webhook",
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
