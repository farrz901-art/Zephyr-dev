from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import httpx

from zephyr_ingest.destinations.webhook import WebhookDestination


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
        run_meta = rec.get("run_meta")

        if (
            not isinstance(sha256, str)
            or not isinstance(run_id, str)
            or not isinstance(run_meta, dict)
        ):
            # invalid record, keep it (treat as failed)
            failed += 1
            continue

        payload: dict[str, Any] = {
            "sha256": sha256,
            "run_meta": run_meta,
            "artifacts": {
                "out_dir": str((out_root / sha256).resolve()),
                "run_meta_path": str((out_root / sha256 / "run_meta.json").resolve()),
                "elements_path": str((out_root / sha256 / "elements.json").resolve()),
                "normalized_path": str((out_root / sha256 / "normalized.txt").resolve()),
            },
        }

        if dry_run:
            continue

        receipt = dest.post_payload(payload=payload, idempotency_key=f"{sha256}:{run_id}")

        if receipt.ok:
            succeeded += 1
            if move_done:
                target = done_dir / fp.name
                fp.replace(target)
                moved += 1
        else:
            failed += 1

    return ReplayStats(
        total=len(files),
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        moved_to_done=moved,
    )
