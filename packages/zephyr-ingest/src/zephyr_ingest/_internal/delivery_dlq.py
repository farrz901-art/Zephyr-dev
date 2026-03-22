from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_ingest.destinations.base import DeliveryReceipt


def _slug(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    return s[:80] or "unknown"


def write_delivery_dlq(
    *,
    out_root: Path,
    sha256: str,
    meta: RunMetaV1,
    receipt: DeliveryReceipt,
) -> Path:
    """Write a delivery DLQ record for a failed delivery attempt."""
    dlq_dir = out_root / "_dlq" / "delivery"
    dlq_dir.mkdir(parents=True, exist_ok=True)

    fn = f"{sha256}__{meta.run_id}__{_slug(receipt.destination)}.json"
    path = dlq_dir / fn

    payload: dict[str, Any] = {
        "sha256": sha256,
        "run_id": meta.run_id,
        "pipeline_version": meta.pipeline_version,
        "timestamp_utc": meta.timestamp_utc,
        "schema_version": meta.schema_version,
        "outcome": None if meta.outcome is None else str(meta.outcome),
        "destination_receipt": asdict(receipt),
        "run_meta": meta.to_dict(),
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    return path
