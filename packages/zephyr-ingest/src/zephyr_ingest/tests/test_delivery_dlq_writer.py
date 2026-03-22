from __future__ import annotations

import json
from pathlib import Path

from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest.destinations.base import DeliveryReceipt


def test_write_delivery_dlq_creates_file(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-23T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )
    receipt = DeliveryReceipt(destination="webhook", ok=False, details={"status_code": 500})

    out = write_delivery_dlq(out_root=tmp_path / "out", sha256="abc", meta=meta, receipt=receipt)

    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["sha256"] == "abc"
    assert data["run_id"] == "r1"
    assert data["destination_receipt"]["ok"] is False
