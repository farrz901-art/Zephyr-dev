from __future__ import annotations

from pathlib import Path

from zephyr_core import PartitionResult, RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.fanout import FanoutDestination


class OkDest:
    name = "ok"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(destination=self.name, ok=True)


class BadDest:
    name = "bad"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(destination=self.name, ok=False)


def test_fanout_all_ok(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r",
        pipeline_version="p1",
        timestamp_utc="2026-03-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )
    fan = FanoutDestination(destinations=(OkDest(), OkDest()))
    r = fan(out_root=tmp_path, sha256="x", meta=meta, result=None)
    assert r.ok is True


def test_fanout_any_fail(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r",
        pipeline_version="p1",
        timestamp_utc="2026-03-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )
    fan = FanoutDestination(destinations=(OkDest(), BadDest()))
    r = fan(out_root=tmp_path, sha256="x", meta=meta, result=None)
    assert r.ok is False
