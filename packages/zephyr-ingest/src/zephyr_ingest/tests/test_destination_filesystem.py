from __future__ import annotations

from pathlib import Path

from zephyr_core import RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest.destinations.filesystem import FilesystemDestination


def test_filesystem_destination_writes_run_meta(tmp_path: Path) -> None:
    dest = FilesystemDestination()

    meta = RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=None,
        document=None,
        engine=None,
        error=None,
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=meta, result=None)
    assert receipt.ok is True

    assert (tmp_path / "out" / "abc" / "run_meta.json").exists()
