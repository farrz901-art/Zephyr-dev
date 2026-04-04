from __future__ import annotations

import json
from pathlib import Path

from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunMetaV1,
    ZephyrElement,
)
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


def test_filesystem_destination_writes_success_artifacts(tmp_path: Path) -> None:
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
    result = PartitionResult(
        document=DocumentMetadata(
            filename="doc.txt",
            mime_type=None,
            sha256="abc",
            size_bytes=5,
            created_at_utc="2026-03-21T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured",
            backend="local",
            version="0.1",
            strategy=PartitionStrategy.AUTO,
        ),
        elements=[ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})],
        normalized_text="Hello",
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc", meta=meta, result=result)
    assert receipt.ok is True
    assert receipt.details == {"out_dir": str(tmp_path / "out" / "abc")}

    out_dir = tmp_path / "out" / "abc"
    assert (out_dir / "run_meta.json").exists()
    assert (out_dir / "elements.json").exists()
    assert (out_dir / "normalized.txt").read_text(encoding="utf-8") == "Hello"
    assert json.loads((out_dir / "elements.json").read_text(encoding="utf-8")) == [
        {
            "element_id": "1",
            "type": "Title",
            "text": "Hello",
            "metadata": {},
        }
    ]
