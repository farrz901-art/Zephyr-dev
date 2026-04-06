from __future__ import annotations

import json
from pathlib import Path

from it_stream import process_file
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


def test_filesystem_destination_writes_it_stream_artifacts(tmp_path: Path) -> None:
    dest = FilesystemDestination()
    input_path = tmp_path / "messages.json"
    input_path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "type": "RECORD",
                        "record": {
                            "stream": "customers",
                            "data": {"id": 1, "name": "Ada"},
                        },
                    },
                    {
                        "type": "STATE",
                        "state": {"data": {"cursor": "2026-01-01"}},
                    },
                    {
                        "type": "LOG",
                        "log": {"level": "INFO", "message": "completed sync"},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    meta = RunMetaV1(
        run_id="r-it",
        pipeline_version="p-it",
        timestamp_utc="2026-03-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=None,
        document=None,
        engine=None,
        error=None,
    )
    result = process_file(
        filename=str(input_path),
        strategy=PartitionStrategy.AUTO,
        sha256="abc-it",
        size_bytes=input_path.stat().st_size,
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc-it", meta=meta, result=result)

    assert receipt.ok is True

    out_dir = tmp_path / "out" / "abc-it"
    assert (out_dir / "run_meta.json").exists()
    assert (out_dir / "records.jsonl").read_text(encoding="utf-8") == (
        '{"data":{"id":1,"name":"Ada"},"emitted_at":null,"record_index":0,"stream":"customers"}'
    )
    checkpoint = json.loads((out_dir / "checkpoint.json").read_text(encoding="utf-8"))
    assert checkpoint["flow_kind"] == "it"
    assert checkpoint["schema_version"] == 1
    assert checkpoint["stream"] == "customers"
    assert checkpoint["task_identity_key"] == (
        '{"kind":"it","pipeline_version":"p-it","sha256":"abc-it"}'
    )
    assert checkpoint["checkpoints"] == [
        {
            "checkpoint_identity_key": (
                '{"kind":"it","progress":{"cursor":"2026-01-01"},'
                '"stream":"customers","task":{"kind":"it","pipeline_version":"p-it","sha256":"abc-it"}}'
            ),
            "checkpoint_index": 0,
            "progress": {"cursor": "2026-01-01"},
        }
    ]
    assert (out_dir / "logs.jsonl").read_text(encoding="utf-8") == (
        '{"level":"INFO","log_index":0,"message":"completed sync"}'
    )
