from __future__ import annotations

from pathlib import Path
from typing import Any

from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    ZephyrElement,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.runner import RunnerConfig, run_documents


class OkDest:
    name = "okdest"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: Any, result: Any = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(destination="okdest", ok=True, details={})


def test_runner_passes_run_context_and_hash_to_partition_fn(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")
    size_bytes = f.stat().st_size

    doc = DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="a.txt",
        extension=".txt",
        size_bytes=size_bytes,
    )

    ctx = RunContext.new(
        pipeline_version="p-test", run_id="r-test", timestamp_utc="2026-01-01T00:00:00Z"
    )
    cfg = RunnerConfig(out_root=tmp_path / "out", workers=1, destination=OkDest())

    captured: dict[str, object] = {}

    def fake_partition_fn(
        *,
        filename: str,
        strategy: PartitionStrategy = PartitionStrategy.AUTO,
        unique_element_ids: bool = True,
        backend: Any | None = None,
        run_id: str | None = None,
        pipeline_version: str | None = None,
        sha256: str | None = None,
        size_bytes: int | None = None,
    ) -> PartitionResult:
        captured["filename"] = filename
        captured["run_id"] = run_id
        captured["pipeline_version"] = pipeline_version
        captured["sha256"] = sha256
        captured["size_bytes"] = size_bytes

        # minimal valid PartitionResult
        doc_meta = DocumentMetadata(
            filename="a.txt",
            mime_type="text/plain",
            sha256=str(sha256),
            size_bytes=int(size_bytes or 0),
            created_at_utc="2026-01-01T00:00:00Z",
        )
        engine = EngineInfo(
            name="unstructured", backend="dummy", version="0", strategy=PartitionStrategy.AUTO
        )
        return PartitionResult(
            document=doc_meta,
            engine=engine,
            elements=[ZephyrElement(element_id="e1", type="Text", text="hello", metadata={})],
            normalized_text="hello",
            warnings=[],
        )

    run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=fake_partition_fn, destination=OkDest()
    )

    assert captured["run_id"] == "r-test"
    assert captured["pipeline_version"] == "p-test"
    assert captured["size_bytes"] == size_bytes
    assert isinstance(captured["sha256"], str)
