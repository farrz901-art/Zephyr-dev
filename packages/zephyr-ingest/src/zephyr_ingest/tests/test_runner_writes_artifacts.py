from __future__ import annotations

# 为测试文件增加 relaxation，因为 Mock 函数的类型推断在 strict 下极度困难
# pyright: reportUnknownParameterType=false
# pyright: reportUnknownVariableType=false
from pathlib import Path
from typing import Any

from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    RunMetaV1,
    ZephyrElement,
)
from zephyr_ingest.runner import RunnerConfig, run_documents


def test_runner_writes_batch_report(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    docs = [
        DocumentRef(
            uri=str(f),
            source="local_file",
            discovered_at_utc="2026-03-19T00:00:00Z",
            filename="a.txt",
            extension=".txt",
            size_bytes=f.stat().st_size,
        )
    ]

    def fake_partition_fn(**kwargs: Any) -> PartitionResult:
        return PartitionResult(
            document=DocumentMetadata(
                filename="a.txt",
                mime_type="text/plain",
                sha256="dummy",
                size_bytes=1,
                created_at_utc="2026-03-19T00:00:00Z",
            ),
            engine=EngineInfo(
                name="unstructured",
                backend="local",
                version="0.0.0",
                strategy=PartitionStrategy.AUTO,
            ),
            elements=[ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})],
            normalized_text="Hello",
            warnings=[],
        )

    calls: list[tuple[str, RunMetaV1, bool]] = []

    def fake_writer(
        *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> Path:
        calls.append((sha256, meta, result is not None))
        d = out_root / sha256
        d.mkdir(parents=True, exist_ok=True)
        (d / "run_meta.json").write_text("{}", encoding="utf-8")
        return d

    out_root = tmp_path / "out"
    cfg = RunnerConfig(out_root=out_root, strategy=PartitionStrategy.AUTO)
    ctx = RunContext.new(pipeline_version="p1", timestamp_utc="2026-03-19T00:00:00Z", run_id="r1")

    stats = run_documents(
        docs=docs,
        cfg=cfg,
        ctx=ctx,
        partition_fn=fake_partition_fn,
        artifacts_writer=fake_writer,
    )

    assert stats.total == 1
    assert stats.success == 1
    assert (out_root / "batch_report.json").exists()
    assert len(calls) == 1
