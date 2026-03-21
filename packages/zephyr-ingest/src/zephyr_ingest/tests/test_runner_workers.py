from __future__ import annotations

# 为测试文件放松部分检查
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false
import time
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
from zephyr_ingest.runner import RunnerConfig, run_documents


def _fake_ok(filename: str) -> PartitionResult:
    return PartitionResult(
        document=DocumentMetadata(
            filename=filename,
            mime_type="text/plain",
            sha256="dummy",
            size_bytes=1,
            created_at_utc="2026-03-20T00:00:00Z",
        ),
        # 修复：使用枚举对象 PartitionStrategy.AUTO 而不是字符串 "auto"
        engine=EngineInfo(
            name="test", backend="local", version="0", strategy=PartitionStrategy.AUTO
        ),
        elements=[ZephyrElement(element_id="1", type="Title", text="hi", metadata={})],
        normalized_text="hi",
        warnings=[],
    )


def test_concurrent_execution_completes(tmp_path: Path) -> None:
    # 修复：显式标注列表类型
    docs: list[DocumentRef] = []
    for i in range(3):
        f = tmp_path / f"{i}.txt"
        f.write_text(str(i))
        docs.append(
            DocumentRef(
                uri=str(f),
                source="local",
                discovered_at_utc="now",
                filename=f.name,
                extension=".txt",
                size_bytes=1,
            )
        )

    cfg = RunnerConfig(out_root=tmp_path / "out", workers=2)
    ctx = RunContext.new()

    # 修复：为 lambda 参数增加 Any 注解
    stats = run_documents(docs=docs, cfg=cfg, ctx=ctx, partition_fn=lambda **kw: _fake_ok("x"))

    assert stats.total == 3
    assert stats.success == 3


def test_concurrent_same_sha_is_locked(tmp_path: Path) -> None:
    content = "same"
    f1 = tmp_path / "1.txt"
    f2 = tmp_path / "2.txt"
    f1.write_text(content)
    f2.write_text(content)

    docs = [
        DocumentRef(
            uri=str(f1),
            source="local",
            discovered_at_utc="now",
            filename="1.txt",
            extension=".txt",
            size_bytes=len(content),
        ),
        DocumentRef(
            uri=str(f2),
            source="local",
            discovered_at_utc="now",
            filename="2.txt",
            extension=".txt",
            size_bytes=len(content),
        ),
    ]

    def slow_partition(**kwargs: Any) -> PartitionResult:
        time.sleep(0.2)
        return _fake_ok("1.txt")

    cfg = RunnerConfig(out_root=tmp_path / "out", workers=2)
    ctx = RunContext.new()

    stats = run_documents(docs=docs, cfg=cfg, ctx=ctx, partition_fn=slow_partition)

    assert stats.total == 2
    assert stats.success == 1
    assert stats.skipped_existing == 1
