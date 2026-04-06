from __future__ import annotations

import json

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
from zephyr_ingest.destinations.base import DeliveryReceipt
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

    class MockDestination:
        name = "fake_report_dest"  # 显式提供协议要求的属性

        def __init__(self, calls_list: list[Any]) -> None:
            self.calls = calls_list

        def __call__(
            self,
            *,
            out_root: Path,
            sha256: str,
            meta: RunMetaV1,
            result: PartitionResult | None = None,
        ) -> DeliveryReceipt:
            # 记录调用信息
            self.calls.append((sha256, meta, result is not None))

            # 模拟文件写入
            d = out_root / sha256
            d.mkdir(parents=True, exist_ok=True)
            (d / "run_meta.json").write_text("{}", encoding="utf-8")

            return DeliveryReceipt(destination=self.name, ok=True, details={"out_dir": str(d)})

    # calls: list[tuple[str, RunMetaV1, bool]] = []

    # 1. 核心记录器
    calls: list[Any] = []

    dest_instance = MockDestination(calls)

    # # 注入 Protocol 要求的属性
    # cast(Any, dest_instance).name = "fake_report_dest"

    out_root = tmp_path / "out"
    cfg = RunnerConfig(out_root=out_root, strategy=PartitionStrategy.AUTO)
    ctx = RunContext.new(pipeline_version="p1", timestamp_utc="2026-03-19T00:00:00Z", run_id="r1")

    stats = run_documents(
        docs=docs,
        cfg=cfg,
        ctx=ctx,
        partition_fn=fake_partition_fn,
        destination=dest_instance,  # 使用新的参数名
    )

    assert stats.total == 1
    assert stats.success == 1
    assert (out_root / "batch_report.json").exists()

    report = json.loads((out_root / "batch_report.json").read_text(encoding="utf-8"))
    assert "counts_by_extension" in report
    assert report["counts_by_extension"][".txt"] == 1
    assert "retry" in report
    assert report["retry"]["retry_attempts_total"] == 0
    assert "durations_ms" in report
    assert report["durations_ms"]["max"] is not None

    assert len(calls) == 1
    assert calls[0][1].to_dict()["provenance"] == {
        "run_origin": "intake",
        "delivery_origin": "primary",
    }
