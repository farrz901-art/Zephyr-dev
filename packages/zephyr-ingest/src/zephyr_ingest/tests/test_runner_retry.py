from __future__ import annotations

from pathlib import Path
from typing import Any

from zephyr_core import DocumentRef, ErrorCode, RunContext, ZephyrError
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.contracts.v1.models import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    ZephyrElement,
)
from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_ingest.runner import RetryConfig, RunnerConfig, run_documents


def _one_doc(tmp_path: Path) -> DocumentRef:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")
    return DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-03-20T00:00:00Z",
        filename="a.txt",
        extension=".txt",
        size_bytes=f.stat().st_size,
    )


def _ok_result() -> PartitionResult:
    return PartitionResult(
        document=DocumentMetadata(
            filename="a.txt",
            mime_type="text/plain",
            sha256="dummy",
            size_bytes=1,
            created_at_utc="2026-03-20T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured", backend="local", version="0.0.0", strategy=PartitionStrategy.AUTO
        ),
        elements=[ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})],
        normalized_text="Hello",
        warnings=[],
    )


def test_retry_then_success(tmp_path: Path) -> None:
    doc = _one_doc(tmp_path)
    calls = {"n": 0}
    captured: list[RunMetaV1] = []

    def flakey_partition_fn(**kwargs: Any) -> PartitionResult:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message="transient",
                details={"retryable": True},
            )
        return _ok_result()

    def writer(
        *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> Path:
        captured.append(meta)
        d = out_root / sha256
        d.mkdir(parents=True, exist_ok=True)
        (d / "run_meta.json").write_text("{}", encoding="utf-8")
        return d

    cfg = RunnerConfig(
        out_root=tmp_path / "out",
        strategy=PartitionStrategy.AUTO,
        retry=RetryConfig(enabled=True, max_attempts=3, base_backoff_ms=0, max_backoff_ms=0),
    )
    ctx = RunContext.new(pipeline_version="p1", timestamp_utc="2026-03-20T00:00:00Z", run_id="r1")

    stats = run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=flakey_partition_fn, artifacts_writer=writer
    )

    assert stats.total == 1
    assert stats.success == 1
    assert captured[-1].metrics.attempts == 3


def test_no_retry_when_not_retryable(tmp_path: Path) -> None:
    doc = _one_doc(tmp_path)
    captured: list[RunMetaV1] = []

    def fail_partition_fn(**kwargs: Any) -> PartitionResult:
        raise ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED, message="perm", details={"retryable": False}
        )

    def writer(
        *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> Path:
        captured.append(meta)
        d = out_root / sha256
        d.mkdir(parents=True, exist_ok=True)
        (d / "run_meta.json").write_text("{}", encoding="utf-8")
        return d

    cfg = RunnerConfig(
        out_root=tmp_path / "out",
        strategy=PartitionStrategy.AUTO,
        retry=RetryConfig(enabled=True, max_attempts=5, base_backoff_ms=0, max_backoff_ms=0),
    )
    ctx = RunContext.new(pipeline_version="p1", timestamp_utc="2026-03-20T00:00:00Z", run_id="r2")

    stats = run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=fail_partition_fn, artifacts_writer=writer
    )

    assert stats.total == 1
    assert stats.failed == 1
    assert captured[-1].metrics.attempts == 1


def test_retry_exhausted(tmp_path: Path) -> None:
    doc = _one_doc(tmp_path)
    captured: list[RunMetaV1] = []
    calls = {"n": 0}

    def always_retryable_fail(**kwargs: Any) -> PartitionResult:
        calls["n"] += 1
        raise ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED, message="boom", details={"retryable": True}
        )

    def writer(
        *, out_root: Path, sha256: str, meta: RunMetaV1, result: PartitionResult | None = None
    ) -> Path:
        captured.append(meta)
        d = out_root / sha256
        d.mkdir(parents=True, exist_ok=True)
        (d / "run_meta.json").write_text("{}", encoding="utf-8")
        return d

    cfg = RunnerConfig(
        out_root=tmp_path / "out",
        strategy=PartitionStrategy.AUTO,
        retry=RetryConfig(enabled=True, max_attempts=2, base_backoff_ms=0, max_backoff_ms=0),
    )
    ctx = RunContext.new(pipeline_version="p1", timestamp_utc="2026-03-20T00:00:00Z", run_id="r3")

    stats = run_documents(
        docs=[doc], cfg=cfg, ctx=ctx, partition_fn=always_retryable_fail, artifacts_writer=writer
    )

    assert stats.total == 1
    assert stats.failed == 1
    assert calls["n"] == 2
    assert captured[-1].metrics.attempts == 2
