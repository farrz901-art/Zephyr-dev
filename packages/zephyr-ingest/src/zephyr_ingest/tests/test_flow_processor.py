from __future__ import annotations

from pathlib import Path

import pytest

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
from zephyr_ingest.flow_processor import (
    DEFAULT_FLOW_KIND,
    UnsFlowProcessor,
    build_processor_for_flow_kind,
)
from zephyr_ingest.runner import RunnerConfig, run_documents


class OkDest:
    name = "okdest"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: object, result: object = None
    ) -> DeliveryReceipt:
        return DeliveryReceipt(destination="okdest", ok=True, details={})


def test_build_processor_for_uns_flow_kind() -> None:
    processor = build_processor_for_flow_kind(flow_kind="uns")
    assert isinstance(processor, UnsFlowProcessor)


def test_build_processor_for_it_flow_kind_is_not_implemented() -> None:
    with pytest.raises(NotImplementedError, match="Flow kind 'it' is not implemented"):
        build_processor_for_flow_kind(flow_kind="it")


def test_runner_defaults_to_uns_flow_kind(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    doc = DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="a.txt",
        extension=".txt",
        size_bytes=f.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-test", run_id="r-test", timestamp_utc="2026-01-01T00:00:00Z"
    )
    cfg = RunnerConfig(out_root=tmp_path / "out", workers=1, destination=OkDest())

    captured: dict[str, object] = {}

    class RecordingProcessor:
        def process(
            self,
            *,
            doc: DocumentRef,
            strategy: PartitionStrategy,
            unique_element_ids: bool,
            run_id: str | None,
            pipeline_version: str | None,
            sha256: str,
        ) -> PartitionResult:
            captured["processor_used"] = True
            return PartitionResult(
                document=DocumentMetadata(
                    filename="a.txt",
                    mime_type="text/plain",
                    sha256=sha256,
                    size_bytes=doc.size_bytes or 0,
                    created_at_utc="2026-01-01T00:00:00Z",
                ),
                engine=EngineInfo(
                    name="processor",
                    backend="test",
                    version="0",
                    strategy=PartitionStrategy.AUTO,
                ),
                elements=[ZephyrElement(element_id="e1", type="Text", text="hello", metadata={})],
                normalized_text="hello",
                warnings=[],
            )

    def fake_build_processor_for_flow_kind(*, flow_kind: str, backend: object | None) -> object:
        captured["flow_kind"] = flow_kind
        captured["backend"] = backend
        return RecordingProcessor()

    import zephyr_ingest.runner as runner_mod

    monkeypatch.setattr(
        runner_mod, "build_processor_for_flow_kind", fake_build_processor_for_flow_kind
    )

    run_documents(docs=[doc], cfg=cfg, ctx=ctx, destination=OkDest())

    assert captured["flow_kind"] == DEFAULT_FLOW_KIND
    assert captured["backend"] is None
    assert captured["processor_used"] is True
