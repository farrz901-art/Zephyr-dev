from __future__ import annotations

import json
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
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.flow_processor import (
    DEFAULT_FLOW_KIND,
    ItFlowProcessor,
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


def test_build_processor_for_it_flow_kind() -> None:
    processor = build_processor_for_flow_kind(flow_kind="it")
    assert isinstance(processor, ItFlowProcessor)


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


def test_it_flow_processor_returns_real_partition_result(tmp_path: Path) -> None:
    f = tmp_path / "records.json"
    f.write_text(
        json_dumps_messages(
            [
                {
                    "type": "RECORD",
                    "record": {
                        "stream": "customers",
                        "data": {"id": 1, "name": "Ada"},
                        "emitted_at": "2026-01-01T00:00:00Z",
                    },
                },
                {
                    "type": "STATE",
                    "state": {"data": {"cursor": "c-1"}},
                },
                {
                    "type": "LOG",
                    "log": {"level": "INFO", "message": "processed page"},
                },
            ]
        ),
        encoding="utf-8",
    )
    doc = DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="records.json",
        extension=".json",
        size_bytes=f.stat().st_size,
    )

    processor = ItFlowProcessor()
    result = processor.process(
        doc=doc,
        strategy=PartitionStrategy.AUTO,
        unique_element_ids=True,
        run_id="r-test",
        pipeline_version="p-test",
        sha256="sha-test",
    )

    assert result.document.sha256 == "sha-test"
    assert result.engine.name == "it-stream"
    assert result.engine.backend == "airbyte-message-json"
    assert len(result.elements) == 3
    assert result.elements[0].metadata["flow_kind"] == "it"
    assert result.elements[0].metadata["stream"] == "customers"
    assert result.elements[1].metadata["artifact_kind"] == "state"
    assert result.elements[2].metadata["artifact_kind"] == "log"
    assert '"name": "Ada"' in result.normalized_text


def test_runner_routes_to_it_flow_kind(tmp_path: Path) -> None:
    f = tmp_path / "records.json"
    f.write_text(
        json_dumps_messages(
            [
                {
                    "type": "RECORD",
                    "record": {
                        "stream": "orders",
                        "data": {"order_id": "o-1", "amount": 12},
                    },
                }
            ]
        ),
        encoding="utf-8",
    )

    doc = DocumentRef(
        uri=str(f),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="records.json",
        extension=".json",
        size_bytes=f.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-test", run_id="r-test", timestamp_utc="2026-01-01T00:00:00Z"
    )
    cfg = RunnerConfig(
        out_root=tmp_path / "out",
        workers=1,
        destination=FilesystemDestination(),
    )

    stats = run_documents(
        docs=[doc],
        cfg=cfg,
        ctx=ctx,
        flow_kind="it",
        destination=FilesystemDestination(),
    )

    assert stats.total == 1
    assert stats.success == 1
    assert (tmp_path / "out").exists()
    # out_dir = next((tmp_path / "out").iterdir())
    output_subdirs = [p for p in (tmp_path / "out").iterdir() if p.is_dir()]
    # 确保真的生成了目录
    assert len(output_subdirs) > 0, (
        f"Expected an output directory in {tmp_path / 'out'}, but found none."
    )
    # 取第一个目录作为我们的目标
    out_dir = output_subdirs[0]
    run_meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    assert run_meta["engine"]["name"] == "it-stream"
    assert (out_dir / "records.jsonl").read_text(encoding="utf-8") == (
        '{"data":{"amount":12,"order_id":"o-1"},"emitted_at":null,"record_index":0,"stream":"orders"}'
    )


def json_dumps_messages(messages: list[dict[str, object]]) -> str:
    return json.dumps(
        {"messages": messages},
        ensure_ascii=False,
        indent=2,
    )
