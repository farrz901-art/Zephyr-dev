from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import cast

import pytest

from it_stream import (
    ItTaskIdentityV1,
    normalize_it_task_identity_key,
)
from it_stream.sources import http_source as it_http_source
from uns_stream.sources import http_source as uns_http_source
from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    ZephyrElement,
)
from zephyr_ingest._internal.utils import sha256_file
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.sqlite import SqliteDestination
from zephyr_ingest.flow_processor import (
    DEFAULT_FLOW_KIND,
    ItFlowProcessor,
    UnsFlowProcessor,
    build_processor_for_flow_kind,
    normalize_flow_input_identity_sha,
)
from zephyr_ingest.runner import RunnerConfig, run_documents
from zephyr_ingest.task_idempotency import normalize_uns_task_idempotency_key
from zephyr_ingest.task_v1 import TaskIdentityV1


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
    checkpoint = json.loads((out_dir / "checkpoint.json").read_text(encoding="utf-8"))
    assert run_meta["engine"]["name"] == "it-stream"
    assert (out_dir / "records.jsonl").read_text(encoding="utf-8") == (
        '{"data":{"amount":12,"order_id":"o-1"},"emitted_at":null,"record_index":0,"stream":"orders"}'
    )
    assert checkpoint["flow_kind"] == "it"
    assert checkpoint["task_identity_key"] == (
        '{"kind":"it","pipeline_version":"p-test","sha256":"'
        + run_meta["document"]["sha256"]
        + '"}'
    )
    assert checkpoint["checkpoints"] == []


def test_runner_routes_http_json_source_specs_through_it_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_json_cursor_v1",
                    "stream": "customers",
                    "url": "https://example.test/api/customers",
                    "cursor_param": "cursor",
                    "query": {"limit": "2"},
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        full_url = cast("str", getattr(request, "full_url"))

        class _FakeResponse:
            def __init__(self, payload: dict[str, object]) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

        if "cursor=c-1" in full_url:
            return _FakeResponse({"records": [{"id": 2}], "next_cursor": None})
        return _FakeResponse({"records": [{"id": 1}], "next_cursor": "c-1"})

    monkeypatch.setattr(it_http_source, "urlopen", fake_urlopen)

    doc = DocumentRef(
        uri=str(spec_path),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="source.json",
        extension=".json",
        size_bytes=spec_path.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-http", run_id="r-http", timestamp_utc="2026-01-01T00:00:00Z"
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

    expected_sha = normalize_flow_input_identity_sha(
        flow_kind="it",
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )
    expected_task_identity_key = normalize_it_task_identity_key(
        identity=ItTaskIdentityV1(
            pipeline_version="p-http",
            sha256=expected_sha,
        )
    )

    assert stats.total == 1
    assert stats.success == 1
    out_dir = tmp_path / "out" / expected_sha
    run_meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    checkpoint = json.loads((out_dir / "checkpoint.json").read_text(encoding="utf-8"))

    assert run_meta["engine"]["name"] == "it-stream"
    assert run_meta["engine"]["backend"] == "http-json-cursor"
    assert run_meta["provenance"]["task_identity_key"] == expected_task_identity_key
    assert checkpoint["task_identity_key"] == expected_task_identity_key
    assert checkpoint["checkpoints"][0]["progress"] == {
        "cursor": "c-1",
        "page_number": 1,
        "record_count": 1,
        "source_url": "https://example.test/api/customers",
    }


def test_runner_routes_http_document_source_specs_through_uns_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_document_v1",
                    "url": "https://example.test/docs/report.txt",
                    "accept": "text/plain",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _FakeResponse:
        headers = {"Content-Type": "text/plain; charset=utf-8"}

        def read(self) -> bytes:
            return b"remote hello"

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        assert getattr(request, "full_url") == "https://example.test/docs/report.txt"
        return _FakeResponse()

    def fake_auto_partition(
        *,
        filename: str,
        strategy: PartitionStrategy = PartitionStrategy.AUTO,
        unique_element_ids: bool = True,
        backend: object | None = None,
        run_id: str | None = None,
        pipeline_version: str | None = None,
        sha256: str | None = None,
        size_bytes: int | None = None,
    ) -> PartitionResult:
        del strategy, unique_element_ids, backend, run_id, pipeline_version
        fetched_path = Path(filename)
        assert fetched_path.read_text(encoding="utf-8") == "remote hello"
        return PartitionResult(
            document=DocumentMetadata(
                filename=fetched_path.name,
                mime_type="text/plain",
                sha256=sha256 or "temp-sha",
                size_bytes=size_bytes or 0,
                created_at_utc="2026-01-01T00:00:00Z",
            ),
            engine=EngineInfo(
                name="unstructured",
                backend="test",
                version="0",
                strategy=PartitionStrategy.AUTO,
            ),
            elements=[
                ZephyrElement(
                    element_id="e1",
                    type="NarrativeText",
                    text="remote hello",
                    metadata={"kind": "text"},
                )
            ],
            normalized_text="remote hello",
            warnings=[],
        )

    monkeypatch.setattr(uns_http_source, "urlopen", fake_urlopen)
    monkeypatch.setattr(uns_http_source, "auto_partition", fake_auto_partition)

    doc = DocumentRef(
        uri=str(spec_path),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="source.json",
        extension=".json",
        size_bytes=spec_path.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-http", run_id="r-http", timestamp_utc="2026-01-01T00:00:00Z"
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
        flow_kind="uns",
        destination=FilesystemDestination(),
    )

    expected_sha = normalize_flow_input_identity_sha(
        flow_kind="uns",
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )

    assert stats.total == 1
    assert stats.success == 1
    out_dir = tmp_path / "out" / expected_sha
    run_meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    elements = json.loads((out_dir / "elements.json").read_text(encoding="utf-8"))

    assert run_meta["document"]["filename"] == "report.txt"
    assert run_meta["document"]["mime_type"] == "text/plain"
    assert run_meta["document"]["sha256"] == expected_sha
    assert run_meta["document"]["size_bytes"] == len(b"remote hello")
    assert run_meta["provenance"]["task_identity_key"] == normalize_uns_task_idempotency_key(
        identity=TaskIdentityV1(
            pipeline_version="p-http",
            sha256=expected_sha,
        )
    )
    assert elements[0]["metadata"]["source_kind"] == "http_document_v1"
    assert elements[0]["metadata"]["source_url"] == "https://example.test/docs/report.txt"
    assert elements[0]["metadata"]["fetched_filename"] == "report.txt"
    assert elements[0]["metadata"]["fetched_mime_type"] == "text/plain"


def test_it_http_source_and_sqlite_destination_integrate_through_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_json_cursor_v1",
                    "stream": "customers",
                    "url": "https://example.test/api/customers",
                    "cursor_param": "cursor",
                    "query": {"limit": "2"},
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        full_url = cast("str", getattr(request, "full_url"))

        class _FakeResponse:
            def __init__(self, payload: dict[str, object]) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

        if "cursor=c-1" in full_url:
            return _FakeResponse({"records": [{"id": 2}], "next_cursor": None})
        return _FakeResponse({"records": [{"id": 1}], "next_cursor": "c-1"})

    monkeypatch.setattr(it_http_source, "urlopen", fake_urlopen)

    ctx = RunContext.new(
        pipeline_version="p-it-sqlite",
        run_id="r-it-sqlite",
        timestamp_utc="2026-01-01T00:00:00Z",
    )
    expected_sha = normalize_flow_input_identity_sha(
        flow_kind="it",
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )
    out_root = tmp_path / "out"
    db_path = tmp_path / "delivery.db"

    stats = run_documents(
        docs=[
            DocumentRef(
                uri=str(spec_path),
                source="local_file",
                discovered_at_utc="2026-01-01T00:00:00Z",
                filename="source.json",
                extension=".json",
                size_bytes=spec_path.stat().st_size,
            )
        ],
        cfg=RunnerConfig(
            out_root=out_root,
            workers=1,
            destination=SqliteDestination(db_path=db_path, table_name="delivery_rows"),
        ),
        ctx=ctx,
        flow_kind="it",
        destination=SqliteDestination(db_path=db_path, table_name="delivery_rows"),
    )

    assert stats.total == 1
    assert stats.success == 1

    output_subdirs = [path for path in out_root.iterdir() if path.is_dir()]
    assert len(output_subdirs) == 1
    out_dir = output_subdirs[0]
    assert out_dir.name == expected_sha
    delivery_receipt = json.loads((out_dir / "delivery_receipt.json").read_text(encoding="utf-8"))
    batch_report = json.loads((out_root / "batch_report.json").read_text(encoding="utf-8"))

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT identity_key, payload_json FROM delivery_rows WHERE identity_key = ?",
            (f"{expected_sha}:{ctx.run_id}",),
        ).fetchone()
    finally:
        conn.close()

    assert delivery_receipt["destination"] == "sqlite"
    assert delivery_receipt["ok"] is True
    assert batch_report["delivery"]["by_destination"]["sqlite"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert row is not None
    payload = json.loads(row[1])
    assert payload["sha256"] == expected_sha
    assert payload["run_meta"]["provenance"]["task_identity_key"] == (
        '{"kind":"it","pipeline_version":"p-it-sqlite","sha256":"' + expected_sha + '"}'
    )
    assert payload["artifacts"]["out_dir"].endswith(expected_sha)
    assert payload["artifacts"]["records_path"].endswith("records.jsonl")
    assert payload["artifacts"]["state_path"].endswith("checkpoint.json")
    assert payload["artifacts"]["logs_path"].endswith("logs.jsonl")


def test_uns_http_source_and_sqlite_destination_integrate_through_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_document_v1",
                    "url": "https://example.test/docs/report.txt",
                    "accept": "text/plain",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _FakeResponse:
        headers = {"Content-Type": "text/plain; charset=utf-8"}

        def read(self) -> bytes:
            return b"remote hello"

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        assert getattr(request, "full_url") == "https://example.test/docs/report.txt"
        return _FakeResponse()

    def fake_auto_partition(
        *,
        filename: str,
        strategy: PartitionStrategy = PartitionStrategy.AUTO,
        unique_element_ids: bool = True,
        backend: object | None = None,
        run_id: str | None = None,
        pipeline_version: str | None = None,
        sha256: str | None = None,
        size_bytes: int | None = None,
    ) -> PartitionResult:
        del strategy, unique_element_ids, backend, run_id, pipeline_version
        fetched_path = Path(filename)
        assert fetched_path.read_text(encoding="utf-8") == "remote hello"
        return PartitionResult(
            document=DocumentMetadata(
                filename=fetched_path.name,
                mime_type="text/plain",
                sha256=sha256 or "temp-sha",
                size_bytes=size_bytes or 0,
                created_at_utc="2026-01-01T00:00:00Z",
            ),
            engine=EngineInfo(
                name="unstructured",
                backend="test",
                version="0",
                strategy=PartitionStrategy.AUTO,
            ),
            elements=[
                ZephyrElement(
                    element_id="e1",
                    type="NarrativeText",
                    text="remote hello",
                    metadata={"kind": "text"},
                )
            ],
            normalized_text="remote hello",
            warnings=[],
        )

    monkeypatch.setattr(uns_http_source, "urlopen", fake_urlopen)
    monkeypatch.setattr(uns_http_source, "auto_partition", fake_auto_partition)

    ctx = RunContext.new(
        pipeline_version="p-uns-sqlite",
        run_id="r-uns-sqlite",
        timestamp_utc="2026-01-01T00:00:00Z",
    )
    expected_sha = normalize_flow_input_identity_sha(
        flow_kind="uns",
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )
    out_root = tmp_path / "out"
    db_path = tmp_path / "delivery.db"

    stats = run_documents(
        docs=[
            DocumentRef(
                uri=str(spec_path),
                source="local_file",
                discovered_at_utc="2026-01-01T00:00:00Z",
                filename="source.json",
                extension=".json",
                size_bytes=spec_path.stat().st_size,
            )
        ],
        cfg=RunnerConfig(
            out_root=out_root,
            workers=1,
            destination=SqliteDestination(db_path=db_path, table_name="delivery_rows"),
        ),
        ctx=ctx,
        flow_kind="uns",
        destination=SqliteDestination(db_path=db_path, table_name="delivery_rows"),
    )

    assert stats.total == 1
    assert stats.success == 1

    output_subdirs = [path for path in out_root.iterdir() if path.is_dir()]
    assert len(output_subdirs) == 1
    out_dir = output_subdirs[0]
    assert out_dir.name == expected_sha
    delivery_receipt = json.loads((out_dir / "delivery_receipt.json").read_text(encoding="utf-8"))
    batch_report = json.loads((out_root / "batch_report.json").read_text(encoding="utf-8"))

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT identity_key, payload_json FROM delivery_rows WHERE identity_key = ?",
            (f"{expected_sha}:{ctx.run_id}",),
        ).fetchone()
    finally:
        conn.close()

    assert delivery_receipt["destination"] == "sqlite"
    assert delivery_receipt["ok"] is True
    assert batch_report["delivery"]["by_destination"]["sqlite"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert row is not None
    payload = json.loads(row[1])
    assert payload["sha256"] == expected_sha
    assert payload["run_meta"]["provenance"][
        "task_identity_key"
    ] == normalize_uns_task_idempotency_key(
        identity=TaskIdentityV1(
            pipeline_version="p-uns-sqlite",
            sha256=expected_sha,
        )
    )
    assert payload["artifacts"]["out_dir"].endswith(expected_sha)
    assert "records_path" not in payload["artifacts"]
    assert "state_path" not in payload["artifacts"]
    assert "logs_path" not in payload["artifacts"]


def json_dumps_messages(messages: list[dict[str, object]]) -> str:
    return json.dumps(
        {"messages": messages},
        ensure_ascii=False,
        indent=2,
    )
