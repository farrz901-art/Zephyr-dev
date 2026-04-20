from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import cast
from urllib.request import Request

import httpx
import pytest

from it_stream import (
    ItTaskIdentityV1,
    normalize_it_task_identity_key,
)
from it_stream import service as it_service
from it_stream.sources import clickhouse_source as it_clickhouse_source
from it_stream.sources import http_source as it_http_source
from it_stream.sources import kafka_source as it_kafka_source
from it_stream.sources import mongodb_source as it_mongodb_source
from it_stream.sources import postgresql_source as it_postgresql_source
from uns_stream.sources import confluence_source as uns_confluence_source
from uns_stream.sources import git_source as uns_git_source
from uns_stream.sources import google_drive_source as uns_google_drive_source
from uns_stream.sources import http_source as uns_http_source
from uns_stream.sources import s3_source as uns_s3_source
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
    describe_flow_processor_shared_field_semantics,
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


def test_flow_processor_shared_field_semantics_are_explicit() -> None:
    uns_semantics = describe_flow_processor_shared_field_semantics(flow_kind="uns")
    it_semantics = describe_flow_processor_shared_field_semantics(flow_kind="it")

    assert uns_semantics.unique_element_ids == "applied"
    assert uns_semantics.run_id == "applied"
    assert uns_semantics.pipeline_version == "applied"
    assert it_semantics.unique_element_ids == "orchestration_context_only"
    assert it_semantics.run_id == "orchestration_context_only"
    assert it_semantics.pipeline_version == "orchestration_context_only"
    assert "flow-local" in it_semantics.note


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
    assert checkpoint["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": expected_task_identity_key,
    }
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

    def fake_http_get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        follow_redirects: bool,
        trust_env: bool,
    ) -> httpx.Response:
        assert url == "https://example.test/docs/report.txt"
        assert headers == {"Accept": "text/plain"}
        assert timeout == 10.0
        assert follow_redirects is True
        assert trust_env is False
        return httpx.Response(
            200,
            content=b"remote hello",
            headers={"Content-Type": "text/plain; charset=utf-8"},
            request=httpx.Request("GET", url),
        )

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

    monkeypatch.setattr(uns_http_source.httpx, "get", fake_http_get)
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


def test_runner_routes_s3_document_source_specs_through_uns_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "s3_document_v1",
                    "bucket": "docs-bucket",
                    "key": "reports/report.txt",
                    "region": "us-east-1",
                    "endpoint_url": "https://s3.example.test",
                    "version_id": "v1",
                    "access_key": "key-a",
                    "secret_key": "secret-a",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _FakeBody:
        def read(self) -> bytes:
            return b"remote hello"

    class _FakeClient:
        def get_object(
            self,
            *,
            Bucket: str,
            Key: str,
            VersionId: str | None = None,
        ) -> dict[str, object]:
            assert Bucket == "docs-bucket"
            assert Key == "reports/report.txt"
            assert VersionId == "v1"
            return {
                "Body": _FakeBody(),
                "ContentType": "text/plain; charset=utf-8",
                "ETag": '"etag-1"',
                "VersionId": "v1",
            }

    def fake_build_s3_document_source_client(
        *,
        config: uns_s3_source.S3DocumentSourceConfigV1,
    ) -> uns_s3_source.S3DocumentSourceClientProtocol:
        assert config.bucket == "docs-bucket"
        assert config.key == "reports/report.txt"
        return _FakeClient()

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

    monkeypatch.setattr(
        uns_s3_source,
        "_build_s3_document_source_client",
        fake_build_s3_document_source_client,
    )
    monkeypatch.setattr(uns_s3_source, "auto_partition", fake_auto_partition)

    doc = DocumentRef(
        uri=str(spec_path),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="source.json",
        extension=".json",
        size_bytes=spec_path.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-s3", run_id="r-s3", timestamp_utc="2026-01-01T00:00:00Z"
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
            pipeline_version="p-s3",
            sha256=expected_sha,
        )
    )
    assert elements[0]["metadata"]["source_kind"] == "s3_document_v1"
    assert elements[0]["metadata"]["source_bucket"] == "docs-bucket"
    assert elements[0]["metadata"]["source_key"] == "reports/report.txt"
    assert elements[0]["metadata"]["source_version_id"] == "v1"
    assert elements[0]["metadata"]["source_etag"] == '"etag-1"'
    assert elements[0]["metadata"]["fetched_filename"] == "report.txt"
    assert elements[0]["metadata"]["fetched_mime_type"] == "text/plain"


def test_runner_routes_git_document_source_specs_through_uns_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": str(repo_root),
                    "commit": "abc1234",
                    "relative_path": "docs/report.txt",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def fake_fetch_git_document_source(
        *,
        config: uns_git_source.GitDocumentSourceConfigV1,
    ) -> uns_git_source.GitDocumentSourceFetchV1:
        assert config.commit == "abc1234"
        return uns_git_source.GitDocumentSourceFetchV1(
            repo_root=config.repo_root,
            commit=config.commit,
            resolved_commit="abcdef1234567890",
            relative_path=config.relative_path,
            blob_sha="blob1234567890",
            filename="report.txt",
            mime_type="text/plain",
            content=b"git hello",
        )

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
        assert fetched_path.read_text(encoding="utf-8") == "git hello"
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
                    text="git hello",
                    metadata={"kind": "text"},
                )
            ],
            normalized_text="git hello",
            warnings=[],
        )

    monkeypatch.setattr(uns_git_source, "fetch_git_document_source", fake_fetch_git_document_source)
    monkeypatch.setattr(uns_git_source, "auto_partition", fake_auto_partition)

    doc = DocumentRef(
        uri=str(spec_path),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="source.json",
        extension=".json",
        size_bytes=spec_path.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-git", run_id="r-git", timestamp_utc="2026-01-01T00:00:00Z"
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
    assert run_meta["provenance"]["task_identity_key"] == normalize_uns_task_idempotency_key(
        identity=TaskIdentityV1(
            pipeline_version="p-git",
            sha256=expected_sha,
        )
    )
    assert elements[0]["metadata"]["source_kind"] == "git_document_v1"
    assert elements[0]["metadata"]["source_commit"] == "abcdef1234567890"
    assert elements[0]["metadata"]["source_requested_commit"] == "abc1234"
    assert elements[0]["metadata"]["source_relative_path"] == "docs/report.txt"
    assert elements[0]["metadata"]["source_blob_sha"] == "blob1234567890"
    assert elements[0]["metadata"]["fetched_filename"] == "report.txt"
    assert elements[0]["metadata"]["fetched_mime_type"] == "text/plain"


def test_runner_routes_google_drive_document_source_specs_through_uns_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "google_drive_document_v1",
                    "file_id": "file-123",
                    "drive_id": "drive-1",
                    "acquisition_mode": "export",
                    "export_mime_type": "application/pdf",
                    "access_token": "token-a",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _FakeResponse:
        headers = {
            "Content-Type": "application/pdf",
            "Content-Disposition": 'attachment; filename="report.pdf"',
            "ETag": '"etag-1"',
        }

        def read(self) -> bytes:
            return b"drive hello"

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        typed_request = cast("Request", request)
        assert (
            typed_request.full_url == "https://www.googleapis.com/drive/v3/files/file-123/export"
            "?mimeType=application%2Fpdf&supportsAllDrives=true"
        )
        assert typed_request.headers["Authorization"] == "Bearer token-a"
        assert typed_request.headers["Accept"] == "application/pdf"
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
        assert fetched_path.read_bytes() == b"drive hello"
        return PartitionResult(
            document=DocumentMetadata(
                filename=fetched_path.name,
                mime_type="application/pdf",
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
                    text="drive hello",
                    metadata={"kind": "text"},
                )
            ],
            normalized_text="drive hello",
            warnings=[],
        )

    monkeypatch.setattr(uns_google_drive_source, "urlopen", fake_urlopen)
    monkeypatch.setattr(uns_google_drive_source, "auto_partition", fake_auto_partition)

    doc = DocumentRef(
        uri=str(spec_path),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="source.json",
        extension=".json",
        size_bytes=spec_path.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-drive", run_id="r-drive", timestamp_utc="2026-01-01T00:00:00Z"
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

    assert run_meta["document"]["filename"] == "report.pdf"
    assert run_meta["document"]["mime_type"] == "application/pdf"
    assert run_meta["document"]["sha256"] == expected_sha
    assert run_meta["provenance"]["task_identity_key"] == normalize_uns_task_idempotency_key(
        identity=TaskIdentityV1(
            pipeline_version="p-drive",
            sha256=expected_sha,
        )
    )
    assert elements[0]["metadata"]["source_kind"] == "google_drive_document_v1"
    assert elements[0]["metadata"]["source_file_id"] == "file-123"
    assert elements[0]["metadata"]["source_drive_id"] == "drive-1"
    assert elements[0]["metadata"]["source_acquisition_mode"] == "export"
    assert elements[0]["metadata"]["source_export_mime_type"] == "application/pdf"
    assert elements[0]["metadata"]["source_etag"] == '"etag-1"'
    assert elements[0]["metadata"]["fetched_filename"] == "report.pdf"
    assert elements[0]["metadata"]["fetched_mime_type"] == "application/pdf"


def test_runner_routes_confluence_document_source_specs_through_uns_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "confluence_document_v1",
                    "site_url": "https://example.atlassian.net",
                    "page_id": "12345",
                    "space_key": "ENG",
                    "page_version": 7,
                    "access_token": "token-a",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _FakeResponse:
        def read(self) -> bytes:
            return json.dumps(
                {
                    "title": "Product Spec",
                    "space": {"key": "ENG"},
                    "version": {"number": 7},
                    "body": {"storage": {"value": "<p>hello confluence</p>"}},
                },
                ensure_ascii=False,
            ).encode("utf-8")

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert (
            typed_request.full_url == "https://example.atlassian.net/wiki/rest/api/content/12345"
            "?expand=space,version,body.storage"
        )
        assert typed_request.headers["Authorization"] == "Bearer token-a"
        assert typed_request.headers["Accept"] == "application/json"
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
        assert fetched_path.read_text(encoding="utf-8") == "<p>hello confluence</p>"
        return PartitionResult(
            document=DocumentMetadata(
                filename=fetched_path.name,
                mime_type="text/html",
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
                    text="hello confluence",
                    metadata={"kind": "text"},
                )
            ],
            normalized_text="hello confluence",
            warnings=[],
        )

    monkeypatch.setattr(uns_confluence_source, "urlopen", fake_urlopen)
    monkeypatch.setattr(uns_confluence_source, "auto_partition", fake_auto_partition)

    doc = DocumentRef(
        uri=str(spec_path),
        source="local_file",
        discovered_at_utc="2026-01-01T00:00:00Z",
        filename="source.json",
        extension=".json",
        size_bytes=spec_path.stat().st_size,
    )
    ctx = RunContext.new(
        pipeline_version="p-confluence",
        run_id="r-confluence",
        timestamp_utc="2026-01-01T00:00:00Z",
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

    assert run_meta["document"]["filename"] == "product-spec.html"
    assert run_meta["document"]["mime_type"] == "text/html"
    assert run_meta["document"]["sha256"] == expected_sha
    assert run_meta["provenance"]["task_identity_key"] == normalize_uns_task_idempotency_key(
        identity=TaskIdentityV1(
            pipeline_version="p-confluence",
            sha256=expected_sha,
        )
    )
    assert elements[0]["metadata"]["source_kind"] == "confluence_document_v1"
    assert elements[0]["metadata"]["source_site_url"] == "https://example.atlassian.net"
    assert elements[0]["metadata"]["source_page_id"] == "12345"
    assert elements[0]["metadata"]["source_space_key"] == "ENG"
    assert elements[0]["metadata"]["source_requested_page_version"] == 7
    assert elements[0]["metadata"]["source_page_version"] == 7
    assert elements[0]["metadata"]["source_body_format"] == "storage"
    assert elements[0]["metadata"]["source_page_title"] == "Product Spec"
    assert elements[0]["metadata"]["fetched_filename"] == "product-spec.html"
    assert elements[0]["metadata"]["fetched_mime_type"] == "text/html"


def test_runner_routes_second_round_uns_source_batch_through_shared_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    http_spec = tmp_path / "http-source.json"
    s3_spec = tmp_path / "s3-source.json"
    git_spec = tmp_path / "git-source.json"

    http_spec.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_document_v1",
                    "url": "https://example.test/docs/http-report.txt",
                    "accept": "text/plain",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    s3_spec.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "s3_document_v1",
                    "bucket": "docs-bucket",
                    "key": "reports/s3-report.txt",
                    "region": "us-east-1",
                    "endpoint_url": "https://s3.example.test",
                    "version_id": "v1",
                    "access_key": "key-a",
                    "secret_key": "secret-a",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    git_spec.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": str(tmp_path / "repo"),
                    "commit": "abc1234",
                    "relative_path": "docs/git-report.txt",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def fake_http_get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        follow_redirects: bool,
        trust_env: bool,
    ) -> httpx.Response:
        assert url == "https://example.test/docs/http-report.txt"
        assert headers == {"Accept": "text/plain"}
        assert timeout == 10.0
        assert follow_redirects is True
        assert trust_env is False
        return httpx.Response(
            200,
            content=b"http hello",
            headers={"Content-Type": "text/plain; charset=utf-8"},
            request=httpx.Request("GET", url),
        )

    class _FakeS3Body:
        def read(self) -> bytes:
            return b"s3 hello"

    class _FakeS3Client:
        def get_object(
            self,
            *,
            Bucket: str,
            Key: str,
            VersionId: str | None = None,
        ) -> dict[str, object]:
            assert Bucket == "docs-bucket"
            assert Key == "reports/s3-report.txt"
            assert VersionId == "v1"
            return {
                "Body": _FakeS3Body(),
                "ContentType": "text/plain; charset=utf-8",
                "ETag": '"etag-1"',
                "VersionId": "v1",
            }

    def fake_build_s3_document_source_client(
        *,
        config: uns_s3_source.S3DocumentSourceConfigV1,
    ) -> uns_s3_source.S3DocumentSourceClientProtocol:
        assert config.bucket == "docs-bucket"
        return _FakeS3Client()

    def fake_fetch_git_document_source(
        *,
        config: uns_git_source.GitDocumentSourceConfigV1,
    ) -> uns_git_source.GitDocumentSourceFetchV1:
        assert config.commit == "abc1234"
        return uns_git_source.GitDocumentSourceFetchV1(
            repo_root=config.repo_root,
            commit=config.commit,
            resolved_commit="abcdef1234567890",
            relative_path=config.relative_path,
            blob_sha="blob1234567890",
            filename="git-report.txt",
            mime_type="text/plain",
            content=b"git hello",
        )

    def fake_uns_auto_partition(
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
        text = fetched_path.read_text(encoding="utf-8")
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
                    text=text,
                    metadata={"kind": "text"},
                )
            ],
            normalized_text=text,
            warnings=[],
        )

    monkeypatch.setattr(uns_http_source.httpx, "get", fake_http_get)
    monkeypatch.setattr(uns_http_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(
        uns_s3_source,
        "_build_s3_document_source_client",
        fake_build_s3_document_source_client,
    )
    monkeypatch.setattr(uns_s3_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(uns_git_source, "fetch_git_document_source", fake_fetch_git_document_source)
    monkeypatch.setattr(uns_git_source, "auto_partition", fake_uns_auto_partition)

    docs = [
        DocumentRef(
            uri=str(http_spec),
            source="local_file",
            discovered_at_utc="2026-01-01T00:00:00Z",
            filename="http-source.json",
            extension=".json",
            size_bytes=http_spec.stat().st_size,
        ),
        DocumentRef(
            uri=str(s3_spec),
            source="local_file",
            discovered_at_utc="2026-01-01T00:00:00Z",
            filename="s3-source.json",
            extension=".json",
            size_bytes=s3_spec.stat().st_size,
        ),
        DocumentRef(
            uri=str(git_spec),
            source="local_file",
            discovered_at_utc="2026-01-01T00:00:00Z",
            filename="git-source.json",
            extension=".json",
            size_bytes=git_spec.stat().st_size,
        ),
    ]
    ctx = RunContext.new(
        pipeline_version="p-uns-batch",
        run_id="r-uns-batch",
        timestamp_utc="2026-01-01T00:00:00Z",
    )
    out_root = tmp_path / "out"
    cfg = RunnerConfig(
        out_root=out_root,
        workers=1,
        destination=FilesystemDestination(),
    )

    stats = run_documents(
        docs=docs,
        cfg=cfg,
        ctx=ctx,
        flow_kind="uns",
        destination=FilesystemDestination(),
    )

    expected_shas = {
        "http": normalize_flow_input_identity_sha(
            flow_kind="uns",
            filename=str(http_spec),
            default_sha=sha256_file(http_spec),
        ),
        "s3": normalize_flow_input_identity_sha(
            flow_kind="uns",
            filename=str(s3_spec),
            default_sha=sha256_file(s3_spec),
        ),
        "git": normalize_flow_input_identity_sha(
            flow_kind="uns",
            filename=str(git_spec),
            default_sha=sha256_file(git_spec),
        ),
    }

    assert stats.total == 3
    assert stats.success == 3
    assert len(set(expected_shas.values())) == 3

    expected_filenames = {
        "http": "http-report.txt",
        "s3": "s3-report.txt",
        "git": "git-report.txt",
    }
    expected_source_kinds = {
        "http": "http_document_v1",
        "s3": "s3_document_v1",
        "git": "git_document_v1",
    }
    expected_metadata: dict[str, dict[str, str]] = {
        "http": {"source_url": "https://example.test/docs/http-report.txt"},
        "s3": {
            "source_bucket": "docs-bucket",
            "source_key": "reports/s3-report.txt",
            "source_version_id": "v1",
        },
        "git": {
            "source_commit": "abcdef1234567890",
            "source_requested_commit": "abc1234",
            "source_relative_path": "docs/git-report.txt",
        },
    }

    for name, expected_sha in expected_shas.items():
        out_dir = out_root / expected_sha
        run_meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
        elements = json.loads((out_dir / "elements.json").read_text(encoding="utf-8"))
        assert run_meta["document"]["filename"] == expected_filenames[name]
        assert run_meta["document"]["mime_type"] == "text/plain"
        assert run_meta["document"]["sha256"] == expected_sha
        assert run_meta["provenance"]["task_identity_key"] == normalize_uns_task_idempotency_key(
            identity=TaskIdentityV1(
                pipeline_version="p-uns-batch",
                sha256=expected_sha,
            )
        )
        assert elements[0]["metadata"]["source_kind"] == expected_source_kinds[name]
        assert elements[0]["metadata"]["fetched_filename"] == expected_filenames[name]
        assert elements[0]["metadata"]["fetched_mime_type"] == "text/plain"
        for key, value in expected_metadata[name].items():
            assert elements[0]["metadata"][key] == value


def test_runner_routes_second_second_round_uns_source_batch_through_shared_execution_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    http_spec = tmp_path / "http-source.json"
    drive_spec = tmp_path / "drive-source.json"
    confluence_spec = tmp_path / "confluence-source.json"

    http_spec.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_document_v1",
                    "url": "https://example.test/docs/http-report.txt",
                    "accept": "text/plain",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    drive_spec.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "google_drive_document_v1",
                    "file_id": "file-123",
                    "drive_id": "drive-1",
                    "acquisition_mode": "export",
                    "export_mime_type": "application/pdf",
                    "access_token": "token-a",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    confluence_spec.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "confluence_document_v1",
                    "site_url": "https://example.atlassian.net",
                    "page_id": "12345",
                    "space_key": "ENG",
                    "page_version": 7,
                    "access_token": "token-a",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _FakeHttpResponse:
        headers = {"Content-Type": "text/plain; charset=utf-8"}

        def read(self) -> bytes:
            return b"http hello"

        def __enter__(self) -> _FakeHttpResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeDriveResponse:
        headers = {
            "Content-Type": "application/pdf",
            "Content-Disposition": 'attachment; filename="report.pdf"',
            "ETag": '"etag-1"',
        }

        def read(self) -> bytes:
            return b"drive hello"

        def __enter__(self) -> _FakeDriveResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeConfluenceResponse:
        def read(self) -> bytes:
            return json.dumps(
                {
                    "title": "Product Spec",
                    "space": {"key": "ENG"},
                    "version": {"number": 7},
                    "body": {"storage": {"value": "<p>hello confluence</p>"}},
                },
                ensure_ascii=False,
            ).encode("utf-8")

        def __enter__(self) -> _FakeConfluenceResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def fake_http_get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        follow_redirects: bool,
        trust_env: bool,
    ) -> httpx.Response:
        assert url == "https://example.test/docs/http-report.txt"
        assert headers == {"Accept": "text/plain"}
        assert timeout == 10.0
        assert follow_redirects is True
        assert trust_env is False
        return httpx.Response(
            200,
            content=b"http hello",
            headers={"Content-Type": "text/plain; charset=utf-8"},
            request=httpx.Request("GET", url),
        )

    def fake_drive_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert (
            typed_request.full_url == "https://www.googleapis.com/drive/v3/files/file-123/export"
            "?mimeType=application%2Fpdf&supportsAllDrives=true"
        )
        return _FakeDriveResponse()

    def fake_confluence_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert (
            typed_request.full_url == "https://example.atlassian.net/wiki/rest/api/content/12345"
            "?expand=space,version,body.storage"
        )
        return _FakeConfluenceResponse()

    def fake_uns_auto_partition(
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
        if fetched_path.suffix == ".pdf":
            text = "drive hello"
            mime_type = "application/pdf"
        elif fetched_path.suffix == ".html":
            text = fetched_path.read_text(encoding="utf-8")
            mime_type = "text/html"
        else:
            text = fetched_path.read_text(encoding="utf-8")
            mime_type = "text/plain"
        return PartitionResult(
            document=DocumentMetadata(
                filename=fetched_path.name,
                mime_type=mime_type,
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
                    text=text,
                    metadata={"kind": "text"},
                )
            ],
            normalized_text=text,
            warnings=[],
        )

    monkeypatch.setattr(uns_http_source.httpx, "get", fake_http_get)
    monkeypatch.setattr(uns_http_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(uns_google_drive_source, "urlopen", fake_drive_urlopen)
    monkeypatch.setattr(uns_google_drive_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(uns_confluence_source, "urlopen", fake_confluence_urlopen)
    monkeypatch.setattr(uns_confluence_source, "auto_partition", fake_uns_auto_partition)

    docs = [
        DocumentRef(
            uri=str(http_spec),
            source="local_file",
            discovered_at_utc="2026-01-01T00:00:00Z",
            filename="http-source.json",
            extension=".json",
            size_bytes=http_spec.stat().st_size,
        ),
        DocumentRef(
            uri=str(drive_spec),
            source="local_file",
            discovered_at_utc="2026-01-01T00:00:00Z",
            filename="drive-source.json",
            extension=".json",
            size_bytes=drive_spec.stat().st_size,
        ),
        DocumentRef(
            uri=str(confluence_spec),
            source="local_file",
            discovered_at_utc="2026-01-01T00:00:00Z",
            filename="confluence-source.json",
            extension=".json",
            size_bytes=confluence_spec.stat().st_size,
        ),
    ]
    ctx = RunContext.new(
        pipeline_version="p-uns-batch-2",
        run_id="r-uns-batch-2",
        timestamp_utc="2026-01-01T00:00:00Z",
    )
    out_root = tmp_path / "out"
    cfg = RunnerConfig(
        out_root=out_root,
        workers=1,
        destination=FilesystemDestination(),
    )

    stats = run_documents(
        docs=docs,
        cfg=cfg,
        ctx=ctx,
        flow_kind="uns",
        destination=FilesystemDestination(),
    )

    expected_shas = {
        "http": normalize_flow_input_identity_sha(
            flow_kind="uns",
            filename=str(http_spec),
            default_sha=sha256_file(http_spec),
        ),
        "drive": normalize_flow_input_identity_sha(
            flow_kind="uns",
            filename=str(drive_spec),
            default_sha=sha256_file(drive_spec),
        ),
        "confluence": normalize_flow_input_identity_sha(
            flow_kind="uns",
            filename=str(confluence_spec),
            default_sha=sha256_file(confluence_spec),
        ),
    }

    assert stats.total == 3
    assert stats.success == 3
    assert len(set(expected_shas.values())) == 3

    expected_filenames = {
        "http": "http-report.txt",
        "drive": "report.pdf",
        "confluence": "product-spec.html",
    }
    expected_mime_types = {
        "http": "text/plain",
        "drive": "application/pdf",
        "confluence": "text/html",
    }
    expected_source_kinds = {
        "http": "http_document_v1",
        "drive": "google_drive_document_v1",
        "confluence": "confluence_document_v1",
    }
    expected_metadata: dict[str, dict[str, object]] = {
        "http": {"source_url": "https://example.test/docs/http-report.txt"},
        "drive": {
            "source_file_id": "file-123",
            "source_drive_id": "drive-1",
            "source_acquisition_mode": "export",
            "source_export_mime_type": "application/pdf",
        },
        "confluence": {
            "source_site_url": "https://example.atlassian.net",
            "source_page_id": "12345",
            "source_space_key": "ENG",
            "source_requested_page_version": 7,
            "source_page_version": 7,
            "source_body_format": "storage",
        },
    }

    for name, expected_sha in expected_shas.items():
        out_dir = out_root / expected_sha
        run_meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
        elements = json.loads((out_dir / "elements.json").read_text(encoding="utf-8"))
        assert run_meta["document"]["filename"] == expected_filenames[name]
        assert run_meta["document"]["mime_type"] == expected_mime_types[name]
        assert run_meta["document"]["sha256"] == expected_sha
        assert run_meta["provenance"]["task_identity_key"] == normalize_uns_task_idempotency_key(
            identity=TaskIdentityV1(
                pipeline_version="p-uns-batch-2",
                sha256=expected_sha,
            )
        )
        assert elements[0]["metadata"]["source_kind"] == expected_source_kinds[name]
        assert elements[0]["metadata"]["fetched_filename"] == expected_filenames[name]
        assert elements[0]["metadata"]["fetched_mime_type"] == expected_mime_types[name]
        for key, value in expected_metadata[name].items():
            assert elements[0]["metadata"][key] == value


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

    def fake_http_get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        follow_redirects: bool,
        trust_env: bool,
    ) -> httpx.Response:
        assert url == "https://example.test/docs/report.txt"
        assert headers == {"Accept": "text/plain"}
        assert timeout == 10.0
        assert follow_redirects is True
        assert trust_env is False
        return httpx.Response(
            200,
            content=b"remote hello",
            headers={"Content-Type": "text/plain; charset=utf-8"},
            request=httpx.Request("GET", url),
        )

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

    monkeypatch.setattr(uns_http_source.httpx, "get", fake_http_get)
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


def test_shared_sqlite_delivery_keeps_expanded_source_world_architecture_locked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    forbidden_persisted_surface_keys = {
        "scheduler",
        "autoscaling",
        "deployment_profile",
        "environment_overlay",
        "dashboard",
        "alerting",
        "tracing",
        "worker_pool",
    }
    forbidden_credential_values = {
        "secret-a",
        "key-a",
        "token-a",
        "mongodb://reader:secret@mongo.test:27017/?replicaSet=rs0",
        "postgresql://reader:secret@db.test/app",
    }

    def _write_spec(path: Path, payload: dict[str, object]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _doc_for(path: Path) -> DocumentRef:
        return DocumentRef(
            uri=str(path),
            source="local_file",
            discovered_at_utc="2026-01-01T00:00:00Z",
            filename=path.name,
            extension=path.suffix,
            size_bytes=path.stat().st_size,
        )

    def _read_json_dict(path: Path) -> dict[str, object]:
        return cast("dict[str, object]", json.loads(path.read_text(encoding="utf-8")))

    it_http_spec = tmp_path / "it-http-source.json"
    postgresql_spec = tmp_path / "postgresql-source.json"
    clickhouse_spec = tmp_path / "clickhouse-source.json"
    kafka_spec = tmp_path / "kafka-source.json"
    mongodb_spec = tmp_path / "mongodb-source.json"
    uns_http_spec = tmp_path / "uns-http-source.json"
    s3_spec = tmp_path / "s3-source.json"
    git_spec = tmp_path / "git-source.json"
    drive_spec = tmp_path / "drive-source.json"
    confluence_spec = tmp_path / "confluence-source.json"

    _write_spec(
        it_http_spec,
        {
            "source": {
                "kind": "http_json_cursor_v1",
                "stream": "customers",
                "url": "https://example.test/api/customers",
                "cursor_param": "cursor",
                "query": {"limit": "2"},
            }
        },
    )
    _write_spec(
        postgresql_spec,
        {
            "source": {
                "kind": "postgresql_incremental_v1",
                "stream": "customers",
                "connection_name": "warehouse-primary",
                "dsn": "postgresql://reader:secret@db.test/app",
                "schema": "public",
                "table": "customers",
                "columns": ["id", "updated_at", "name"],
                "cursor_column": "updated_at",
                "cursor_start": "2026-01-01T00:00:00Z",
                "batch_size": 2,
            }
        },
    )
    _write_spec(
        clickhouse_spec,
        {
            "source": {
                "kind": "clickhouse_incremental_v1",
                "stream": "warehouse_customers",
                "connection_name": "analytics-primary",
                "url": "https://clickhouse.example.test:8443",
                "database": "analytics",
                "table": "customer_events",
                "columns": ["customer_id", "event_ts", "segment"],
                "cursor_column": "event_ts",
                "cursor_start": "2026-02-01T00:00:00Z",
                "batch_size": 2,
                "username": "reader",
                "password": "secret",
            }
        },
    )
    _write_spec(
        kafka_spec,
        {
            "source": {
                "kind": "kafka_partition_offset_v1",
                "stream": "orders",
                "connection_name": "events-primary",
                "brokers": ["kafka-1.example.test:9092"],
                "topic": "orders.events",
                "partition": 2,
                "offset_start": 9,
                "batch_size": 2,
            }
        },
    )
    _write_spec(
        mongodb_spec,
        {
            "source": {
                "kind": "mongodb_incremental_v1",
                "stream": "customer_docs",
                "connection_name": "mongo-primary",
                "uri": "mongodb://reader:secret@mongo.test:27017/?replicaSet=rs0",
                "database": "analytics",
                "collection": "customer_docs",
                "fields": ["doc_id", "doc_cursor", "segment"],
                "cursor_field": "doc_cursor",
                "cursor_start": "cust-0000",
                "batch_size": 2,
            }
        },
    )
    _write_spec(
        uns_http_spec,
        {
            "source": {
                "kind": "http_document_v1",
                "url": "https://example.test/docs/http-report.txt",
                "accept": "text/plain",
            }
        },
    )
    _write_spec(
        s3_spec,
        {
            "source": {
                "kind": "s3_document_v1",
                "bucket": "docs-bucket",
                "key": "reports/s3-report.txt",
                "region": "us-east-1",
                "endpoint_url": "https://s3.example.test",
                "version_id": "v1",
                "access_key": "key-a",
                "secret_key": "secret-a",
            }
        },
    )
    _write_spec(
        git_spec,
        {
            "source": {
                "kind": "git_document_v1",
                "repo_root": str(tmp_path / "repo"),
                "commit": "abc1234",
                "relative_path": "docs/git-report.txt",
            }
        },
    )
    _write_spec(
        drive_spec,
        {
            "source": {
                "kind": "google_drive_document_v1",
                "file_id": "file-123",
                "drive_id": "drive-1",
                "acquisition_mode": "export",
                "export_mime_type": "application/pdf",
                "access_token": "token-a",
            }
        },
    )
    _write_spec(
        confluence_spec,
        {
            "source": {
                "kind": "confluence_document_v1",
                "site_url": "https://example.atlassian.net",
                "page_id": "12345",
                "space_key": "ENG",
                "page_version": 7,
                "access_token": "token-a",
            }
        },
    )

    def fake_fetch_http_json_cursor_source(
        *,
        config: it_http_source.HttpJsonCursorSourceConfigV1,
    ) -> it_http_source.HttpJsonCursorSourceDocumentV1:
        assert config.stream == "customers"
        assert config.url == "https://example.test/api/customers"
        return it_http_source.HttpJsonCursorSourceDocumentV1(
            stream=config.stream,
            records=[{"id": 1}, {"id": 2}],
            states=[{"cursor": "c-1"}],
            logs=[("INFO", "http page fetched")],
        )

    def fake_fetch_postgresql_incremental_source(
        *,
        config: it_postgresql_source.PostgresqlIncrementalSourceConfigV1,
    ) -> it_postgresql_source.PostgresqlIncrementalSourceDocumentV1:
        assert config.connection_name == "warehouse-primary"
        return it_postgresql_source.PostgresqlIncrementalSourceDocumentV1(
            stream=config.stream,
            records=[
                it_postgresql_source.PostgresqlIncrementalSourceRecordV1(
                    data={
                        "id": "cust-1",
                        "updated_at": "2026-01-02T00:00:00Z",
                        "name": "Ada",
                    },
                    cursor="2026-01-02T00:00:00Z",
                )
            ],
            states=[{"cursor": "2026-01-02T00:00:00Z"}],
            logs=[("INFO", "postgresql page fetched")],
        )

    def fake_fetch_clickhouse_incremental_source(
        *,
        config: it_clickhouse_source.ClickHouseIncrementalSourceConfigV1,
    ) -> it_clickhouse_source.ClickHouseIncrementalSourceDocumentV1:
        assert config.connection_name == "analytics-primary"
        return it_clickhouse_source.ClickHouseIncrementalSourceDocumentV1(
            stream=config.stream,
            records=[
                it_clickhouse_source.ClickHouseIncrementalSourceRecordV1(
                    data={
                        "customer_id": "cust-1",
                        "event_ts": "2026-02-02T00:00:00Z",
                        "segment": "gold",
                    },
                    cursor="2026-02-02T00:00:00Z",
                )
            ],
            states=[{"cursor": "2026-02-02T00:00:00Z"}],
            logs=[("INFO", "clickhouse page fetched")],
        )

    def fake_fetch_kafka_partition_source(
        *,
        config: it_kafka_source.KafkaPartitionSourceConfigV1,
    ) -> it_kafka_source.KafkaPartitionSourceDocumentV1:
        assert config.topic == "orders.events"
        assert config.partition == 2
        return it_kafka_source.KafkaPartitionSourceDocumentV1(
            stream=config.stream,
            records=[
                it_kafka_source.KafkaPartitionSourceRecordV1(
                    data={"order_id": "ord-1", "status": "created"},
                    cursor="00000000000000000009",
                )
            ],
            states=[{"offset": "00000000000000000009"}],
            logs=[("INFO", "kafka page fetched")],
        )

    def fake_fetch_mongodb_incremental_source(
        *,
        config: it_mongodb_source.MongoDBIncrementalSourceConfigV1,
    ) -> it_mongodb_source.MongoDBIncrementalSourceDocumentV1:
        assert config.database == "analytics"
        assert config.collection == "customer_docs"
        return it_mongodb_source.MongoDBIncrementalSourceDocumentV1(
            stream=config.stream,
            records=[
                it_mongodb_source.MongoDBIncrementalSourceRecordV1(
                    data={
                        "doc_id": "cust-1",
                        "doc_cursor": "cust-0001",
                        "segment": "gold",
                    },
                    cursor="cust-0001",
                )
            ],
            states=[{"cursor": "cust-0001"}],
            logs=[("INFO", "mongodb page fetched")],
        )

    class _FakeUnsHttpResponse:
        headers = {"Content-Type": "text/plain; charset=utf-8"}

        def read(self) -> bytes:
            return b"http hello"

        def __enter__(self) -> _FakeUnsHttpResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeDriveResponse:
        headers = {
            "Content-Type": "application/pdf",
            "Content-Disposition": 'attachment; filename="report.pdf"',
            "ETag": '"etag-1"',
        }

        def read(self) -> bytes:
            return b"drive hello"

        def __enter__(self) -> _FakeDriveResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeConfluenceResponse:
        def read(self) -> bytes:
            return json.dumps(
                {
                    "title": "Product Spec",
                    "space": {"key": "ENG"},
                    "version": {"number": 7},
                    "body": {"storage": {"value": "<p>hello confluence</p>"}},
                },
                ensure_ascii=False,
            ).encode("utf-8")

        def __enter__(self) -> _FakeConfluenceResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeS3Body:
        def read(self) -> bytes:
            return b"s3 hello"

    class _FakeS3Client:
        def get_object(
            self,
            *,
            Bucket: str,
            Key: str,
            VersionId: str | None = None,
        ) -> dict[str, object]:
            assert Bucket == "docs-bucket"
            assert Key == "reports/s3-report.txt"
            assert VersionId == "v1"
            return {
                "Body": _FakeS3Body(),
                "ContentType": "text/plain; charset=utf-8",
                "ETag": '"etag-1"',
                "VersionId": "v1",
            }

    def fake_uns_http_get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        follow_redirects: bool,
        trust_env: bool,
    ) -> httpx.Response:
        assert url == "https://example.test/docs/http-report.txt"
        assert headers == {"Accept": "text/plain"}
        assert timeout == 10.0
        assert follow_redirects is True
        assert trust_env is False
        return httpx.Response(
            200,
            content=_FakeUnsHttpResponse().read(),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            request=httpx.Request("GET", url),
        )

    def fake_drive_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert (
            typed_request.full_url == "https://www.googleapis.com/drive/v3/files/file-123/export"
            "?mimeType=application%2Fpdf&supportsAllDrives=true"
        )
        assert typed_request.headers["Authorization"] == "Bearer token-a"
        assert typed_request.headers["Accept"] == "application/pdf"
        return _FakeDriveResponse()

    def fake_confluence_urlopen(request: object, timeout: float = 10.0) -> object:
        del timeout
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert (
            typed_request.full_url == "https://example.atlassian.net/wiki/rest/api/content/12345"
            "?expand=space,version,body.storage"
        )
        return _FakeConfluenceResponse()

    def fake_build_s3_document_source_client(
        *,
        config: uns_s3_source.S3DocumentSourceConfigV1,
    ) -> uns_s3_source.S3DocumentSourceClientProtocol:
        assert config.bucket == "docs-bucket"
        return _FakeS3Client()

    def fake_fetch_git_document_source(
        *,
        config: uns_git_source.GitDocumentSourceConfigV1,
    ) -> uns_git_source.GitDocumentSourceFetchV1:
        assert config.commit == "abc1234"
        return uns_git_source.GitDocumentSourceFetchV1(
            repo_root=config.repo_root,
            commit=config.commit,
            resolved_commit="abcdef1234567890",
            relative_path=config.relative_path,
            blob_sha="blob1234567890",
            filename="git-report.txt",
            mime_type="text/plain",
            content=b"git hello",
        )

    def fake_uns_auto_partition(
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
        text = fetched_path.read_text(encoding="utf-8")
        return PartitionResult(
            document=DocumentMetadata(
                filename=fetched_path.name,
                mime_type="application/pdf" if fetched_path.suffix == ".pdf" else "text/plain",
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
                    text=text,
                    metadata={"kind": "text"},
                )
            ],
            normalized_text=text,
            warnings=[],
        )

    monkeypatch.setattr(
        it_service, "fetch_http_json_cursor_source", fake_fetch_http_json_cursor_source
    )
    monkeypatch.setattr(
        it_service,
        "fetch_postgresql_incremental_source",
        fake_fetch_postgresql_incremental_source,
    )
    monkeypatch.setattr(
        it_service,
        "fetch_clickhouse_incremental_source",
        fake_fetch_clickhouse_incremental_source,
    )
    monkeypatch.setattr(
        it_service,
        "fetch_kafka_partition_source",
        fake_fetch_kafka_partition_source,
    )
    monkeypatch.setattr(
        it_service,
        "fetch_mongodb_incremental_source",
        fake_fetch_mongodb_incremental_source,
    )
    monkeypatch.setattr(uns_http_source.httpx, "get", fake_uns_http_get)
    monkeypatch.setattr(uns_http_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(
        uns_s3_source,
        "_build_s3_document_source_client",
        fake_build_s3_document_source_client,
    )
    monkeypatch.setattr(uns_s3_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(uns_git_source, "fetch_git_document_source", fake_fetch_git_document_source)
    monkeypatch.setattr(uns_git_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(uns_google_drive_source, "urlopen", fake_drive_urlopen)
    monkeypatch.setattr(uns_google_drive_source, "auto_partition", fake_uns_auto_partition)
    monkeypatch.setattr(uns_confluence_source, "urlopen", fake_confluence_urlopen)
    monkeypatch.setattr(uns_confluence_source, "auto_partition", fake_uns_auto_partition)

    it_docs = [
        _doc_for(it_http_spec),
        _doc_for(postgresql_spec),
        _doc_for(clickhouse_spec),
        _doc_for(kafka_spec),
        _doc_for(mongodb_spec),
    ]
    uns_docs = [
        _doc_for(uns_http_spec),
        _doc_for(s3_spec),
        _doc_for(git_spec),
        _doc_for(drive_spec),
        _doc_for(confluence_spec),
    ]

    sqlite_destination = SqliteDestination(
        db_path=tmp_path / "delivery.db",
        table_name="delivery_rows",
    )
    it_out_root = tmp_path / "it-out"
    uns_out_root = tmp_path / "uns-out"
    it_ctx = RunContext.new(
        pipeline_version="p-expanded-it",
        run_id="r-expanded-it",
        timestamp_utc="2026-01-01T00:00:00Z",
    )
    uns_ctx = RunContext.new(
        pipeline_version="p-expanded-uns",
        run_id="r-expanded-uns",
        timestamp_utc="2026-01-01T00:00:00Z",
    )

    it_stats = run_documents(
        docs=it_docs,
        cfg=RunnerConfig(out_root=it_out_root, workers=1, destination=sqlite_destination),
        ctx=it_ctx,
        flow_kind="it",
        destination=sqlite_destination,
    )
    uns_stats = run_documents(
        docs=uns_docs,
        cfg=RunnerConfig(out_root=uns_out_root, workers=1, destination=sqlite_destination),
        ctx=uns_ctx,
        flow_kind="uns",
        destination=sqlite_destination,
    )

    expected_it = {
        "http": (
            normalize_flow_input_identity_sha(
                flow_kind="it",
                filename=str(it_http_spec),
                default_sha=sha256_file(it_http_spec),
            ),
            "http-json-cursor",
        ),
        "postgresql": (
            normalize_flow_input_identity_sha(
                flow_kind="it",
                filename=str(postgresql_spec),
                default_sha=sha256_file(postgresql_spec),
            ),
            "postgresql-incremental",
        ),
        "clickhouse": (
            normalize_flow_input_identity_sha(
                flow_kind="it",
                filename=str(clickhouse_spec),
                default_sha=sha256_file(clickhouse_spec),
            ),
            "clickhouse-incremental",
        ),
        "kafka": (
            normalize_flow_input_identity_sha(
                flow_kind="it",
                filename=str(kafka_spec),
                default_sha=sha256_file(kafka_spec),
            ),
            "kafka-partition-offset",
        ),
        "mongodb": (
            normalize_flow_input_identity_sha(
                flow_kind="it",
                filename=str(mongodb_spec),
                default_sha=sha256_file(mongodb_spec),
            ),
            "mongodb-incremental",
        ),
    }
    expected_uns = {
        "http": (
            normalize_flow_input_identity_sha(
                flow_kind="uns",
                filename=str(uns_http_spec),
                default_sha=sha256_file(uns_http_spec),
            ),
            "http-report.txt",
            "text/plain",
        ),
        "s3": (
            normalize_flow_input_identity_sha(
                flow_kind="uns",
                filename=str(s3_spec),
                default_sha=sha256_file(s3_spec),
            ),
            "s3-report.txt",
            "text/plain",
        ),
        "git": (
            normalize_flow_input_identity_sha(
                flow_kind="uns",
                filename=str(git_spec),
                default_sha=sha256_file(git_spec),
            ),
            "git-report.txt",
            "text/plain",
        ),
        "drive": (
            normalize_flow_input_identity_sha(
                flow_kind="uns",
                filename=str(drive_spec),
                default_sha=sha256_file(drive_spec),
            ),
            "report.pdf",
            "application/pdf",
        ),
        "confluence": (
            normalize_flow_input_identity_sha(
                flow_kind="uns",
                filename=str(confluence_spec),
                default_sha=sha256_file(confluence_spec),
            ),
            "product-spec.html",
            "text/html",
        ),
    }

    assert it_stats.total == 5
    assert it_stats.success == 5
    assert uns_stats.total == 5
    assert uns_stats.success == 5
    assert (
        len(
            {
                *[sha for sha, _backend in expected_it.values()],
                *[sha for sha, _filename, _mime_type in expected_uns.values()],
            }
        )
        == 10
    )

    it_batch_report = _read_json_dict(it_out_root / "batch_report.json")
    uns_batch_report = _read_json_dict(uns_out_root / "batch_report.json")
    assert cast("dict[str, object]", it_batch_report["delivery"])["total"] == 5
    assert cast("dict[str, object]", it_batch_report["delivery"])["ok"] == 5
    assert cast("dict[str, object]", it_batch_report["delivery"])["failed"] == 0
    assert cast("dict[str, object]", it_batch_report["delivery"])["dlq_written_total"] == 0
    assert cast("dict[str, object]", it_batch_report["delivery"])["by_destination"] == {
        "sqlite": {"total": 5, "ok": 5, "failed": 0}
    }
    assert it_batch_report["workers"] == 1
    assert it_batch_report["executor"] == "serial"
    assert cast("dict[str, object]", it_batch_report["retry"]) == {
        "enabled": True,
        "max_attempts": 3,
        "base_backoff_ms": 200,
        "max_backoff_ms": 5000,
        "retry_attempts_total": 0,
        "retried_success": 0,
        "retryable_failed": 0,
    }
    assert cast("dict[str, object]", it_batch_report["metrics"])["docs_total"] == 5
    assert cast("dict[str, object]", it_batch_report["metrics"])["docs_success_total"] == 5
    assert cast("dict[str, object]", it_batch_report["metrics"])["delivery_total"] == 5
    assert cast("dict[str, object]", it_batch_report["metrics"])["dlq_written_total"] == 0
    assert set(cast("dict[str, object]", it_batch_report["stage_durations_ms"])) == {
        "hash_ms",
        "partition_ms",
        "delivery_ms",
    }
    assert cast("dict[str, object]", uns_batch_report["delivery"])["total"] == 5
    assert cast("dict[str, object]", uns_batch_report["delivery"])["ok"] == 5
    assert cast("dict[str, object]", uns_batch_report["delivery"])["failed"] == 0
    assert cast("dict[str, object]", uns_batch_report["delivery"])["dlq_written_total"] == 0
    assert cast("dict[str, object]", uns_batch_report["delivery"])["by_destination"] == {
        "sqlite": {"total": 5, "ok": 5, "failed": 0}
    }
    assert uns_batch_report["workers"] == 1
    assert uns_batch_report["executor"] == "serial"
    assert cast("dict[str, object]", uns_batch_report["retry"]) == {
        "enabled": True,
        "max_attempts": 3,
        "base_backoff_ms": 200,
        "max_backoff_ms": 5000,
        "retry_attempts_total": 0,
        "retried_success": 0,
        "retryable_failed": 0,
    }
    assert cast("dict[str, object]", uns_batch_report["metrics"])["docs_total"] == 5
    assert cast("dict[str, object]", uns_batch_report["metrics"])["docs_success_total"] == 5
    assert cast("dict[str, object]", uns_batch_report["metrics"])["delivery_total"] == 5
    assert cast("dict[str, object]", uns_batch_report["metrics"])["dlq_written_total"] == 0
    assert set(cast("dict[str, object]", uns_batch_report["stage_durations_ms"])) == {
        "hash_ms",
        "partition_ms",
        "delivery_ms",
    }

    for report in (it_batch_report, uns_batch_report):
        report_json = json.dumps(report, sort_keys=True)
        for key in forbidden_persisted_surface_keys:
            assert key not in report_json

    conn = sqlite3.connect(tmp_path / "delivery.db")
    try:
        rows = conn.execute("SELECT payload_json FROM delivery_rows").fetchall()
    finally:
        conn.close()

    payloads_by_sha: dict[str, dict[str, object]] = {}
    persisted_payload_jsons: list[str] = []
    for row in rows:
        payload_json = cast("str", row[0])
        persisted_payload_jsons.append(payload_json)
        payload = cast("dict[str, object]", json.loads(payload_json))
        sha = cast("str", payload["sha256"])
        payloads_by_sha[sha] = payload

    assert len(payloads_by_sha) == 10

    for payload_json in persisted_payload_jsons:
        for key in forbidden_persisted_surface_keys:
            assert key not in payload_json
        for value in forbidden_credential_values:
            assert value not in payload_json

    observed_task_identity_keys: set[str] = set()

    for name, (expected_sha, expected_backend) in expected_it.items():
        payload = payloads_by_sha[expected_sha]
        run_meta = cast("dict[str, object]", payload["run_meta"])
        provenance = cast("dict[str, object]", run_meta["provenance"])
        artifacts = cast("dict[str, object]", payload["artifacts"])

        expected_task_identity_key = normalize_it_task_identity_key(
            identity=ItTaskIdentityV1(
                pipeline_version="p-expanded-it",
                sha256=expected_sha,
            )
        )

        assert cast("dict[str, object]", run_meta["engine"])["name"] == "it-stream", name
        assert cast("dict[str, object]", run_meta["engine"])["backend"] == expected_backend, name
        assert provenance["run_origin"] == "intake"
        assert provenance["delivery_origin"] == "primary"
        assert provenance["execution_mode"] == "batch"
        assert provenance["task_id"] == expected_sha
        assert provenance["task_identity_key"] == expected_task_identity_key
        observed_task_identity_keys.add(expected_task_identity_key)
        assert cast("str", artifacts["out_dir"]).endswith(expected_sha)
        assert cast("str", artifacts["records_path"]).endswith("records.jsonl")
        assert cast("str", artifacts["state_path"]).endswith("checkpoint.json")
        assert cast("str", artifacts["logs_path"]).endswith("logs.jsonl")

    for name, (expected_sha, expected_filename, expected_mime_type) in expected_uns.items():
        payload = payloads_by_sha[expected_sha]
        run_meta = cast("dict[str, object]", payload["run_meta"])
        provenance = cast("dict[str, object]", run_meta["provenance"])
        artifacts = cast("dict[str, object]", payload["artifacts"])
        document = cast("dict[str, object]", run_meta["document"])

        expected_task_identity_key = normalize_uns_task_idempotency_key(
            identity=TaskIdentityV1(
                pipeline_version="p-expanded-uns",
                sha256=expected_sha,
            )
        )

        assert cast("dict[str, object]", run_meta["engine"])["name"] == "unstructured", name
        assert provenance["run_origin"] == "intake"
        assert provenance["delivery_origin"] == "primary"
        assert provenance["execution_mode"] == "batch"
        assert provenance["task_id"] == expected_sha
        assert provenance["task_identity_key"] == expected_task_identity_key
        observed_task_identity_keys.add(expected_task_identity_key)
        assert cast("str", artifacts["out_dir"]).endswith(expected_sha)
        assert "records_path" not in artifacts
        assert "state_path" not in artifacts
        assert "logs_path" not in artifacts
        assert document["filename"] == expected_filename
        assert document["mime_type"] == expected_mime_type

    assert len(observed_task_identity_keys) == 10


def json_dumps_messages(messages: list[dict[str, object]]) -> str:
    return json.dumps(
        {"messages": messages},
        ensure_ascii=False,
        indent=2,
    )
