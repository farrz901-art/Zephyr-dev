from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from uns_stream.sources import http_source
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)


def test_normalize_uns_input_identity_sha_is_stable_for_http_source_specs(tmp_path: Path) -> None:
    spec_a = tmp_path / "source-a.json"
    spec_b = tmp_path / "source-b.json"

    spec_a.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_document_v1",
                    "url": "https://example.test/docs/report.txt",
                    "accept": "text/plain",
                    "timeout_s": 5.0,
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    spec_b.write_text(
        json.dumps(
            {
                "source": {
                    "timeout_s": 30.0,
                    "accept": "text/plain",
                    "url": "https://example.test/docs/report.txt",
                    "kind": "http_document_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    assert http_source.normalize_uns_input_identity_sha(
        filename=str(spec_a),
        default_sha="fallback-a",
    ) == http_source.normalize_uns_input_identity_sha(
        filename=str(spec_b),
        default_sha="fallback-b",
    )


def test_load_http_document_source_config_rejects_invalid_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_s"):
        http_source.load_http_document_source_config(
            {
                "source": {
                    "kind": "http_document_v1",
                    "url": "https://example.test/docs/report.txt",
                    "timeout_s": 0,
                }
            }
        )


def test_process_file_fetches_remote_document_and_preserves_provenance(
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

    def fake_get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        follow_redirects: bool,
        trust_env: bool,
    ) -> httpx.Response:
        assert url == "https://example.test/docs/report.txt"
        assert timeout == 10.0
        assert headers == {"Accept": "text/plain"}
        assert follow_redirects is True
        assert trust_env is False
        return httpx.Response(
            200,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            content=b"remote hello",
            request=httpx.Request("GET", url),
        )

    captured: dict[str, object] = {}

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
        captured["filename"] = fetched_path.name
        captured["content"] = fetched_path.read_text(encoding="utf-8")
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

    monkeypatch.setattr(http_source.httpx, "get", fake_get)
    monkeypatch.setattr(http_source, "auto_partition", fake_auto_partition)

    result = http_source.process_file(
        filename=str(spec_path),
        sha256="task-sha",
    )

    assert captured["filename"] == "report.txt"
    assert captured["content"] == "remote hello"
    assert result.document.filename == "report.txt"
    assert result.document.mime_type == "text/plain"
    assert result.document.sha256 == "task-sha"
    assert result.document.size_bytes == len(b"remote hello")
    assert result.elements[0].metadata["source_url"] == "https://example.test/docs/report.txt"
    assert result.elements[0].metadata["source_kind"] == "http_document_v1"
    assert result.elements[0].metadata["fetched_filename"] == "report.txt"
    assert result.elements[0].metadata["fetched_mime_type"] == "text/plain"
    assert "headers" not in result.elements[0].metadata
    assert "content" not in result.elements[0].metadata
    assert "response_text" not in result.elements[0].metadata


@pytest.mark.parametrize(("status_code", "retryable"), [(404, False), (503, True)])
def test_fetch_http_document_source_maps_http_error_retryability(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    retryable: bool,
) -> None:
    config = http_source.HttpDocumentSourceConfigV1(
        url="https://example.test/docs/report.txt",
        accept=None,
        timeout_s=5.0,
    )

    def fake_get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        follow_redirects: bool,
        trust_env: bool,
    ) -> httpx.Response:
        del headers, timeout, follow_redirects, trust_env
        return httpx.Response(
            status_code,
            headers={},
            content=b"boom",
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(http_source.httpx, "get", fake_get)

    with pytest.raises(ZephyrError) as exc_info:
        http_source.fetch_http_document_source(config=config)

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details is not None
    assert exc_info.value.details.get("retryable") is retryable
