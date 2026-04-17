from __future__ import annotations

import json
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from uns_stream.sources import confluence_source
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)


def test_normalize_uns_input_identity_sha_is_stable_for_confluence_source_specs(
    tmp_path: Path,
) -> None:
    spec_a = tmp_path / "source-a.json"
    spec_b = tmp_path / "source-b.json"

    spec_a.write_text(
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
    spec_b.write_text(
        json.dumps(
            {
                "source": {
                    "access_token": "token-b",
                    "page_version": 7,
                    "space_key": "ENG",
                    "page_id": "12345",
                    "site_url": "https://example.atlassian.net/",
                    "kind": "confluence_document_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    assert confluence_source.normalize_uns_input_identity_sha(
        filename=str(spec_a),
        default_sha="fallback-a",
    ) == confluence_source.normalize_uns_input_identity_sha(
        filename=str(spec_b),
        default_sha="fallback-b",
    )


def test_load_confluence_document_source_config_rejects_non_positive_page_version() -> None:
    with pytest.raises(ValueError, match="page_version"):
        confluence_source.load_confluence_document_source_config(
            {
                "source": {
                    "kind": "confluence_document_v1",
                    "site_url": "https://example.atlassian.net",
                    "page_id": "12345",
                    "page_version": 0,
                    "access_token": "token-a",
                }
            }
        )


def test_process_file_fetches_confluence_document_and_preserves_provenance(
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
        assert timeout == 10.0
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert (
            typed_request.full_url == "https://example.atlassian.net/wiki/rest/api/content/12345"
            "?expand=space,version,body.storage"
        )
        assert typed_request.headers["Authorization"] == "Bearer token-a"
        assert typed_request.headers["Accept"] == "application/json"
        return _FakeResponse()

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

    monkeypatch.setattr(confluence_source, "urlopen", fake_urlopen)
    monkeypatch.setattr(confluence_source, "auto_partition", fake_auto_partition)

    result = confluence_source.process_file(
        filename=str(spec_path),
        sha256="task-sha",
    )

    assert captured["filename"] == "product-spec.html"
    assert captured["content"] == "<p>hello confluence</p>"
    assert result.document.filename == "product-spec.html"
    assert result.document.mime_type == "text/html"
    assert result.document.sha256 == "task-sha"
    assert result.elements[0].metadata["source_kind"] == "confluence_document_v1"
    assert result.elements[0].metadata["source_site_url"] == "https://example.atlassian.net"
    assert result.elements[0].metadata["source_page_id"] == "12345"
    assert result.elements[0].metadata["source_space_key"] == "ENG"
    assert result.elements[0].metadata["source_requested_page_version"] == 7
    assert result.elements[0].metadata["source_page_version"] == 7
    assert result.elements[0].metadata["source_body_format"] == "storage"
    assert result.elements[0].metadata["source_page_title"] == "Product Spec"
    assert result.elements[0].metadata["fetched_filename"] == "product-spec.html"
    assert result.elements[0].metadata["fetched_mime_type"] == "text/html"


def test_fetch_confluence_document_source_uses_basic_auth_when_email_is_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = confluence_source.ConfluenceDocumentSourceConfigV1(
        site_url="https://example.atlassian.net",
        page_id="12345",
        access_token="token-a",
        space_key="ENG",
        page_version=7,
        timeout_s=10.0,
        email="reader@example.test",
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
        assert timeout == 10.0
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert typed_request.headers["Authorization"].startswith("Basic ")
        assert typed_request.headers["Accept"] == "application/json"
        return _FakeResponse()

    monkeypatch.setattr(confluence_source, "urlopen", fake_urlopen)

    fetched = confluence_source.fetch_confluence_document_source(config=config)

    assert fetched.site_url == "https://example.atlassian.net"
    assert fetched.page_id == "12345"
    assert fetched.space_key == "ENG"
    assert fetched.requested_page_version == 7
    assert fetched.resolved_page_version == 7
    assert fetched.filename == "product-spec.html"
    assert fetched.mime_type == "text/html"


@pytest.mark.parametrize(("status_code", "retryable"), [(404, False), (503, True)])
def test_fetch_confluence_document_source_maps_error_retryability(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    retryable: bool,
) -> None:
    config = confluence_source.ConfluenceDocumentSourceConfigV1(
        site_url="https://example.atlassian.net",
        page_id="12345",
        access_token="token-a",
        space_key=None,
        page_version=None,
        timeout_s=10.0,
    )

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del request, timeout
        raise HTTPError(
            url="https://example.atlassian.net/wiki/rest/api/content/12345",
            code=status_code,
            msg="boom",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr(confluence_source, "urlopen", fake_urlopen)

    with pytest.raises(ZephyrError) as exc_info:
        confluence_source.fetch_confluence_document_source(config=config)

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details is not None
    assert exc_info.value.details.get("retryable") is retryable
    assert exc_info.value.details.get("status_code") == status_code
