from __future__ import annotations

import json
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from uns_stream.sources import google_drive_source
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)


def test_normalize_uns_input_identity_sha_is_stable_for_google_drive_source_specs(
    tmp_path: Path,
) -> None:
    spec_a = tmp_path / "source-a.json"
    spec_b = tmp_path / "source-b.json"

    spec_a.write_text(
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
    spec_b.write_text(
        json.dumps(
            {
                "source": {
                    "access_token": "token-b",
                    "export_mime_type": "application/pdf",
                    "acquisition_mode": "export",
                    "drive_id": "drive-1",
                    "file_id": "file-123",
                    "kind": "google_drive_document_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    assert google_drive_source.normalize_uns_input_identity_sha(
        filename=str(spec_a),
        default_sha="fallback-a",
    ) == google_drive_source.normalize_uns_input_identity_sha(
        filename=str(spec_b),
        default_sha="fallback-b",
    )


def test_load_google_drive_document_source_config_requires_export_mime_type_for_export() -> None:
    with pytest.raises(ValueError, match="export_mime_type"):
        google_drive_source.load_google_drive_document_source_config(
            {
                "source": {
                    "kind": "google_drive_document_v1",
                    "file_id": "file-123",
                    "access_token": "token-a",
                    "acquisition_mode": "export",
                }
            }
        )


def test_process_file_fetches_google_drive_document_and_preserves_provenance(
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
            return b"pdf bytes"

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        assert timeout == 10.0
        typed_request = request if isinstance(request, Request) else None
        assert typed_request is not None
        assert (
            typed_request.full_url == "https://www.googleapis.com/drive/v3/files/file-123/export"
            "?mimeType=application%2Fpdf&supportsAllDrives=true"
        )
        assert typed_request.headers["Authorization"] == "Bearer token-a"
        assert typed_request.headers["Accept"] == "application/pdf"
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
        captured["content"] = fetched_path.read_bytes()
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

    monkeypatch.setattr(google_drive_source, "urlopen", fake_urlopen)
    monkeypatch.setattr(google_drive_source, "auto_partition", fake_auto_partition)

    result = google_drive_source.process_file(
        filename=str(spec_path),
        sha256="task-sha",
    )

    assert captured["filename"] == "report.pdf"
    assert captured["content"] == b"pdf bytes"
    assert result.document.filename == "report.pdf"
    assert result.document.mime_type == "application/pdf"
    assert result.document.sha256 == "task-sha"
    assert result.document.size_bytes == len(b"pdf bytes")
    assert result.elements[0].metadata["source_kind"] == "google_drive_document_v1"
    assert result.elements[0].metadata["source_file_id"] == "file-123"
    assert result.elements[0].metadata["source_drive_id"] == "drive-1"
    assert result.elements[0].metadata["source_acquisition_mode"] == "export"
    assert result.elements[0].metadata["source_export_mime_type"] == "application/pdf"
    assert result.elements[0].metadata["source_etag"] == '"etag-1"'
    assert result.elements[0].metadata["fetched_filename"] == "report.pdf"
    assert result.elements[0].metadata["fetched_mime_type"] == "application/pdf"


@pytest.mark.parametrize(("status_code", "retryable"), [(404, False), (503, True)])
def test_fetch_google_drive_document_source_maps_error_retryability(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    retryable: bool,
) -> None:
    config = google_drive_source.GoogleDriveDocumentSourceConfigV1(
        file_id="file-123",
        access_token="token-a",
        acquisition_mode="download",
        export_mime_type=None,
        drive_id=None,
        timeout_s=10.0,
    )

    def fake_urlopen(request: object, timeout: float = 10.0) -> object:
        del request, timeout
        raise HTTPError(
            url="https://www.googleapis.com/drive/v3/files/file-123?alt=media",
            code=status_code,
            msg="boom",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr(google_drive_source, "urlopen", fake_urlopen)

    with pytest.raises(ZephyrError) as exc_info:
        google_drive_source.fetch_google_drive_document_source(config=config)

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details is not None
    assert exc_info.value.details.get("retryable") is retryable
    assert exc_info.value.details.get("status_code") == status_code
