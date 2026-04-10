from __future__ import annotations

import json
from pathlib import Path

import pytest

from uns_stream.sources import s3_source
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)


def test_normalize_uns_input_identity_sha_is_stable_for_s3_source_specs(tmp_path: Path) -> None:
    spec_a = tmp_path / "source-a.json"
    spec_b = tmp_path / "source-b.json"

    spec_a.write_text(
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
                    "session_token": "token-a",
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
                    "session_token": "token-b",
                    "secret_key": "secret-b",
                    "access_key": "key-b",
                    "version_id": "v1",
                    "endpoint_url": "https://s3.example.test",
                    "region": "us-east-1",
                    "key": "reports/report.txt",
                    "bucket": "docs-bucket",
                    "kind": "s3_document_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    assert s3_source.normalize_uns_input_identity_sha(
        filename=str(spec_a),
        default_sha="fallback-a",
    ) == s3_source.normalize_uns_input_identity_sha(
        filename=str(spec_b),
        default_sha="fallback-b",
    )


def test_load_s3_document_source_config_rejects_missing_bucket() -> None:
    with pytest.raises(ValueError, match="bucket"):
        s3_source.load_s3_document_source_config(
            {
                "source": {
                    "kind": "s3_document_v1",
                    "key": "reports/report.txt",
                    "region": "us-east-1",
                    "access_key": "key-a",
                    "secret_key": "secret-a",
                }
            }
        )


def test_process_file_fetches_s3_document_and_preserves_provenance(
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
        config: s3_source.S3DocumentSourceConfigV1,
    ) -> s3_source.S3DocumentSourceClientProtocol:
        assert config.bucket == "docs-bucket"
        assert config.key == "reports/report.txt"
        return _FakeClient()

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

    monkeypatch.setattr(
        s3_source,
        "_build_s3_document_source_client",
        fake_build_s3_document_source_client,
    )
    monkeypatch.setattr(s3_source, "auto_partition", fake_auto_partition)

    result = s3_source.process_file(
        filename=str(spec_path),
        sha256="task-sha",
    )

    assert captured["filename"] == "report.txt"
    assert captured["content"] == "remote hello"
    assert result.document.filename == "report.txt"
    assert result.document.mime_type == "text/plain"
    assert result.document.sha256 == "task-sha"
    assert result.document.size_bytes == len(b"remote hello")
    assert result.elements[0].metadata["source_kind"] == "s3_document_v1"
    assert result.elements[0].metadata["source_bucket"] == "docs-bucket"
    assert result.elements[0].metadata["source_key"] == "reports/report.txt"
    assert result.elements[0].metadata["source_version_id"] == "v1"
    assert result.elements[0].metadata["source_etag"] == '"etag-1"'
    assert result.elements[0].metadata["fetched_filename"] == "report.txt"
    assert result.elements[0].metadata["fetched_mime_type"] == "text/plain"
    assert "content" not in result.elements[0].metadata


@pytest.mark.parametrize(("status_code", "retryable"), [(404, False), (503, True)])
def test_fetch_s3_document_source_maps_error_retryability(
    status_code: int,
    retryable: bool,
) -> None:
    config = s3_source.S3DocumentSourceConfigV1(
        bucket="docs-bucket",
        key="reports/report.txt",
        region="us-east-1",
        access_key="key-a",
        secret_key="secret-a",
        endpoint_url=None,
        session_token=None,
        version_id="v1",
    )

    class _FakeClientError(Exception):
        def __init__(self) -> None:
            super().__init__("boom")
            self.response = {
                "ResponseMetadata": {"HTTPStatusCode": status_code},
                "Error": {"Code": "NoSuchKey", "Message": "missing"},
            }

    class _FakeClient:
        def get_object(
            self,
            *,
            Bucket: str,
            Key: str,
            VersionId: str | None = None,
        ) -> dict[str, object]:
            del Bucket, Key, VersionId
            raise _FakeClientError()

    with pytest.raises(ZephyrError) as exc_info:
        s3_source.fetch_s3_document_source(config=config, client=_FakeClient())

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details is not None
    assert exc_info.value.details.get("retryable") is retryable
    assert exc_info.value.details.get("status_code") == status_code
