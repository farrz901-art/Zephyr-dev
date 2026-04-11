from __future__ import annotations

import json
from pathlib import Path

from uns_stream.sources import (
    confluence_source,
    git_source,
    google_drive_source,
    http_source,
    normalize_uns_input_identity_sha,
    process_file,
    s3_source,
)
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)


def _fake_auto_partition(
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
    content = fetched_path.read_text(encoding="utf-8")
    encoded_content = content.encode("utf-8")
    return PartitionResult(
        document=DocumentMetadata(
            filename=fetched_path.name,
            mime_type="text/plain",
            sha256=sha256 or "temp-sha",
            size_bytes=size_bytes or len(encoded_content),
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
                text=content,
                metadata={"kind": "text"},
            )
        ],
        normalized_text=content,
        warnings=[],
    )


def test_second_round_uns_source_batch_uses_shared_identity_boundary(tmp_path: Path) -> None:
    http_spec = tmp_path / "http-source.json"
    s3_spec_a = tmp_path / "s3-source-a.json"
    s3_spec_b = tmp_path / "s3-source-b.json"
    git_spec_a = tmp_path / "git-source-a.json"
    git_spec_b = tmp_path / "git-source-b.json"

    http_spec.write_text(
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
    s3_spec_a.write_text(
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
    s3_spec_b.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "s3_document_v1",
                    "bucket": "docs-bucket",
                    "key": "reports/report.txt",
                    "region": "us-east-1",
                    "endpoint_url": "https://s3.example.test",
                    "version_id": "v1",
                    "access_key": "key-b",
                    "secret_key": "secret-b",
                    "session_token": "token-b",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    git_spec_a.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": str(tmp_path / "repo"),
                    "commit": "abc1234",
                    "relative_path": "docs\\report.txt",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    git_spec_b.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": str(tmp_path / "repo"),
                    "commit": "abc1234",
                    "relative_path": "docs/report.txt",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    http_sha = normalize_uns_input_identity_sha(
        filename=str(http_spec),
        default_sha="http-fallback",
    )
    s3_sha_a = normalize_uns_input_identity_sha(
        filename=str(s3_spec_a),
        default_sha="s3-fallback-a",
    )
    s3_sha_b = normalize_uns_input_identity_sha(
        filename=str(s3_spec_b),
        default_sha="s3-fallback-b",
    )
    git_sha_a = normalize_uns_input_identity_sha(
        filename=str(git_spec_a),
        default_sha="git-fallback-a",
    )
    git_sha_b = normalize_uns_input_identity_sha(
        filename=str(git_spec_b),
        default_sha="git-fallback-b",
    )

    assert s3_sha_a == s3_sha_b
    assert git_sha_a == git_sha_b
    assert len({http_sha, s3_sha_a, git_sha_a}) == 3


def test_second_second_round_uns_source_batch_uses_shared_identity_boundary(
    tmp_path: Path,
) -> None:
    http_spec = tmp_path / "http-source.json"
    drive_spec_a = tmp_path / "drive-source-a.json"
    drive_spec_b = tmp_path / "drive-source-b.json"
    confluence_spec_a = tmp_path / "confluence-source-a.json"
    confluence_spec_b = tmp_path / "confluence-source-b.json"

    http_spec.write_text(
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
    drive_spec_a.write_text(
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
    drive_spec_b.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "google_drive_document_v1",
                    "access_token": "token-b",
                    "export_mime_type": "application/pdf",
                    "acquisition_mode": "export",
                    "drive_id": "drive-1",
                    "file_id": "file-123",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    confluence_spec_a.write_text(
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
    confluence_spec_b.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "confluence_document_v1",
                    "access_token": "token-b",
                    "page_version": 7,
                    "space_key": "ENG",
                    "page_id": "12345",
                    "site_url": "https://example.atlassian.net/",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    http_sha = normalize_uns_input_identity_sha(
        filename=str(http_spec),
        default_sha="http-fallback",
    )
    drive_sha_a = normalize_uns_input_identity_sha(
        filename=str(drive_spec_a),
        default_sha="drive-fallback-a",
    )
    drive_sha_b = normalize_uns_input_identity_sha(
        filename=str(drive_spec_b),
        default_sha="drive-fallback-b",
    )
    confluence_sha_a = normalize_uns_input_identity_sha(
        filename=str(confluence_spec_a),
        default_sha="confluence-fallback-a",
    )
    confluence_sha_b = normalize_uns_input_identity_sha(
        filename=str(confluence_spec_b),
        default_sha="confluence-fallback-b",
    )

    assert drive_sha_a == drive_sha_b
    assert confluence_sha_a == confluence_sha_b
    assert len({http_sha, drive_sha_a, confluence_sha_a}) == 3


def test_full_second_round_uns_source_breadth_is_architecture_locked_to_document_boundary(
    tmp_path: Path,
) -> None:
    specs = {
        "http": tmp_path / "http-source.json",
        "s3": tmp_path / "s3-source.json",
        "git": tmp_path / "git-source.json",
        "google_drive": tmp_path / "google-drive-source.json",
        "confluence": tmp_path / "confluence-source.json",
    }
    specs["http"].write_text(
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
    specs["s3"].write_text(
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
    specs["git"].write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": str(tmp_path / "repo"),
                    "commit": "abc1234",
                    "relative_path": "docs/report.txt",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    specs["google_drive"].write_text(
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
    specs["confluence"].write_text(
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

    def fake_fetch_http_document_source(
        *,
        config: http_source.HttpDocumentSourceConfigV1,
    ) -> http_source.HttpDocumentSourceFetchV1:
        return http_source.HttpDocumentSourceFetchV1(
            source_url=config.url,
            filename="report.txt",
            mime_type="text/plain",
            content=b"http hello",
        )

    def fake_fetch_s3_document_source(
        *,
        config: s3_source.S3DocumentSourceConfigV1,
        client: s3_source.S3DocumentSourceClientProtocol | None = None,
    ) -> s3_source.S3DocumentSourceFetchV1:
        del client
        return s3_source.S3DocumentSourceFetchV1(
            bucket=config.bucket,
            key=config.key,
            version_id=config.version_id,
            etag='"etag-1"',
            filename="report.txt",
            mime_type="text/plain",
            content=b"s3 hello",
        )

    def fake_fetch_git_document_source(
        *,
        config: git_source.GitDocumentSourceConfigV1,
    ) -> git_source.GitDocumentSourceFetchV1:
        return git_source.GitDocumentSourceFetchV1(
            repo_root=config.repo_root,
            commit=config.commit,
            resolved_commit="abcdef1234567890",
            relative_path=config.relative_path,
            blob_sha="blob1234567890",
            filename="report.txt",
            mime_type="text/plain",
            content=b"git hello",
        )

    def fake_fetch_google_drive_document_source(
        *,
        config: google_drive_source.GoogleDriveDocumentSourceConfigV1,
    ) -> google_drive_source.GoogleDriveDocumentSourceFetchV1:
        return google_drive_source.GoogleDriveDocumentSourceFetchV1(
            file_id=config.file_id,
            drive_id=config.drive_id,
            acquisition_mode=config.acquisition_mode,
            export_mime_type=config.export_mime_type,
            etag='"etag-2"',
            filename="report.pdf",
            mime_type="application/pdf",
            content=b"drive hello",
        )

    def fake_fetch_confluence_document_source(
        *,
        config: confluence_source.ConfluenceDocumentSourceConfigV1,
    ) -> confluence_source.ConfluenceDocumentSourceFetchV1:
        return confluence_source.ConfluenceDocumentSourceFetchV1(
            site_url=config.site_url,
            page_id=config.page_id,
            space_key=config.space_key,
            requested_page_version=config.page_version,
            resolved_page_version=7,
            page_title="Product Spec",
            filename="product-spec.html",
            mime_type="text/html",
            body_format="storage",
            content=b"<p>confluence hello</p>",
        )

    original_http_auto_partition = http_source.auto_partition
    original_s3_auto_partition = s3_source.auto_partition
    original_git_auto_partition = git_source.auto_partition
    original_google_drive_auto_partition = google_drive_source.auto_partition
    original_confluence_auto_partition = confluence_source.auto_partition
    original_fetch_http_document_source = http_source.fetch_http_document_source
    original_fetch_s3_document_source = s3_source.fetch_s3_document_source
    original_fetch_git_document_source = git_source.fetch_git_document_source
    original_fetch_google_drive_document_source = (
        google_drive_source.fetch_google_drive_document_source
    )
    original_fetch_confluence_document_source = confluence_source.fetch_confluence_document_source

    http_source.auto_partition = _fake_auto_partition
    s3_source.auto_partition = _fake_auto_partition
    git_source.auto_partition = _fake_auto_partition
    google_drive_source.auto_partition = _fake_auto_partition
    confluence_source.auto_partition = _fake_auto_partition
    http_source.fetch_http_document_source = fake_fetch_http_document_source
    s3_source.fetch_s3_document_source = fake_fetch_s3_document_source
    git_source.fetch_git_document_source = fake_fetch_git_document_source
    google_drive_source.fetch_google_drive_document_source = fake_fetch_google_drive_document_source
    confluence_source.fetch_confluence_document_source = fake_fetch_confluence_document_source
    try:
        results = {
            source_name: process_file(
                filename=str(spec_path),
                sha256=f"task-sha-{source_name}",
            )
            for source_name, spec_path in specs.items()
        }
    finally:
        http_source.auto_partition = original_http_auto_partition
        s3_source.auto_partition = original_s3_auto_partition
        git_source.auto_partition = original_git_auto_partition
        google_drive_source.auto_partition = original_google_drive_auto_partition
        confluence_source.auto_partition = original_confluence_auto_partition
        http_source.fetch_http_document_source = original_fetch_http_document_source
        s3_source.fetch_s3_document_source = original_fetch_s3_document_source
        git_source.fetch_git_document_source = original_fetch_git_document_source
        google_drive_source.fetch_google_drive_document_source = (
            original_fetch_google_drive_document_source
        )
        confluence_source.fetch_confluence_document_source = (
            original_fetch_confluence_document_source
        )

    metadata_by_source = {
        source_name: results[source_name].elements[0].metadata for source_name in results
    }

    for source_name, result in results.items():
        metadata = metadata_by_source[source_name]
        assert result.document.sha256 == f"task-sha-{source_name}"
        assert (
            metadata["source_kind"]
            == json.loads(specs[source_name].read_text(encoding="utf-8"))["source"]["kind"]
        )
        assert metadata["fetched_filename"] == result.document.filename
        assert metadata["fetched_mime_type"] == result.document.mime_type
        assert "access_token" not in metadata
        assert "secret_key" not in metadata
        assert "session_token" not in metadata
        assert "headers" not in metadata
        assert "content" not in metadata
        assert "response_text" not in metadata
        assert "cursor" not in metadata
        assert "progress_kind" not in metadata
        assert "checkpoint_identity_key" not in metadata
        assert "parent_checkpoint_identity_key" not in metadata

    http_metadata = metadata_by_source["http"]
    assert http_metadata["source_url"] == "https://example.test/docs/report.txt"
    assert "source_bucket" not in http_metadata
    assert "source_relative_path" not in http_metadata
    assert "source_file_id" not in http_metadata
    assert "source_page_id" not in http_metadata

    s3_metadata = metadata_by_source["s3"]
    assert s3_metadata["source_bucket"] == "docs-bucket"
    assert s3_metadata["source_key"] == "reports/report.txt"
    assert s3_metadata["source_version_id"] == "v1"
    assert s3_metadata["source_etag"] == '"etag-1"'
    assert "source_url" not in s3_metadata
    assert "source_relative_path" not in s3_metadata
    assert "source_file_id" not in s3_metadata
    assert "source_page_id" not in s3_metadata

    git_metadata = metadata_by_source["git"]
    assert git_metadata["source_commit"] == "abcdef1234567890"
    assert git_metadata["source_requested_commit"] == "abc1234"
    assert git_metadata["source_relative_path"] == "docs/report.txt"
    assert git_metadata["source_blob_sha"] == "blob1234567890"
    assert "source_bucket" not in git_metadata
    assert "source_file_id" not in git_metadata
    assert "source_page_id" not in git_metadata

    drive_metadata = metadata_by_source["google_drive"]
    assert drive_metadata["source_file_id"] == "file-123"
    assert drive_metadata["source_drive_id"] == "drive-1"
    assert drive_metadata["source_acquisition_mode"] == "export"
    assert drive_metadata["source_export_mime_type"] == "application/pdf"
    assert drive_metadata["source_etag"] == '"etag-2"'
    assert "source_bucket" not in drive_metadata
    assert "source_relative_path" not in drive_metadata
    assert "source_page_id" not in drive_metadata

    confluence_metadata = metadata_by_source["confluence"]
    assert confluence_metadata["source_site_url"] == "https://example.atlassian.net"
    assert confluence_metadata["source_page_id"] == "12345"
    assert confluence_metadata["source_space_key"] == "ENG"
    assert confluence_metadata["source_requested_page_version"] == 7
    assert confluence_metadata["source_page_version"] == 7
    assert confluence_metadata["source_body_format"] == "storage"
    assert confluence_metadata["source_page_title"] == "Product Spec"
    assert "source_bucket" not in confluence_metadata
    assert "source_relative_path" not in confluence_metadata
    assert "source_file_id" not in confluence_metadata
