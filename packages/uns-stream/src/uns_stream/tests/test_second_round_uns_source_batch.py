from __future__ import annotations

import json
from pathlib import Path

from uns_stream.sources import normalize_uns_input_identity_sha


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
