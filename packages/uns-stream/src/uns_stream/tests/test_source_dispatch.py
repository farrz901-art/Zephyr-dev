from __future__ import annotations

import json
from pathlib import Path
from typing import Callable
from unittest.mock import patch

from uns_stream.sources import (
    SUPPORTED_SOURCE_KINDS,
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
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    encoded = content.encode("utf-8")
    return PartitionResult(
        document=DocumentMetadata(
            filename=fetched_path.name,
            mime_type="text/plain",
            sha256=sha256 or "sha-test",
            size_bytes=size_bytes or len(encoded),
            created_at_utc="2026-04-20T00:00:00Z",
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


def _assert_dispatch_error(
    path: Path,
    *,
    expected_problem: str,
    expected_kind: str = "unknown",
) -> None:
    actions: tuple[Callable[[], object], ...] = (
        lambda: normalize_uns_input_identity_sha(filename=str(path), default_sha="fallback"),
        lambda: process_file(filename=str(path)),
    )
    for action in actions:
        dispatch_error: ZephyrError | None = None
        try:
            action()
        except ZephyrError as error:
            dispatch_error = error
        else:
            raise AssertionError("expected uns-stream source dispatch to fail closed")
        assert dispatch_error is not None
        assert dispatch_error.code == ErrorCode.IO_READ_FAILED
        details = dispatch_error.details
        assert details is not None
        assert details["retryable"] is False
        assert details["source_dispatch_problem"] == expected_problem
        assert details["source_kind"] == expected_kind
        assert "supported_source_kinds" in details


def test_uns_source_dispatch_rejects_malformed_json_fail_closed(tmp_path: Path) -> None:
    spec = tmp_path / "malformed-source.json"
    spec.write_text('{"source": ', encoding="utf-8")

    _assert_dispatch_error(spec, expected_problem="malformed_json_source_spec")


def test_uns_source_dispatch_rejects_missing_or_empty_kind_fail_closed(tmp_path: Path) -> None:
    missing_source = tmp_path / "missing-source.json"
    missing_kind = tmp_path / "missing-kind.json"
    empty_kind = tmp_path / "empty-kind.json"
    _write_json(missing_source, {"title": "not a source spec"})
    _write_json(missing_kind, {"source": {}})
    _write_json(empty_kind, {"source": {"kind": ""}})

    _assert_dispatch_error(
        missing_source,
        expected_problem="source_field_missing_or_not_object",
    )
    _assert_dispatch_error(missing_kind, expected_problem="source_kind_missing_or_empty")
    _assert_dispatch_error(empty_kind, expected_problem="source_kind_missing_or_empty")


def test_uns_source_dispatch_rejects_unknown_kind_fail_closed(tmp_path: Path) -> None:
    spec = tmp_path / "unknown-kind.json"
    _write_json(spec, {"source": {"kind": "foo_document_v1", "url": "https://example.test"}})

    _assert_dispatch_error(
        spec,
        expected_problem="unsupported_source_kind",
        expected_kind="foo_document_v1",
    )


def test_uns_source_dispatch_keeps_http_as_explicit_lane_not_default_sink(
    tmp_path: Path,
) -> None:
    spec = tmp_path / "http-source.json"
    _write_json(
        spec,
        {
            "source": {
                "kind": "http_document_v1",
                "url": "https://example.test/docs/report.txt",
                "accept": "text/plain",
            }
        },
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

    with (
        patch.object(http_source, "fetch_http_document_source", fake_fetch_http_document_source),
        patch.object(http_source, "auto_partition", _fake_auto_partition),
    ):
        identity = normalize_uns_input_identity_sha(filename=str(spec), default_sha="fallback")
        result = process_file(filename=str(spec), sha256="sha-http")

    assert identity != "fallback"
    assert result.document.sha256 == "sha-http"
    assert result.elements[0].metadata["source_kind"] == "http_document_v1"
    assert result.elements[0].metadata["source_url"] == "https://example.test/docs/report.txt"


def test_uns_source_dispatch_supported_kind_set_matches_current_bounded_surface() -> None:
    assert SUPPORTED_SOURCE_KINDS == (
        http_source.HTTP_DOCUMENT_SOURCE_KIND,
        s3_source.S3_DOCUMENT_SOURCE_KIND,
        git_source.GIT_DOCUMENT_SOURCE_KIND,
        google_drive_source.GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND,
        confluence_source.CONFLUENCE_DOCUMENT_SOURCE_KIND,
    )
