from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from uns_stream.backends.base import PartitionBackend
from uns_stream.sources import (
    confluence_source,
    git_source,
    google_drive_source,
    http_source,
    s3_source,
)
from zephyr_core import PartitionResult, PartitionStrategy


def _load_source_kind(*, filename: str) -> str | None:
    path = Path(filename)
    if path.suffix.lower() != ".json":
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    source = cast("dict[str, object]", raw).get("source")
    if not isinstance(source, dict):
        return None
    kind = cast("dict[str, object]", source).get("kind")
    if not isinstance(kind, str) or not kind:
        return None
    return kind


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    source_kind = _load_source_kind(filename=filename)
    if source_kind == http_source.HTTP_DOCUMENT_SOURCE_KIND:
        return http_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind == s3_source.S3_DOCUMENT_SOURCE_KIND:
        return s3_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind == git_source.GIT_DOCUMENT_SOURCE_KIND:
        return git_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind == google_drive_source.GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND:
        return google_drive_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind == confluence_source.CONFLUENCE_DOCUMENT_SOURCE_KIND:
        return confluence_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    return default_sha


def process_file(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> PartitionResult:
    source_kind = _load_source_kind(filename=filename)
    if source_kind == s3_source.S3_DOCUMENT_SOURCE_KIND:
        return s3_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind == git_source.GIT_DOCUMENT_SOURCE_KIND:
        return git_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind == google_drive_source.GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND:
        return google_drive_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind == confluence_source.CONFLUENCE_DOCUMENT_SOURCE_KIND:
        return confluence_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    return http_source.process_file(
        filename=filename,
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
        run_id=run_id,
        pipeline_version=pipeline_version,
        sha256=sha256,
        size_bytes=size_bytes,
    )


__all__ = [
    "confluence_source",
    "git_source",
    "google_drive_source",
    "http_source",
    "normalize_uns_input_identity_sha",
    "process_file",
    "s3_source",
]
