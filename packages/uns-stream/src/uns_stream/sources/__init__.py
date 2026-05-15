from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Mapping, cast

from uns_stream.backends.base import PartitionBackend
from uns_stream.sources import (
    confluence_source,
    git_source,
    google_drive_source,
    http_source,
    s3_source,
)
from zephyr_core import ErrorCode, PartitionResult, PartitionStrategy, ZephyrError

SUPPORTED_SOURCE_KINDS: Final[tuple[str, ...]] = (
    http_source.HTTP_DOCUMENT_SOURCE_KIND,
    s3_source.S3_DOCUMENT_SOURCE_KIND,
    git_source.GIT_DOCUMENT_SOURCE_KIND,
    google_drive_source.GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND,
    confluence_source.CONFLUENCE_DOCUMENT_SOURCE_KIND,
)
_SUPPORTED_SOURCE_KINDS = frozenset(SUPPORTED_SOURCE_KINDS)


@dataclass(frozen=True, slots=True)
class _SourceKindResolution:
    kind: str | None
    is_source_spec_candidate: bool
    problem: str | None = None


def _unsupported_source_error(
    *,
    filename: str,
    problem: str,
    kind: str | None = None,
) -> ZephyrError:
    details: dict[str, object] = {
        "retryable": False,
        "filename": Path(filename).name,
        "source_kind": "unknown" if kind is None else kind,
        "source_dispatch_problem": problem,
        "supported_source_kinds": sorted(_SUPPORTED_SOURCE_KINDS),
    }
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=f"uns-stream source dispatch rejected input: {problem}",
        details=details,
    )


def _load_source_kind(*, filename: str) -> _SourceKindResolution:
    path = Path(filename)
    if path.suffix.lower() != ".json":
        return _SourceKindResolution(kind=None, is_source_spec_candidate=False)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _SourceKindResolution(
            kind=None,
            is_source_spec_candidate=True,
            problem="malformed_json_source_spec",
        )
    if not isinstance(raw, dict):
        return _SourceKindResolution(
            kind=None,
            is_source_spec_candidate=True,
            problem="top_level_source_spec_must_be_object",
        )
    source = cast("dict[str, object]", raw).get("source")
    if not isinstance(source, dict):
        return _SourceKindResolution(
            kind=None,
            is_source_spec_candidate=True,
            problem="source_field_missing_or_not_object",
        )
    kind = cast("dict[str, object]", source).get("kind")
    if not isinstance(kind, str) or not kind:
        return _SourceKindResolution(
            kind=None,
            is_source_spec_candidate=True,
            problem="source_kind_missing_or_empty",
        )
    if kind not in _SUPPORTED_SOURCE_KINDS:
        return _SourceKindResolution(
            kind=kind,
            is_source_spec_candidate=True,
            problem="unsupported_source_kind",
        )
    return _SourceKindResolution(kind=kind, is_source_spec_candidate=True)


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    source_kind = _load_source_kind(filename=filename)
    if source_kind.problem is not None:
        raise _unsupported_source_error(
            filename=filename,
            kind=source_kind.kind,
            problem=source_kind.problem,
        )
    if not source_kind.is_source_spec_candidate:
        return default_sha
    if source_kind.kind == http_source.HTTP_DOCUMENT_SOURCE_KIND:
        return http_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind.kind == s3_source.S3_DOCUMENT_SOURCE_KIND:
        return s3_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind.kind == git_source.GIT_DOCUMENT_SOURCE_KIND:
        return git_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind.kind == google_drive_source.GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND:
        return google_drive_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    if source_kind.kind == confluence_source.CONFLUENCE_DOCUMENT_SOURCE_KIND:
        return confluence_source.normalize_uns_input_identity_sha(
            filename=filename,
            default_sha=default_sha,
        )
    raise AssertionError(f"Unhandled supported uns-stream source kind: {source_kind.kind}")


def process_file(
    *,
    filename: str,
    strategy: PartitionStrategy | None = None,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    profile: str | None = None,
    languages: list[str] | None = None,
    detect_language_per_element: bool | None = None,
    language_fallback: object | None = None,
    skip_infer_table_types: list[str] | None = None,
    infer_table_structure: bool | None = None,
    pdf_infer_table_structure: bool | None = None,
    extract_image_block_types: list[str] | None = None,
    extract_image_block_output_dir: str | None = None,
    extract_image_block_to_payload: bool | None = None,
    data_source_metadata: object | None = None,
    metadata_filename: str | None = None,
    hi_res_model_name: str | None = None,
    model_name: str | None = None,
    starting_page_number: int | None = None,
    extra_partition_kwargs: Mapping[str, object] | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> PartitionResult:
    source_kind = _load_source_kind(filename=filename)
    if source_kind.problem is not None:
        raise _unsupported_source_error(
            filename=filename,
            kind=source_kind.kind,
            problem=source_kind.problem,
        )
    if not source_kind.is_source_spec_candidate:
        return http_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            profile=profile,
            languages=languages,
            detect_language_per_element=detect_language_per_element,
            language_fallback=language_fallback,
            skip_infer_table_types=skip_infer_table_types,
            infer_table_structure=infer_table_structure,
            pdf_infer_table_structure=pdf_infer_table_structure,
            extract_image_block_types=extract_image_block_types,
            extract_image_block_output_dir=extract_image_block_output_dir,
            extract_image_block_to_payload=extract_image_block_to_payload,
            data_source_metadata=data_source_metadata,
            metadata_filename=metadata_filename,
            hi_res_model_name=hi_res_model_name,
            model_name=model_name,
            starting_page_number=starting_page_number,
            extra_partition_kwargs=extra_partition_kwargs,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind.kind == http_source.HTTP_DOCUMENT_SOURCE_KIND:
        return http_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            profile=profile,
            languages=languages,
            detect_language_per_element=detect_language_per_element,
            language_fallback=language_fallback,
            skip_infer_table_types=skip_infer_table_types,
            infer_table_structure=infer_table_structure,
            pdf_infer_table_structure=pdf_infer_table_structure,
            extract_image_block_types=extract_image_block_types,
            extract_image_block_output_dir=extract_image_block_output_dir,
            extract_image_block_to_payload=extract_image_block_to_payload,
            data_source_metadata=data_source_metadata,
            metadata_filename=metadata_filename,
            hi_res_model_name=hi_res_model_name,
            model_name=model_name,
            starting_page_number=starting_page_number,
            extra_partition_kwargs=extra_partition_kwargs,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind.kind == s3_source.S3_DOCUMENT_SOURCE_KIND:
        return s3_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            profile=profile,
            languages=languages,
            detect_language_per_element=detect_language_per_element,
            language_fallback=language_fallback,
            skip_infer_table_types=skip_infer_table_types,
            infer_table_structure=infer_table_structure,
            pdf_infer_table_structure=pdf_infer_table_structure,
            extract_image_block_types=extract_image_block_types,
            extract_image_block_output_dir=extract_image_block_output_dir,
            extract_image_block_to_payload=extract_image_block_to_payload,
            data_source_metadata=data_source_metadata,
            metadata_filename=metadata_filename,
            hi_res_model_name=hi_res_model_name,
            model_name=model_name,
            starting_page_number=starting_page_number,
            extra_partition_kwargs=extra_partition_kwargs,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind.kind == git_source.GIT_DOCUMENT_SOURCE_KIND:
        return git_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            profile=profile,
            languages=languages,
            detect_language_per_element=detect_language_per_element,
            language_fallback=language_fallback,
            skip_infer_table_types=skip_infer_table_types,
            infer_table_structure=infer_table_structure,
            pdf_infer_table_structure=pdf_infer_table_structure,
            extract_image_block_types=extract_image_block_types,
            extract_image_block_output_dir=extract_image_block_output_dir,
            extract_image_block_to_payload=extract_image_block_to_payload,
            data_source_metadata=data_source_metadata,
            metadata_filename=metadata_filename,
            hi_res_model_name=hi_res_model_name,
            model_name=model_name,
            starting_page_number=starting_page_number,
            extra_partition_kwargs=extra_partition_kwargs,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind.kind == google_drive_source.GOOGLE_DRIVE_DOCUMENT_SOURCE_KIND:
        return google_drive_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            profile=profile,
            languages=languages,
            detect_language_per_element=detect_language_per_element,
            language_fallback=language_fallback,
            skip_infer_table_types=skip_infer_table_types,
            infer_table_structure=infer_table_structure,
            pdf_infer_table_structure=pdf_infer_table_structure,
            extract_image_block_types=extract_image_block_types,
            extract_image_block_output_dir=extract_image_block_output_dir,
            extract_image_block_to_payload=extract_image_block_to_payload,
            data_source_metadata=data_source_metadata,
            metadata_filename=metadata_filename,
            hi_res_model_name=hi_res_model_name,
            model_name=model_name,
            starting_page_number=starting_page_number,
            extra_partition_kwargs=extra_partition_kwargs,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    if source_kind.kind == confluence_source.CONFLUENCE_DOCUMENT_SOURCE_KIND:
        return confluence_source.process_file(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            profile=profile,
            languages=languages,
            detect_language_per_element=detect_language_per_element,
            language_fallback=language_fallback,
            skip_infer_table_types=skip_infer_table_types,
            infer_table_structure=infer_table_structure,
            pdf_infer_table_structure=pdf_infer_table_structure,
            extract_image_block_types=extract_image_block_types,
            extract_image_block_output_dir=extract_image_block_output_dir,
            extract_image_block_to_payload=extract_image_block_to_payload,
            data_source_metadata=data_source_metadata,
            metadata_filename=metadata_filename,
            hi_res_model_name=hi_res_model_name,
            model_name=model_name,
            starting_page_number=starting_page_number,
            extra_partition_kwargs=extra_partition_kwargs,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    raise AssertionError(f"Unhandled supported uns-stream source kind: {source_kind.kind}")


__all__ = [
    "confluence_source",
    "git_source",
    "google_drive_source",
    "http_source",
    "normalize_uns_input_identity_sha",
    "process_file",
    "s3_source",
    "SUPPORTED_SOURCE_KINDS",
]
