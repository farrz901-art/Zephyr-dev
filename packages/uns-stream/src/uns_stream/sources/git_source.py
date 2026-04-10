from __future__ import annotations

import hashlib
import json
import mimetypes
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import cast
from uuid import uuid4

from uns_stream.backends.base import PartitionBackend
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import (
    DocumentMetadata,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)

GIT_DOCUMENT_SOURCE_KIND = "git_document_v1"


@dataclass(frozen=True, slots=True)
class GitDocumentSourceConfigV1:
    repo_root: str
    commit: str
    relative_path: str


@dataclass(frozen=True, slots=True)
class GitDocumentSourceFetchV1:
    repo_root: str
    commit: str
    resolved_commit: str
    relative_path: str
    blob_sha: str
    filename: str
    mime_type: str | None
    content: bytes


def is_git_document_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == GIT_DOCUMENT_SOURCE_KIND


def _normalize_repo_root(value: str, *, base_dir: Path | None = None) -> str:
    raw_path = Path(value)
    if base_dir is not None and not raw_path.is_absolute():
        raw_path = (base_dir / raw_path).resolve()
    else:
        raw_path = raw_path.resolve()
    return str(raw_path)


def _normalize_relative_path(value: str) -> str:
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or normalized.startswith("/"):
        raise ValueError(
            "uns-stream git source field 'source.relative_path' must be a relative path"
        )
    if normalized in {"", "."}:
        raise ValueError(
            "uns-stream git source field 'source.relative_path' must be a non-empty relative path"
        )
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(
            "uns-stream git source field 'source.relative_path' must not contain "
            "'.' or '..' segments"
        )
    return path.as_posix()


def load_git_document_source_config(
    raw: dict[str, object],
    *,
    base_dir: Path | None = None,
) -> GitDocumentSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("uns-stream git source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    def _required_str(field_name: str) -> str:
        value = typed_source.get(field_name)
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"uns-stream git source field 'source.{field_name}' must be a non-empty string"
            )
        return value

    return GitDocumentSourceConfigV1(
        repo_root=_normalize_repo_root(_required_str("repo_root"), base_dir=base_dir),
        commit=_required_str("commit"),
        relative_path=_normalize_relative_path(_required_str("relative_path")),
    )


def normalize_git_document_source_identity_sha(*, config: GitDocumentSourceConfigV1) -> str:
    canonical = json.dumps(
        {
            "kind": GIT_DOCUMENT_SOURCE_KIND,
            "repo_root": config.repo_root,
            "commit": config.commit,
            "relative_path": config.relative_path,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _source_error(
    *,
    message: str,
    retryable: bool,
    details: dict[str, object] | None = None,
) -> ZephyrError:
    merged_details: dict[str, object] = {
        "retryable": retryable,
        "source_kind": GIT_DOCUMENT_SOURCE_KIND,
    }
    if details is not None:
        merged_details.update(details)
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=message,
        details=merged_details,
    )


def _run_git_command(*, repo_root: str, args: list[str]) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", "-C", repo_root, *args],
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise _source_error(
            message="uns-stream git source requires the git executable",
            retryable=False,
            details={"repo_root": repo_root, "exc_type": type(exc).__name__},
        ) from exc
    except OSError as exc:
        raise _source_error(
            message="uns-stream git source command failed to start",
            retryable=False,
            details={
                "repo_root": repo_root,
                "exc_type": type(exc).__name__,
                "exc": str(exc),
            },
        ) from exc


def _run_git_text(
    *,
    repo_root: str,
    args: list[str],
    details: dict[str, object],
    message: str,
) -> str:
    result = _run_git_command(repo_root=repo_root, args=args)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise _source_error(
            message=message,
            retryable=False,
            details={**details, "stderr": stderr, "git_args": list(args)},
        )
    return result.stdout.decode("utf-8", errors="strict").strip()


def _run_git_bytes(
    *,
    repo_root: str,
    args: list[str],
    details: dict[str, object],
    message: str,
) -> bytes:
    result = _run_git_command(repo_root=repo_root, args=args)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise _source_error(
            message=message,
            retryable=False,
            details={**details, "stderr": stderr, "git_args": list(args)},
        )
    return bytes(result.stdout)


def _infer_filename(*, relative_path: str) -> str:
    basename = Path(relative_path).name
    if basename:
        return basename
    return "document"


def _infer_mime_type(*, filename: str) -> str | None:
    mime_type, _ = mimetypes.guess_type(filename)
    if not isinstance(mime_type, str) or not mime_type:
        return None
    return mime_type.lower()


def fetch_git_document_source(*, config: GitDocumentSourceConfigV1) -> GitDocumentSourceFetchV1:
    details: dict[str, object] = {
        "repo_root": config.repo_root,
        "commit": config.commit,
        "relative_path": config.relative_path,
    }
    resolved_commit = _run_git_text(
        repo_root=config.repo_root,
        args=["rev-parse", "--verify", f"{config.commit}^{{commit}}"],
        details=details,
        message="uns-stream git source commit resolution failed",
    )
    blob_sha = _run_git_text(
        repo_root=config.repo_root,
        args=["rev-parse", "--verify", f"{config.commit}:{config.relative_path}"],
        details=details,
        message="uns-stream git source blob resolution failed",
    )
    content = _run_git_bytes(
        repo_root=config.repo_root,
        args=["show", f"{config.commit}:{config.relative_path}"],
        details=details,
        message="uns-stream git source blob read failed",
    )
    filename = _infer_filename(relative_path=config.relative_path)
    return GitDocumentSourceFetchV1(
        repo_root=config.repo_root,
        commit=config.commit,
        resolved_commit=resolved_commit,
        relative_path=config.relative_path,
        blob_sha=blob_sha,
        filename=filename,
        mime_type=_infer_mime_type(filename=filename),
        content=content,
    )


def _load_git_document_source_config_from_path(path: Path) -> GitDocumentSourceConfigV1 | None:
    if path.suffix.lower() != ".json":
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    typed_raw = cast("dict[str, object]", raw)
    if not is_git_document_source_spec(typed_raw):
        return None

    try:
        return load_git_document_source_config(typed_raw, base_dir=path.parent)
    except ValueError as err:
        source_raw = typed_raw.get("source")
        repo_root = None
        commit = None
        relative_path = None
        if isinstance(source_raw, dict):
            typed_source = cast("dict[str, object]", source_raw)
            repo_root_candidate = typed_source.get("repo_root")
            commit_candidate = typed_source.get("commit")
            relative_path_candidate = typed_source.get("relative_path")
            if isinstance(repo_root_candidate, str):
                repo_root = repo_root_candidate
            if isinstance(commit_candidate, str):
                commit = commit_candidate
            if isinstance(relative_path_candidate, str):
                relative_path = relative_path_candidate
        raise _source_error(
            message=str(err),
            retryable=False,
            details={
                "repo_root": repo_root,
                "commit": commit,
                "relative_path": relative_path,
            },
        ) from err


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    config = _load_git_document_source_config_from_path(Path(filename))
    if config is None:
        return default_sha
    return normalize_git_document_source_identity_sha(config=config)


def _with_fetch_provenance(
    *,
    result: PartitionResult,
    fetched: GitDocumentSourceFetchV1,
    sha256: str,
) -> PartitionResult:
    elements: list[ZephyrElement] = []
    for element in result.elements:
        metadata = dict(element.metadata)
        metadata["source_kind"] = GIT_DOCUMENT_SOURCE_KIND
        metadata["source_repo_root"] = fetched.repo_root
        metadata["source_commit"] = fetched.resolved_commit
        metadata["source_requested_commit"] = fetched.commit
        metadata["source_relative_path"] = fetched.relative_path
        metadata["source_blob_sha"] = fetched.blob_sha
        metadata["fetched_filename"] = fetched.filename
        if fetched.mime_type is not None:
            metadata["fetched_mime_type"] = fetched.mime_type
        elements.append(
            ZephyrElement(
                element_id=element.element_id,
                type=element.type,
                text=element.text,
                metadata=metadata,
            )
        )

    return PartitionResult(
        document=DocumentMetadata(
            filename=fetched.filename,
            mime_type=fetched.mime_type,
            sha256=sha256,
            size_bytes=len(fetched.content),
            created_at_utc=result.document.created_at_utc,
        ),
        engine=result.engine,
        elements=elements,
        normalized_text=result.normalized_text,
        warnings=list(result.warnings),
    )


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
    path = Path(filename)
    config = _load_git_document_source_config_from_path(path)
    if config is None:
        return auto_partition(
            filename=filename,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=size_bytes,
        )

    fetched = fetch_git_document_source(config=config)
    tmp_dir = path.parent / f".zephyr-uns-git-{uuid4().hex}"
    tmp_dir.mkdir(parents=False, exist_ok=False)
    try:
        fetched_path = tmp_dir / fetched.filename
        fetched_path.write_bytes(fetched.content)
        result = auto_partition(
            filename=str(fetched_path),
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=len(fetched.content),
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    resolved_sha = sha256 if sha256 is not None else result.document.sha256
    return _with_fetch_provenance(result=result, fetched=fetched, sha256=resolved_sha)
