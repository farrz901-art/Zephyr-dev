from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from uns_stream.sources import git_source
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)


def test_normalize_uns_input_identity_sha_is_stable_for_git_source_specs(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    spec_a = tmp_path / "source-a.json"
    spec_b = tmp_path / "source-b.json"

    spec_a.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": str(repo_root),
                    "commit": "abc1234",
                    "relative_path": "docs\\report.txt",
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
                    "relative_path": "docs/report.txt",
                    "commit": "abc1234",
                    "repo_root": str(repo_root),
                    "kind": "git_document_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    assert git_source.normalize_uns_input_identity_sha(
        filename=str(spec_a),
        default_sha="fallback-a",
    ) == git_source.normalize_uns_input_identity_sha(
        filename=str(spec_b),
        default_sha="fallback-b",
    )


def test_load_git_document_source_config_rejects_parent_segments() -> None:
    with pytest.raises(ValueError, match="relative_path"):
        git_source.load_git_document_source_config(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": "repo",
                    "commit": "abc1234",
                    "relative_path": "../secrets.txt",
                }
            }
        )


def test_process_file_fetches_git_document_and_preserves_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    spec_path = tmp_path / "source.json"
    spec_path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": str(repo_root),
                    "commit": "abc1234",
                    "relative_path": "docs/report.txt",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def fake_fetch_git_document_source(
        *,
        config: git_source.GitDocumentSourceConfigV1,
    ) -> git_source.GitDocumentSourceFetchV1:
        assert config.commit == "abc1234"
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
                    text="git hello",
                    metadata={"kind": "text"},
                )
            ],
            normalized_text="git hello",
            warnings=[],
        )

    monkeypatch.setattr(git_source, "fetch_git_document_source", fake_fetch_git_document_source)
    monkeypatch.setattr(git_source, "auto_partition", fake_auto_partition)

    result = git_source.process_file(
        filename=str(spec_path),
        sha256="task-sha",
    )

    assert captured["filename"] == "report.txt"
    assert captured["content"] == "git hello"
    assert result.document.filename == "report.txt"
    assert result.document.mime_type == "text/plain"
    assert result.document.sha256 == "task-sha"
    assert result.elements[0].metadata["source_kind"] == "git_document_v1"
    assert result.elements[0].metadata["source_commit"] == "abcdef1234567890"
    assert result.elements[0].metadata["source_requested_commit"] == "abc1234"
    assert result.elements[0].metadata["source_relative_path"] == "docs/report.txt"
    assert result.elements[0].metadata["source_blob_sha"] == "blob1234567890"
    assert result.elements[0].metadata["fetched_filename"] == "report.txt"
    assert result.elements[0].metadata["fetched_mime_type"] == "text/plain"


def test_fetch_git_document_source_maps_git_error_as_non_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = git_source.GitDocumentSourceConfigV1(
        repo_root="E:/repo",
        commit="abc1234",
        relative_path="docs/report.txt",
    )

    def fake_run_git_command(
        *,
        repo_root: str,
        args: list[str],
    ) -> subprocess.CompletedProcess[bytes]:
        del repo_root
        return subprocess.CompletedProcess(
            args=["git", *args],
            returncode=1,
            stdout=b"",
            stderr=b"fatal: bad object",
        )

    monkeypatch.setattr(git_source, "_run_git_command", fake_run_git_command)

    with pytest.raises(ZephyrError) as exc_info:
        git_source.fetch_git_document_source(config=config)

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details is not None
    assert exc_info.value.details.get("retryable") is False
    assert exc_info.value.details.get("stderr") == "fatal: bad object"
