from __future__ import annotations

import json
from pathlib import Path

import pytest

from uns_stream.sources import normalize_uns_input_identity_sha, process_file
from uns_stream.tests._p45_source_wave3_helpers import (
    assert_no_secret_fields,
    init_git_text_repo,
    write_git_source_spec,
)
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


@pytest.mark.auth_local_real
def test_p45_m55_git_document_local_real_success_identity_and_partition_path(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    commit = init_git_text_repo(
        repo_root,
        relative_path="docs/report.txt",
        content="Zephyr P4.5 git fixture document.\n",
    )

    left = tmp_path / "git-left.json"
    right = tmp_path / "git-right.json"
    third = tmp_path / "git-third.json"
    write_git_source_spec(
        left,
        repo_root=str(repo_root),
        commit=commit,
        relative_path="docs/report.txt",
    )
    right.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "git_document_v1",
                    "repo_root": "repo",
                    "commit": commit,
                    "relative_path": "docs\\report.txt",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_git_source_spec(
        third,
        repo_root=str(repo_root),
        commit=commit,
        relative_path="docs/other.txt",
    )

    first_sha = normalize_uns_input_identity_sha(filename=str(left), default_sha="git-left")
    second_sha = normalize_uns_input_identity_sha(filename=str(right), default_sha="git-right")
    different_sha = normalize_uns_input_identity_sha(filename=str(third), default_sha="git-third")

    assert first_sha == second_sha
    assert first_sha != different_sha

    result = process_file(
        filename=str(left),
        strategy=PartitionStrategy.AUTO,
        sha256=first_sha,
    )

    assert result.document.filename == "report.txt"
    assert result.document.mime_type == "text/plain"
    assert result.document.sha256 == first_sha
    assert result.engine.name == "unstructured"
    assert result.engine.backend == "local"
    assert len(result.elements) > 0
    assert "Zephyr P4.5 git fixture document." in result.normalized_text
    for element in result.elements:
        assert element.metadata["source_kind"] == "git_document_v1"
        assert element.metadata["source_repo_root"] == str(repo_root.resolve())
        assert element.metadata["source_requested_commit"] == commit
        assert element.metadata["source_commit"] == commit
        assert element.metadata["source_relative_path"] == "docs/report.txt"
        assert isinstance(element.metadata["source_blob_sha"], str)
        assert element.metadata["fetched_filename"] == "report.txt"
        assert element.metadata["fetched_mime_type"] == "text/plain"
        assert_no_secret_fields(element.metadata)

    missing_path_spec = tmp_path / "git-missing-path.json"
    write_git_source_spec(
        missing_path_spec,
        repo_root=str(repo_root),
        commit=commit,
        relative_path="docs/missing.txt",
    )
    with pytest.raises(ZephyrError) as missing_path_exc:
        process_file(
            filename=str(missing_path_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="git-missing-path",
        )

    assert missing_path_exc.value.code == ErrorCode.IO_READ_FAILED
    missing_path_details = missing_path_exc.value.details
    assert missing_path_details is not None
    assert missing_path_details["retryable"] is False
    assert missing_path_details["source_kind"] == "git_document_v1"
    assert missing_path_details["repo_root"] == str(repo_root.resolve())
    assert missing_path_details["relative_path"] == "docs/missing.txt"

    missing_commit_spec = tmp_path / "git-missing-commit.json"
    write_git_source_spec(
        missing_commit_spec,
        repo_root=str(repo_root),
        commit="deadbeef",
        relative_path="docs/report.txt",
    )
    with pytest.raises(ZephyrError) as missing_commit_exc:
        process_file(
            filename=str(missing_commit_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="git-missing-commit",
        )

    assert missing_commit_exc.value.code == ErrorCode.IO_READ_FAILED
    missing_commit_details = missing_commit_exc.value.details
    assert missing_commit_details is not None
    assert missing_commit_details["retryable"] is False
    assert missing_commit_details["source_kind"] == "git_document_v1"
    assert missing_commit_details["commit"] == "deadbeef"
