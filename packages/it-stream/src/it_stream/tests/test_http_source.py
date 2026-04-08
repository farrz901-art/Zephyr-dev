from __future__ import annotations

import json
from email.message import Message
from pathlib import Path
from typing import cast
from urllib.error import HTTPError

import pytest

from it_stream import (
    ItCheckpointIdentityV1,
    ItCheckpointProvenanceV1,
    ItCheckpointResumeCursorContinuationV1,
    ItCheckpointResumeProvenanceV1,
    ItResumeRecoveryError,
    ItTaskIdentityV1,
    dump_it_artifacts,
    load_it_resume_selection,
    normalize_it_checkpoint_identity_key,
    normalize_it_input_identity_sha,
    process_file,
    resume_file,
)
from it_stream.artifacts import load_it_checkpoint
from it_stream.sources import http_source
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def _write_http_source_spec(
    path: Path,
    *,
    url: str,
    stream: str = "customers",
    cursor_param: str = "cursor",
    query: dict[str, str] | None = None,
    reverse_order: bool = False,
) -> None:
    payload = {
        "source": {
            "kind": "http_json_cursor_v1",
            "stream": stream,
            "url": url,
            "cursor_param": cursor_param,
            "query": query or {},
        }
    }
    if reverse_order:
        payload = {
            "source": {
                "query": query or {},
                "cursor_param": cursor_param,
                "url": url,
                "stream": stream,
                "kind": "http_json_cursor_v1",
            }
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_http_source_config_validation_rejects_missing_stream(tmp_path: Path) -> None:
    path = tmp_path / "source.json"
    path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_json_cursor_v1",
                    "url": "https://example.test/api/customers",
                    "cursor_param": "cursor",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ZephyrError) as exc_info:
        process_file(
            filename=str(path),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-http-invalid",
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details == {
        "retryable": False,
        "source_kind": "http_json_cursor_v1",
    }


def test_http_source_identity_sha_is_stable_for_equivalent_specs(tmp_path: Path) -> None:
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    third = tmp_path / "third.json"

    _write_http_source_spec(
        left,
        url="https://example.test/api/customers",
        query={"limit": "2", "region": "us"},
    )
    _write_http_source_spec(
        right,
        url="https://example.test/api/customers",
        query={"region": "us", "limit": "2"},
        reverse_order=True,
    )
    _write_http_source_spec(
        third,
        url="https://example.test/api/customers",
        query={"limit": "5", "region": "us"},
    )

    first = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-left")
    second = normalize_it_input_identity_sha(filename=str(right), default_sha="fallback-right")
    different = normalize_it_input_identity_sha(filename=str(third), default_sha="fallback-third")

    assert first == second
    assert first != different


def test_http_source_builds_zephyr_owned_records_and_checkpoint_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_http_source_spec(path, url="https://example.test/api/customers", query={"limit": "2"})

    def fake_urlopen(request: object, timeout: float = 10.0) -> _FakeResponse:
        del timeout
        full_url = cast("str", getattr(request, "full_url"))
        if "cursor=c-1" in full_url:
            return _FakeResponse(
                {
                    "records": [{"id": 2, "name": "Bob"}],
                    "next_cursor": None,
                }
            )
        return _FakeResponse(
            {
                "records": [{"id": 1, "name": "Ada"}],
                "next_cursor": "c-1",
            }
        )

    monkeypatch.setattr(http_source, "urlopen", fake_urlopen)

    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-http-001",
        size_bytes=path.stat().st_size,
    )

    assert result.engine.name == "it-stream"
    assert result.engine.backend == "http-json-cursor"
    assert [element.metadata["artifact_kind"] for element in result.elements] == [
        "record",
        "record",
        "state",
        "log",
        "log",
        "log",
    ]
    assert result.elements[2].metadata["data"] == {
        "cursor": "c-1",
        "page_number": 1,
        "source_url": "https://example.test/api/customers",
        "record_count": 1,
    }

    out_dir = tmp_path / "artifacts"
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result, pipeline_version="p-http")
    checkpoint_row = json.loads((out_dir / "checkpoint.json").read_text(encoding="utf-8"))
    record_lines = (out_dir / "records.jsonl").read_text(encoding="utf-8").splitlines()
    log_lines = (out_dir / "logs.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(artifacts.records) == 2
    assert checkpoint_row["checkpoints"] == [
        {
            "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                identity=ItCheckpointIdentityV1(
                    task=ItTaskIdentityV1(
                        pipeline_version="p-http",
                        sha256="sha-http-001",
                    ),
                    stream="customers",
                    progress_kind="cursor_v1",
                    progress={
                        "cursor": "c-1",
                        "page_number": 1,
                        "source_url": "https://example.test/api/customers",
                        "record_count": 1,
                    },
                )
            ),
            "checkpoint_index": 0,
            "parent_checkpoint_identity_key": None,
            "progress_kind": "cursor_v1",
            "progress": {
                "cursor": "c-1",
                "page_number": 1,
                "record_count": 1,
                "source_url": "https://example.test/api/customers",
            },
        }
    ]
    assert checkpoint_row["provenance"] == {
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "resumed_from_checkpoint_identity_key": None,
        "run_origin": "intake",
        "task_identity_key": ('{"kind":"it","pipeline_version":"p-http","sha256":"sha-http-001"}'),
    }
    assert json.loads(record_lines[0]) == {
        "data": {"id": 1, "name": "Ada"},
        "emitted_at": None,
        "record_index": 0,
        "stream": "customers",
    }
    assert json.loads(log_lines[0]) == {
        "level": "INFO",
        "log_index": 0,
        "message": "fetched page=1 records=1 next_cursor=c-1",
    }
    assert json.loads(log_lines[-1]) == {
        "level": "INFO",
        "log_index": 2,
        "message": "source exhausted after page=2",
    }
    assert "records" not in checkpoint_row
    assert "next_cursor" not in checkpoint_row


def test_http_source_real_path_recovery_supports_state_only_resume_and_resumed_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_http_source_spec(path, url="https://example.test/api/customers")

    def fake_urlopen(request: object, timeout: float = 10.0) -> _FakeResponse:
        del timeout
        full_url = cast("str", getattr(request, "full_url"))
        if "cursor=c-2" in full_url:
            return _FakeResponse({"records": [], "next_cursor": None})
        if "cursor=c-1" in full_url:
            return _FakeResponse({"records": [], "next_cursor": "c-2"})
        return _FakeResponse({"records": [], "next_cursor": "c-1"})

    monkeypatch.setattr(http_source, "urlopen", fake_urlopen)

    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-http-resume",
        size_bytes=path.stat().st_size,
    )
    initial_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "initial-artifacts",
        result=initial,
        pipeline_version="p-http-resume",
    )

    assert initial.engine.backend == "http-json-cursor"
    assert [element.metadata["artifact_kind"] for element in initial.elements] == [
        "state",
        "state",
        "log",
        "log",
        "log",
        "log",
    ]

    first_checkpoint, second_checkpoint = initial_artifacts.checkpoint.checkpoints
    assert first_checkpoint.parent_checkpoint_identity_key is None
    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )

    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "initial-artifacts" / "checkpoint.json",
        pipeline_version="p-http-resume",
        sha256="sha-http-resume",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )

    assert selection.selected_checkpoint.mode == "explicit_checkpoint_identity"
    assert selection.selected_checkpoint.checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )
    assert selection.selected_checkpoint.parent_checkpoint_identity_key is None
    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.entry.progress == {
        "cursor": "c-1",
        "page_number": 1,
        "record_count": 0,
        "source_url": "https://example.test/api/customers",
    }
    assert selection.checkpoint.task_identity_key != first_checkpoint.checkpoint_identity_key
    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "c-1"

    resumed = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "initial-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-http-resume",
        strategy=PartitionStrategy.AUTO,
        sha256="sha-http-resume",
        size_bytes=path.stat().st_size,
    )

    assert [element.metadata["artifact_kind"] for element in resumed.elements] == ["state"]
    assert resumed.elements[0].metadata["data"] == {
        "cursor": "c-2",
        "page_number": 2,
        "source_url": "https://example.test/api/customers",
        "record_count": 0,
    }

    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "resumed-artifacts",
        result=resumed,
        pipeline_version="p-http-resume",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-http-resume",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(path=tmp_path / "resumed-artifacts" / "checkpoint.json")

    assert loaded_resumed.provenance == ItCheckpointProvenanceV1(
        task_identity_key=initial_artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(exclusive_after_cursor="c-1"),
        ),
    )
    assert loaded_resumed.checkpoints[0].parent_checkpoint_identity_key is None
    assert loaded_resumed.checkpoints[0].progress_kind == "cursor_v1"
    assert loaded_resumed.checkpoints[0].checkpoint_identity_key != (
        loaded_resumed.task_identity_key
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


def test_http_source_real_path_recovery_blocks_when_records_lack_resume_timestamps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_http_source_spec(path, url="https://example.test/api/customers")

    def fake_urlopen(request: object, timeout: float = 10.0) -> _FakeResponse:
        del timeout
        full_url = cast("str", getattr(request, "full_url"))
        if "cursor=c-1" in full_url:
            return _FakeResponse({"records": [{"id": 2}], "next_cursor": None})
        return _FakeResponse({"records": [{"id": 1}], "next_cursor": "c-1"})

    monkeypatch.setattr(http_source, "urlopen", fake_urlopen)

    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-http-blocked-resume",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "artifacts",
        result=initial,
        pipeline_version="p-http-blocked",
    )
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
        pipeline_version="p-http-blocked",
        sha256="sha-http-blocked-resume",
    )

    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.continuation is not None
    assert selection.continuation.exclusive_after_cursor == "c-1"

    with pytest.raises(ItResumeRecoveryError, match="record.emitted_at") as exc_info:
        resume_file(
            filename=str(path),
            checkpoint_path=tmp_path / "artifacts" / "checkpoint.json",
            pipeline_version="p-http-blocked",
            strategy=PartitionStrategy.AUTO,
            sha256="sha-http-blocked-resume",
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.issue.status == "blocked"
    assert exc_info.value.issue.code == "missing_record_emitted_at"
    assert exc_info.value.issue.checkpoint_identity_key == (
        artifacts.checkpoint.checkpoints[0].checkpoint_identity_key
    )
    assert exc_info.value.issue.progress_kind == "cursor_v1"


@pytest.mark.parametrize(
    ("status_code", "expected_retryable"),
    [
        (503, True),
        (404, False),
    ],
)
def test_http_source_maps_basic_http_failures_to_retryable_semantics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status_code: int,
    expected_retryable: bool,
) -> None:
    path = tmp_path / "source.json"
    _write_http_source_spec(path, url="https://example.test/api/customers")

    def fake_urlopen(request: object, timeout: float = 10.0) -> _FakeResponse:
        del timeout
        raise HTTPError(
            cast("str", getattr(request, "full_url")),
            status_code,
            "boom",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr(http_source, "urlopen", fake_urlopen)

    with pytest.raises(ZephyrError) as exc_info:
        process_file(
            filename=str(path),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-http-err",
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details is not None
    assert exc_info.value.details["retryable"] is expected_retryable
    assert exc_info.value.details["status_code"] == status_code
