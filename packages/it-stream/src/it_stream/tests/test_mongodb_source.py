from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from it_stream import (
    ItCheckpointProvenanceV1,
    ItCheckpointResumeCursorContinuationV1,
    ItCheckpointResumeProvenanceV1,
    dump_it_artifacts,
    load_it_resume_selection,
    normalize_it_input_identity_sha,
    process_file,
    resume_file,
)
from it_stream.artifacts import load_it_checkpoint
from it_stream.sources import mongodb_source
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


class _FakeMongoCollectionHandle:
    def __init__(self, pages: dict[str | None, list[dict[str, object]]]) -> None:
        self._pages = pages

    def find(
        self,
        filter: dict[str, object],
        projection: dict[str, bool],
        sort: list[tuple[str, int]],
        limit: int,
    ) -> list[dict[str, object]]:
        del projection
        del sort
        after_cursor: str | None = None
        raw_cursor = filter.get("doc_cursor")
        if isinstance(raw_cursor, dict):
            typed_cursor = cast("dict[str, object]", raw_cursor)
            candidate = typed_cursor.get("$gt")
            if isinstance(candidate, str):
                after_cursor = candidate
        return list(self._pages.get(after_cursor, []))[:limit]

    def close(self) -> None:
        return None


def _write_mongodb_source_spec(
    path: Path,
    *,
    uri: str = "mongodb://reader:secret@mongo.test:27017/?replicaSet=rs0",
    reverse_order: bool = False,
) -> None:
    payload = {
        "source": {
            "kind": "mongodb_incremental_v1",
            "stream": "customer_docs",
            "connection_name": "mongo-primary",
            "uri": uri,
            "database": "analytics",
            "collection": "customer_docs",
            "fields": ["doc_id", "doc_cursor", "segment"],
            "cursor_field": "doc_cursor",
            "cursor_start": "cust-0000",
            "batch_size": 2,
        }
    }
    if reverse_order:
        payload = {
            "source": {
                "batch_size": 2,
                "cursor_start": "cust-0000",
                "cursor_field": "doc_cursor",
                "fields": ["doc_id", "doc_cursor", "segment"],
                "collection": "customer_docs",
                "database": "analytics",
                "uri": uri,
                "connection_name": "mongo-primary",
                "stream": "customer_docs",
                "kind": "mongodb_incremental_v1",
            }
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_mongodb_source_identity_sha_is_stable_for_equivalent_specs(tmp_path: Path) -> None:
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    third = tmp_path / "third.json"

    _write_mongodb_source_spec(left)
    _write_mongodb_source_spec(
        right,
        uri="mongodb://reader:rotated@mongo.test:27017/?replicaSet=rs0",
        reverse_order=True,
    )
    _write_mongodb_source_spec(third)
    third_payload = json.loads(third.read_text(encoding="utf-8"))
    third_payload["source"]["cursor_start"] = "cust-0099"
    third.write_text(json.dumps(third_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    first = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-left")
    second = normalize_it_input_identity_sha(filename=str(right), default_sha="fallback-right")
    different = normalize_it_input_identity_sha(filename=str(third), default_sha="fallback-third")

    assert first == second
    assert first != different


def test_mongodb_source_builds_cursor_checkpoints_and_resume_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_mongodb_source_spec(path)

    pages: dict[str | None, list[dict[str, object]]] = {
        "cust-0000": [
            {"doc_id": "cust-1", "doc_cursor": "cust-0001", "segment": "gold"},
            {"doc_id": "cust-2", "doc_cursor": "cust-0002", "segment": "silver"},
        ],
        "cust-0002": [{"doc_id": "cust-3", "doc_cursor": "cust-0003", "segment": "bronze"}],
        "cust-0003": [],
    }

    def fake_connect(
        *,
        config: mongodb_source.MongoDBIncrementalSourceConfigV1,
    ) -> _FakeMongoCollectionHandle:
        assert config.connection_name == "mongo-primary"
        assert config.database == "analytics"
        assert config.collection == "customer_docs"
        return _FakeMongoCollectionHandle(pages)

    monkeypatch.setattr(mongodb_source, "_connect_mongodb_incremental_source", fake_connect)

    identity_sha = normalize_it_input_identity_sha(filename=str(path), default_sha="fallback")
    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert result.engine.name == "it-stream"
    assert result.engine.backend == "mongodb-incremental"
    assert [element.metadata["artifact_kind"] for element in result.elements] == [
        "record",
        "record",
        "record",
        "state",
        "state",
        "log",
        "log",
        "log",
    ]
    assert result.elements[0].metadata["emitted_at"] == "cust-0001"
    assert result.elements[3].metadata["data"] == {
        "cursor": "cust-0002",
        "connection_name": "mongo-primary",
        "database": "analytics",
        "collection": "customer_docs",
        "cursor_field": "doc_cursor",
        "fields": ["doc_id", "doc_cursor", "segment"],
        "read_direction": "asc",
        "document_count": 2,
    }

    out_dir = tmp_path / "artifacts"
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result, pipeline_version="p-mongo")
    first_checkpoint, second_checkpoint = artifacts.checkpoint.checkpoints

    assert first_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )

    selection = load_it_resume_selection(
        checkpoint_path=out_dir / "checkpoint.json",
        pipeline_version="p-mongo",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )

    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "cust-0002"

    resumed = resume_file(
        filename=str(path),
        checkpoint_path=out_dir / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-mongo",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert [element.metadata["artifact_kind"] for element in resumed.elements] == [
        "record",
        "state",
    ]
    assert resumed.elements[0].metadata["data"] == {
        "doc_id": "cust-3",
        "doc_cursor": "cust-0003",
        "segment": "bronze",
    }
    assert resumed.elements[0].metadata["emitted_at"] == "cust-0003"
    assert resumed.elements[1].metadata["data"]["cursor"] == "cust-0003"

    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "resumed-artifacts",
        result=resumed,
        pipeline_version="p-mongo",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-mongo-resume",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(path=tmp_path / "resumed-artifacts" / "checkpoint.json")

    assert loaded_resumed.provenance == ItCheckpointProvenanceV1(
        task_identity_key=artifacts.checkpoint.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(exclusive_after_cursor="cust-0002"),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


def test_mongodb_source_requires_strictly_advancing_cursor_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_mongodb_source_spec(path)

    pages: dict[str | None, list[dict[str, object]]] = {
        "cust-0000": [
            {"doc_id": "cust-1", "doc_cursor": "cust-0002", "segment": "gold"},
            {"doc_id": "cust-2", "doc_cursor": "cust-0002", "segment": "silver"},
        ]
    }

    def fake_connect(
        *,
        config: mongodb_source.MongoDBIncrementalSourceConfigV1,
    ) -> _FakeMongoCollectionHandle:
        del config
        return _FakeMongoCollectionHandle(pages)

    monkeypatch.setattr(mongodb_source, "_connect_mongodb_incremental_source", fake_connect)

    with pytest.raises(ZephyrError) as exc_info:
        process_file(
            filename=str(path),
            strategy=PartitionStrategy.AUTO,
            sha256=normalize_it_input_identity_sha(filename=str(path), default_sha="fallback"),
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details == {
        "retryable": False,
        "source_kind": "mongodb_incremental_v1",
        "connection_name": "mongo-primary",
        "database": "analytics",
        "collection": "customer_docs",
        "cursor_field": "doc_cursor",
        "cursor": "cust-0002",
        "previous_cursor": "cust-0002",
    }
