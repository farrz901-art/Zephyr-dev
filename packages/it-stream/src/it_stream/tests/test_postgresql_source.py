from __future__ import annotations

import json
from pathlib import Path

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
from it_stream.sources import postgresql_source
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


class _FakePostgresqlCursor:
    def __init__(self, pages: dict[str | None, list[tuple[object, ...]]]) -> None:
        self._pages = pages
        self._rows: list[tuple[object, ...]] = []

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        del query
        after_cursor: str | None
        if len(params) == 1:
            after_cursor = None
        else:
            value = params[0]
            after_cursor = value if isinstance(value, str) else None
        self._rows = list(self._pages.get(after_cursor, []))

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)

    def close(self) -> None:
        return None


class _FakePostgresqlConnection:
    def __init__(self, pages: dict[str | None, list[tuple[object, ...]]]) -> None:
        self._pages = pages

    def cursor(self) -> _FakePostgresqlCursor:
        return _FakePostgresqlCursor(self._pages)

    def close(self) -> None:
        return None


def _write_postgresql_source_spec(
    path: Path,
    *,
    dsn: str = "postgresql://reader:secret@db.test/app",
    reverse_order: bool = False,
) -> None:
    payload = {
        "source": {
            "kind": "postgresql_incremental_v1",
            "stream": "customers",
            "connection_name": "warehouse-primary",
            "dsn": dsn,
            "schema": "public",
            "table": "customers",
            "columns": ["id", "updated_at", "name"],
            "cursor_column": "updated_at",
            "cursor_start": "2026-01-01T00:00:00Z",
            "batch_size": 2,
        }
    }
    if reverse_order:
        payload = {
            "source": {
                "batch_size": 2,
                "cursor_start": "2026-01-01T00:00:00Z",
                "cursor_column": "updated_at",
                "columns": ["id", "updated_at", "name"],
                "table": "customers",
                "schema": "public",
                "dsn": dsn,
                "connection_name": "warehouse-primary",
                "stream": "customers",
                "kind": "postgresql_incremental_v1",
            }
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_postgresql_source_identity_sha_is_stable_for_equivalent_specs(tmp_path: Path) -> None:
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    third = tmp_path / "third.json"

    _write_postgresql_source_spec(left, dsn="postgresql://reader:secret@db.test/app")
    _write_postgresql_source_spec(
        right,
        dsn="postgresql://reader:rotated@db.test/app",
        reverse_order=True,
    )
    _write_postgresql_source_spec(third)
    third_payload = json.loads(third.read_text(encoding="utf-8"))
    third_payload["source"]["cursor_start"] = "2026-01-05T00:00:00Z"
    third.write_text(json.dumps(third_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    first = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-left")
    second = normalize_it_input_identity_sha(filename=str(right), default_sha="fallback-right")
    different = normalize_it_input_identity_sha(filename=str(third), default_sha="fallback-third")

    assert first == second
    assert first != different


def test_postgresql_source_builds_cursor_checkpoints_and_resume_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_postgresql_source_spec(path)

    pages: dict[str | None, list[tuple[object, ...]]] = {
        "2026-01-01T00:00:00Z": [
            ("cust-1", "2026-01-02T00:00:00Z", "Ada"),
            ("cust-2", "2026-01-03T00:00:00Z", "Bob"),
        ],
        "2026-01-03T00:00:00Z": [("cust-3", "2026-01-04T00:00:00Z", "Cy")],
        "2026-01-04T00:00:00Z": [],
    }

    def fake_connect(
        *,
        config: postgresql_source.PostgresqlIncrementalSourceConfigV1,
    ) -> _FakePostgresqlConnection:
        assert config.connection_name == "warehouse-primary"
        assert config.schema == "public"
        assert config.table == "customers"
        return _FakePostgresqlConnection(pages)

    monkeypatch.setattr(postgresql_source, "_connect_postgresql_source", fake_connect)

    identity_sha = normalize_it_input_identity_sha(filename=str(path), default_sha="fallback")
    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert result.engine.name == "it-stream"
    assert result.engine.backend == "postgresql-incremental"
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
    assert result.elements[0].metadata["emitted_at"] == "2026-01-02T00:00:00Z"
    assert result.elements[3].metadata["data"] == {
        "cursor": "2026-01-03T00:00:00Z",
        "connection_name": "warehouse-primary",
        "schema": "public",
        "table": "customers",
        "cursor_column": "updated_at",
        "columns": ["id", "updated_at", "name"],
        "read_direction": "asc",
        "row_count": 2,
    }

    out_dir = tmp_path / "artifacts"
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result, pipeline_version="p-pg")
    first_checkpoint, second_checkpoint = artifacts.checkpoint.checkpoints

    assert first_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )

    selection = load_it_resume_selection(
        checkpoint_path=out_dir / "checkpoint.json",
        pipeline_version="p-pg",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )

    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "2026-01-03T00:00:00Z"

    resumed = resume_file(
        filename=str(path),
        checkpoint_path=out_dir / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-pg",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert [element.metadata["artifact_kind"] for element in resumed.elements] == [
        "record",
        "state",
    ]
    assert resumed.elements[0].metadata["data"] == {
        "id": "cust-3",
        "updated_at": "2026-01-04T00:00:00Z",
        "name": "Cy",
    }
    assert resumed.elements[0].metadata["emitted_at"] == "2026-01-04T00:00:00Z"
    assert resumed.elements[1].metadata["data"]["cursor"] == "2026-01-04T00:00:00Z"

    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "resumed-artifacts",
        result=resumed,
        pipeline_version="p-pg",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-pg-resume",
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
            continuation=ItCheckpointResumeCursorContinuationV1(
                exclusive_after_cursor="2026-01-03T00:00:00Z"
            ),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


def test_postgresql_source_requires_strictly_advancing_cursor_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_postgresql_source_spec(path)

    pages: dict[str | None, list[tuple[object, ...]]] = {
        "2026-01-01T00:00:00Z": [
            ("cust-1", "2026-01-03T00:00:00Z", "Ada"),
            ("cust-2", "2026-01-03T00:00:00Z", "Bob"),
        ],
    }

    def fake_connect(
        *,
        config: postgresql_source.PostgresqlIncrementalSourceConfigV1,
    ) -> _FakePostgresqlConnection:
        del config
        return _FakePostgresqlConnection(pages)

    monkeypatch.setattr(postgresql_source, "_connect_postgresql_source", fake_connect)

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
        "source_kind": "postgresql_incremental_v1",
        "connection_name": "warehouse-primary",
        "schema": "public",
        "table": "customers",
        "cursor_column": "updated_at",
        "cursor": "2026-01-03T00:00:00Z",
        "previous_cursor": "2026-01-03T00:00:00Z",
    }
