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
from it_stream.sources import clickhouse_source
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


class _FakeClickHouseQueryResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.result_rows = rows


class _FakeClickHouseClient:
    def __init__(self, pages: dict[str | None, list[tuple[object, ...]]]) -> None:
        self._pages = pages

    def query(
        self,
        query: str,
        parameters: dict[str, object] | None = None,
    ) -> _FakeClickHouseQueryResult:
        del query
        after_cursor = None
        if parameters is not None:
            raw_after_cursor = parameters.get("after_cursor")
            if isinstance(raw_after_cursor, str):
                after_cursor = raw_after_cursor
        return _FakeClickHouseQueryResult(list(self._pages.get(after_cursor, [])))

    def close(self) -> None:
        return None


def _write_clickhouse_source_spec(
    path: Path,
    *,
    password: str = "secret",
    reverse_order: bool = False,
) -> None:
    payload = {
        "source": {
            "kind": "clickhouse_incremental_v1",
            "stream": "warehouse_customers",
            "connection_name": "analytics-primary",
            "url": "https://clickhouse.example.test:8443",
            "database": "analytics",
            "table": "customer_events",
            "columns": ["customer_id", "event_ts", "segment"],
            "cursor_column": "event_ts",
            "cursor_start": "2026-02-01T00:00:00Z",
            "batch_size": 2,
            "username": "reader",
            "password": password,
        }
    }
    if reverse_order:
        payload = {
            "source": {
                "password": password,
                "username": "reader",
                "batch_size": 2,
                "cursor_start": "2026-02-01T00:00:00Z",
                "cursor_column": "event_ts",
                "columns": ["customer_id", "event_ts", "segment"],
                "table": "customer_events",
                "database": "analytics",
                "url": "https://clickhouse.example.test:8443",
                "connection_name": "analytics-primary",
                "stream": "warehouse_customers",
                "kind": "clickhouse_incremental_v1",
            }
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_clickhouse_source_identity_sha_is_stable_for_equivalent_specs(tmp_path: Path) -> None:
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    third = tmp_path / "third.json"

    _write_clickhouse_source_spec(left, password="secret")
    _write_clickhouse_source_spec(right, password="rotated-secret", reverse_order=True)
    _write_clickhouse_source_spec(third)
    third_payload = json.loads(third.read_text(encoding="utf-8"))
    third_payload["source"]["cursor_start"] = "2026-02-05T00:00:00Z"
    third.write_text(json.dumps(third_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    first = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-left")
    second = normalize_it_input_identity_sha(filename=str(right), default_sha="fallback-right")
    different = normalize_it_input_identity_sha(filename=str(third), default_sha="fallback-third")

    assert first == second
    assert first != different


def test_clickhouse_source_builds_cursor_checkpoints_and_resume_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_clickhouse_source_spec(path)

    pages: dict[str | None, list[tuple[object, ...]]] = {
        "2026-02-01T00:00:00Z": [
            ("cust-1", "2026-02-02T00:00:00Z", "gold"),
            ("cust-2", "2026-02-03T00:00:00Z", "silver"),
        ],
        "2026-02-03T00:00:00Z": [("cust-3", "2026-02-04T00:00:00Z", "bronze")],
        "2026-02-04T00:00:00Z": [],
    }

    def fake_connect(
        *,
        config: clickhouse_source.ClickHouseIncrementalSourceConfigV1,
    ) -> _FakeClickHouseClient:
        assert config.connection_name == "analytics-primary"
        assert config.database == "analytics"
        assert config.table == "customer_events"
        return _FakeClickHouseClient(pages)

    monkeypatch.setattr(clickhouse_source, "_connect_clickhouse_source", fake_connect)

    identity_sha = normalize_it_input_identity_sha(filename=str(path), default_sha="fallback")
    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert result.engine.name == "it-stream"
    assert result.engine.backend == "clickhouse-incremental"
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
    assert result.elements[0].metadata["emitted_at"] == "2026-02-02T00:00:00Z"
    assert result.elements[3].metadata["data"] == {
        "cursor": "2026-02-03T00:00:00Z",
        "connection_name": "analytics-primary",
        "database": "analytics",
        "table": "customer_events",
        "cursor_column": "event_ts",
        "columns": ["customer_id", "event_ts", "segment"],
        "query_mode": "incremental_table_query",
        "read_direction": "asc",
        "row_count": 2,
    }

    out_dir = tmp_path / "artifacts"
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result, pipeline_version="p-ch")
    first_checkpoint, second_checkpoint = artifacts.checkpoint.checkpoints

    assert first_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.progress_kind == "cursor_v1"
    assert second_checkpoint.parent_checkpoint_identity_key == (
        first_checkpoint.checkpoint_identity_key
    )

    selection = load_it_resume_selection(
        checkpoint_path=out_dir / "checkpoint.json",
        pipeline_version="p-ch",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )

    assert selection.selected_checkpoint.progress_kind == "cursor_v1"
    assert selection.continuation is not None
    assert selection.continuation.progress_kind == "cursor_v1"
    assert selection.continuation.exclusive_after_cursor == "2026-02-03T00:00:00Z"

    resumed = resume_file(
        filename=str(path),
        checkpoint_path=out_dir / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-ch",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )

    assert [element.metadata["artifact_kind"] for element in resumed.elements] == [
        "record",
        "state",
    ]
    assert resumed.elements[0].metadata["data"] == {
        "customer_id": "cust-3",
        "event_ts": "2026-02-04T00:00:00Z",
        "segment": "bronze",
    }
    assert resumed.elements[0].metadata["emitted_at"] == "2026-02-04T00:00:00Z"
    assert resumed.elements[1].metadata["data"]["cursor"] == "2026-02-04T00:00:00Z"

    resumed_artifacts = dump_it_artifacts(
        out_dir=tmp_path / "resumed-artifacts",
        result=resumed,
        pipeline_version="p-ch",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-ch-resume",
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
                exclusive_after_cursor="2026-02-03T00:00:00Z"
            ),
        ),
    )
    assert resumed_artifacts.checkpoint.provenance == loaded_resumed.provenance


def test_clickhouse_source_requires_strictly_advancing_cursor_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_clickhouse_source_spec(path)

    pages: dict[str | None, list[tuple[object, ...]]] = {
        "2026-02-01T00:00:00Z": [
            ("cust-1", "2026-02-03T00:00:00Z", "gold"),
            ("cust-2", "2026-02-03T00:00:00Z", "silver"),
        ]
    }

    def fake_connect(
        *,
        config: clickhouse_source.ClickHouseIncrementalSourceConfigV1,
    ) -> _FakeClickHouseClient:
        del config
        return _FakeClickHouseClient(pages)

    monkeypatch.setattr(clickhouse_source, "_connect_clickhouse_source", fake_connect)

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
        "source_kind": "clickhouse_incremental_v1",
        "connection_name": "analytics-primary",
        "database": "analytics",
        "table": "customer_events",
        "cursor_column": "event_ts",
        "cursor": "2026-02-03T00:00:00Z",
        "previous_cursor": "2026-02-03T00:00:00Z",
    }
