from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

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
from it_stream.sources import clickhouse_source, http_source, postgresql_source
from zephyr_core import PartitionStrategy

SecondRoundSourceName = Literal["postgresql", "clickhouse"]
SourceName = Literal["postgresql", "clickhouse", "http"]


@dataclass(frozen=True, slots=True)
class _SourceGovernanceBundle:
    source_name: SourceName
    backend: str
    initial_artifact_kinds: list[str]
    resumed_artifact_kinds: list[str]
    task_identity_key: str
    first_checkpoint_identity_key: str
    first_progress: dict[str, object]
    second_progress: dict[str, object] | None
    selection_progress_kind: str
    continuation_cursor: str
    loaded_resumed_provenance: ItCheckpointProvenanceV1
    loaded_resumed_checkpoint_identity_key: str
    loaded_resumed_parent_checkpoint_identity_key: str | None


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


class _FakeHttpResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

    def __enter__(self) -> _FakeHttpResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def _write_postgresql_source_spec(
    path: Path,
    *,
    dsn: str = "postgresql://reader:secret@db.test/app",
    cursor_start: str = "2026-01-01T00:00:00Z",
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
            "cursor_start": cursor_start,
            "batch_size": 2,
        }
    }
    if reverse_order:
        payload = {
            "source": {
                "batch_size": 2,
                "cursor_start": cursor_start,
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


def _write_clickhouse_source_spec(
    path: Path,
    *,
    password: str = "secret",
    cursor_start: str = "2026-02-01T00:00:00Z",
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
            "cursor_start": cursor_start,
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
                "cursor_start": cursor_start,
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


def _write_http_source_spec(path: Path, *, url: str) -> None:
    payload = {
        "source": {
            "kind": "http_json_cursor_v1",
            "stream": "customers",
            "url": url,
            "cursor_param": "cursor",
            "query": {},
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_postgresql_bundle(
    *,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> _SourceGovernanceBundle:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "postgresql-source.json"
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
        return _FakePostgresqlConnection(pages)

    monkeypatch.setattr(postgresql_source, "_connect_postgresql_source", fake_connect)
    identity_sha = normalize_it_input_identity_sha(filename=str(path), default_sha="fallback")
    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "postgresql-artifacts",
        result=initial,
        pipeline_version="p-pg",
    )
    first_checkpoint, second_checkpoint = artifacts.checkpoint.checkpoints
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "postgresql-artifacts" / "checkpoint.json",
        pipeline_version="p-pg",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "postgresql-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-pg",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )
    dump_it_artifacts(
        out_dir=tmp_path / "postgresql-resumed-artifacts",
        result=resumed,
        pipeline_version="p-pg",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-pg-resume",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "postgresql-resumed-artifacts" / "checkpoint.json"
    )

    return _SourceGovernanceBundle(
        source_name="postgresql",
        backend=initial.engine.backend,
        initial_artifact_kinds=[
            cast("str", element.metadata["artifact_kind"]) for element in initial.elements
        ],
        resumed_artifact_kinds=[
            cast("str", element.metadata["artifact_kind"]) for element in resumed.elements
        ],
        task_identity_key=artifacts.checkpoint.task_identity_key,
        first_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        first_progress=first_checkpoint.progress,
        second_progress=second_checkpoint.progress,
        selection_progress_kind=selection.selected_checkpoint.progress_kind,
        continuation_cursor=(
            "" if selection.continuation is None else selection.continuation.exclusive_after_cursor
        ),
        loaded_resumed_provenance=cast("ItCheckpointProvenanceV1", loaded_resumed.provenance),
        loaded_resumed_checkpoint_identity_key=loaded_resumed.checkpoints[
            0
        ].checkpoint_identity_key,
        loaded_resumed_parent_checkpoint_identity_key=loaded_resumed.checkpoints[
            0
        ].parent_checkpoint_identity_key,
    )


def _run_clickhouse_bundle(
    *,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> _SourceGovernanceBundle:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "clickhouse-source.json"
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
        return _FakeClickHouseClient(pages)

    monkeypatch.setattr(clickhouse_source, "_connect_clickhouse_source", fake_connect)
    identity_sha = normalize_it_input_identity_sha(filename=str(path), default_sha="fallback")
    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "clickhouse-artifacts",
        result=initial,
        pipeline_version="p-ch",
    )
    first_checkpoint, second_checkpoint = artifacts.checkpoint.checkpoints
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "clickhouse-artifacts" / "checkpoint.json",
        pipeline_version="p-ch",
        sha256=identity_sha,
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "clickhouse-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-ch",
        strategy=PartitionStrategy.AUTO,
        sha256=identity_sha,
        size_bytes=path.stat().st_size,
    )
    dump_it_artifacts(
        out_dir=tmp_path / "clickhouse-resumed-artifacts",
        result=resumed,
        pipeline_version="p-ch",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-ch-resume",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "clickhouse-resumed-artifacts" / "checkpoint.json"
    )

    return _SourceGovernanceBundle(
        source_name="clickhouse",
        backend=initial.engine.backend,
        initial_artifact_kinds=[
            cast("str", element.metadata["artifact_kind"]) for element in initial.elements
        ],
        resumed_artifact_kinds=[
            cast("str", element.metadata["artifact_kind"]) for element in resumed.elements
        ],
        task_identity_key=artifacts.checkpoint.task_identity_key,
        first_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        first_progress=first_checkpoint.progress,
        second_progress=second_checkpoint.progress,
        selection_progress_kind=selection.selected_checkpoint.progress_kind,
        continuation_cursor=(
            "" if selection.continuation is None else selection.continuation.exclusive_after_cursor
        ),
        loaded_resumed_provenance=cast("ItCheckpointProvenanceV1", loaded_resumed.provenance),
        loaded_resumed_checkpoint_identity_key=loaded_resumed.checkpoints[
            0
        ].checkpoint_identity_key,
        loaded_resumed_parent_checkpoint_identity_key=loaded_resumed.checkpoints[
            0
        ].parent_checkpoint_identity_key,
    )


def _run_http_bundle(
    *,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> _SourceGovernanceBundle:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "http-source.json"
    _write_http_source_spec(path, url="https://example.test/api/customers")

    def fake_urlopen(request: object, timeout: float = 10.0) -> _FakeHttpResponse:
        del timeout
        full_url = cast("str", getattr(request, "full_url"))
        if "cursor=c-2" in full_url:
            return _FakeHttpResponse({"records": [], "next_cursor": None})
        if "cursor=c-1" in full_url:
            return _FakeHttpResponse({"records": [], "next_cursor": "c-2"})
        return _FakeHttpResponse({"records": [], "next_cursor": "c-1"})

    monkeypatch.setattr(http_source, "urlopen", fake_urlopen)
    initial = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-http-batch",
        size_bytes=path.stat().st_size,
    )
    artifacts = dump_it_artifacts(
        out_dir=tmp_path / "http-artifacts",
        result=initial,
        pipeline_version="p-http",
    )
    first_checkpoint, second_checkpoint = artifacts.checkpoint.checkpoints
    selection = load_it_resume_selection(
        checkpoint_path=tmp_path / "http-artifacts" / "checkpoint.json",
        pipeline_version="p-http",
        sha256="sha-http-batch",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
    )
    resumed = resume_file(
        filename=str(path),
        checkpoint_path=tmp_path / "http-artifacts" / "checkpoint.json",
        checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        pipeline_version="p-http",
        strategy=PartitionStrategy.AUTO,
        sha256="sha-http-batch",
        size_bytes=path.stat().st_size,
    )
    dump_it_artifacts(
        out_dir=tmp_path / "http-resumed-artifacts",
        result=resumed,
        pipeline_version="p-http",
        run_provenance=selection.to_run_provenance(
            execution_mode="worker",
            task_id="task-http-resume",
        ),
        resume_provenance=selection.to_checkpoint_resume_provenance(),
    )
    loaded_resumed = load_it_checkpoint(
        path=tmp_path / "http-resumed-artifacts" / "checkpoint.json"
    )

    return _SourceGovernanceBundle(
        source_name="http",
        backend=initial.engine.backend,
        initial_artifact_kinds=[
            cast("str", element.metadata["artifact_kind"]) for element in initial.elements
        ],
        resumed_artifact_kinds=[
            cast("str", element.metadata["artifact_kind"]) for element in resumed.elements
        ],
        task_identity_key=artifacts.checkpoint.task_identity_key,
        first_checkpoint_identity_key=first_checkpoint.checkpoint_identity_key,
        first_progress=first_checkpoint.progress,
        second_progress=second_checkpoint.progress,
        selection_progress_kind=selection.selected_checkpoint.progress_kind,
        continuation_cursor=(
            "" if selection.continuation is None else selection.continuation.exclusive_after_cursor
        ),
        loaded_resumed_provenance=cast("ItCheckpointProvenanceV1", loaded_resumed.provenance),
        loaded_resumed_checkpoint_identity_key=loaded_resumed.checkpoints[
            0
        ].checkpoint_identity_key,
        loaded_resumed_parent_checkpoint_identity_key=loaded_resumed.checkpoints[
            0
        ].parent_checkpoint_identity_key,
    )


def test_first_second_round_source_batch_shares_task_identity_boundary_without_secret_coupling(
    tmp_path: Path,
) -> None:
    observed_identities: dict[SecondRoundSourceName, str] = {}

    postgresql_left = tmp_path / "postgresql-left.json"
    postgresql_right = tmp_path / "postgresql-right.json"
    postgresql_third = tmp_path / "postgresql-third.json"
    _write_postgresql_source_spec(postgresql_left, dsn="postgresql://reader:secret@db.test/app")
    _write_postgresql_source_spec(
        postgresql_right,
        dsn="postgresql://reader:rotated@db.test/app",
        reverse_order=True,
    )
    _write_postgresql_source_spec(postgresql_third, cursor_start="2026-01-05T00:00:00Z")

    first_postgresql = normalize_it_input_identity_sha(
        filename=str(postgresql_left),
        default_sha="fallback-left",
    )
    second_postgresql = normalize_it_input_identity_sha(
        filename=str(postgresql_right),
        default_sha="fallback-right",
    )
    different_postgresql = normalize_it_input_identity_sha(
        filename=str(postgresql_third),
        default_sha="fallback-third",
    )

    assert first_postgresql == second_postgresql
    assert first_postgresql != different_postgresql
    observed_identities["postgresql"] = first_postgresql

    clickhouse_left = tmp_path / "clickhouse-left.json"
    clickhouse_right = tmp_path / "clickhouse-right.json"
    clickhouse_third = tmp_path / "clickhouse-third.json"
    _write_clickhouse_source_spec(clickhouse_left, password="secret")
    _write_clickhouse_source_spec(
        clickhouse_right,
        password="rotated-secret",
        reverse_order=True,
    )
    _write_clickhouse_source_spec(clickhouse_third, cursor_start="2026-02-05T00:00:00Z")

    first_clickhouse = normalize_it_input_identity_sha(
        filename=str(clickhouse_left),
        default_sha="fallback-left",
    )
    second_clickhouse = normalize_it_input_identity_sha(
        filename=str(clickhouse_right),
        default_sha="fallback-right",
    )
    different_clickhouse = normalize_it_input_identity_sha(
        filename=str(clickhouse_third),
        default_sha="fallback-third",
    )

    assert first_clickhouse == second_clickhouse
    assert first_clickhouse != different_clickhouse
    observed_identities["clickhouse"] = first_clickhouse

    assert len(set(observed_identities.values())) == 2


def test_first_second_round_source_batch_uses_shared_cursor_governance_and_preserves_http_baseline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    postgresql_bundle = _run_postgresql_bundle(
        monkeypatch=monkeypatch,
        tmp_path=tmp_path / "postgresql",
    )
    clickhouse_bundle = _run_clickhouse_bundle(
        monkeypatch=monkeypatch,
        tmp_path=tmp_path / "clickhouse",
    )
    http_bundle = _run_http_bundle(
        monkeypatch=monkeypatch,
        tmp_path=tmp_path / "http",
    )

    second_round_batch = (postgresql_bundle, clickhouse_bundle)
    for bundle in second_round_batch:
        assert bundle.backend in {"postgresql-incremental", "clickhouse-incremental"}
        assert bundle.initial_artifact_kinds == [
            "record",
            "record",
            "record",
            "state",
            "state",
            "log",
            "log",
            "log",
        ]
        assert bundle.resumed_artifact_kinds == ["record", "state"]
        assert bundle.selection_progress_kind == "cursor_v1"
        assert bundle.continuation_cursor == cast("str", bundle.first_progress["cursor"])
        assert bundle.first_checkpoint_identity_key != bundle.task_identity_key
        assert bundle.second_progress is not None
        assert cast("str", bundle.second_progress["cursor"]) > cast(
            "str", bundle.first_progress["cursor"]
        )
        assert bundle.loaded_resumed_provenance == ItCheckpointProvenanceV1(
            task_identity_key=bundle.task_identity_key,
            run_origin="resume",
            delivery_origin="primary",
            execution_mode="worker",
            resumed_from_checkpoint_identity_key=bundle.first_checkpoint_identity_key,
            resume=ItCheckpointResumeProvenanceV1(
                checkpoint_identity_key=bundle.first_checkpoint_identity_key,
                progress_kind="cursor_v1",
                continuation=ItCheckpointResumeCursorContinuationV1(
                    exclusive_after_cursor=bundle.continuation_cursor
                ),
            ),
        )
        assert bundle.loaded_resumed_parent_checkpoint_identity_key is None
        assert bundle.loaded_resumed_checkpoint_identity_key != bundle.task_identity_key

    assert postgresql_bundle.first_progress["schema"] == "public"
    assert "database" not in postgresql_bundle.first_progress
    assert "query_mode" not in postgresql_bundle.first_progress
    assert clickhouse_bundle.first_progress["database"] == "analytics"
    assert clickhouse_bundle.first_progress["query_mode"] == "incremental_table_query"
    assert "schema" not in clickhouse_bundle.first_progress

    assert http_bundle.backend == "http-json-cursor"
    assert http_bundle.initial_artifact_kinds == ["state", "state", "log", "log", "log", "log"]
    assert http_bundle.resumed_artifact_kinds == ["state"]
    assert http_bundle.selection_progress_kind == "cursor_v1"
    assert http_bundle.continuation_cursor == cast("str", http_bundle.first_progress["cursor"])
    assert http_bundle.first_checkpoint_identity_key != http_bundle.task_identity_key
    assert http_bundle.loaded_resumed_provenance == ItCheckpointProvenanceV1(
        task_identity_key=http_bundle.task_identity_key,
        run_origin="resume",
        delivery_origin="primary",
        execution_mode="worker",
        resumed_from_checkpoint_identity_key=http_bundle.first_checkpoint_identity_key,
        resume=ItCheckpointResumeProvenanceV1(
            checkpoint_identity_key=http_bundle.first_checkpoint_identity_key,
            progress_kind="cursor_v1",
            continuation=ItCheckpointResumeCursorContinuationV1(
                exclusive_after_cursor=http_bundle.continuation_cursor
            ),
        ),
    )
    assert http_bundle.loaded_resumed_parent_checkpoint_identity_key is None
    assert http_bundle.loaded_resumed_checkpoint_identity_key != http_bundle.task_identity_key
    assert http_bundle.first_progress["source_url"] == "https://example.test/api/customers"
    assert http_bundle.first_progress["page_number"] == 1
    assert "schema" not in http_bundle.first_progress
    assert "database" not in http_bundle.first_progress
