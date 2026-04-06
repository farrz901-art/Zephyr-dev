from __future__ import annotations

import pytest

from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)


def _make_uns_task(
    *,
    task_id: str,
    pipeline_version: str,
    sha256: str,
    uri: str,
) -> TaskV1:
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1(
                uri=uri,
                source="local_file",
                discovered_at_utc="2026-04-05T00:00:00Z",
                filename="doc.pdf",
                extension=".pdf",
                size_bytes=128,
            )
        ),
        execution=TaskExecutionV1(),
        identity=TaskIdentityV1(
            pipeline_version=pipeline_version,
            sha256=sha256,
        ),
    )


def _make_it_task(
    *,
    task_id: str,
    pipeline_version: str,
    sha256: str,
    uri: str,
) -> TaskV1:
    return TaskV1(
        task_id=task_id,
        kind="it",
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1(
                uri=uri,
                source="airbyte",
                discovered_at_utc="2026-04-05T00:00:00Z",
                filename="records.json",
                extension=".json",
                size_bytes=64,
            )
        ),
        execution=TaskExecutionV1(),
        identity=TaskIdentityV1(
            pipeline_version=pipeline_version,
            sha256=sha256,
        ),
    )


def test_uns_task_idempotency_normalization_is_deterministic() -> None:
    task = _make_uns_task(
        task_id="task-001",
        pipeline_version="p-ingest",
        sha256="abc123",
        uri="/tmp/one.pdf",
    )

    first = normalize_task_idempotency_key(task)
    second = normalize_task_idempotency_key(task)

    assert first == second
    assert first == '{"kind":"uns","pipeline_version":"p-ingest","sha256":"abc123"}'


def test_equivalent_uns_tasks_share_normalized_key() -> None:
    left = _make_uns_task(
        task_id="task-001",
        pipeline_version="p-ingest",
        sha256="abc123",
        uri="/tmp/one.pdf",
    )
    right = _make_uns_task(
        task_id="task-002",
        pipeline_version="p-ingest",
        sha256="abc123",
        uri="/tmp/two.pdf",
    )

    assert normalize_task_idempotency_key(left) == normalize_task_idempotency_key(right)


def test_non_equivalent_uns_tasks_produce_different_keys() -> None:
    left = _make_uns_task(
        task_id="task-001",
        pipeline_version="p-ingest",
        sha256="abc123",
        uri="/tmp/one.pdf",
    )
    right = _make_uns_task(
        task_id="task-002",
        pipeline_version="p-ingest-next",
        sha256="abc123",
        uri="/tmp/one.pdf",
    )
    third = _make_uns_task(
        task_id="task-003",
        pipeline_version="p-ingest",
        sha256="def456",
        uri="/tmp/one.pdf",
    )

    assert normalize_task_idempotency_key(left) != normalize_task_idempotency_key(right)
    assert normalize_task_idempotency_key(left) != normalize_task_idempotency_key(third)


def test_uns_task_normalization_requires_identity() -> None:
    task = TaskV1(
        task_id="task-001",
        kind="uns",
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1(
                uri="/tmp/one.pdf",
                source="local_file",
                discovered_at_utc="2026-04-05T00:00:00Z",
                filename="doc.pdf",
                extension=".pdf",
                size_bytes=128,
            )
        ),
        execution=TaskExecutionV1(),
    )

    with pytest.raises(ValueError, match="UNS task identity is required"):
        normalize_task_idempotency_key(task)


def test_it_task_idempotency_normalization_is_deterministic() -> None:
    task = _make_it_task(
        task_id="task-it-001",
        pipeline_version="p-it",
        sha256="xyz789",
        uri="s3://bucket/object.json",
    )

    first = normalize_task_idempotency_key(task)
    second = normalize_task_idempotency_key(task)

    assert first == second
    assert first == '{"kind":"it","pipeline_version":"p-it","sha256":"xyz789"}'


def test_equivalent_it_tasks_share_normalized_key() -> None:
    left = _make_it_task(
        task_id="task-it-001",
        pipeline_version="p-it",
        sha256="xyz789",
        uri="s3://bucket/one.json",
    )
    right = _make_it_task(
        task_id="task-it-002",
        pipeline_version="p-it",
        sha256="xyz789",
        uri="s3://bucket/two.json",
    )

    assert normalize_task_idempotency_key(left) == normalize_task_idempotency_key(right)


def test_non_equivalent_it_tasks_produce_different_keys() -> None:
    left = _make_it_task(
        task_id="task-it-001",
        pipeline_version="p-it",
        sha256="xyz789",
        uri="s3://bucket/object.json",
    )
    right = _make_it_task(
        task_id="task-it-002",
        pipeline_version="p-it-next",
        sha256="xyz789",
        uri="s3://bucket/object.json",
    )
    third = _make_it_task(
        task_id="task-it-003",
        pipeline_version="p-it",
        sha256="xyz999",
        uri="s3://bucket/object.json",
    )

    assert normalize_task_idempotency_key(left) != normalize_task_idempotency_key(right)
    assert normalize_task_idempotency_key(left) != normalize_task_idempotency_key(third)


def test_it_task_normalization_requires_identity() -> None:
    task = TaskV1(
        task_id="task-it-001",
        kind="it",
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1(
                uri="s3://bucket/object.json",
                source="airbyte",
                discovered_at_utc="2026-04-05T00:00:00Z",
                filename="object.json",
                extension=".json",
                size_bytes=64,
            )
        ),
        execution=TaskExecutionV1(),
    )

    with pytest.raises(ValueError, match="IT task identity is required"):
        normalize_task_idempotency_key(task)
