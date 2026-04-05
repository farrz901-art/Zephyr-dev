from __future__ import annotations

import pytest

from zephyr_core import DocumentRef, PartitionStrategy
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)


def test_task_v1_to_dict_round_trip_for_uns_work() -> None:
    doc = DocumentRef(
        uri="/tmp/example.pdf",
        source="local_file",
        discovered_at_utc="2026-04-05T00:00:00Z",
        filename="example.pdf",
        extension=".pdf",
        size_bytes=128,
    )
    task = TaskV1(
        task_id="task-001",
        kind="uns",
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
        execution=TaskExecutionV1(
            strategy=PartitionStrategy.FAST,
            unique_element_ids=False,
        ),
        identity=TaskIdentityV1(
            pipeline_version="p-ingest",
            sha256="abc123",
        ),
    )

    payload = task.to_dict()

    assert payload == {
        "task_id": "task-001",
        "kind": "uns",
        "inputs": {
            "document": {
                "uri": "/tmp/example.pdf",
                "source": "local_file",
                "discovered_at_utc": "2026-04-05T00:00:00Z",
                "filename": "example.pdf",
                "extension": ".pdf",
                "size_bytes": 128,
            }
        },
        "execution": {
            "strategy": "fast",
            "unique_element_ids": False,
        },
        "identity": {
            "pipeline_version": "p-ingest",
            "sha256": "abc123",
        },
    }

    restored = TaskV1.from_dict(payload)

    assert restored == task
    assert restored.inputs.document.to_document_ref() == doc


def test_task_v1_allows_future_it_kind_without_runtime_binding() -> None:
    task = TaskV1.from_dict(
        {
            "task_id": "task-it-001",
            "kind": "it",
            "inputs": {
                "document": {
                    "uri": "s3://bucket/object.json",
                    "source": "airbyte",
                    "discovered_at_utc": "2026-04-05T00:00:00Z",
                    "filename": "object.json",
                    "extension": ".json",
                    "size_bytes": 64,
                }
            },
            "execution": {
                "strategy": "auto",
                "unique_element_ids": True,
            },
            "identity": {
                "pipeline_version": "p-it",
                "sha256": "xyz789",
            },
        }
    )

    assert task.kind == "it"
    assert task.execution.strategy is PartitionStrategy.AUTO


def test_task_v1_rejects_unsupported_kind() -> None:
    with pytest.raises(ValueError, match="Unsupported task kind"):
        TaskV1.from_dict(
            {
                "task_id": "task-x-001",
                "kind": "other",
                "inputs": {
                    "document": {
                        "uri": "/tmp/example.pdf",
                        "source": "local_file",
                        "discovered_at_utc": "2026-04-05T00:00:00Z",
                        "filename": "example.pdf",
                        "extension": ".pdf",
                        "size_bytes": 128,
                    }
                },
                "execution": {
                    "strategy": "auto",
                    "unique_element_ids": True,
                },
            }
        )
