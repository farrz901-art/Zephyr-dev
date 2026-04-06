from __future__ import annotations

import json

from it_stream.identity import ItTaskIdentityV1, normalize_it_task_identity_key
from zephyr_ingest.task_v1 import TaskIdentityV1, TaskV1


def normalize_uns_task_idempotency_key(*, identity: TaskIdentityV1) -> str:
    return json.dumps(
        {
            "kind": "uns",
            "pipeline_version": identity.pipeline_version,
            "sha256": identity.sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def normalize_it_task_idempotency_key(*, identity: TaskIdentityV1) -> str:
    return normalize_it_task_identity_key(
        identity=ItTaskIdentityV1(
            pipeline_version=identity.pipeline_version,
            sha256=identity.sha256,
        )
    )


def normalize_task_idempotency_key(task: TaskV1) -> str:
    if task.kind == "uns":
        if task.identity is None:
            raise ValueError("UNS task identity is required for idempotency normalization")
        return normalize_uns_task_idempotency_key(identity=task.identity)

    if task.kind == "it":
        if task.identity is None:
            raise ValueError("IT task identity is required for idempotency normalization")
        return normalize_it_task_idempotency_key(identity=task.identity)

    raise ValueError(f"Unsupported task kind: {task.kind}")
