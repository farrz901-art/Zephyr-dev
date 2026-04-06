from __future__ import annotations

import json

from zephyr_core import ExecutionModeV1
from zephyr_core import RunProvenanceV1 as ExportedRunProvenanceV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.models import DocumentMetadata
from zephyr_core.contracts.v1.run_meta import (
    EngineMetaV1,
    MetricsV1,
    RunMetaV1,
    RunProvenanceV1,
)
from zephyr_core.contracts.v1.run_meta import (
    ExecutionModeV1 as InternalExecutionModeV1,
)
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION


def test_run_meta_v1_is_json_serializable_and_has_keys() -> None:
    meta = RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-17T00:00:00Z",
        document=DocumentMetadata(
            filename="a.txt",
            mime_type="text/plain",
            sha256="abc",
            size_bytes=1,
            created_at_utc="2026-03-17T00:00:00Z",
        ),
        outcome=RunOutcome.SUCCESS,
        engine=EngineMetaV1(name="unstructured", backend="local", version="0.0.0", strategy="auto"),
        metrics=MetricsV1(duration_ms=1, elements_count=2, normalized_text_len=3),
        warnings=[],
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="batch",
            task_id="task-1",
            task_identity_key="identity-1",
        ),
    )

    d = meta.to_dict()
    s = json.dumps(d, ensure_ascii=False)  # must not raise
    obj = json.loads(s)

    assert obj["schema_version"] == RUN_META_SCHEMA_VERSION
    assert obj["outcome"] == "success"
    assert obj["run_id"] == "r1"
    assert obj["document"]["sha256"] == "abc"
    assert "metrics" in obj
    assert obj["provenance"] == {
        "run_origin": "intake",
        "delivery_origin": "primary",
        "execution_mode": "batch",
        "task_id": "task-1",
        "task_identity_key": "identity-1",
    }


def test_run_provenance_exports_are_part_of_zephyr_core_public_contract() -> None:
    provenance = ExportedRunProvenanceV1(
        run_origin="intake",
        delivery_origin="primary",
        execution_mode="worker",
    )

    execution_mode: ExecutionModeV1 = "batch"
    internal_execution_mode: InternalExecutionModeV1 = execution_mode

    assert provenance.to_dict().get("execution_mode") == "worker"
    assert internal_execution_mode == "batch"
