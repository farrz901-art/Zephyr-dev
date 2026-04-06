from __future__ import annotations

import json

from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.models import DocumentMetadata
from zephyr_core.contracts.v1.run_meta import (
    EngineMetaV1,
    MetricsV1,
    RunMetaV1,
    RunProvenanceV1,
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
        provenance=RunProvenanceV1(run_origin="intake", delivery_origin="primary"),
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
    }
