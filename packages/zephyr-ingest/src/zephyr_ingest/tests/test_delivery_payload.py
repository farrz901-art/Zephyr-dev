from __future__ import annotations

import json
from pathlib import Path

from zephyr_core.contracts.v1.run_meta import EngineMetaV1, RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1


def test_build_delivery_payload_v1_is_json_serializable(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-24T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )

    payload = build_delivery_payload_v1(out_root=tmp_path / "out", sha256="abc", meta=meta)

    # must be JSON-serializable
    s = json.dumps(payload, ensure_ascii=False)
    obj = json.loads(s)

    assert obj["schema_version"] == 1
    assert obj["sha256"] == "abc"
    assert "run_meta" in obj
    assert "artifacts" in obj
    assert obj["artifacts"]["out_dir"].endswith("out/abc") or obj["artifacts"]["out_dir"].endswith(
        "out\\abc"
    )


def test_build_delivery_payload_v1_adds_it_artifact_paths_for_it_stream(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r-it",
        pipeline_version="p-it",
        timestamp_utc="2026-03-24T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        engine=EngineMetaV1(
            name="it-stream",
            backend="airbyte-message-json",
            version="0.1.0",
            strategy="auto",
        ),
    )

    payload = build_delivery_payload_v1(out_root=tmp_path / "out", sha256="it123", meta=meta)

    artifacts = payload["artifacts"]
    records_path = artifacts.get("records_path")
    state_path = artifacts.get("state_path")
    logs_path = artifacts.get("logs_path")

    assert isinstance(records_path, str)
    assert isinstance(state_path, str)
    assert isinstance(logs_path, str)

    assert records_path.endswith("out/it123/records.jsonl") or records_path.endswith(
        "out\\it123\\records.jsonl"
    )
    assert state_path.endswith("out/it123/state.jsonl") or state_path.endswith(
        "out\\it123\\state.jsonl"
    )
    assert logs_path.endswith("out/it123/logs.jsonl") or logs_path.endswith(
        "out\\it123\\logs.jsonl"
    )


def test_build_delivery_payload_v1_keeps_uns_payload_shape_without_it_artifacts(
    tmp_path: Path,
) -> None:
    meta = RunMetaV1(
        run_id="r-uns",
        pipeline_version="p-uns",
        timestamp_utc="2026-03-24T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        engine=EngineMetaV1(
            name="unstructured",
            backend="local",
            version="0.1.0",
            strategy="auto",
        ),
    )

    payload = build_delivery_payload_v1(out_root=tmp_path / "out", sha256="uns123", meta=meta)

    assert payload["artifacts"].get("records_path") is None
    assert payload["artifacts"].get("state_path") is None
    assert payload["artifacts"].get("logs_path") is None
