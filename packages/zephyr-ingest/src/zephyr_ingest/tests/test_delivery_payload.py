from __future__ import annotations

import json
from pathlib import Path

from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)
from zephyr_core.contracts.v1.run_meta import (
    EngineMetaV1,
    MetricsV1,
    RunMetaV1,
    RunProvenanceV1,
)
from zephyr_core.contracts.v2.delivery_payload import DeliveryContentEvidenceV1, DeliveryPayloadV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1


def _require_content_evidence(payload: DeliveryPayloadV1) -> DeliveryContentEvidenceV1:
    content_evidence = payload.get("content_evidence")
    assert content_evidence is not None
    return content_evidence


def _require_str(evidence: DeliveryContentEvidenceV1, key: str) -> str:
    value = evidence.get(key)
    assert isinstance(value, str)
    return value


def _require_int(evidence: DeliveryContentEvidenceV1, key: str) -> int:
    value = evidence.get(key)
    assert isinstance(value, int)
    return value


def _require_records_preview(evidence: DeliveryContentEvidenceV1) -> list[dict[str, object]]:
    value = evidence.get("records_preview")
    assert isinstance(value, list)
    return value


def _build_result(*, flow: str, marker: str) -> PartitionResult:
    if flow == "uns":
        return PartitionResult(
            document=DocumentMetadata(
                filename="uns.txt",
                mime_type="text/plain",
                sha256="abc",
                size_bytes=12,
                created_at_utc="2026-03-24T00:00:00Z",
            ),
            engine=EngineInfo(
                name="unstructured",
                backend="local",
                version="0.1.0",
                strategy=PartitionStrategy.AUTO,
            ),
            elements=[
                ZephyrElement(
                    element_id="e1",
                    type="NarrativeText",
                    text=f"UNS {marker}",
                    metadata={"flow_kind": "uns"},
                ),
                ZephyrElement(
                    element_id="e2",
                    type="Title",
                    text="Header",
                    metadata={"flow_kind": "uns"},
                ),
            ],
            normalized_text=f"UNS normalized {marker}",
        )
    return PartitionResult(
        document=DocumentMetadata(
            filename="it.json",
            mime_type="application/json",
            sha256="it123",
            size_bytes=42,
            created_at_utc="2026-03-24T00:00:00Z",
        ),
        engine=EngineInfo(
            name="it-stream",
            backend="http-json-cursor",
            version="0.1.0",
            strategy=PartitionStrategy.AUTO,
        ),
        elements=[
            ZephyrElement(
                element_id="r1",
                type="StructuredRecord",
                text=json.dumps({"marker": marker, "index": 1}, ensure_ascii=False),
                metadata={"data": {"marker": marker, "index": 1}},
            ),
            ZephyrElement(
                element_id="r2",
                type="StructuredRecord",
                text=json.dumps({"marker": marker, "index": 2}, ensure_ascii=False),
                metadata={"data": {"marker": marker, "index": 2}},
            ),
            ZephyrElement(
                element_id="s1",
                type="StructuredState",
                text=json.dumps({"cursor": "c2"}, ensure_ascii=False),
                metadata={"data": {"cursor": "c2"}},
            ),
            ZephyrElement(
                element_id="l1",
                type="StructuredLog",
                text=f"processed {marker}",
                metadata={"level": "info"},
            ),
        ],
        normalized_text=f"IT normalized {marker}",
    )


def _build_meta_for_result(*, result: PartitionResult, run_id: str) -> RunMetaV1:
    return RunMetaV1(
        run_id=run_id,
        pipeline_version="p1",
        timestamp_utc="2026-03-24T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        engine=EngineMetaV1(
            name=result.engine.name,
            backend=result.engine.backend,
            version=result.engine.version,
            strategy=str(result.engine.strategy),
        ),
        metrics=MetricsV1(
            duration_ms=0,
            elements_count=len(result.elements),
            normalized_text_len=len(result.normalized_text),
            attempts=1,
        ),
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="batch",
            task_id=f"task-{run_id}",
            task_identity_key=f"task-ident-{run_id}",
        ),
    )


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


def test_build_delivery_payload_v1_preserves_run_meta_provenance(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r-prov",
        pipeline_version="p-prov",
        timestamp_utc="2026-03-24T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        provenance=RunProvenanceV1(
            run_origin="resume",
            delivery_origin="primary",
            execution_mode="worker",
            task_id="task-run-123",
            checkpoint_identity_key="cp-123",
            task_identity_key="task-123",
        ),
    )

    payload = build_delivery_payload_v1(out_root=tmp_path / "out", sha256="abc", meta=meta)

    assert payload["run_meta"]["provenance"] == {
        "run_origin": "resume",
        "delivery_origin": "primary",
        "execution_mode": "worker",
        "task_id": "task-run-123",
        "checkpoint_identity_key": "cp-123",
        "task_identity_key": "task-123",
    }


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
    assert state_path.endswith("out/it123/checkpoint.json") or state_path.endswith(
        "out\\it123\\checkpoint.json"
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


def test_build_delivery_payload_v1_prefers_result_content_evidence_for_uns_without_artifacts(
    tmp_path: Path,
) -> None:
    marker = "uns-remote-only-marker"
    result = _build_result(flow="uns", marker=marker)
    meta = _build_meta_for_result(result=result, run_id="r-uns-result")

    payload = build_delivery_payload_v1(
        out_root=tmp_path / "out",
        sha256="abc",
        meta=meta,
        result=result,
    )

    evidence = _require_content_evidence(payload)
    assert _require_str(evidence, "evidence_kind") == "normalized_text_preview_v1"
    assert _require_str(evidence, "normalized_text_status") == "available"
    assert marker in _require_str(evidence, "normalized_text_preview")
    assert _require_int(evidence, "elements_count") == 2
    assert evidence.get("records_preview") is None


def test_build_delivery_payload_v1_prefers_result_content_evidence_for_it_without_artifacts(
    tmp_path: Path,
) -> None:
    marker = "it-remote-only-marker"
    result = _build_result(flow="it", marker=marker)
    meta = _build_meta_for_result(result=result, run_id="r-it-result")

    payload = build_delivery_payload_v1(
        out_root=tmp_path / "out",
        sha256="it123",
        meta=meta,
        result=result,
    )

    evidence = _require_content_evidence(payload)
    assert _require_str(evidence, "evidence_kind") == "normalized_text_and_records_preview_v1"
    assert _require_str(evidence, "normalized_text_status") == "available"
    assert marker in _require_str(evidence, "normalized_text_preview")
    assert _require_str(evidence, "records_status") == "available"
    records_preview = _require_records_preview(evidence)
    assert records_preview[0]["marker"] == marker
    assert _require_str(evidence, "structured_state_visibility") == "available"
    assert _require_str(evidence, "structured_log_visibility") == "available"
