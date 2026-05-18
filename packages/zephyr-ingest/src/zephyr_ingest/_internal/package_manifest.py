from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from zephyr_core.contracts.v2.package_manifest_builder import (
    ArtifactBuildSpecV1,
    build_package_manifest_v1,
    write_package_manifest_v1,
)

PACKAGE_MANIFEST_FILENAME = "package_manifest.json"


def _engine_name(run_meta: Mapping[str, object]) -> str | None:
    engine_obj = run_meta.get("engine")
    if not isinstance(engine_obj, Mapping):
        return None
    engine = cast("Mapping[str, object]", engine_obj)
    name = engine.get("name")
    if not isinstance(name, str):
        return None
    return name


def _engine_strategy(run_meta: Mapping[str, object]) -> str | None:
    engine_obj = run_meta.get("engine")
    if not isinstance(engine_obj, Mapping):
        return None
    engine = cast("Mapping[str, object]", engine_obj)
    strategy = engine.get("strategy")
    if not isinstance(strategy, str):
        return None
    return strategy


def _flow_kind(run_meta: Mapping[str, object]) -> str | None:
    name = _engine_name(run_meta)
    if name == "it-stream":
        return "it"
    if name is None:
        return None
    return "uns"


def _manifest_metadata(
    *,
    run_meta: Mapping[str, object],
) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    document_obj = run_meta.get("document")
    if isinstance(document_obj, Mapping):
        document = cast("Mapping[str, object]", document_obj)
        filename = document.get("filename")
        if isinstance(filename, str):
            metadata["document_filename"] = filename
    provenance_obj = run_meta.get("provenance")
    if isinstance(provenance_obj, Mapping):
        provenance = cast("Mapping[str, object]", provenance_obj)
        task_identity_key = provenance.get("task_identity_key")
        if isinstance(task_identity_key, str):
            metadata["task_identity_key"] = task_identity_key
        checkpoint_identity_key = provenance.get("checkpoint_identity_key")
        if isinstance(checkpoint_identity_key, str):
            metadata["checkpoint_identity_key"] = checkpoint_identity_key
    return metadata


def _artifact_specs_for_run(
    *,
    out_dir: Path,
    run_meta: Mapping[str, object],
) -> list[ArtifactBuildSpecV1]:
    is_it_stream = _engine_name(run_meta) == "it-stream"
    specs = [
        ArtifactBuildSpecV1(
            path=out_dir / "run_meta.json",
            artifact_kind="run_meta",
            role="metadata",
            required=True,
            producer="zephyr-ingest",
            media_type="application/json",
        ),
        ArtifactBuildSpecV1(
            path=out_dir / "elements.json",
            artifact_kind="elements",
            role="primary_structured_output",
            required=True,
            producer="zephyr-ingest",
            media_type="application/json",
        ),
        ArtifactBuildSpecV1(
            path=out_dir / "normalized.txt",
            artifact_kind="normalized_text",
            role="primary_text_output",
            required=True,
            producer="zephyr-ingest",
            media_type="text/plain",
        ),
        ArtifactBuildSpecV1(
            path=out_dir / "delivery_receipt.json",
            artifact_kind="delivery_receipt",
            role="delivery_evidence",
            required=False,
            producer="zephyr-ingest",
            media_type="application/json",
        ),
        ArtifactBuildSpecV1(
            path=out_dir / "usage_record.json",
            artifact_kind="usage_record",
            role="usage_fact",
            required=False,
            producer="zephyr-ingest",
            media_type="application/json",
        ),
    ]
    if is_it_stream:
        specs.extend(
            [
                ArtifactBuildSpecV1(
                    path=out_dir / "records.jsonl",
                    artifact_kind="it_records",
                    role="structured_records",
                    required=True,
                    producer="it-stream",
                    media_type="application/x-ndjson",
                ),
                ArtifactBuildSpecV1(
                    path=out_dir / "checkpoint.json",
                    artifact_kind="checkpoint",
                    role="progress_state",
                    required=True,
                    producer="it-stream",
                    media_type="application/json",
                ),
                ArtifactBuildSpecV1(
                    path=out_dir / "logs.jsonl",
                    artifact_kind="structured_logs",
                    role="structured_logs",
                    required=True,
                    producer="it-stream",
                    media_type="application/x-ndjson",
                ),
            ]
        )
    return specs


def write_package_manifest_for_run(
    *,
    out_dir: Path,
    source_sha256: str,
    run_meta: Mapping[str, object],
    profile: str | None = None,
    workflow_id: str | None = None,
    node_id: str | None = None,
    source_id: str | None = None,
    source_kind: str | None = None,
) -> Path:
    timestamp_obj = run_meta.get("timestamp_utc")
    created_at_utc = timestamp_obj if isinstance(timestamp_obj, str) else ""
    manifest = build_package_manifest_v1(
        out_dir=out_dir,
        source_sha256=source_sha256,
        run_meta=run_meta,
        artifact_specs=_artifact_specs_for_run(out_dir=out_dir, run_meta=run_meta),
        source_id=source_id,
        workflow_id=workflow_id,
        node_id=node_id,
        profile=profile,
        source_kind=source_kind,
        flow_kind=_flow_kind(run_meta),
        strategy_identity=_engine_strategy(run_meta),
        created_at_utc=created_at_utc,
        metadata=_manifest_metadata(run_meta=run_meta),
    )
    manifest_path = out_dir / PACKAGE_MANIFEST_FILENAME
    write_package_manifest_v1(path=manifest_path, manifest=manifest)
    return manifest_path


def load_package_manifest_if_available(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    raw_obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw_obj, dict):
        return None
    return cast("dict[str, Any]", raw_obj)
