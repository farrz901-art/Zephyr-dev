from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import (
    DELIVERY_PAYLOAD_SCHEMA_VERSION,
    ArtifactsPathsV1,
    DeliveryPayloadV1,
)


def _is_it_stream_run_meta(*, run_meta: dict[str, Any]) -> bool:
    engine_obj = run_meta.get("engine")
    if not isinstance(engine_obj, dict):
        return False
    engine = cast("dict[str, object]", engine_obj)
    name = engine.get("name")
    return isinstance(name, str) and name == "it-stream"


def build_artifacts_paths_v1(*, out_root: Path, sha256: str) -> ArtifactsPathsV1:
    out_dir = (out_root / sha256).resolve()
    return cast(
        ArtifactsPathsV1,
        {
            "out_dir": str(out_dir),
            "run_meta_path": str((out_dir / "run_meta.json").resolve()),
            "elements_path": str((out_dir / "elements.json").resolve()),
            "normalized_path": str((out_dir / "normalized.txt").resolve()),
        },
    )


def build_artifacts_paths_for_run_meta_v1(
    *,
    out_root: Path,
    sha256: str,
    run_meta: dict[str, Any],
) -> ArtifactsPathsV1:
    paths = build_artifacts_paths_v1(out_root=out_root, sha256=sha256)
    if _is_it_stream_run_meta(run_meta=run_meta):
        paths["records_path"] = str(((out_root / sha256) / "records.jsonl").resolve())
        paths["state_path"] = str(((out_root / sha256) / "checkpoint.json").resolve())
        paths["logs_path"] = str(((out_root / sha256) / "logs.jsonl").resolve())
    return paths


def build_delivery_payload_v1(
    *,
    out_root: Path,
    sha256: str,
    meta: RunMetaV1,
) -> DeliveryPayloadV1:
    run_meta = meta.to_dict()

    payload: DeliveryPayloadV1 = {
        "schema_version": DELIVERY_PAYLOAD_SCHEMA_VERSION,
        "sha256": sha256,
        "run_meta": run_meta,
        "artifacts": build_artifacts_paths_for_run_meta_v1(
            out_root=out_root,
            sha256=sha256,
            run_meta=run_meta,
        ),
    }
    return payload


def build_delivery_payload_v1_from_run_meta_dict(
    *,
    out_root: Path,
    sha256: str,
    run_meta: dict[str, Any],
) -> DeliveryPayloadV1:
    payload: DeliveryPayloadV1 = {
        "schema_version": DELIVERY_PAYLOAD_SCHEMA_VERSION,
        "sha256": sha256,
        "run_meta": run_meta,
        "artifacts": build_artifacts_paths_for_run_meta_v1(
            out_root=out_root,
            sha256=sha256,
            run_meta=run_meta,
        ),
    }
    return payload
