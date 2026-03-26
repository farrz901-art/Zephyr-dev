from __future__ import annotations

from pathlib import Path
from typing import Any, TypedDict

from zephyr_core.contracts.v1.run_meta import RunMetaV1


class ArtifactsPathsV1(TypedDict):
    out_dir: str
    run_meta_path: str
    elements_path: str
    normalized_path: str


def build_artifacts_paths_v1(*, out_root: Path, sha256: str) -> ArtifactsPathsV1:
    out_dir = (out_root / sha256).resolve()
    return {
        "out_dir": str(out_dir),
        "run_meta_path": str((out_dir / "run_meta.json").resolve()),
        "elements_path": str((out_dir / "elements.json").resolve()),
        "normalized_path": str((out_dir / "normalized.txt").resolve()),
    }


class DeliveryPayloadV1(TypedDict):
    schema_version: int
    sha256: str
    run_meta: dict[str, Any]
    artifacts: ArtifactsPathsV1


DELIVERY_PAYLOAD_SCHEMA_VERSION = 1


def build_delivery_payload_v1(
    *,
    out_root: Path,
    sha256: str,
    meta: RunMetaV1,
) -> DeliveryPayloadV1:
    # out_dir = (out_root / sha256).resolve()

    payload: DeliveryPayloadV1 = {
        "schema_version": DELIVERY_PAYLOAD_SCHEMA_VERSION,
        "sha256": sha256,
        "run_meta": meta.to_dict(),
        # "artifacts": {
        #     "out_dir": str(out_dir),
        #     "run_meta_path": str((out_dir / "run_meta.json").resolve()),
        #     "elements_path": str((out_dir / "elements.json").resolve()),
        #     "normalized_path": str((out_dir / "normalized.txt").resolve()),
        # },
        "artifacts": build_artifacts_paths_v1(out_root=out_root, sha256=sha256),
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
        "artifacts": build_artifacts_paths_v1(out_root=out_root, sha256=sha256),
    }
    return payload
