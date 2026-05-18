from __future__ import annotations

import hashlib
import json
import mimetypes
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from zephyr_core import ErrorCode, ZephyrError
from zephyr_core.contracts.v2.package_identity import build_artifact_id_v1, build_package_id_v1
from zephyr_core.contracts.v2.package_manifest import (
    ARTIFACT_DESCRIPTOR_SCHEMA_VERSION,
    PACKAGE_MANIFEST_SCHEMA_VERSION,
    ArtifactDescriptorV1,
    PackageManifestV1,
    PackageRunMetaV1,
    validate_package_manifest_v1,
)


@dataclass(frozen=True, slots=True)
class ArtifactBuildSpecV1:
    path: Path
    artifact_kind: str
    role: str
    required: bool
    producer: str
    media_type: str | None = None
    created_at_utc: str | None = None
    metadata: dict[str, Any] | None = None


def _canonical_json_text(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_relative_path(*, out_dir: Path, path: Path) -> str:
    resolved_out_dir = out_dir.resolve()
    resolved_path = path.resolve()
    try:
        relative = resolved_path.relative_to(resolved_out_dir)
        return relative.as_posix()
    except ValueError:
        return resolved_path.name


def build_package_run_meta_v1(
    *,
    run_meta: Mapping[str, object],
    profile: str | None,
    source_kind: str | None,
    flow_kind: str | None,
) -> PackageRunMetaV1:
    engine_obj = run_meta.get("engine")
    engine = cast(
        "Mapping[str, object] | None",
        engine_obj if isinstance(engine_obj, Mapping) else None,
    )
    run_id_obj = run_meta.get("run_id")
    pipeline_version_obj = run_meta.get("pipeline_version")
    timestamp_obj = run_meta.get("timestamp_utc")
    engine_name_obj = engine.get("name") if engine is not None else None
    engine_backend_obj = engine.get("backend") if engine is not None else None
    strategy_obj = engine.get("strategy") if engine is not None else None
    return {
        "run_id": run_id_obj if isinstance(run_id_obj, str) else None,
        "pipeline_version": pipeline_version_obj if isinstance(pipeline_version_obj, str) else None,
        "engine_name": engine_name_obj if isinstance(engine_name_obj, str) else None,
        "engine_backend": engine_backend_obj if isinstance(engine_backend_obj, str) else None,
        "profile": profile,
        "strategy": strategy_obj if isinstance(strategy_obj, str) else None,
        "source_kind": source_kind,
        "flow_kind": flow_kind,
        "created_at_utc": timestamp_obj if isinstance(timestamp_obj, str) else None,
    }


def build_artifact_descriptor_v1(
    *,
    package_id: str,
    out_dir: Path,
    spec: ArtifactBuildSpecV1,
) -> ArtifactDescriptorV1 | None:
    path = spec.path.resolve()
    if not path.exists():
        if spec.required:
            raise ZephyrError(
                ErrorCode.DELIVERY_INVALID_PAYLOAD,
                f"Required package artifact missing: {spec.path.name}",
                details={
                    "artifact_kind": spec.artifact_kind,
                    "role": spec.role,
                    "path": str(path),
                },
            )
        return None

    relative_path = _normalize_relative_path(out_dir=out_dir, path=path)
    media_type = spec.media_type
    if media_type is None:
        guessed_type, _ = mimetypes.guess_type(path.name)
        media_type = guessed_type

    descriptor: ArtifactDescriptorV1 = {
        "schema_version": ARTIFACT_DESCRIPTOR_SCHEMA_VERSION,
        "artifact_id": build_artifact_id_v1(
            package_id=package_id,
            artifact_kind=spec.artifact_kind,
            role=spec.role,
            relative_path=relative_path,
            producer=spec.producer,
        ),
        "artifact_kind": spec.artifact_kind,
        "role": spec.role,
        "path": str(path),
        "relative_path": relative_path,
        "media_type": media_type,
        "sha256": _sha256_path(path),
        "size_bytes": path.stat().st_size,
        "required": spec.required,
        "producer": spec.producer,
        "created_at_utc": spec.created_at_utc,
    }
    if spec.metadata is not None:
        descriptor["metadata"] = dict(spec.metadata)
    return descriptor


def _sorted_descriptors(
    descriptors: Sequence[ArtifactDescriptorV1],
) -> list[ArtifactDescriptorV1]:
    return sorted(
        descriptors,
        key=lambda item: (
            item["artifact_kind"],
            item["role"],
            item["relative_path"],
            item["producer"],
            item["artifact_id"],
        ),
    )


def _primary_artifact_id(descriptors: Sequence[ArtifactDescriptorV1]) -> str | None:
    preferred_roles = (
        "primary_text_output",
        "primary_structured_output",
        "structured_records",
        "metadata",
    )
    for role in preferred_roles:
        for descriptor in descriptors:
            if descriptor["role"] == role:
                return descriptor["artifact_id"]
    if descriptors:
        return descriptors[0]["artifact_id"]
    return None


def compute_manifest_sha256_v1(manifest: PackageManifestV1) -> str:
    payload = dict(manifest)
    payload["manifest_sha256"] = None
    return hashlib.sha256(
        _canonical_json_text(cast("Mapping[str, object]", payload)).encode("utf-8")
    ).hexdigest()


def build_package_manifest_v1(
    *,
    out_dir: Path,
    source_sha256: str,
    run_meta: Mapping[str, object],
    artifact_specs: Sequence[ArtifactBuildSpecV1],
    source_id: str | None,
    workflow_id: str | None,
    node_id: str | None,
    profile: str | None,
    source_kind: str | None,
    flow_kind: str | None,
    strategy_identity: str | None,
    created_at_utc: str,
    metadata: dict[str, Any] | None = None,
) -> PackageManifestV1:
    package_run_meta = build_package_run_meta_v1(
        run_meta=run_meta,
        profile=profile,
        source_kind=source_kind,
        flow_kind=flow_kind,
    )
    package_id = build_package_id_v1(
        source_sha256=source_sha256,
        profile=profile,
        strategy_identity=strategy_identity,
        engine_name=package_run_meta["engine_name"],
        engine_backend=package_run_meta["engine_backend"],
        workflow_id=workflow_id,
        node_id=node_id,
        source_id=source_id,
    )
    descriptors = [
        descriptor
        for descriptor in (
            build_artifact_descriptor_v1(package_id=package_id, out_dir=out_dir, spec=spec)
            for spec in artifact_specs
        )
        if descriptor is not None
    ]
    ordered_descriptors = _sorted_descriptors(descriptors)
    manifest: PackageManifestV1 = {
        "schema_version": PACKAGE_MANIFEST_SCHEMA_VERSION,
        "package_id": package_id,
        "source_sha256": source_sha256,
        "source_id": source_id,
        "workflow_id": workflow_id,
        "node_id": node_id,
        "profile": profile,
        "strategy_identity": strategy_identity,
        "package_run_meta": package_run_meta,
        "artifacts": ordered_descriptors,
        "primary_artifact_id": _primary_artifact_id(ordered_descriptors),
        "manifest_sha256": None,
        "created_at_utc": created_at_utc,
    }
    if metadata is not None:
        manifest["metadata"] = dict(metadata)
    manifest["manifest_sha256"] = compute_manifest_sha256_v1(manifest)
    return validate_package_manifest_v1(manifest)


def write_package_manifest_v1(*, path: Path, manifest: PackageManifestV1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(_canonical_json_text(manifest), encoding="utf-8")
    tmp_path.replace(path)
