from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, NotRequired, TypedDict, cast

PACKAGE_MANIFEST_SCHEMA_VERSION: Literal[1] = 1
ARTIFACT_DESCRIPTOR_SCHEMA_VERSION: Literal[1] = 1


class ArtifactDescriptorV1(TypedDict):
    schema_version: Literal[1]
    artifact_id: str
    artifact_kind: str
    role: str
    path: str
    relative_path: str
    media_type: str | None
    sha256: str | None
    size_bytes: int | None
    required: bool
    producer: str
    created_at_utc: str | None
    metadata: NotRequired[dict[str, Any]]


class PackageRunMetaV1(TypedDict):
    run_id: str | None
    pipeline_version: str | None
    engine_name: str | None
    engine_backend: str | None
    profile: str | None
    strategy: str | None
    source_kind: str | None
    flow_kind: str | None
    created_at_utc: str | None


class PackageManifestV1(TypedDict):
    schema_version: Literal[1]
    package_id: str
    source_sha256: str
    source_id: str | None
    workflow_id: str | None
    node_id: str | None
    profile: str | None
    strategy_identity: str | None
    package_run_meta: PackageRunMetaV1
    artifacts: list[ArtifactDescriptorV1]
    primary_artifact_id: str | None
    manifest_sha256: str | None
    created_at_utc: str
    metadata: NotRequired[dict[str, Any]]


def _require_str(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise TypeError(f"Package manifest field '{key}' must be a string")
    return value


def _require_optional_str(data: Mapping[str, object], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Package manifest field '{key}' must be a string or null")
    return value


def _require_bool(data: Mapping[str, object], key: str) -> bool:
    value = data.get(key)
    if not isinstance(value, bool):
        raise TypeError(f"Package manifest field '{key}' must be a boolean")
    return value


def _require_optional_int(data: Mapping[str, object], key: str) -> int | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Package manifest field '{key}' must be an integer or null")
    return value


def _require_optional_mapping(data: Mapping[str, object], key: str) -> dict[str, Any] | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError(f"Package manifest field '{key}' must be an object or null")
    return dict(cast("Mapping[str, Any]", value))


def validate_artifact_descriptor_v1(payload: Mapping[str, object]) -> ArtifactDescriptorV1:
    schema_version = payload.get("schema_version")
    if schema_version != ARTIFACT_DESCRIPTOR_SCHEMA_VERSION:
        raise ValueError("Artifact descriptor schema_version must be 1")

    artifact: ArtifactDescriptorV1 = {
        "schema_version": ARTIFACT_DESCRIPTOR_SCHEMA_VERSION,
        "artifact_id": _require_str(payload, "artifact_id"),
        "artifact_kind": _require_str(payload, "artifact_kind"),
        "role": _require_str(payload, "role"),
        "path": _require_str(payload, "path"),
        "relative_path": _require_str(payload, "relative_path"),
        "media_type": _require_optional_str(payload, "media_type"),
        "sha256": _require_optional_str(payload, "sha256"),
        "size_bytes": _require_optional_int(payload, "size_bytes"),
        "required": _require_bool(payload, "required"),
        "producer": _require_str(payload, "producer"),
        "created_at_utc": _require_optional_str(payload, "created_at_utc"),
    }
    metadata = _require_optional_mapping(payload, "metadata")
    if metadata is not None:
        artifact["metadata"] = metadata
    return artifact


def validate_package_run_meta_v1(payload: Mapping[str, object]) -> PackageRunMetaV1:
    return {
        "run_id": _require_optional_str(payload, "run_id"),
        "pipeline_version": _require_optional_str(payload, "pipeline_version"),
        "engine_name": _require_optional_str(payload, "engine_name"),
        "engine_backend": _require_optional_str(payload, "engine_backend"),
        "profile": _require_optional_str(payload, "profile"),
        "strategy": _require_optional_str(payload, "strategy"),
        "source_kind": _require_optional_str(payload, "source_kind"),
        "flow_kind": _require_optional_str(payload, "flow_kind"),
        "created_at_utc": _require_optional_str(payload, "created_at_utc"),
    }


def validate_package_manifest_v1(payload: Mapping[str, object]) -> PackageManifestV1:
    schema_version = payload.get("schema_version")
    if schema_version != PACKAGE_MANIFEST_SCHEMA_VERSION:
        raise ValueError("Package manifest schema_version must be 1")

    package_run_meta_obj = payload.get("package_run_meta")
    if not isinstance(package_run_meta_obj, Mapping):
        raise TypeError("Package manifest field 'package_run_meta' must be an object")

    artifacts_obj = payload.get("artifacts")
    if not isinstance(artifacts_obj, list):
        raise TypeError("Package manifest field 'artifacts' must be a list")

    manifest: PackageManifestV1 = {
        "schema_version": PACKAGE_MANIFEST_SCHEMA_VERSION,
        "package_id": _require_str(payload, "package_id"),
        "source_sha256": _require_str(payload, "source_sha256"),
        "source_id": _require_optional_str(payload, "source_id"),
        "workflow_id": _require_optional_str(payload, "workflow_id"),
        "node_id": _require_optional_str(payload, "node_id"),
        "profile": _require_optional_str(payload, "profile"),
        "strategy_identity": _require_optional_str(payload, "strategy_identity"),
        "package_run_meta": validate_package_run_meta_v1(
            cast("Mapping[str, object]", package_run_meta_obj)
        ),
        "artifacts": [],
        "primary_artifact_id": _require_optional_str(payload, "primary_artifact_id"),
        "manifest_sha256": _require_optional_str(payload, "manifest_sha256"),
        "created_at_utc": _require_str(payload, "created_at_utc"),
    }
    artifacts: list[ArtifactDescriptorV1] = []
    artifact_items = cast("list[object]", artifacts_obj)
    for artifact_obj in artifact_items:
        if not isinstance(artifact_obj, Mapping):
            raise TypeError("Package manifest artifacts entries must be objects")
        artifacts.append(
            validate_artifact_descriptor_v1(cast("Mapping[str, object]", artifact_obj))
        )
    manifest["artifacts"] = artifacts
    metadata = _require_optional_mapping(payload, "metadata")
    if metadata is not None:
        manifest["metadata"] = metadata
    return manifest
