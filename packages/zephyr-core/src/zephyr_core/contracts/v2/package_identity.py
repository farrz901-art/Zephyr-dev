from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

IDENTITY_NULL_MARKER = "__null__"
IDENTITY_EMPTY_MARKER = "__empty__"


def normalize_identity_part(value: object) -> str:
    if value is None:
        return IDENTITY_NULL_MARKER
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, str):
        normalized = value.replace("\\", "/").strip()
        if normalized == "":
            return IDENTITY_EMPTY_MARKER
        return normalized
    if isinstance(value, Mapping):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return json.dumps(
            list(cast("Sequence[object]", value)),
            ensure_ascii=False,
            sort_keys=False,
            separators=(",", ":"),
        )
    return str(value)


def _canonical_hash(payload: Mapping[str, object]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(body).hexdigest()


def build_package_id_v1(
    *,
    source_sha256: str,
    profile: str | None,
    strategy_identity: str | None,
    engine_name: str | None,
    engine_backend: str | None,
    workflow_id: str | None,
    node_id: str | None,
    source_id: str | None,
) -> str:
    digest = _canonical_hash(
        {
            "namespace": "package_manifest_v1",
            "source_sha256": normalize_identity_part(source_sha256),
            "profile": normalize_identity_part(profile),
            "strategy_identity": normalize_identity_part(strategy_identity),
            "engine_name": normalize_identity_part(engine_name),
            "engine_backend": normalize_identity_part(engine_backend),
            "workflow_id": normalize_identity_part(workflow_id),
            "node_id": normalize_identity_part(node_id),
            "source_id": normalize_identity_part(source_id),
        }
    )
    return f"pkg_v1_{digest}"


def build_artifact_id_v1(
    *,
    package_id: str,
    artifact_kind: str,
    role: str,
    relative_path: str,
    producer: str,
) -> str:
    digest = _canonical_hash(
        {
            "namespace": "artifact_descriptor_v1",
            "package_id": normalize_identity_part(package_id),
            "artifact_kind": normalize_identity_part(artifact_kind),
            "role": normalize_identity_part(role),
            "relative_path": normalize_identity_part(relative_path),
            "producer": normalize_identity_part(producer),
        }
    )
    return f"art_v1_{digest}"
