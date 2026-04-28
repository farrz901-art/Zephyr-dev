from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import (
    DELIVERY_PAYLOAD_SCHEMA_VERSION,
    ArtifactsPathsV1,
    DeliveryContentEvidenceV1,
    DeliveryPayloadV1,
)

DELIVERY_NORMALIZED_TEXT_PREVIEW_MAX_CHARS = 16 * 1024
DELIVERY_RECORDS_PREVIEW_MAX_RECORDS = 10


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


def _truncate_text(*, text: str, max_chars: int) -> tuple[str, bool]:
    if len(text) <= max_chars:
        return text, False
    return text[:max_chars], True


def _read_text_if_available(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        return None


def _elements_count(*, elements_path: Path) -> int | None:
    raw_text = _read_text_if_available(elements_path)
    if raw_text is None:
        return None
    try:
        raw_obj = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(raw_obj, list):
        return None
    return len(cast(list[object], raw_obj))


def _records_preview(
    *,
    records_path: Path,
) -> tuple[list[dict[str, object]], int, bool, str]:
    raw_text = _read_text_if_available(records_path)
    if raw_text is None:
        return ([], 0, False, "missing")

    lines = [line for line in raw_text.splitlines() if line.strip()]
    preview: list[dict[str, object]] = []
    for line in lines[:DELIVERY_RECORDS_PREVIEW_MAX_RECORDS]:
        try:
            parsed_obj = json.loads(line)
        except json.JSONDecodeError:
            return (
                [],
                len(lines),
                len(lines) > DELIVERY_RECORDS_PREVIEW_MAX_RECORDS,
                "invalid_jsonl",
            )
        if not isinstance(parsed_obj, dict):
            return (
                [],
                len(lines),
                len(lines) > DELIVERY_RECORDS_PREVIEW_MAX_RECORDS,
                "invalid_jsonl",
            )
        preview.append(cast(dict[str, object], parsed_obj))
    return (
        preview,
        len(lines),
        len(lines) > DELIVERY_RECORDS_PREVIEW_MAX_RECORDS,
        "available",
    )


def build_delivery_content_evidence_v1(
    *,
    artifacts: ArtifactsPathsV1,
) -> DeliveryContentEvidenceV1:
    evidence: DeliveryContentEvidenceV1 = {}

    normalized_path = Path(artifacts["normalized_path"])
    normalized_text = _read_text_if_available(normalized_path)
    if normalized_text is None:
        evidence["normalized_text_status"] = "missing"
    else:
        preview, truncated = _truncate_text(
            text=normalized_text,
            max_chars=DELIVERY_NORMALIZED_TEXT_PREVIEW_MAX_CHARS,
        )
        evidence["normalized_text_status"] = "available"
        evidence["normalized_text_preview"] = preview
        evidence["normalized_text_len"] = len(normalized_text)
        evidence["normalized_text_sha256"] = hashlib.sha256(
            normalized_text.encode("utf-8")
        ).hexdigest()
        evidence["normalized_text_truncated"] = truncated

    count = _elements_count(elements_path=Path(artifacts["elements_path"]))
    if count is not None:
        evidence["elements_count"] = count

    records_path_raw = artifacts.get("records_path")
    if isinstance(records_path_raw, str):
        records_preview, records_count, records_truncated, records_status = _records_preview(
            records_path=Path(records_path_raw)
        )
        evidence["records_status"] = records_status
        if records_status == "available":
            evidence["records_preview"] = records_preview
            evidence["records_count"] = records_count
            evidence["records_truncated"] = records_truncated
        elif records_count > 0:
            evidence["records_count"] = records_count
            evidence["records_truncated"] = records_truncated

    if "normalized_text_preview" in evidence and "records_preview" in evidence:
        evidence["evidence_kind"] = "normalized_text_and_records_preview_v1"
    elif "normalized_text_preview" in evidence:
        evidence["evidence_kind"] = "normalized_text_preview_v1"
    elif "records_preview" in evidence:
        evidence["evidence_kind"] = "records_preview_v1"
    elif "elements_count" in evidence:
        evidence["evidence_kind"] = "elements_count_only_v1"
    else:
        evidence["evidence_kind"] = "artifact_reference_only_v1"

    return evidence


def build_delivery_payload_v1(
    *,
    out_root: Path,
    sha256: str,
    meta: RunMetaV1,
) -> DeliveryPayloadV1:
    run_meta = meta.to_dict()
    artifacts = build_artifacts_paths_for_run_meta_v1(
        out_root=out_root,
        sha256=sha256,
        run_meta=run_meta,
    )

    payload: DeliveryPayloadV1 = {
        "schema_version": DELIVERY_PAYLOAD_SCHEMA_VERSION,
        "sha256": sha256,
        "run_meta": run_meta,
        "artifacts": artifacts,
        "content_evidence": build_delivery_content_evidence_v1(artifacts=artifacts),
    }
    return payload


def build_delivery_payload_v1_from_run_meta_dict(
    *,
    out_root: Path,
    sha256: str,
    run_meta: dict[str, Any],
) -> DeliveryPayloadV1:
    artifacts = build_artifacts_paths_for_run_meta_v1(
        out_root=out_root,
        sha256=sha256,
        run_meta=run_meta,
    )
    payload: DeliveryPayloadV1 = {
        "schema_version": DELIVERY_PAYLOAD_SCHEMA_VERSION,
        "sha256": sha256,
        "run_meta": run_meta,
        "artifacts": artifacts,
        "content_evidence": build_delivery_content_evidence_v1(artifacts=artifacts),
    }
    return payload
