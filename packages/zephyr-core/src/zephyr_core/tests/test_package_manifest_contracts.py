from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

import pytest

from zephyr_core import ErrorCode, ZephyrError
from zephyr_core.contracts.v2.package_identity import (
    build_artifact_id_v1,
    build_package_id_v1,
    normalize_identity_part,
)
from zephyr_core.contracts.v2.package_manifest import (
    PACKAGE_MANIFEST_SCHEMA_VERSION,
    validate_package_manifest_v1,
)
from zephyr_core.contracts.v2.package_manifest_builder import (
    ArtifactBuildSpecV1,
    build_package_manifest_v1,
    compute_manifest_sha256_v1,
)

pytestmark = [pytest.mark.contract, pytest.mark.fixture]


def _run_meta_dict(
    *,
    engine_name: str = "unstructured",
    backend: str = "local",
) -> dict[str, object]:
    return {
        "run_id": "run-m2",
        "pipeline_version": "p6k-m2",
        "timestamp_utc": "2026-05-18T00:00:00Z",
        "engine": {
            "name": engine_name,
            "backend": backend,
            "version": "0.22.28",
            "strategy": "auto",
        },
        "provenance": {
            "task_id": "task-m2",
            "task_identity_key": '{"kind":"uns","sha256":"sha-doc"}',
            "checkpoint_identity_key": "checkpoint-m2",
        },
        "document": {
            "filename": "invoice.txt",
            "sha256": "sha-doc",
            "mime_type": "text/plain",
            "size_bytes": 5,
            "created_at_utc": "2026-05-18T00:00:00Z",
        },
    }


def test_package_manifest_round_trip_and_hash_are_deterministic(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "run_meta.json").write_text('{"ok":true}\n', encoding="utf-8")
    (out_dir / "elements.json").write_text("[]\n", encoding="utf-8")
    (out_dir / "normalized.txt").write_text("hello\n", encoding="utf-8")

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
    ]

    manifest_a = build_package_manifest_v1(
        out_dir=out_dir,
        source_sha256="sha-doc",
        run_meta=_run_meta_dict(),
        artifact_specs=specs,
        source_id="source.uns.http_document.v1",
        workflow_id=None,
        node_id=None,
        profile="invoice",
        source_kind="http_document_v1",
        flow_kind="uns",
        strategy_identity="auto",
        created_at_utc="2026-05-18T00:00:00Z",
    )
    manifest_b = build_package_manifest_v1(
        out_dir=out_dir,
        source_sha256="sha-doc",
        run_meta=_run_meta_dict(),
        artifact_specs=specs,
        source_id="source.uns.http_document.v1",
        workflow_id=None,
        node_id=None,
        profile="invoice",
        source_kind="http_document_v1",
        flow_kind="uns",
        strategy_identity="auto",
        created_at_utc="2026-05-18T00:00:00Z",
    )

    assert manifest_a["schema_version"] == PACKAGE_MANIFEST_SCHEMA_VERSION
    assert manifest_a["package_id"] == manifest_b["package_id"]
    assert manifest_a["manifest_sha256"] == manifest_b["manifest_sha256"]
    assert compute_manifest_sha256_v1(manifest_a) == manifest_a["manifest_sha256"]

    tampered = dict(manifest_a)
    tampered["manifest_sha256"] = "tampered"
    assert compute_manifest_sha256_v1(cast("Any", tampered)) == manifest_a["manifest_sha256"]

    restored = validate_package_manifest_v1(
        cast("dict[str, object]", json.loads(json.dumps(manifest_a, ensure_ascii=False)))
    )
    assert restored["primary_artifact_id"] == manifest_a["primary_artifact_id"]
    assert restored["artifacts"][0]["relative_path"].count("\\") == 0


def test_package_identity_changes_across_profile_strategy_backend_and_path() -> None:
    stable = build_package_id_v1(
        source_sha256="sha-doc",
        profile="default",
        strategy_identity="auto",
        engine_name="unstructured",
        engine_backend="local",
        workflow_id=None,
        node_id=None,
        source_id="source.uns.http_document.v1",
    )
    same = build_package_id_v1(
        source_sha256="sha-doc",
        profile="default",
        strategy_identity="auto",
        engine_name="unstructured",
        engine_backend="local",
        workflow_id=None,
        node_id=None,
        source_id="source.uns.http_document.v1",
    )
    different_profile = build_package_id_v1(
        source_sha256="sha-doc",
        profile="invoice",
        strategy_identity="auto",
        engine_name="unstructured",
        engine_backend="local",
        workflow_id=None,
        node_id=None,
        source_id="source.uns.http_document.v1",
    )
    different_strategy = build_package_id_v1(
        source_sha256="sha-doc",
        profile="default",
        strategy_identity="hi_res",
        engine_name="unstructured",
        engine_backend="local",
        workflow_id=None,
        node_id=None,
        source_id="source.uns.http_document.v1",
    )
    different_backend = build_package_id_v1(
        source_sha256="sha-doc",
        profile="default",
        strategy_identity="auto",
        engine_name="unstructured",
        engine_backend="uns-api",
        workflow_id=None,
        node_id=None,
        source_id="source.uns.http_document.v1",
    )

    assert stable == same
    assert stable != different_profile
    assert stable != different_strategy
    assert stable != different_backend
    assert normalize_identity_part(r"nested\artifact\file.json") == "nested/artifact/file.json"
    assert build_artifact_id_v1(
        package_id=stable,
        artifact_kind="elements",
        role="primary_structured_output",
        relative_path=r"nested\artifact\file.json",
        producer="zephyr-ingest",
    ) == build_artifact_id_v1(
        package_id=stable,
        artifact_kind="elements",
        role="primary_structured_output",
        relative_path="nested/artifact/file.json",
        producer="zephyr-ingest",
    )


def test_package_manifest_builder_validates_required_and_skips_optional_missing(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "run_meta.json").write_text('{"ok":true}\n', encoding="utf-8")
    os.environ["ZE_M2_TEST_SECRET"] = "super-secret-value"

    manifest = build_package_manifest_v1(
        out_dir=out_dir,
        source_sha256="sha-doc",
        run_meta=_run_meta_dict(),
        artifact_specs=[
            ArtifactBuildSpecV1(
                path=out_dir / "run_meta.json",
                artifact_kind="run_meta",
                role="metadata",
                required=True,
                producer="zephyr-ingest",
            ),
            ArtifactBuildSpecV1(
                path=out_dir / "usage_record.json",
                artifact_kind="usage_record",
                role="usage_fact",
                required=False,
                producer="zephyr-ingest",
            ),
        ],
        source_id="source.uns.http_document.v1",
        workflow_id=None,
        node_id=None,
        profile=None,
        source_kind="http_document_v1",
        flow_kind="uns",
        strategy_identity="auto",
        created_at_utc="2026-05-18T00:00:00Z",
    )
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True)
    assert "super-secret-value" not in encoded
    assert len(manifest["artifacts"]) == 1
    assert manifest["artifacts"][0]["size_bytes"] == (out_dir / "run_meta.json").stat().st_size
    assert manifest["artifacts"][0]["sha256"] is not None

    with pytest.raises(ZephyrError) as exc_info:
        build_package_manifest_v1(
            out_dir=out_dir,
            source_sha256="sha-doc",
            run_meta=_run_meta_dict(),
            artifact_specs=[
                ArtifactBuildSpecV1(
                    path=out_dir / "elements.json",
                    artifact_kind="elements",
                    role="primary_structured_output",
                    required=True,
                    producer="zephyr-ingest",
                )
            ],
            source_id="source.uns.http_document.v1",
            workflow_id=None,
            node_id=None,
            profile=None,
            source_kind="http_document_v1",
            flow_kind="uns",
            strategy_identity="auto",
            created_at_utc="2026-05-18T00:00:00Z",
        )
    assert exc_info.value.code == ErrorCode.DELIVERY_INVALID_PAYLOAD


def test_package_manifest_validator_rejects_missing_required_fields() -> None:
    with pytest.raises((TypeError, ValueError)):
        validate_package_manifest_v1(
            {
                "schema_version": 1,
                "source_sha256": "sha-doc",
            }
        )
