from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from it_stream import process_file
from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    RunMetaV1,
    ZephyrElement,
)
from zephyr_core.contracts.v1.run_meta import EngineMetaV1
from zephyr_core.contracts.v2.package_manifest import validate_package_manifest_v1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_payload import (
    build_delivery_payload_v1,
    build_delivery_payload_v1_from_run_meta_dict,
)
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.runner import RunnerConfig, run_documents

pytestmark = [pytest.mark.contract, pytest.mark.fixture]


def _uns_result() -> PartitionResult:
    return PartitionResult(
        document=DocumentMetadata(
            filename="doc.txt",
            mime_type="text/plain",
            sha256="sha-uns",
            size_bytes=5,
            created_at_utc="2026-05-18T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured",
            backend="local",
            version="0.22.28",
            strategy=PartitionStrategy.AUTO,
        ),
        elements=[ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})],
        normalized_text="Hello",
    )


def _meta(*, engine_name: str = "unstructured", backend: str = "local") -> RunMetaV1:
    return RunMetaV1(
        run_id="r-m2",
        pipeline_version="p-m2",
        timestamp_utc="2026-05-18T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        engine=EngineMetaV1(
            name=engine_name,
            backend=backend,
            version="0.22.28",
            strategy="auto",
        ),
    )


def test_filesystem_destination_writes_uns_package_manifest(tmp_path: Path) -> None:
    dest = FilesystemDestination()
    receipt = dest(out_root=tmp_path / "out", sha256="sha-uns", meta=_meta(), result=_uns_result())

    assert receipt.ok is True
    manifest_path = tmp_path / "out" / "sha-uns" / "package_manifest.json"
    assert manifest_path.exists()
    manifest = validate_package_manifest_v1(
        cast("dict[str, object]", json.loads(manifest_path.read_text(encoding="utf-8")))
    )
    assert manifest["source_sha256"] == "sha-uns"
    assert manifest["package_id"] != "sha-uns"
    assert {artifact["artifact_kind"] for artifact in manifest["artifacts"]} >= {
        "run_meta",
        "elements",
        "normalized_text",
    }
    for artifact in manifest["artifacts"]:
        assert Path(artifact["path"]).exists()
        assert "\\" not in artifact["relative_path"]


def test_filesystem_destination_writes_it_package_manifest(tmp_path: Path) -> None:
    dest = FilesystemDestination()
    input_path = tmp_path / "messages.json"
    input_path.write_text(
        json.dumps(
            {
                "messages": [
                    {"type": "RECORD", "record": {"stream": "customers", "data": {"id": 1}}},
                    {"type": "STATE", "state": {"data": {"cursor": "2026-01-01"}}},
                    {"type": "LOG", "log": {"level": "INFO", "message": "done"}},
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    result = process_file(
        filename=str(input_path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-it",
        size_bytes=input_path.stat().st_size,
    )
    receipt = dest(
        out_root=tmp_path / "out",
        sha256="sha-it",
        meta=_meta(engine_name="it-stream", backend="airbyte-message-json"),
        result=result,
    )

    assert receipt.ok is True
    manifest_path = tmp_path / "out" / "sha-it" / "package_manifest.json"
    manifest = validate_package_manifest_v1(
        cast("dict[str, object]", json.loads(manifest_path.read_text(encoding="utf-8")))
    )
    assert manifest["package_run_meta"]["flow_kind"] == "it"
    assert {artifact["artifact_kind"] for artifact in manifest["artifacts"]} >= {
        "run_meta",
        "elements",
        "normalized_text",
        "it_records",
        "checkpoint",
        "structured_logs",
    }


def test_delivery_payload_adds_package_manifest_path_and_manifest_evidence(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    out_dir = out_root / "sha-uns"
    out_dir.mkdir(parents=True)
    (out_dir / "run_meta.json").write_text('{"ok":true}\n', encoding="utf-8")
    (out_dir / "elements.json").write_text("[]\n", encoding="utf-8")
    (out_dir / "normalized.txt").write_text("hello\n", encoding="utf-8")
    (out_dir / "package_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "package_id": "pkg_v1_demo",
                "source_sha256": "sha-uns",
                "source_id": None,
                "workflow_id": None,
                "node_id": None,
                "profile": None,
                "strategy_identity": "auto",
                "package_run_meta": {
                    "run_id": "r-m2",
                    "pipeline_version": "p-m2",
                    "engine_name": "unstructured",
                    "engine_backend": "local",
                    "profile": None,
                    "strategy": "auto",
                    "source_kind": None,
                    "flow_kind": "uns",
                    "created_at_utc": "2026-05-18T00:00:00Z",
                },
                "artifacts": [
                    {
                        "schema_version": 1,
                        "artifact_id": "art_v1_a",
                        "artifact_kind": "run_meta",
                        "role": "metadata",
                        "path": str((out_dir / "run_meta.json").resolve()),
                        "relative_path": "run_meta.json",
                        "media_type": "application/json",
                        "sha256": None,
                        "size_bytes": 12,
                        "required": True,
                        "producer": "zephyr-ingest",
                        "created_at_utc": None,
                    },
                    {
                        "schema_version": 1,
                        "artifact_id": "art_v1_b",
                        "artifact_kind": "elements",
                        "role": "primary_structured_output",
                        "path": str((out_dir / "elements.json").resolve()),
                        "relative_path": "elements.json",
                        "media_type": "application/json",
                        "sha256": None,
                        "size_bytes": 3,
                        "required": True,
                        "producer": "zephyr-ingest",
                        "created_at_utc": None,
                    },
                ],
                "primary_artifact_id": "art_v1_b",
                "manifest_sha256": "abc",
                "created_at_utc": "2026-05-18T00:00:00Z",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = build_delivery_payload_v1_from_run_meta_dict(
        out_root=out_root,
        sha256="sha-uns",
        run_meta=_meta().to_dict(),
    )
    package_manifest_path = payload["artifacts"].get("package_manifest_path")
    assert isinstance(package_manifest_path, str)
    assert package_manifest_path.endswith("package_manifest.json")
    evidence = payload.get("content_evidence", {})
    assert evidence.get("package_manifest_available") is True
    assert evidence.get("artifact_count") == 2
    assert evidence.get("primary_artifact_kinds") == ["run_meta", "elements"]


def test_runner_refreshes_package_manifest_with_profile_and_source_identity(tmp_path: Path) -> None:
    doc_path = tmp_path / "source.txt"
    doc_path.write_text("hello", encoding="utf-8")
    doc = DocumentRef(
        uri=str(doc_path),
        source="http_document_v1",
        discovered_at_utc="2026-05-18T00:00:00Z",
        filename="source.txt",
        extension=".txt",
        size_bytes=doc_path.stat().st_size,
    )
    cfg = RunnerConfig(
        out_root=tmp_path / "out",
        strategy=PartitionStrategy.AUTO,
        destination=FilesystemDestination(),
        partition_options={"profile": "invoice"},
    )
    ctx = RunContext.new(
        pipeline_version="p-m2",
        run_id="run-m2",
        timestamp_utc="2026-05-18T00:00:00Z",
    )

    def fake_partition(**kwargs: Any) -> PartitionResult:
        del kwargs
        return _uns_result()

    stats = run_documents(
        docs=[doc],
        cfg=cfg,
        ctx=ctx,
        partition_fn=fake_partition,
        destination=FilesystemDestination(),
    )

    assert stats.success == 1
    out_dirs = [path for path in (tmp_path / "out").iterdir() if path.is_dir()]
    assert len(out_dirs) == 1
    manifest = validate_package_manifest_v1(
        cast(
            "dict[str, object]",
            json.loads((out_dirs[0] / "package_manifest.json").read_text(encoding="utf-8")),
        )
    )
    assert manifest["profile"] == "invoice"
    assert manifest["source_id"] == "http_document_v1"
    assert {artifact["artifact_kind"] for artifact in manifest["artifacts"]} >= {
        "delivery_receipt",
        "usage_record",
    }


def test_build_delivery_payload_v1_keeps_old_payload_compatibility_without_manifest_file(
    tmp_path: Path,
) -> None:
    payload = build_delivery_payload_v1(out_root=tmp_path / "out", sha256="sha-old", meta=_meta())
    assert payload["schema_version"] == 1
    assert payload["artifacts"]["run_meta_path"].endswith("run_meta.json")
    assert payload["artifacts"].get("package_manifest_path") is None
    evidence = payload.get("content_evidence", {})
    assert evidence.get("package_manifest_available") is None
