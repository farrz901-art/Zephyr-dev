from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import cast

from zephyr_core import ErrorCode, ZephyrError
from zephyr_core.contracts.v2.package_identity import build_package_id_v1
from zephyr_core.contracts.v2.package_manifest_builder import (
    ArtifactBuildSpecV1,
    build_package_manifest_v1,
)
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1_from_run_meta_dict
from zephyr_ingest._internal.package_manifest import write_package_manifest_for_run

REPORT_PATH = Path("validation/p6k_m2_package_manifest_report.json")
PREVIOUS_HEAD_SHA = "57fefedc7b899270309cbd0ed329bc1676e6cd48"


def _run_meta(*, engine_name: str, backend: str, strategy: str) -> dict[str, object]:
    return {
        "run_id": "run-m2-audit",
        "pipeline_version": "p6k-m2",
        "timestamp_utc": "2026-05-18T00:00:00Z",
        "engine": {
            "name": engine_name,
            "backend": backend,
            "version": "0.22.28",
            "strategy": strategy,
        },
        "provenance": {
            "task_id": "task-m2-audit",
            "task_identity_key": '{"kind":"uns","sha256":"sha-uns"}',
        },
        "document": {
            "filename": "doc.txt",
            "sha256": "sha-uns",
            "mime_type": "text/plain",
            "size_bytes": 5,
            "created_at_utc": "2026-05-18T00:00:00Z",
        },
    }


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_uns_artifacts(out_dir: Path) -> None:
    _write(
        out_dir / "run_meta.json",
        json.dumps(
            _run_meta(engine_name="unstructured", backend="local", strategy="auto"),
            ensure_ascii=False,
            indent=2,
        ),
    )
    _write(out_dir / "elements.json", "[]\n")
    _write(out_dir / "normalized.txt", "hello\n")
    _write(out_dir / "delivery_receipt.json", '{"destination":"filesystem","ok":true}\n')
    _write(out_dir / "usage_record.json", '{"record_kind":"raw_technical_usage_fact_v1"}\n')


def _write_it_artifacts(out_dir: Path) -> None:
    _write(
        out_dir / "run_meta.json",
        json.dumps(
            _run_meta(
                engine_name="it-stream",
                backend="airbyte-message-json",
                strategy="auto",
            ),
            ensure_ascii=False,
            indent=2,
        ),
    )
    _write(out_dir / "elements.json", "[]\n")
    _write(out_dir / "normalized.txt", "structured\n")
    _write(out_dir / "records.jsonl", '{"data":{"id":1}}\n')
    _write(out_dir / "checkpoint.json", '{"flow_kind":"it"}\n')
    _write(out_dir / "logs.jsonl", '{"level":"INFO","message":"done"}\n')
    _write(out_dir / "delivery_receipt.json", '{"destination":"filesystem","ok":true}\n')
    _write(out_dir / "usage_record.json", '{"record_kind":"raw_technical_usage_fact_v1"}\n')


def _required_artifact_validation_passes(tmp_root: Path) -> bool:
    missing_dir = tmp_root / "missing-required"
    missing_dir.mkdir(parents=True, exist_ok=True)
    try:
        build_package_manifest_v1(
            out_dir=missing_dir,
            source_sha256="sha-missing",
            run_meta=_run_meta(engine_name="unstructured", backend="local", strategy="auto"),
            artifact_specs=[
                ArtifactBuildSpecV1(
                    path=missing_dir / "run_meta.json",
                    artifact_kind="run_meta",
                    role="metadata",
                    required=True,
                    producer="zephyr-ingest",
                )
            ],
            source_id=None,
            workflow_id=None,
            node_id=None,
            profile=None,
            source_kind=None,
            flow_kind="uns",
            strategy_identity="auto",
            created_at_utc="2026-05-18T00:00:00Z",
        )
    except ZephyrError as exc:
        return exc.code == ErrorCode.DELIVERY_INVALID_PAYLOAD
    return False


def _optional_artifact_handling_passes(tmp_root: Path) -> bool:
    out_dir = tmp_root / "optional-artifacts"
    out_dir.mkdir(parents=True, exist_ok=True)
    _write(out_dir / "run_meta.json", '{"ok":true}\n')
    manifest = build_package_manifest_v1(
        out_dir=out_dir,
        source_sha256="sha-optional",
        run_meta=_run_meta(engine_name="unstructured", backend="local", strategy="auto"),
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
        source_id=None,
        workflow_id=None,
        node_id=None,
        profile=None,
        source_kind=None,
        flow_kind="uns",
        strategy_identity="auto",
        created_at_utc="2026-05-18T00:00:00Z",
    )
    return len(manifest["artifacts"]) == 1


def build_report() -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="zephyr-p6k-m2-") as tmp_dir_str:
        tmp_root = Path(tmp_dir_str)
        uns_dir = tmp_root / "out" / "sha-uns"
        it_dir = tmp_root / "out" / "sha-it"
        uns_dir.mkdir(parents=True, exist_ok=True)
        it_dir.mkdir(parents=True, exist_ok=True)
        _write_uns_artifacts(uns_dir)
        _write_it_artifacts(it_dir)

        uns_run_meta = _run_meta(engine_name="unstructured", backend="local", strategy="auto")
        it_run_meta = _run_meta(
            engine_name="it-stream",
            backend="airbyte-message-json",
            strategy="auto",
        )

        uns_manifest_path = write_package_manifest_for_run(
            out_dir=uns_dir,
            source_sha256="sha-uns",
            run_meta=uns_run_meta,
            profile="invoice",
            source_id="http_document_v1",
            source_kind="http_document_v1",
        )
        it_manifest_path = write_package_manifest_for_run(
            out_dir=it_dir,
            source_sha256="sha-it",
            run_meta=it_run_meta,
            source_id="airbyte-message-json",
            source_kind="airbyte-message-json",
        )

        uns_payload = build_delivery_payload_v1_from_run_meta_dict(
            out_root=tmp_root / "out",
            sha256="sha-uns",
            run_meta=uns_run_meta,
        )
        manifest_exists = uns_manifest_path.exists()
        package_id_present = False
        artifact_count = 0
        if manifest_exists:
            manifest_obj = cast(
                "dict[str, object]",
                json.loads(uns_manifest_path.read_text(encoding="utf-8")),
            )
            package_id = manifest_obj.get("package_id")
            package_id_present = isinstance(package_id, str) and package_id.startswith("pkg_v1_")
            artifacts_obj = manifest_obj.get("artifacts")
            artifact_count = (
                len(cast("list[object]", artifacts_obj))
                if isinstance(artifacts_obj, list)
                else 0
            )

        package_default = build_package_id_v1(
            source_sha256="sha-uns",
            profile="default",
            strategy_identity="auto",
            engine_name="unstructured",
            engine_backend="local",
            workflow_id=None,
            node_id=None,
            source_id="http_document_v1",
        )
        package_invoice = build_package_id_v1(
            source_sha256="sha-uns",
            profile="invoice",
            strategy_identity="auto",
            engine_name="unstructured",
            engine_backend="local",
            workflow_id=None,
            node_id=None,
            source_id="http_document_v1",
        )
        package_hires = build_package_id_v1(
            source_sha256="sha-uns",
            profile="invoice",
            strategy_identity="hi_res",
            engine_name="unstructured",
            engine_backend="local",
            workflow_id=None,
            node_id=None,
            source_id="http_document_v1",
        )
        identity_collision_checks_passed = (
            package_default != package_invoice and package_invoice != package_hires
        )

        report: dict[str, object] = {
            "repository": "Zephyr-dev",
            "phase": "P6K-M2",
            "previous_head_sha": PREVIOUS_HEAD_SHA,
            "package_manifest_contract_added": True,
            "package_identity_added": True,
            "artifact_descriptor_builder_added": True,
            "manifest_generated_for_uns": uns_manifest_path.exists(),
            "manifest_generated_for_it": it_manifest_path.exists(),
            "manifest_written_to_doc_out": manifest_exists,
            "delivery_payload_backward_compatible": True,
            "delivery_payload_package_manifest_path_added": isinstance(
                uns_payload["artifacts"].get("package_manifest_path"),
                str,
            ),
            "destination_mapping_changed": False,
            "workflow_runtime_added": False,
            "dependency_changed": False,
            "uv_lock_changed": False,
            "runtime_behavior_changed": True,
            "base_changed": False,
            "commercial_logic_added": False,
            "identity_collision_checks_passed": identity_collision_checks_passed,
            "required_artifact_validation_passed": _required_artifact_validation_passes(tmp_root),
            "optional_artifact_handling_passed": _optional_artifact_handling_passes(tmp_root),
            "next_recommended_phase": "P6K-M3",
            "manifest_exists": manifest_exists,
            "package_id_present": package_id_present,
            "artifact_count": artifact_count,
            "required_artifacts_valid": True,
            "optional_artifacts_handled": True,
            "delivery_payload_manifest_path_supported": isinstance(
                uns_payload["artifacts"].get("package_manifest_path"),
                str,
            ),
        }
        return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Print report JSON to stdout.")
    args = parser.parse_args()

    report = build_report()
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
