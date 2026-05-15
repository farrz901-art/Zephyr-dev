from __future__ import annotations

from typing import Any

from _p6k_m0_common import VALIDATION_DIR, write_json

OUTPUT_PATH = VALIDATION_DIR / "p6k_m0_package_manifest_gap.json"


def build_report() -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": 1,
        "report_id": "zephyr.dev.p6k.m0.package_manifest_gap.v1",
        "current_delivery_payload_v1": {
            "schema_version": 1,
            "fields": ["schema_version", "sha256", "run_meta", "artifacts", "content_evidence"],
            "artifacts_paths_v1": [
                "out_dir",
                "run_meta_path",
                "elements_path",
                "normalized_path",
                "records_path?",
                "state_path?",
                "logs_path?",
            ],
        },
        "current_artifacts": {
            "uns": [
                "elements.json",
                "normalized.txt",
                "run_meta.json",
                "delivery_receipt.json",
                "batch_report.json",
            ],
            "it": [
                "records.jsonl",
                "checkpoint.json",
                "logs.jsonl",
                "run_meta.json",
                "delivery_receipt.json",
                "batch_report.json",
            ],
            "shared_content_evidence": True,
        },
        "missing_package_manifest_concepts": [
            "package_id",
            "artifact_id",
            "workflow_id",
            "node_id",
            "source_id",
            "profile",
            "strategy_identity",
            "package_manifest.json",
            "artifact_descriptors",
            "package_run_meta",
            "node_output_identity",
            "package_aware_content_evidence",
        ],
        "delivery_payload_compatibility_issue": (
            "DeliveryPayloadV1 points at artifact paths and summary evidence, "
            "but it cannot describe "
            "multiple package-scoped artifacts or stable node-level identities."
        ),
        "collision_risks": [
            "Current artifact layout is rooted by sha256, so the same content "
            "processed under multiple profiles or strategies can collide.",
            "Delivery payload does not distinguish artifact kinds with stable artifact IDs.",
            "Destination replay can identify a run, but not a package graph with per-node outputs.",
        ],
        "p6k_m2_expected_scope": [
            "PackageManifestV1",
            "package_id and artifact descriptors",
            "strategy and profile identity",
            "package-aware content evidence",
        ],
        "status": "pass",
    }
    return report


def main() -> None:
    report = build_report()
    write_json(OUTPUT_PATH, report)
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
