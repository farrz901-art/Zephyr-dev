from __future__ import annotations

from typing import Any

from _p6k_m0_common import VALIDATION_DIR, write_json

OUTPUT_PATH = VALIDATION_DIR / "p6k_m0_destination_package_mapping_gap.json"


def build_report() -> dict[str, Any]:
    destination_reports = {
        "filesystem": {
            "current_behavior": (
                "Writes artifact directory rooted by sha256; does not emit "
                "DeliveryPayloadV1 externally."
            ),
            "content_evidence_carried": "local artifact files only",
            "package_mapping_exists": False,
            "future_mapping": "package_id/manifest.json plus package-scoped artifact files",
            "mapping_change_level": "direct_compatible",
        },
        "s3": {
            "current_behavior": (
                "Writes one DeliveryPayloadV1 JSON object keyed by "
                "idempotency key."
            ),
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": "prefix by package_id with manifest and artifact objects",
            "mapping_change_level": "direct_compatible",
        },
        "kafka": {
            "current_behavior": (
                "Publishes DeliveryPayloadV1 JSON with key based on delivery "
                "identity."
            ),
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": "key=package_id:artifact_id with package envelope or artifact event",
            "mapping_change_level": "direct_compatible",
        },
        "webhook": {
            "current_behavior": "POSTs DeliveryPayloadV1 to a single endpoint.",
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": "package envelope or artifact event webhook shape",
            "mapping_change_level": "direct_compatible",
        },
        "opensearch": {
            "current_behavior": (
                "Indexes a document containing delivery_identity and nested "
                "payload."
            ),
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": "doc id = package_id:artifact_id or chunk id with package metadata",
            "mapping_change_level": "needs_mapping_change",
        },
        "loki": {
            "current_behavior": "Pushes one payload JSON line with delivery labels.",
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": "package event labels plus artifact event JSON log line",
            "mapping_change_level": "direct_compatible",
        },
        "clickhouse": {
            "current_behavior": "Appends one row with identity key and payload_json.",
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": (
                "package/artifact/element or chunk tables keyed by package_id "
                "and artifact_id"
            ),
            "mapping_change_level": "needs_mapping_change",
        },
        "mongodb": {
            "current_behavior": (
                "Upserts one document keyed by delivery identity with nested "
                "payload."
            ),
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": "package and artifact documents keyed by package_id and artifact_id",
            "mapping_change_level": "needs_mapping_change",
        },
        "sqlite": {
            "current_behavior": "Upserts one row with payload_json keyed by delivery identity.",
            "content_evidence_carried": True,
            "package_mapping_exists": False,
            "future_mapping": "package/artifact tables or compatibility wrapper over payload_json",
            "mapping_change_level": "needs_mapping_change",
        },
        "weaviate": {
            "current_behavior": (
                "Indexes one object from result summary fields, not the full "
                "payload."
            ),
            "content_evidence_carried": "partial",
            "package_mapping_exists": False,
            "future_mapping": "chunk-level objects with package metadata and artifact lineage",
            "mapping_change_level": "needs_mapping_change",
        },
    }

    direct_compatible = sorted(
        name
        for name, data in destination_reports.items()
        if data["mapping_change_level"] == "direct_compatible"
    )
    needs_mapping_changes = sorted(
        name
        for name, data in destination_reports.items()
        if data["mapping_change_level"] == "needs_mapping_change"
    )

    report: dict[str, Any] = {
        "schema_version": 1,
        "report_id": "zephyr.dev.p6k.m0.destination_package_mapping_gap.v1",
        "destinations": destination_reports,
        "direct_compatible": direct_compatible,
        "needs_mapping_changes": needs_mapping_changes,
        "status": "pass",
    }
    return report


def main() -> None:
    report = build_report()
    write_json(OUTPUT_PATH, report)
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
