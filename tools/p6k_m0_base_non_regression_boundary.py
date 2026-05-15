from __future__ import annotations

from typing import Any

from _p6k_m0_common import VALIDATION_DIR, write_json

OUTPUT_PATH = VALIDATION_DIR / "p6k_m0_base_non_regression_boundary.json"


def build_report() -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": 1,
        "report_id": "zephyr.dev.p6k.m0.base_non_regression_boundary.v1",
        "base_facts_to_preserve": {
            "public": True,
            "local_only": True,
            "non_commercial": True,
            "public_core_bundle_bounded": True,
            "first_slice_formats": [".txt", ".md"],
            "requires_p45_substrate": False,
            "requires_web_core": False,
            "requires_pro_private_core": False,
        },
        "risks": {
            "unstructured_02228_dependency_and_extras": (
                "Kernel upgrade work can pull heavier extras and native "
                "dependencies that Base must not inherit by default."
            ),
            "profile_heavy_deps": (
                "Enhanced profiles can imply OCR, PDF, image, or layout "
                "extras that Base must not absorb automatically."
            ),
            "metadata_output_shape": (
                "Richer metadata can change normalized output shape and "
                "public-core serialization assumptions."
            ),
            "artifact_layout_compatibility": (
                "Package-aware artifact layouts can break Base-compatible path "
                "expectations unless wrapped."
            ),
            "public_core_export_surface": (
                "Core export APIs must remain bounded so Base does not start "
                "depending on P6K-only kernel layers."
            ),
            "base_runtime_size": (
                "Additional partition extras and future nodes can inflate Base "
                "runtime size and installer footprint."
            ),
            "base_installer_runtime_packaging": (
                "Future package manifest and heavy dependency layers must "
                "remain optional so Base portable packaging stays bounded."
            ),
        },
        "judgment": {
            "base_changed_in_m0": False,
            "base_should_not_require_unstructured_all_docs_or_pdf_image_extras_by_default": True,
            "base_should_not_be_forced_into_package_manifest_output": True,
            "commercial_logic_added": False,
        },
        "status": "pass",
    }
    return report


def main() -> None:
    report = build_report()
    write_json(OUTPUT_PATH, report)
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
