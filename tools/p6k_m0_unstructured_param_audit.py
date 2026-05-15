from __future__ import annotations

from typing import Any

from _p6k_m0_common import (
    PROFILE_PLAN,
    REQUIRED_CLI_FLAGS,
    REQUIRED_METADATA_FIELDS,
    TARGET_PARTITION_PARAMS,
    TARGET_UNSTRUCTURED_VERSION,
    VALIDATION_DIR,
    argparse_flags,
    benchmark_plan,
    current_image_partition_signature,
    current_partition_signature,
    current_pdf_partition_signature,
    current_unstructured_version,
    explicit_metadata_test_coverage,
    file_contains,
    function_has_var_keyword,
    function_parameter_names,
    write_json,
)

OUTPUT_PATH = VALIDATION_DIR / "p6k_m0_unstructured_02228_gap.json"


def build_report() -> dict[str, Any]:
    current_version = current_unstructured_version()
    wrapper_params = function_parameter_names(
        "packages/uns-stream/src/uns_stream/partition/auto.py",
        "partition",
    )
    service_has_kwargs = function_has_var_keyword(
        "packages/uns-stream/src/uns_stream/service.py",
        "partition_file",
    )
    local_backend_has_kwargs = function_has_var_keyword(
        "packages/uns-stream/src/uns_stream/backends/local_unstructured.py",
        "partition_elements",
    )
    current_flags = argparse_flags("packages/zephyr-ingest/src/zephyr_ingest/cli.py")
    missing_cli_flags = [flag for flag in REQUIRED_CLI_FLAGS if flag not in current_flags]
    explicit_metadata_fields = explicit_metadata_test_coverage()
    missing_metadata_fields = [
        field for field in REQUIRED_METADATA_FIELDS if field not in explicit_metadata_fields
    ]
    missing_wrapper_params = [
        field
        for field in TARGET_PARTITION_PARAMS
        if field != "**kwargs" and field not in wrapper_params
    ]
    profile_layer_defined = file_contains(
        "packages/uns-stream/src/uns_stream/_internal/enhanced_partition.py",
        "supported_partition_profiles",
    )

    http_api_alias_support = file_contains(
        "packages/uns-stream/src/uns_stream/backends/http_uns_api.py",
        "pdf_infer_table_structure",
    ) and file_contains(
        "packages/uns-stream/src/uns_stream/backends/http_uns_api.py",
        "infer_table_structure",
    )

    parameter_layer_matrix: dict[str, dict[str, object]] = {}
    for parameter in TARGET_PARTITION_PARAMS:
        wrapper_exposed = parameter in wrapper_params
        parameter_layer_matrix[parameter] = {
            "unstructured_partition_support": True,
            "wrapper_exposed": wrapper_exposed,
            "backend_can_pass_local": local_backend_has_kwargs,
            "backend_can_pass_service": service_has_kwargs,
            "profile_layer_defined": profile_layer_defined,
            "cli_exposed": (
                False
                if parameter == "**kwargs"
                else f"--{parameter.replace('_', '-')}" in current_flags
            ),
            "metadata_preservation_guarded": parameter in explicit_metadata_fields,
            "benchmark_plan_recorded": True,
        }

    upgrade_risks = [
        "auto.partition exposes richer parameters than Zephyr wrapper surface exports."
        if missing_wrapper_params
        else "Wrapper surface now covers the planned enhanced partition parameter set.",
        (
            "metadata normalization currently allows extras but explicit "
            "preservation tests do not protect target fields."
        )
        if missing_metadata_fields
        else "Explicit metadata preservation guards cover the target enhanced fields.",
        "CLI does not yet expose enhanced partition profile controls."
        if missing_cli_flags
        else "CLI exposes the planned enhanced partition profile controls.",
        "No benchmark evidence is frozen yet for Chinese invoice and contract samples.",
    ]
    if current_version != TARGET_UNSTRUCTURED_VERSION:
        upgrade_risks.insert(
            0,
            f"Current environment is still on unstructured {current_version}, "
            f"not {TARGET_UNSTRUCTURED_VERSION}.",
        )

    report: dict[str, Any] = {
        "schema_version": 1,
        "report_id": "zephyr.dev.p6k.m0.unstructured_02228_gap.v1",
        "current_unstructured_version": current_version,
        "target_unstructured_version": TARGET_UNSTRUCTURED_VERSION,
        "current_partition_signature": current_partition_signature(),
        "current_pdf_partition_signature": current_pdf_partition_signature(),
        "current_image_partition_signature": current_image_partition_signature(),
        "wrapper_exposed_params": wrapper_params,
        "backend_passthrough_status": {
            "service_partition_kwargs": service_has_kwargs,
            "local_backend_kwargs": local_backend_has_kwargs,
            "http_api_alias_support_for_infer_table_structure": http_api_alias_support,
            "status": "backend passthrough exists in embryo form",
        },
        "parameter_layer_matrix": parameter_layer_matrix,
        "missing_wrapper_params": missing_wrapper_params,
        "missing_profile_layer": not profile_layer_defined,
        "profile_plan_frozen_for_m1": PROFILE_PLAN,
        "metadata_fields_explicitly_guarded_now": explicit_metadata_fields,
        "missing_metadata_guards_for_m1": missing_metadata_fields,
        "missing_cli_flags_for_m1": missing_cli_flags,
        "benchmark_plan_for_m1": benchmark_plan(),
        "upgrade_risks": upgrade_risks,
        "base_public_core_risk": [
            "Dependency and extras growth can enlarge Base bundle size if "
            "kernel changes are back-ported carelessly.",
            "Metadata shape changes can break Base public-core compatibility wrappers.",
            "Heavy profile defaults can unintentionally pull OCR, image, or PDF extras into Base.",
        ],
        "judgment": {
            "underlying_partition_is_existing_unstructured_capability": True,
            "zephyr_should_not_rewrite_partition_backend": True,
            "zephyr_should_implement_selective_enhanced_profiles": True,
            "zephyr_should_not_default_enable_all_parameters": True,
            "zephyr_should_not_change_vlm_or_ocr_only_route_in_m1": True,
            "zephyr_should_preserve_governance_metadata": [
                "run_id",
                "pipeline_version",
                "sha256",
                "size_bytes",
                "engine version",
                "strategy",
                "duration",
                "warnings and retryability",
            ],
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
