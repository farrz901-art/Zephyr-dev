from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_source_contracts import (
    P5_SOURCE_CONTRACT_MATRIX_PATH,
    P5_SOURCE_CONTRACT_REPORT_PATH,
    P5_SOURCE_SURFACE_CLASSIFICATION_PATH,
    P5_SOURCE_USAGE_LINKAGE_PATH,
    P5_USAGE_RUNTIME_CONTRACT_PATH,
    P5_USAGE_RUNTIME_OUTPUT_SURFACE_PATH,
    P5_USAGE_RUNTIME_REPORT_PATH,
    build_p5_source_contract_matrix,
    format_p5_source_contract_results,
    format_p5_source_contract_summary,
    render_p5_source_contract_json,
    validate_p5_source_contract_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4-S12 source-contract and runtime-usage package."
    )
    parser.add_argument("--print-matrix-path", action="store_true")
    parser.add_argument("--print-classification-path", action="store_true")
    parser.add_argument("--print-usage-runtime-contract-path", action="store_true")
    parser.add_argument("--print-usage-runtime-output-path", action="store_true")
    parser.add_argument("--print-source-usage-linkage-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-usage-report-path", action="store_true")
    parser.add_argument("--list-sources", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def _print_sources() -> None:
    matrix = build_p5_source_contract_matrix()
    for section in ("uns_stream_sources", "it_stream_sources"):
        sources = matrix[section]
        if not isinstance(sources, list):
            continue
        for source in sources:
            if isinstance(source, dict):
                print(source.get("source_id"))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_matrix_path:
        print(P5_SOURCE_CONTRACT_MATRIX_PATH)
        return 0
    if args.print_classification_path:
        print(P5_SOURCE_SURFACE_CLASSIFICATION_PATH)
        return 0
    if args.print_usage_runtime_contract_path:
        print(P5_USAGE_RUNTIME_CONTRACT_PATH)
        return 0
    if args.print_usage_runtime_output_path:
        print(P5_USAGE_RUNTIME_OUTPUT_SURFACE_PATH)
        return 0
    if args.print_source_usage_linkage_path:
        print(P5_SOURCE_USAGE_LINKAGE_PATH)
        return 0
    if args.print_report_path:
        print(P5_SOURCE_CONTRACT_REPORT_PATH)
        return 0
    if args.print_usage_report_path:
        print(P5_USAGE_RUNTIME_REPORT_PATH)
        return 0
    if args.list_sources:
        _print_sources()
        return 0
    if args.json:
        print(render_p5_source_contract_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_source_contract_artifacts()
        print(format_p5_source_contract_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_source_contract_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
