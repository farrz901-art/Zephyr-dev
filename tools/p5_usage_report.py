from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_usage_facts import (
    P5_USAGE_FACT_MANIFEST_PATH,
    P5_USAGE_FACT_REPORT_PATH,
    P5_USAGE_LINKAGE_CONTRACT_PATH,
    P5_USAGE_OUTPUT_SURFACE_PATH,
    P5_USAGE_SUCCESS_FAILURE_CLASSIFICATION_PATH,
    format_p5_usage_fact_results,
    format_p5_usage_fact_summary,
    render_p5_usage_fact_json,
    validate_p5_usage_fact_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4-S11 raw usage-fact package."
    )
    parser.add_argument(
        "--print-manifest-path",
        action="store_true",
        help="Print the canonical P5-M4-S11 usage fact manifest path and exit.",
    )
    parser.add_argument(
        "--print-output-surface-path",
        action="store_true",
        help="Print the canonical P5-M4-S11 usage output surface path and exit.",
    )
    parser.add_argument(
        "--print-linkage-contract-path",
        action="store_true",
        help="Print the canonical P5-M4-S11 usage linkage contract path and exit.",
    )
    parser.add_argument(
        "--print-success-failure-path",
        action="store_true",
        help="Print the canonical P5-M4-S11 success/failure classification path and exit.",
    )
    parser.add_argument(
        "--print-report-path",
        action="store_true",
        help="Print the canonical P5-M4-S11 usage report path and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the machine-readable raw usage-fact bundle JSON and exit.",
    )
    parser.add_argument(
        "--check-artifacts",
        action="store_true",
        help="Validate raw usage-fact artifacts and return non-zero on failure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_manifest_path:
        print(P5_USAGE_FACT_MANIFEST_PATH)
        return 0
    if args.print_output_surface_path:
        print(P5_USAGE_OUTPUT_SURFACE_PATH)
        return 0
    if args.print_linkage_contract_path:
        print(P5_USAGE_LINKAGE_CONTRACT_PATH)
        return 0
    if args.print_success_failure_path:
        print(P5_USAGE_SUCCESS_FAILURE_CLASSIFICATION_PATH)
        return 0
    if args.print_report_path:
        print(P5_USAGE_FACT_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_usage_fact_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_usage_fact_artifacts()
        print(format_p5_usage_fact_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_usage_fact_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
