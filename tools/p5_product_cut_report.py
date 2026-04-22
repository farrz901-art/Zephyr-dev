from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_product_cuts import (
    P5_DEPENDENCY_SLICE_MANIFEST_PATH,
    P5_DESTINATION_SURFACE_CLASSIFICATION_PATH,
    P5_PACKAGING_OUTPUT_MANIFEST_PATH,
    P5_PACKAGING_SKELETON_REPORT_PATH,
    P5_PRODUCT_CUT_MANIFEST_PATH,
    format_p5_product_cut_results,
    format_p5_product_cut_summary,
    render_p5_product_cut_json,
    validate_p5_product_cut_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4-S10 product-cut skeleton package."
    )
    parser.add_argument(
        "--print-product-cut-path",
        action="store_true",
        help="Print the canonical P5-M4-S10 product-cut manifest path and exit.",
    )
    parser.add_argument(
        "--print-dependency-slice-path",
        action="store_true",
        help="Print the canonical P5-M4-S10 dependency-slice manifest path and exit.",
    )
    parser.add_argument(
        "--print-destination-classification-path",
        action="store_true",
        help="Print the canonical P5-M4-S10 destination classification path and exit.",
    )
    parser.add_argument(
        "--print-packaging-output-path",
        action="store_true",
        help="Print the canonical P5-M4-S10 packaging-output manifest path and exit.",
    )
    parser.add_argument(
        "--print-report-path",
        action="store_true",
        help="Print the canonical P5-M4-S10 packaging skeleton report path and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the machine-readable product-cut skeleton bundle JSON and exit.",
    )
    parser.add_argument(
        "--check-artifacts",
        action="store_true",
        help="Validate product-cut skeleton artifacts and return non-zero on failure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_product_cut_path:
        print(P5_PRODUCT_CUT_MANIFEST_PATH)
        return 0
    if args.print_dependency_slice_path:
        print(P5_DEPENDENCY_SLICE_MANIFEST_PATH)
        return 0
    if args.print_destination_classification_path:
        print(P5_DESTINATION_SURFACE_CLASSIFICATION_PATH)
        return 0
    if args.print_packaging_output_path:
        print(P5_PACKAGING_OUTPUT_MANIFEST_PATH)
        return 0
    if args.print_report_path:
        print(P5_PACKAGING_SKELETON_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_product_cut_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_product_cut_artifacts()
        print(format_p5_product_cut_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_product_cut_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
