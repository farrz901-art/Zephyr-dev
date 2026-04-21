from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_capability_domains import (
    P5_BUILD_CUT_MANIFEST_PATH,
    P5_CAPABILITY_DOMAIN_MATRIX_PATH,
    P5_CAPABILITY_DOMAIN_REPORT_PATH,
    P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH,
    P5_OUTPUT_BOUNDARY_REPORT_PATH,
    format_p5_capability_domain_results,
    format_p5_capability_domain_summary,
    render_p5_capability_domain_json,
    validate_p5_capability_domain_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4-S9 capability-domain package."
    )
    parser.add_argument(
        "--print-matrix-path",
        action="store_true",
        help="Print the canonical P5-M4-S9 capability-domain matrix path and exit.",
    )
    parser.add_argument(
        "--print-report-path",
        action="store_true",
        help="Print the canonical P5-M4-S9 capability-domain report path and exit.",
    )
    parser.add_argument(
        "--print-dependency-manifest-path",
        action="store_true",
        help="Print the canonical P5-M4-S9 dependency-boundary manifest path and exit.",
    )
    parser.add_argument(
        "--print-build-cut-manifest-path",
        action="store_true",
        help="Print the canonical P5-M4-S9 build-cut manifest path and exit.",
    )
    parser.add_argument(
        "--print-output-report-path",
        action="store_true",
        help="Print the canonical P5-M4-S9 output-boundary report path and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the machine-readable capability-domain bundle JSON and exit.",
    )
    parser.add_argument(
        "--check-artifacts",
        action="store_true",
        help="Validate capability-domain artifacts and return non-zero on failure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_matrix_path:
        print(P5_CAPABILITY_DOMAIN_MATRIX_PATH)
        return 0
    if args.print_report_path:
        print(P5_CAPABILITY_DOMAIN_REPORT_PATH)
        return 0
    if args.print_dependency_manifest_path:
        print(P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH)
        return 0
    if args.print_build_cut_manifest_path:
        print(P5_BUILD_CUT_MANIFEST_PATH)
        return 0
    if args.print_output_report_path:
        print(P5_OUTPUT_BOUNDARY_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_capability_domain_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_capability_domain_artifacts()
        print(format_p5_capability_domain_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_capability_domain_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
