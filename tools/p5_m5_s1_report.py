from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_s1 import (
    P5_M5_S1_BASELINE_MATRIX_REPORT_PATH,
    P5_M5_S1_CLEANUP_RULES_PATH,
    P5_M5_S1_EXACT_CARRIERS_PATH,
    P5_M5_S1_EXECUTION_PLAN_PATH,
    format_p5_m5_s1_results,
    format_p5_m5_s1_summary,
    render_p5_m5_s1_json,
    validate_p5_m5_s1_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S1 retained-surface baseline package."
    )
    parser.add_argument(
        "--print-plan-path",
        action="store_true",
        help="Print the canonical P5-M5-S1 execution plan path and exit.",
    )
    parser.add_argument(
        "--print-report-path",
        action="store_true",
        help="Print the canonical P5-M5-S1 baseline report path and exit.",
    )
    parser.add_argument(
        "--print-carriers-path",
        action="store_true",
        help="Print the canonical P5-M5-S1 exact carriers path and exit.",
    )
    parser.add_argument(
        "--print-cleanup-path",
        action="store_true",
        help="Print the canonical P5-M5-S1 cleanup rules path and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the machine-readable P5-M5-S1 bundle JSON and exit.",
    )
    parser.add_argument(
        "--check-artifacts",
        action="store_true",
        help="Validate P5-M5-S1 artifacts and return non-zero on failure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_plan_path:
        print(P5_M5_S1_EXECUTION_PLAN_PATH)
        return 0
    if args.print_report_path:
        print(P5_M5_S1_BASELINE_MATRIX_REPORT_PATH)
        return 0
    if args.print_carriers_path:
        print(P5_M5_S1_EXACT_CARRIERS_PATH)
        return 0
    if args.print_cleanup_path:
        print(P5_M5_S1_CLEANUP_RULES_PATH)
        return 0
    if args.json:
        print(render_p5_m5_s1_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_s1_artifacts()
        print(format_p5_m5_s1_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_s1_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
