from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_s3 import (
    P5_M5_S3_CLEANUP_REPETITION_RULES_PATH,
    P5_M5_S3_EXECUTION_PLAN_PATH,
    P5_M5_S3_REPEATABILITY_CARRIERS_PATH,
    P5_M5_S3_STABILITY_MATRIX_REPORT_PATH,
    P5_M5_S3_STABILITY_SIGNALS_PATH,
    format_p5_m5_s3_results,
    format_p5_m5_s3_summary,
    render_p5_m5_s3_json,
    validate_p5_m5_s3_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S3 stability package."
    )
    parser.add_argument("--print-plan-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-repeatability-path", action="store_true")
    parser.add_argument("--print-cleanup-path", action="store_true")
    parser.add_argument("--print-signals-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_plan_path:
        print(P5_M5_S3_EXECUTION_PLAN_PATH)
        return 0
    if args.print_report_path:
        print(P5_M5_S3_STABILITY_MATRIX_REPORT_PATH)
        return 0
    if args.print_repeatability_path:
        print(P5_M5_S3_REPEATABILITY_CARRIERS_PATH)
        return 0
    if args.print_cleanup_path:
        print(P5_M5_S3_CLEANUP_REPETITION_RULES_PATH)
        return 0
    if args.print_signals_path:
        print(P5_M5_S3_STABILITY_SIGNALS_PATH)
        return 0
    if args.json:
        print(render_p5_m5_s3_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_s3_artifacts()
        print(format_p5_m5_s3_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_s3_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
