from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_s6 import (
    P5_M5_S6_FAMILY_EXPECTATIONS_PATH,
    P5_M5_S6_LOAD_CLASSES_PATH,
    P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH,
    P5_M5_S6_OPERATIONAL_PROFILES_PATH,
    P5_M5_S6_OPERATIONAL_SIGNALS_PATH,
    P5_M5_S6_SUCCESS_CRITERIA_PATH,
    format_p5_m5_s6_results,
    format_p5_m5_s6_summary,
    render_p5_m5_s6_json,
    validate_p5_m5_s6_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S6 bounded operational-envelope package."
    )
    parser.add_argument("--print-profiles-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-load-classes-path", action="store_true")
    parser.add_argument("--print-signals-path", action="store_true")
    parser.add_argument("--print-expectations-path", action="store_true")
    parser.add_argument("--print-success-criteria-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_profiles_path:
        print(P5_M5_S6_OPERATIONAL_PROFILES_PATH)
        return 0
    if args.print_report_path:
        print(P5_M5_S6_OPERATIONAL_ENVELOPE_REPORT_PATH)
        return 0
    if args.print_load_classes_path:
        print(P5_M5_S6_LOAD_CLASSES_PATH)
        return 0
    if args.print_signals_path:
        print(P5_M5_S6_OPERATIONAL_SIGNALS_PATH)
        return 0
    if args.print_expectations_path:
        print(P5_M5_S6_FAMILY_EXPECTATIONS_PATH)
        return 0
    if args.print_success_criteria_path:
        print(P5_M5_S6_SUCCESS_CRITERIA_PATH)
        return 0
    if args.json:
        print(render_p5_m5_s6_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_s6_artifacts()
        print(format_p5_m5_s6_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_s6_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
