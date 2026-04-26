from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_s7 import (
    P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH,
    P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH,
    P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH,
    P5_M5_S7_WORKFLOW_SCENARIOS_PATH,
    P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH,
    P5_M5_S7_WORKFLOW_VIEWS_PATH,
    format_p5_m5_s7_results,
    format_p5_m5_s7_summary,
    render_p5_m5_s7_json,
    validate_p5_m5_s7_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S7 real workflow truth package."
    )
    parser.add_argument("--print-scenarios-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-evidence-map-path", action="store_true")
    parser.add_argument("--print-success-criteria-path", action="store_true")
    parser.add_argument("--print-failure-matrix-path", action="store_true")
    parser.add_argument("--print-views-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_scenarios_path:
        print(P5_M5_S7_WORKFLOW_SCENARIOS_PATH)
        return 0
    if args.print_report_path:
        print(P5_M5_S7_REAL_WORKFLOWS_REPORT_PATH)
        return 0
    if args.print_evidence_map_path:
        print(P5_M5_S7_WORKFLOW_EVIDENCE_MAP_PATH)
        return 0
    if args.print_success_criteria_path:
        print(P5_M5_S7_WORKFLOW_SUCCESS_CRITERIA_PATH)
        return 0
    if args.print_failure_matrix_path:
        print(P5_M5_S7_WORKFLOW_FAILURE_MATRIX_PATH)
        return 0
    if args.print_views_path:
        print(P5_M5_S7_WORKFLOW_VIEWS_PATH)
        return 0
    if args.json:
        print(render_p5_m5_s7_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_s7_artifacts()
        print(format_p5_m5_s7_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_s7_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
