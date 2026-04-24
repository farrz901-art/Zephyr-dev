from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_s4 import (
    P5_M5_S4_COLD_OPERATOR_REPORT_PATH,
    P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH,
    P5_M5_S4_OPERATOR_QUESTIONS_PATH,
    P5_M5_S4_OPERATOR_SCENARIOS_PATH,
    P5_M5_S4_SCORING_RULES_PATH,
    format_p5_m5_s4_results,
    format_p5_m5_s4_summary,
    render_p5_m5_s4_json,
    validate_p5_m5_s4_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S4 cold-operator package."
    )
    parser.add_argument("--print-scenarios-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-questions-path", action="store_true")
    parser.add_argument("--print-evidence-path", action="store_true")
    parser.add_argument("--print-scoring-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_scenarios_path:
        print(P5_M5_S4_OPERATOR_SCENARIOS_PATH)
        return 0
    if args.print_report_path:
        print(P5_M5_S4_COLD_OPERATOR_REPORT_PATH)
        return 0
    if args.print_questions_path:
        print(P5_M5_S4_OPERATOR_QUESTIONS_PATH)
        return 0
    if args.print_evidence_path:
        print(P5_M5_S4_OPERATOR_EVIDENCE_SURFACES_PATH)
        return 0
    if args.print_scoring_path:
        print(P5_M5_S4_SCORING_RULES_PATH)
        return 0
    if args.json:
        print(render_p5_m5_s4_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_s4_artifacts()
        print(format_p5_m5_s4_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_s4_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
