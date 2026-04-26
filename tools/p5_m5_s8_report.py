from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_s8 import (
    P5_M5_S8_CLOSEOUT_CHECKS_PATH,
    P5_M5_S8_CLOSEOUT_REPORT_PATH,
    P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH,
    P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH,
    P5_M5_S8_STAGE_TRUTH_INDEX_PATH,
    P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH,
    P5_M5_S8_USER_VALIDATION_PLAN_PATH,
    P5_USER_VALIDATION_PLAYBOOK_PATH,
    format_p5_m5_s8_results,
    format_p5_m5_s8_summary,
    render_p5_m5_s8_json,
    validate_p5_m5_s8_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S8 final closeout package."
    )
    parser.add_argument("--print-stage-truth-index-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-cross-stage-consistency-path", action="store_true")
    parser.add_argument("--print-closeout-checks-path", action="store_true")
    parser.add_argument("--print-non-claims-path", action="store_true")
    parser.add_argument("--print-user-validation-plan-path", action="store_true")
    parser.add_argument("--print-user-feedback-template-path", action="store_true")
    parser.add_argument("--print-user-validation-playbook-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_stage_truth_index_path:
        print(P5_M5_S8_STAGE_TRUTH_INDEX_PATH)
        return 0
    if args.print_report_path:
        print(P5_M5_S8_CLOSEOUT_REPORT_PATH)
        return 0
    if args.print_cross_stage_consistency_path:
        print(P5_M5_S8_CROSS_STAGE_CONSISTENCY_PATH)
        return 0
    if args.print_closeout_checks_path:
        print(P5_M5_S8_CLOSEOUT_CHECKS_PATH)
        return 0
    if args.print_non_claims_path:
        print(P5_M5_S8_NON_CLAIMS_AND_DEFERRED_SCOPE_PATH)
        return 0
    if args.print_user_validation_plan_path:
        print(P5_M5_S8_USER_VALIDATION_PLAN_PATH)
        return 0
    if args.print_user_feedback_template_path:
        print(P5_M5_S8_USER_FEEDBACK_TEMPLATE_PATH)
        return 0
    if args.print_user_validation_playbook_path:
        print(P5_USER_VALIDATION_PLAYBOOK_PATH)
        return 0
    if args.json:
        print(render_p5_m5_s8_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_s8_artifacts()
        print(format_p5_m5_s8_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_s8_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
