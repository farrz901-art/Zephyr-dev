from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m4_s14_closeout import (
    P5_GOVERNANCE_VOCABULARY_MATRIX_PATH,
    P5_INSPECT_RECEIPT_CONTRACT_PATH,
    P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH,
    P5_M4_RESIDUAL_RISK_PATH,
    P5_M4_S14_CLOSEOUT_REPORT_PATH,
    P5_SOURCE_LINKAGE_V2_PATH,
    P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH,
    format_p5_m4_s14_closeout_results,
    format_p5_m4_s14_closeout_summary,
    render_p5_m4_s14_closeout_json,
    validate_p5_m4_s14_closeout_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4-S14 gated closeout package."
    )
    parser.add_argument("--print-verify-contract-path", action="store_true")
    parser.add_argument("--print-inspect-contract-path", action="store_true")
    parser.add_argument("--print-source-linkage-v2-path", action="store_true")
    parser.add_argument("--print-governance-vocabulary-path", action="store_true")
    parser.add_argument("--print-closeout-matrix-path", action="store_true")
    parser.add_argument("--print-residual-risk-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_verify_contract_path:
        print(P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH)
        return 0
    if args.print_inspect_contract_path:
        print(P5_INSPECT_RECEIPT_CONTRACT_PATH)
        return 0
    if args.print_source_linkage_v2_path:
        print(P5_SOURCE_LINKAGE_V2_PATH)
        return 0
    if args.print_governance_vocabulary_path:
        print(P5_GOVERNANCE_VOCABULARY_MATRIX_PATH)
        return 0
    if args.print_closeout_matrix_path:
        print(P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH)
        return 0
    if args.print_residual_risk_path:
        print(P5_M4_RESIDUAL_RISK_PATH)
        return 0
    if args.print_report_path:
        print(P5_M4_S14_CLOSEOUT_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_m4_s14_closeout_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m4_s14_closeout_artifacts()
        print(format_p5_m4_s14_closeout_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m4_s14_closeout_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
