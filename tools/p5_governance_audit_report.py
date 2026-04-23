from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_governance_audit import (
    P5_GOVERNANCE_ACTION_MATRIX_PATH,
    P5_GOVERNANCE_AUDIT_REPORT_PATH,
    P5_GOVERNANCE_USAGE_LINKAGE_PATH,
    P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH,
    P5_SOURCE_SPEC_PARITY_PATH,
    P5_SOURCE_SPEC_PARITY_REPORT_PATH,
    format_p5_governance_audit_results,
    format_p5_governance_audit_summary,
    render_p5_governance_audit_json,
    validate_p5_governance_audit_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4-S13 governance audit package."
    )
    parser.add_argument("--print-action-matrix-path", action="store_true")
    parser.add_argument("--print-audit-manifest-path", action="store_true")
    parser.add_argument("--print-usage-linkage-path", action="store_true")
    parser.add_argument("--print-source-spec-parity-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-source-parity-report-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_action_matrix_path:
        print(P5_GOVERNANCE_ACTION_MATRIX_PATH)
        return 0
    if args.print_audit_manifest_path:
        print(P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH)
        return 0
    if args.print_usage_linkage_path:
        print(P5_GOVERNANCE_USAGE_LINKAGE_PATH)
        return 0
    if args.print_source_spec_parity_path:
        print(P5_SOURCE_SPEC_PARITY_PATH)
        return 0
    if args.print_report_path:
        print(P5_GOVERNANCE_AUDIT_REPORT_PATH)
        return 0
    if args.print_source_parity_report_path:
        print(P5_SOURCE_SPEC_PARITY_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_governance_audit_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_governance_audit_artifacts()
        print(format_p5_governance_audit_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_governance_audit_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
