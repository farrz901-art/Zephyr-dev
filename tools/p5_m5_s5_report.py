from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_s5 import (
    P5_M5_S5_BUNDLE_LAYOUT_PATH,
    P5_M5_S5_COMPLETENESS_CHECKS_PATH,
    P5_M5_S5_HANDOFF_CONTRACT_PATH,
    P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH,
    P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH,
    P5_M5_S5_RELEASE_MANIFEST_PATH,
    format_p5_m5_s5_results,
    format_p5_m5_s5_summary,
    render_p5_m5_s5_json,
    validate_p5_m5_s5_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S5 release-consumable bundle package."
    )
    parser.add_argument("--print-inventory-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--print-manifest-path", action="store_true")
    parser.add_argument("--print-layout-path", action="store_true")
    parser.add_argument("--print-handoff-path", action="store_true")
    parser.add_argument("--print-completeness-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_inventory_path:
        print(P5_M5_S5_RELEASE_BUNDLE_INVENTORY_PATH)
        return 0
    if args.print_report_path:
        print(P5_M5_S5_RELEASE_BUNDLE_REPORT_PATH)
        return 0
    if args.print_manifest_path:
        print(P5_M5_S5_RELEASE_MANIFEST_PATH)
        return 0
    if args.print_layout_path:
        print(P5_M5_S5_BUNDLE_LAYOUT_PATH)
        return 0
    if args.print_handoff_path:
        print(P5_M5_S5_HANDOFF_CONTRACT_PATH)
        return 0
    if args.print_completeness_path:
        print(P5_M5_S5_COMPLETENESS_CHECKS_PATH)
        return 0
    if args.json:
        print(render_p5_m5_s5_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_s5_artifacts()
        print(format_p5_m5_s5_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_s5_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
