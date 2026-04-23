from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_m5_validation import (
    P5_M5_DEEP_ANCHOR_SELECTION_PATH,
    P5_M5_RETAINED_SURFACE_INVENTORY_PATH,
    P5_M5_VALIDATION_CHARTER_PATH,
    P5_M5_VALIDATION_MATRIX_PATH,
    format_p5_m5_validation_results,
    format_p5_m5_validation_summary,
    render_p5_m5_validation_json,
    validate_p5_m5_validation_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M5-S0 retained-surface validation charter."
    )
    parser.add_argument(
        "--print-matrix-path",
        action="store_true",
        help="Print the canonical P5-M5-S0 validation matrix path and exit.",
    )
    parser.add_argument(
        "--print-charter-path",
        action="store_true",
        help="Print the canonical P5-M5 validation charter path and exit.",
    )
    parser.add_argument(
        "--print-inventory-path",
        action="store_true",
        help="Print the canonical retained-surface inventory path and exit.",
    )
    parser.add_argument(
        "--print-deep-anchor-path",
        action="store_true",
        help="Print the canonical representative deep-anchor selection path and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the machine-readable M5 validation charter bundle JSON and exit.",
    )
    parser.add_argument(
        "--check-artifacts",
        action="store_true",
        help="Validate M5-S0 artifacts and return non-zero on failure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_matrix_path:
        print(P5_M5_VALIDATION_MATRIX_PATH)
        return 0
    if args.print_charter_path:
        print(P5_M5_VALIDATION_CHARTER_PATH)
        return 0
    if args.print_inventory_path:
        print(P5_M5_RETAINED_SURFACE_INVENTORY_PATH)
        return 0
    if args.print_deep_anchor_path:
        print(P5_M5_DEEP_ANCHOR_SELECTION_PATH)
        return 0
    if args.json:
        print(render_p5_m5_validation_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_m5_validation_artifacts()
        print(format_p5_m5_validation_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_m5_validation_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
