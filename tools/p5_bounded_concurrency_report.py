from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_bounded_concurrency import (
    P5_BOUNDED_CONCURRENCY_MATRIX_PATH,
    P5_BOUNDED_CONCURRENCY_REPORT_PATH,
    format_p5_bounded_concurrency_results,
    format_p5_bounded_concurrency_summary,
    render_p5_bounded_concurrency_json,
    validate_p5_bounded_concurrency_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4-S8 bounded concurrency package."
    )
    parser.add_argument(
        "--print-matrix-path",
        action="store_true",
        help="Print the canonical P5-M4-S8 bounded concurrency matrix path and exit.",
    )
    parser.add_argument(
        "--print-report-path",
        action="store_true",
        help="Print the canonical P5-M4-S8 bounded concurrency report path and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the machine-readable bounded concurrency matrix JSON and exit.",
    )
    parser.add_argument(
        "--check-artifacts",
        action="store_true",
        help="Validate the bounded concurrency artifacts and return non-zero on failure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_matrix_path:
        print(P5_BOUNDED_CONCURRENCY_MATRIX_PATH)
        return 0
    if args.print_report_path:
        print(P5_BOUNDED_CONCURRENCY_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_bounded_concurrency_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_bounded_concurrency_artifacts()
        print(format_p5_bounded_concurrency_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_bounded_concurrency_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
