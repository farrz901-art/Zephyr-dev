from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_runtime_boundary import (
    P5_RUNTIME_BOUNDARY_MATRIX_PATH,
    P5_RUNTIME_BOUNDARY_REPORT_PATH,
    format_p5_runtime_boundary_results,
    format_p5_runtime_boundary_summary,
    render_p5_runtime_boundary_json,
    validate_p5_runtime_boundary_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M2 runtime/queue/lock boundary package."
    )
    parser.add_argument(
        "--print-matrix-path",
        action="store_true",
        help="Print the canonical P5-M2 boundary matrix path and exit.",
    )
    parser.add_argument(
        "--print-report-path",
        action="store_true",
        help="Print the canonical P5-M2 boundary report path and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the machine-readable boundary matrix JSON and exit.",
    )
    parser.add_argument(
        "--check-artifacts",
        action="store_true",
        help="Validate the boundary matrix/report artifacts and return non-zero on failure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_matrix_path:
        print(P5_RUNTIME_BOUNDARY_MATRIX_PATH)
        return 0
    if args.print_report_path:
        print(P5_RUNTIME_BOUNDARY_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_runtime_boundary_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_runtime_boundary_artifacts()
        print(format_p5_runtime_boundary_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    print(format_p5_runtime_boundary_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
