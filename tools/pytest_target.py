from __future__ import annotations

import argparse

import pytest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a bounded pytest target with an optional marker expression and optionally treat "
            "zero collected tests as success."
        )
    )
    parser.add_argument(
        "--marker-expr",
        default=None,
        help="Optional pytest -m expression.",
    )
    parser.add_argument(
        "--allow-no-tests",
        action="store_true",
        help="Return success when pytest collects zero tests (exit code 5).",
    )
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Extra pytest argument to append. May be provided multiple times.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Optional pytest collection paths. Defaults to pytest.ini testpaths.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    pytest_args: list[str] = []
    if args.marker_expr:
        pytest_args.extend(["-m", args.marker_expr])
    pytest_args.extend(args.pytest_arg)
    pytest_args.extend(args.paths)
    exit_code = pytest.main(pytest_args)
    if exit_code == 5 and args.allow_no_tests:
        return 0
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
