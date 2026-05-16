from __future__ import annotations

import argparse
from pathlib import Path

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


def extract_basetemp_paths(pytest_args: list[str]) -> tuple[Path, ...]:
    paths: list[Path] = []
    index = 0
    while index < len(pytest_args):
        arg = pytest_args[index]
        if arg.startswith("--basetemp="):
            raw_path = arg.split("=", 1)[1]
            if raw_path:
                paths.append(Path(raw_path))
        elif arg == "--basetemp" and index + 1 < len(pytest_args):
            raw_path = pytest_args[index + 1]
            if raw_path:
                paths.append(Path(raw_path))
            index += 1
        index += 1
    return tuple(paths)


def ensure_basetemp_dirs(pytest_args: list[str]) -> tuple[Path, ...]:
    basetemp_paths = extract_basetemp_paths(pytest_args)
    for path in basetemp_paths:
        path.mkdir(parents=True, exist_ok=True)
    return basetemp_paths


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    pytest_args: list[str] = []
    if args.marker_expr:
        pytest_args.extend(["-m", args.marker_expr])
    pytest_args.extend(args.pytest_arg)
    pytest_args.extend(args.paths)
    ensure_basetemp_dirs(pytest_args)
    exit_code = pytest.main(pytest_args)
    if exit_code == 5 and args.allow_no_tests:
        return 0
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
