from __future__ import annotations

import argparse
from typing import cast

from zephyr_ingest.testing.p45 import (
    check_services,
    format_probe_results,
    format_redacted_env_summary,
    format_runtime_resolution,
    load_p45_env,
    resolve_p45_runtime_paths,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve P4.5 runtime assets from ZEPHYR_P45_HOME or the external default runtime "
            "home, "
            "then probe the local-real and service-live substrate tiers."
        )
    )
    parser.add_argument(
        "--tier",
        choices=("all", "local-real", "service-live"),
        default="all",
        help="Probe the local-real tier, service-live tier, or both.",
    )
    parser.add_argument(
        "--service",
        action="append",
        default=[],
        help="Optional service name filter. May be provided multiple times.",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=3.0,
        help="Per-probe timeout in seconds.",
    )
    parser.add_argument(
        "--show-env",
        action="store_true",
        help="Print a redacted summary of the loaded P4.5 validation environment.",
    )
    parser.add_argument(
        "--print-runtime-home",
        action="store_true",
        help="Print only the resolved P4.5 runtime home path and exit.",
    )
    parser.add_argument(
        "--print-env-dir",
        action="store_true",
        help="Print only the resolved P4.5 runtime env directory and exit.",
    )
    parser.add_argument(
        "--print-compose-path",
        action="store_true",
        help="Print only the resolved P4.5 compose file path and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    runtime_paths = resolve_p45_runtime_paths()
    if args.print_runtime_home:
        print(runtime_paths.home)
        return 0
    if args.print_env_dir:
        print(runtime_paths.env_dir)
        return 0
    if args.print_compose_path:
        print(runtime_paths.compose_path)
        return 0
    tier = None if args.tier == "all" else cast(str, args.tier)
    env = load_p45_env()
    print(format_runtime_resolution(runtime_paths))
    if args.show_env:
        print(format_redacted_env_summary(env))
    results = check_services(
        env,
        tier=tier,
        service_names=tuple(args.service) if args.service else None,
        timeout_s=args.timeout_s,
    )
    print(format_probe_results(results))
    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
