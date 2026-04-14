from __future__ import annotations

import argparse
from typing import cast

from zephyr_ingest.testing.p45 import (
    check_services,
    default_compose_path,
    format_probe_results,
    format_redacted_env_summary,
    load_p45_env,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="P4.5 substrate availability healthcheck for local-real and service-live tiers."
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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    tier = None if args.tier == "all" else cast(str, args.tier)
    env = load_p45_env()
    if args.show_env:
        print(format_redacted_env_summary(env))
    print(f"P4.5 compose file: {default_compose_path()}")
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
