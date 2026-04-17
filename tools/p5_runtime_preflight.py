from __future__ import annotations

import argparse

from zephyr_ingest.testing.p5_runtime_contract import (
    P5_CANONICAL_RUNTIME_HOME,
    P5_RUNTIME_CONTRACT_PATH,
    format_p5_runtime_contract_summary,
    render_p5_preflight_json,
    render_p5_preflight_report,
    validate_p5_runtime_contract,
)
from zephyr_ingest.testing.p45 import load_p45_env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the P5-M1 runtime contract against the external runtime-home, "
            "without leaking secret values."
        )
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Render the preflight report as text or JSON.",
    )
    parser.add_argument(
        "--show-env",
        action="store_true",
        help="Include a redacted env summary grouped by the runtime contract.",
    )
    parser.add_argument(
        "--check-health",
        action="store_true",
        help="Also run the existing repo-side local-real/service-live health probes.",
    )
    parser.add_argument(
        "--health-tier",
        choices=("all", "local-real", "service-live"),
        default="all",
        help="Health probe tier to run when --check-health is enabled.",
    )
    parser.add_argument(
        "--print-runtime-home",
        action="store_true",
        help="Print only the resolved runtime-home path and exit.",
    )
    parser.add_argument(
        "--print-contract-path",
        action="store_true",
        help="Print only the repo-resident runtime contract JSON path and exit.",
    )
    parser.add_argument(
        "--show-contract-summary",
        action="store_true",
        help="Print the runtime contract summary before the preflight report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    env = load_p45_env()
    if args.print_runtime_home:
        print(env.runtime_paths.home)
        return 0
    if args.print_contract_path:
        print(P5_RUNTIME_CONTRACT_PATH)
        return 0

    checks = validate_p5_runtime_contract(
        env,
        expected_runtime_home=P5_CANONICAL_RUNTIME_HOME,
    )
    if args.format == "json":
        print(
            render_p5_preflight_json(
                env,
                checks=checks,
                include_env=args.show_env,
                include_health=args.check_health,
                health_tier=args.health_tier,
            )
        )
    else:
        if args.show_contract_summary:
            print(format_p5_runtime_contract_summary())
            print()
        print(
            render_p5_preflight_report(
                env,
                checks=checks,
                include_env=args.show_env,
                include_health=args.check_health,
                health_tier=args.health_tier,
            )
        )
    return 0 if all(check.ok for check in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
