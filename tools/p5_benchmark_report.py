from __future__ import annotations

import argparse
import json

from zephyr_ingest.testing.p5_benchmark import (
    P5_BENCHMARK_REGISTRY_PATH,
    P5_BENCHMARK_REPORT_PATH,
    format_p5_benchmark_check_results,
    format_p5_benchmark_result,
    iter_p5_benchmark_cases,
    render_p5_benchmark_registry_json,
    run_p5_benchmark_case,
    validate_p5_benchmark_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M4 benchmark/SLI baseline harness package."
    )
    parser.add_argument("--print-registry-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--registry-json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    parser.add_argument("--list-cases", action="store_true")
    parser.add_argument("--case", help="Run one named benchmark case in harness-validation mode.")
    parser.add_argument("--json", action="store_true", help="Print benchmark result as JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_registry_path:
        print(P5_BENCHMARK_REGISTRY_PATH)
        return 0
    if args.print_report_path:
        print(P5_BENCHMARK_REPORT_PATH)
        return 0
    if args.registry_json:
        print(render_p5_benchmark_registry_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_benchmark_artifacts()
        print(format_p5_benchmark_check_results(checks))
        return 0 if all(check.ok for check in checks) else 1
    if args.list_cases:
        for case in iter_p5_benchmark_cases():
            print(case.case_id)
        return 0
    if args.case is None:
        parser.error("choose --case, --list-cases, --registry-json, or an artifact option")

    result = run_p5_benchmark_case(args.case)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(format_p5_benchmark_result(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
