from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

from zephyr_ingest.testing.p5_benchmark import (
    P5_BENCHMARK_REGISTRY_PATH,
    P5_BENCHMARK_REPORT_PATH,
    format_p5_benchmark_check_results,
    format_p5_benchmark_result,
    iter_p5_benchmark_cases,
    record_p5_benchmark_result,
    render_p5_benchmark_registry_json,
    summarize_p5_benchmark_results,
    validate_p5_benchmark_artifacts,
    validate_p5_benchmark_case_contract,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render, validate, or ingest P5-M4 benchmark result artifacts."
    )
    parser.add_argument("--print-registry-path", action="store_true")
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--registry-json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    parser.add_argument("--list-cases", action="store_true")
    parser.add_argument("--carrier-json", help="Print one case's real carrier contract as JSON.")
    parser.add_argument(
        "--validate-case",
        help="Validate one named benchmark case contract without pretending to run it.",
    )
    parser.add_argument(
        "--record-result",
        help="Ingest one named benchmark case result from --result-json.",
    )
    parser.add_argument(
        "--result-json", type=Path, help="JSON observation file for --record-result."
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        nargs="+",
        help="Print an anti-mix-layer summary for one or more benchmark result JSON files.",
    )
    parser.add_argument("--json", action="store_true", help="Print result as JSON.")
    return parser


def _read_json_mapping(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return cast(dict[str, object], raw)


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
    if args.carrier_json is not None:
        payload = validate_p5_benchmark_case_contract(args.carrier_json)
        case_obj = payload["case"]
        assert isinstance(case_obj, dict)
        print(json.dumps(case_obj["carrier"], ensure_ascii=False, indent=2))
        return 0
    if args.validate_case is not None:
        payload = validate_p5_benchmark_case_contract(args.validate_case)
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(format_p5_benchmark_result(payload))
        return 0
    if args.record_result is not None:
        if args.result_json is None:
            parser.error("--record-result requires --result-json")
        observed = _read_json_mapping(args.result_json)
        payload = record_p5_benchmark_result(case_id=args.record_result, observed=observed)
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(format_p5_benchmark_result(payload))
        return 0
    if args.summary_json is not None:
        payloads = [_read_json_mapping(path) for path in args.summary_json]
        summary = summarize_p5_benchmark_results(payloads)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    parser.error(
        "choose --validate-case, --record-result, --carrier-json, --list-cases, "
        "--registry-json, --summary-json, or an artifact option"
    )


if __name__ == "__main__":
    raise SystemExit(main())
