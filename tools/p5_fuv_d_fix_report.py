from __future__ import annotations

import argparse
import json

from zephyr_ingest.testing.p5_fuv_d_fix import (
    P5_FUV_D_FIX_REPORT_PATH,
    collect_p45_s3_bucket_status,
    format_p5_fuv_d_fix_results,
    format_p5_fuv_d_fix_summary,
    render_p5_fuv_d_fix_json,
    s3_optional_dependency_declared,
    validate_p5_fuv_d_fix_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the focused P5 final-user-validation D-fix package."
    )
    parser.add_argument("--print-report-path", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    parser.add_argument("--check-payload-content-evidence", action="store_true")
    parser.add_argument("--check-s3-dependency", action="store_true")
    parser.add_argument("--check-s3-bucket", action="store_true")
    parser.add_argument("--ensure-s3-bucket", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_report_path:
        print(P5_FUV_D_FIX_REPORT_PATH)
        return 0
    if args.json:
        print(render_p5_fuv_d_fix_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_fuv_d_fix_artifacts()
        print(format_p5_fuv_d_fix_results(checks))
        return 0 if all(check.ok for check in checks) else 1
    if args.check_payload_content_evidence:
        checks = validate_p5_fuv_d_fix_artifacts()
        filtered = [
            check
            for check in checks
            if check.name.startswith("p5_fuv_d_fix_content_evidence")
            or check.name == "p5_fuv_d_fix_report_required_phrases"
        ]
        print(format_p5_fuv_d_fix_results(filtered))
        return 0 if all(check.ok for check in filtered) else 1
    if args.check_s3_dependency:
        ok = s3_optional_dependency_declared()
        print(f"p5_fuv_d_fix_s3_optional_dependency_declared -> {'ok' if ok else 'failed'}")
        return 0 if ok else 1
    if args.check_s3_bucket or args.ensure_s3_bucket:
        result = collect_p45_s3_bucket_status(ensure_bucket=args.ensure_s3_bucket)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return (
            0
            if result.get("dependency_installed") is True and result.get("bucket_exists") is True
            else 1
        )

    print(format_p5_fuv_d_fix_summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
