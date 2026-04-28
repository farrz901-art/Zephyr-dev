from __future__ import annotations

import argparse
import json

from zephyr_ingest.testing.p5_fuv_d_fix import collect_p45_s3_bucket_status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Check or explicitly ensure the retained P4.5/P5 S3 bucket without printing secrets."
        )
    )
    parser.add_argument("--check-bucket", action="store_true")
    parser.add_argument("--ensure-bucket", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    ensure_bucket = args.ensure_bucket
    result = collect_p45_s3_bucket_status(ensure_bucket=ensure_bucket)
    if args.json or args.check_bucket or args.ensure_bucket:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return (
        0
        if result.get("dependency_installed") is True and result.get("bucket_exists") is True
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
