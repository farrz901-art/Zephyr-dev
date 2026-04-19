from __future__ import annotations

import argparse
import json
from pathlib import Path

from zephyr_ingest.queue_backend_factory import LocalQueueBackendKind
from zephyr_ingest.testing.p5_recovery_operator import (
    P5_RECOVERY_MATRIX_PATH,
    P5_RECOVERY_RUNBOOK_PATH,
    format_p5_recovery_check_results,
    format_p5_recovery_summary,
    render_p5_recovery_matrix_json,
    summarize_recovery_artifacts,
    validate_p5_recovery_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render or validate the P5-M3 recovery/operator runbook package."
    )
    parser.add_argument("--print-matrix-path", action="store_true")
    parser.add_argument("--print-runbook-path", action="store_true")
    parser.add_argument("--matrix-json", action="store_true")
    parser.add_argument("--check-artifacts", action="store_true")
    parser.add_argument("--out-root", type=Path)
    parser.add_argument("--queue-root", type=Path)
    parser.add_argument(
        "--queue-backend-kind",
        choices=("spool", "sqlite"),
        default="sqlite",
    )
    parser.add_argument("--json", action="store_true", help="Print summary as JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_matrix_path:
        print(P5_RECOVERY_MATRIX_PATH)
        return 0
    if args.print_runbook_path:
        print(P5_RECOVERY_RUNBOOK_PATH)
        return 0
    if args.matrix_json:
        print(render_p5_recovery_matrix_json())
        return 0
    if args.check_artifacts:
        checks = validate_p5_recovery_artifacts()
        print(format_p5_recovery_check_results(checks))
        return 0 if all(check.ok for check in checks) else 1

    queue_backend_kind: LocalQueueBackendKind = args.queue_backend_kind
    summary = summarize_recovery_artifacts(
        out_root=args.out_root,
        queue_root=args.queue_root,
        queue_backend_kind=queue_backend_kind,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(format_p5_recovery_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
