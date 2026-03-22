from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from zephyr_core import RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.versioning import PIPELINE_VERSION
from zephyr_ingest.destinations.base import Destination
from zephyr_ingest.destinations.fanout import FanoutDestination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.runner import RetryConfig, RunnerConfig, run_documents
from zephyr_ingest.sources.local_file import LocalFileSource


@dataclass(frozen=True, slots=True)
class RunCmd:
    path: str
    glob: str
    out: str
    strategy: str
    skip_unsupported: bool
    skip_existing: bool
    force: bool
    unique_element_ids: bool
    pipeline_version: str | None
    run_id: str | None
    timestamp_utc: str | None
    retry_enabled: bool
    max_attempts: int
    base_backoff_ms: int
    max_backoff_ms: int
    workers: int
    destination: str
    webhook_url: str | None
    webhook_timeout_s: float


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="zephyr-ingest")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run ingest pipeline over a local file path")
    run.add_argument("--path", required=True, help="File or directory path")
    run.add_argument("--glob", default="**/*", help="Glob pattern when --path is a directory")
    run.add_argument("--out", default=".cache/out", help="Output root directory")
    run.add_argument(
        "--strategy",
        default="auto",
        choices=["auto", "fast", "hi_res", "ocr_only"],
        help="Partition strategy (mainly for pdf/image)",
    )
    run.add_argument("--skip-unsupported", action="store_true", default=True)
    run.add_argument("--skip-existing", action="store_true", default=True)
    run.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    run.add_argument("--force", action="store_true", default=False)
    run.add_argument("--unique-element-ids", action="store_true", default=True)
    run.add_argument("--no-unique-element-ids", dest="unique_element_ids", action="store_false")

    # RunContext overrides (optional)
    run.add_argument("--pipeline-version", default=None, help="Override pipeline version")
    run.add_argument("--run-id", default=None, help="Override run ID (UUID)")
    run.add_argument("--timestamp-utc", default=None, help="Override timestamp (ISO 8601)")

    # Retry config (P2-M2-04)
    run.add_argument("--no-retry", dest="retry_enabled", action="store_false", default=True)
    run.add_argument("--max-attempts", type=int, default=3)
    run.add_argument("--base-backoff-ms", type=int, default=200)
    run.add_argument("--max-backoff-ms", type=int, default=5000)

    run.add_argument(
        "--workers", type=int, default=1, help="Number of concurrent workers (default: 1)"
    )

    run.add_argument("--destination", default="filesystem", choices=["filesystem"])

    run.add_argument("--webhook-url", default=None, help="Optional HTTP Webhook URL")
    run.add_argument(
        "--webhook-timeout-s", type=float, default=10.0, help="Webhook timeout in seconds"
    )

    return p


def _parse_run_cmd(argv: Sequence[str]) -> RunCmd:
    p = _build_parser()
    ns = p.parse_args(list(argv))

    # ns is argparse.Namespace; in pyright strict we convert to typed RunCmd
    if ns.cmd != "run":
        raise SystemExit("Unsupported command")

    return RunCmd(
        path=str(ns.path),
        glob=str(ns.glob),
        out=str(ns.out),
        strategy=str(ns.strategy),
        skip_unsupported=bool(ns.skip_unsupported),
        skip_existing=bool(ns.skip_existing),
        force=bool(ns.force),
        unique_element_ids=bool(ns.unique_element_ids),
        pipeline_version=ns.pipeline_version
        if ns.pipeline_version is None
        else str(ns.pipeline_version),
        run_id=ns.run_id if ns.run_id is None else str(ns.run_id),
        timestamp_utc=ns.timestamp_utc if ns.timestamp_utc is None else str(ns.timestamp_utc),
        retry_enabled=bool(ns.retry_enabled),
        max_attempts=int(ns.max_attempts),
        base_backoff_ms=int(ns.base_backoff_ms),
        max_backoff_ms=int(ns.max_backoff_ms),
        workers=int(ns.workers),
        destination=str(ns.destination),
        webhook_url=None if ns.webhook_url is None else str(ns.webhook_url),
        webhook_timeout_s=float(ns.webhook_timeout_s),
    )


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    argv = sys.argv[1:] if argv is None else argv

    cmd = _parse_run_cmd(argv)

    fs = FilesystemDestination()
    dest: Destination
    if cmd.webhook_url:
        wh = WebhookDestination(url=cmd.webhook_url, timeout_s=cmd.webhook_timeout_s)
        # 自动开启分叉：本地存储 + Webhook 发送
        dest = FanoutDestination(destinations=(fs, wh))
    else:
        dest = fs

    ctx = RunContext.new(
        pipeline_version=cmd.pipeline_version or PIPELINE_VERSION,
        run_id=cmd.run_id,
        timestamp_utc=cmd.timestamp_utc,
    )

    # dest = FilesystemDestination()

    src = LocalFileSource(path=Path(cmd.path), glob=cmd.glob)
    docs = src.iter_documents()

    cfg = RunnerConfig(
        out_root=Path(cmd.out),
        strategy=PartitionStrategy(cmd.strategy),
        unique_element_ids=cmd.unique_element_ids,
        skip_unsupported=cmd.skip_unsupported,
        skip_existing=cmd.skip_existing,
        force=cmd.force,
        retry=RetryConfig(
            enabled=cmd.retry_enabled,
            max_attempts=cmd.max_attempts,
            base_backoff_ms=cmd.base_backoff_ms,
            max_backoff_ms=cmd.max_backoff_ms,
        ),
        workers=cmd.workers,
        destination=dest,
    )

    run_documents(docs=docs, cfg=cfg, ctx=ctx, destination=dest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
