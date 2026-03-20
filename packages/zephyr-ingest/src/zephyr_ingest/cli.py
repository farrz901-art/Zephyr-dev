from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from zephyr_core import RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.versioning import PIPELINE_VERSION
from zephyr_ingest.runner import RunnerConfig, run_documents
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
    )


def main(argv: Sequence[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv

    cmd = _parse_run_cmd(argv)

    ctx = RunContext.new(
        pipeline_version=cmd.pipeline_version or PIPELINE_VERSION,
        run_id=cmd.run_id,
        timestamp_utc=cmd.timestamp_utc,
    )

    src = LocalFileSource(path=Path(cmd.path), glob=cmd.glob)
    docs = src.iter_documents()

    cfg = RunnerConfig(
        out_root=Path(cmd.out),
        strategy=PartitionStrategy(cmd.strategy),
        unique_element_ids=cmd.unique_element_ids,
        skip_unsupported=cmd.skip_unsupported,
        skip_existing=cmd.skip_existing,
        force=cmd.force,
    )

    run_documents(docs=docs, cfg=cfg, ctx=ctx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
