from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from uns_stream.backends.http_uns_api import HttpUnsApiBackend
from zephyr_core import RunContext
from zephyr_core.contracts.v1.document_ref import DocumentRef
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.versioning import PIPELINE_VERSION
from zephyr_ingest._internal.weaviate_client import (
    WeaviateClientProtocol,
    WeaviateConnectParams,
    connect_weaviate_and_get_collection,
)
from zephyr_ingest.destinations.base import Destination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.replay_delivery import replay_delivery_dlq
from zephyr_ingest.runner import RetryConfig, RunnerConfig, run_documents
from zephyr_ingest.sources.local_file import LocalFileSource


@dataclass(frozen=True, slots=True)
class RunCmd:
    # path: str
    paths: list[str]
    glob: str
    out: str
    strategy: str
    backend: str
    uns_api_url: str
    uns_api_key: str | None
    uns_api_timeout_s: float
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
    stale_lock_ttl_s: int | None
    destination: str
    webhook_url: str | None
    webhook_timeout_s: float
    kafka_topic: str | None
    kafka_brokers: str | None
    kafka_flush_timeout_s: float
    # Weaviate (Commit 2: CLI wiring only; real client in Commit 3)
    weaviate_collection: str | None
    weaviate_max_batch_errors: int

    # Connection params (used in Commit 3; kept now to avoid CLI breaking changes later)
    weaviate_http_host: str
    weaviate_http_port: int
    weaviate_http_secure: bool
    weaviate_grpc_host: str
    weaviate_grpc_port: int
    weaviate_grpc_secure: bool
    weaviate_api_key: str | None
    weaviate_skip_init_checks: bool


@dataclass(frozen=True, slots=True)
class ReplayDeliveryCmd:
    out: str
    webhook_url: str
    webhook_timeout_s: float
    limit: int | None
    dry_run: bool
    move_done: bool


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="zephyr-ingest")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run ingest pipeline over a local file path")
    # run.add_argument("--path", required=True, help="File or directory path")
    run.add_argument(
        "--path", nargs="+", dest="paths", required=True, help="One or more file paths"
    )
    run.add_argument("--glob", default="**/*", help="Glob pattern when --path is a directory")
    run.add_argument("--out", default=".cache/out", help="Output root directory")
    run.add_argument(
        "--strategy",
        default="auto",
        choices=["auto", "fast", "hi_res", "ocr_only"],
        help="Partition strategy (mainly for pdf/image)",
    )

    run.add_argument("--backend", default="local", choices=["local", "uns-api"])
    run.add_argument("--uns-api-url", default="http://localhost:8001/general/v0/general")
    run.add_argument("--uns-api-key", default=None)
    run.add_argument("--uns-api-timeout-s", type=float, default=60.0)

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

    run.add_argument(
        "--stale-lock-ttl-s",
        type=int,
        default=None,
        help="TTL in seconds to break stale file locks (default: None, disabled)",
    )

    run.add_argument("--destination", default="filesystem", choices=["filesystem"])

    run.add_argument("--webhook-url", default=None, help="Optional HTTP Webhook URL")
    run.add_argument(
        "--webhook-timeout-s", type=float, default=10.0, help="Webhook timeout in seconds"
    )

    run.add_argument(
        "--kafka-topic",
        type=str,
        default=None,
        help="Kafka topic name (optional; requires --kafka-brokers)",
    )
    run.add_argument(
        "--kafka-brokers",
        type=str,
        default=None,
        help="Kafka broker addresses, comma-separated (e.g. localhost:9092)",
    )

    run.add_argument(
        "--kafka-flush-timeout-s",
        type=float,
        default=10.0,
        help="Kafka producer flush timeout in seconds",
    )

    # ----------------------
    # Weaviate (Commit 2: parse + wire only; real connect in Commit 3)
    # Enable WeaviateDestination only if --weaviate-collection is set.
    # ----------------------
    run.add_argument(
        "--weaviate-collection",
        type=str,
        default=None,
        help="Weaviate collection name (optional; enables WeaviateDestination)",
    )
    run.add_argument(
        "--weaviate-max-batch-errors",
        type=int,
        default=0,
        help="Max tolerated batch errors; 0 means any error fails delivery (default: 0)",
    )
    run.add_argument("--weaviate-http-host", type=str, default="localhost")
    run.add_argument("--weaviate-http-port", type=int, default=8080)
    run.add_argument("--weaviate-http-secure", action="store_true", default=False)
    run.add_argument("--weaviate-grpc-host", type=str, default="localhost")
    run.add_argument("--weaviate-grpc-port", type=int, default=50051)
    run.add_argument("--weaviate-grpc-secure", action="store_true", default=False)
    run.add_argument("--weaviate-api-key", type=str, default=None)
    run.add_argument(
        "--weaviate-skip-init-checks",
        action="store_true",
        default=False,
        help="Skip Weaviate client init checks (Commit 3 will use this)",
    )

    replay = sub.add_parser("replay-delivery", help="Replay failed delivery DLQ records")
    replay.add_argument(
        "--out", default=".cache/out", help="Output root directory containing _dlq/"
    )
    replay.add_argument(
        "--webhook-url", required=True, help="Webhook URL to resend run_meta payloads to"
    )
    replay.add_argument("--webhook-timeout-s", type=float, default=10.0)
    replay.add_argument("--limit", type=int, default=None)
    replay.add_argument("--dry-run", action="store_true", default=False)
    replay.add_argument("--no-move-done", dest="move_done", action="store_false", default=True)

    return p


def _parse_cmd(argv: Sequence[str]) -> RunCmd | ReplayDeliveryCmd:
    p = _build_parser()
    ns = p.parse_args(list(argv))

    if ns.cmd == "run":
        return RunCmd(
            paths=[str(p) for p in ns.paths],
            glob=str(ns.glob),
            out=str(ns.out),
            strategy=str(ns.strategy),
            backend=str(ns.backend),
            uns_api_url=str(ns.uns_api_url),
            uns_api_key=ns.uns_api_key if ns.uns_api_key is None else str(ns.uns_api_key),
            uns_api_timeout_s=float(ns.uns_api_timeout_s),
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
            stale_lock_ttl_s=None if ns.stale_lock_ttl_s is None else int(ns.stale_lock_ttl_s),
            destination=str(ns.destination),
            webhook_url=None if ns.webhook_url is None else str(ns.webhook_url),
            webhook_timeout_s=float(ns.webhook_timeout_s),
            kafka_topic=ns.kafka_topic,
            kafka_brokers=ns.kafka_brokers,
            kafka_flush_timeout_s=ns.kafka_flush_timeout_s,
            weaviate_collection=None
            if ns.weaviate_collection is None
            else str(ns.weaviate_collection),
            weaviate_max_batch_errors=int(ns.weaviate_max_batch_errors),
            weaviate_http_host=str(ns.weaviate_http_host),
            weaviate_http_port=int(ns.weaviate_http_port),
            weaviate_http_secure=bool(ns.weaviate_http_secure),
            weaviate_grpc_host=str(ns.weaviate_grpc_host),
            weaviate_grpc_port=int(ns.weaviate_grpc_port),
            weaviate_grpc_secure=bool(ns.weaviate_grpc_secure),
            weaviate_api_key=None if ns.weaviate_api_key is None else str(ns.weaviate_api_key),
            weaviate_skip_init_checks=bool(ns.weaviate_skip_init_checks),
        )

    if ns.cmd == "replay-delivery":
        return ReplayDeliveryCmd(
            out=str(ns.out),
            webhook_url=str(ns.webhook_url),
            webhook_timeout_s=float(ns.webhook_timeout_s),
            limit=None if ns.limit is None else int(ns.limit),
            dry_run=bool(ns.dry_run),
            move_done=bool(ns.move_done),
        )

    raise SystemExit("Unsupported command")


def _make_kafka_producer_or_exit(brokers: str):
    """Create real Kafka producer; exit with error if extra not installed."""
    try:
        from zephyr_ingest._internal.kafka_producer import make_kafka_producer

        return make_kafka_producer(brokers=brokers)
    except ImportError as e:
        logging.error("Kafka extra not installed: %s", e)
        logging.error("Install with: uv pip install zephyr-ingest[kafka]")
        import sys

        sys.exit(1)


def _make_weaviate_collection_or_exit(*, cmd: RunCmd) -> tuple[WeaviateClientProtocol, object]:
    try:
        params = WeaviateConnectParams(
            http_host=cmd.weaviate_http_host,
            http_port=cmd.weaviate_http_port,
            http_secure=cmd.weaviate_http_secure,
            grpc_host=cmd.weaviate_grpc_host,
            grpc_port=cmd.weaviate_grpc_port,
            grpc_secure=cmd.weaviate_grpc_secure,
            api_key=cmd.weaviate_api_key,
            skip_init_checks=cmd.weaviate_skip_init_checks,
        )
        client, collection = connect_weaviate_and_get_collection(
            params=params,
            collection_name=cmd.weaviate_collection or "",
        )
        return client, collection
    except ImportError as e:
        logging.error("Weaviate extra not installed: %s", e)
        logging.error("Install with: uv pip install 'zephyr-ingest[weaviate]'")
        sys.exit(1)
    except Exception as e:
        logging.error("Weaviate connect failed: %s", e)
        sys.exit(1)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    argv = sys.argv[1:] if argv is None else argv

    cmd = _parse_cmd(argv)

    weaviate_client: WeaviateClientProtocol | None = None

    try:
        if isinstance(cmd, RunCmd):
            # fs = FilesystemDestination()
            # dest: Destination

            dests: list[Destination] = []

            # 1. 默认包含文件系统（契约要求： artifacts 必须本地落盘）
            fs = FilesystemDestination()
            dests.append(fs)
            # wh = WebhookDestination(url=cmd.webhook_url, timeout_s=cmd.webhook_timeout_s)
            # # 自动开启分叉：本地存储 + Webhook 发送
            # dest = FanoutDestination(destinations=(fs, wh))
            if cmd.webhook_url:
                dests.append(
                    WebhookDestination(url=cmd.webhook_url, timeout_s=cmd.webhook_timeout_s)
                )
            if cmd.kafka_topic and cmd.kafka_brokers:
                from zephyr_ingest.destinations.kafka import KafkaDestination

                producer = _make_kafka_producer_or_exit(cmd.kafka_brokers)
                kafka_dest = KafkaDestination(
                    topic=cmd.kafka_topic,
                    producer=producer,
                    flush_timeout_s=cmd.kafka_flush_timeout_s,
                )

                dests.append(kafka_dest)
            elif cmd.kafka_topic or cmd.kafka_brokers:
                # 健壮性检查：必须成对出现
                logging.error("Both --kafka-topic and --kafka-brokers must be specified together")
                return 1

            # Weaviate (Commit 2: stub collection only)

            if cmd.weaviate_collection is not None:
                from zephyr_ingest.destinations.weaviate import WeaviateDestination

                weaviate_client, collection = _make_weaviate_collection_or_exit(cmd=cmd)

                weaviate_dest = WeaviateDestination(
                    collection_name=cmd.weaviate_collection,
                    collection=collection,  # type: ignore[arg-type]  # 如果 pyright 提示这里不匹配，就把 helper 返回类型收紧为 Protocol
                    max_batch_errors=cmd.weaviate_max_batch_errors,
                )
                dests.append(weaviate_dest)

            if len(dests) == 1:
                dest = dests[0]
            else:
                from zephyr_ingest.destinations.fanout import FanoutDestination

                dest = FanoutDestination(destinations=tuple(dests))

            # else:
            #     dest = fs

            ctx = RunContext.new(
                pipeline_version=cmd.pipeline_version or PIPELINE_VERSION,
                run_id=cmd.run_id,
                timestamp_utc=cmd.timestamp_utc,
            )

            # dest = FilesystemDestination()
            all_docs: list[DocumentRef] = []

            # src = LocalFileSource(path=Path(cmd.path), glob=cmd.glob)
            # docs = src.iter_documents()

            for p_str in cmd.paths:
                src = LocalFileSource(path=Path(p_str), glob=cmd.glob)
                all_docs.extend(list(src.iter_documents()))

            backend_obj: object | None = None
            if cmd.backend == "uns-api":
                backend_obj = HttpUnsApiBackend(
                    url=cmd.uns_api_url,
                    api_key=cmd.uns_api_key,
                    timeout_s=cmd.uns_api_timeout_s,
                    transport=None,
                )

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
                stale_lock_ttl_s=cmd.stale_lock_ttl_s,
                destination=dest,
                backend=backend_obj,
            )

            run_documents(docs=all_docs, cfg=cfg, ctx=ctx, destination=dest)
            return 0

    finally:
        if weaviate_client is not None:
            weaviate_client.close()

    # replay-delivery
    logging.basicConfig(level=logging.INFO)
    stats = replay_delivery_dlq(
        out_root=Path(cmd.out),
        webhook_url=cmd.webhook_url,
        timeout_s=cmd.webhook_timeout_s,
        limit=cmd.limit,
        dry_run=cmd.dry_run,
        move_done=cmd.move_done,
    )
    logging.info("replay stats: %s", stats)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
