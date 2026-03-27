from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, cast

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
from zephyr_ingest.config.argparse_extract import (
    get_bool,
    get_float,
    get_int,
    get_opt_int,
    get_opt_str,
    get_req_str,
    get_str_list,
)
from zephyr_ingest.config.errors import ConfigError
from zephyr_ingest.config.models import KafkaConfigV1, WeaviateConfigV1, WebhookConfigV1
from zephyr_ingest.destinations.base import Destination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.replay_delivery import replay_delivery_dlq
from zephyr_ingest.runner import RetryConfig, RunnerConfig, run_documents
from zephyr_ingest.sources.local_file import LocalFileSource


@dataclass(frozen=True, slots=True)
class RunCmd:
    paths: list[str]
    glob: str
    out: str

    strategy: PartitionStrategy
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

    retry: RetryConfig
    workers: int
    stale_lock_ttl_s: int | None

    webhook: WebhookConfigV1 | None
    kafka: KafkaConfigV1 | None
    weaviate: WeaviateConfigV1 | None


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
    run.add_argument(
        "--path",
        nargs="+",
        dest="paths",
        required=True,
        help="One or more file paths",
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

    # Retry config
    run.add_argument("--no-retry", dest="retry_enabled", action="store_false", default=True)
    run.add_argument("--max-attempts", type=int, default=3)
    run.add_argument("--base-backoff-ms", type=int, default=200)
    run.add_argument("--max-backoff-ms", type=int, default=5000)

    run.add_argument("--workers", type=int, default=1, help="Number of concurrent workers")
    run.add_argument(
        "--stale-lock-ttl-s",
        type=int,
        default=None,
        help="TTL in seconds to break stale file locks (default: None, disabled)",
    )

    # Destinations (config-model driven)
    WebhookConfigV1.add_cli_args(run)
    KafkaConfigV1.add_cli_args(run)
    WeaviateConfigV1.add_cli_args(run)

    replay = sub.add_parser("replay-delivery", help="Replay failed delivery DLQ records")
    replay.add_argument(
        "--out", default=".cache/out", help="Output root directory containing _dlq/"
    )
    replay.add_argument("--webhook-url", required=True, help="Webhook URL to resend payloads to")
    replay.add_argument("--webhook-timeout-s", type=float, default=10.0)
    replay.add_argument("--limit", type=int, default=None)
    replay.add_argument("--dry-run", action="store_true", default=False)
    replay.add_argument("--no-move-done", dest="move_done", action="store_false", default=True)

    return p


def _parse_run_cmd(ns: argparse.Namespace) -> RunCmd:
    strategy_str = get_req_str(ns, "strategy")
    try:
        strategy = PartitionStrategy(strategy_str)
    except ValueError as e:
        raise ConfigError(f"Invalid --strategy: {strategy_str}") from e

    retry = RetryConfig(
        enabled=get_bool(ns, "retry_enabled"),
        max_attempts=get_int(ns, "max_attempts"),
        base_backoff_ms=get_int(ns, "base_backoff_ms"),
        max_backoff_ms=get_int(ns, "max_backoff_ms"),
    )

    webhook = WebhookConfigV1.from_namespace(ns)
    kafka = KafkaConfigV1.from_namespace(ns)
    weaviate = WeaviateConfigV1.from_namespace(ns)

    return RunCmd(
        paths=get_str_list(ns, "paths"),
        glob=get_req_str(ns, "glob"),
        out=get_req_str(ns, "out"),
        strategy=strategy,
        backend=get_req_str(ns, "backend"),
        uns_api_url=get_req_str(ns, "uns_api_url"),
        uns_api_key=get_opt_str(ns, "uns_api_key"),
        uns_api_timeout_s=get_float(ns, "uns_api_timeout_s"),
        skip_unsupported=get_bool(ns, "skip_unsupported"),
        skip_existing=get_bool(ns, "skip_existing"),
        force=get_bool(ns, "force"),
        unique_element_ids=get_bool(ns, "unique_element_ids"),
        pipeline_version=get_opt_str(ns, "pipeline_version"),
        run_id=get_opt_str(ns, "run_id"),
        timestamp_utc=get_opt_str(ns, "timestamp_utc"),
        retry=retry,
        workers=get_int(ns, "workers"),
        stale_lock_ttl_s=get_opt_int(ns, "stale_lock_ttl_s"),
        webhook=webhook,
        kafka=kafka,
        weaviate=weaviate,
    )


def _parse_cmd(argv: Sequence[str]) -> RunCmd | ReplayDeliveryCmd:
    p = _build_parser()
    ns = p.parse_args(list(argv))

    if ns.cmd == "run":
        return _parse_run_cmd(ns)

    if ns.cmd == "replay-delivery":
        return ReplayDeliveryCmd(
            out=get_req_str(ns, "out"),
            webhook_url=get_req_str(ns, "webhook_url"),
            webhook_timeout_s=get_float(ns, "webhook_timeout_s"),
            limit=get_opt_int(ns, "limit"),
            dry_run=get_bool(ns, "dry_run"),
            move_done=get_bool(ns, "move_done"),
        )

    raise SystemExit("Unsupported command")


def _make_kafka_producer_or_exit(brokers: str):
    try:
        from zephyr_ingest._internal.kafka_producer import make_kafka_producer

        return make_kafka_producer(brokers=brokers)
    except ImportError as e:
        logging.error("Kafka extra not installed: %s", e)
        logging.error("Install with: uv pip install zephyr-ingest[kafka]")
        raise SystemExit(1) from e


def _make_weaviate_collection_or_exit(
    *, cfg: WeaviateConfigV1
) -> tuple[WeaviateClientProtocol, object]:
    # NOTE: return type is object here to keep this module
    # independent from destination protocol types.
    # The destination constructor will receive a WeaviateCollectionProtocol
    # due to the factory return.
    try:
        params = WeaviateConnectParams(
            http_host=cfg.http_host,
            http_port=cfg.http_port,
            http_secure=cfg.http_secure,
            grpc_host=cfg.grpc_host,
            grpc_port=cfg.grpc_port,
            grpc_secure=cfg.grpc_secure,
            api_key=cfg.api_key,
            skip_init_checks=cfg.skip_init_checks,
        )
        client, collection = connect_weaviate_and_get_collection(
            params=params,
            collection_name=cfg.collection,
        )
        return client, collection
    except ImportError as e:
        logging.error("Weaviate extra not installed: %s", e)
        logging.error("Install with: uv pip install 'zephyr-ingest[weaviate]'")
        raise SystemExit(1) from e
    except Exception as e:
        logging.error("Weaviate connect failed: %s", e)
        raise SystemExit(1) from e


def _build_destinations(*, cmd: RunCmd) -> tuple[Destination, WeaviateClientProtocol | None]:
    dests: list[Destination] = []
    dests.append(FilesystemDestination())

    if cmd.webhook is not None:
        dests.append(WebhookDestination(url=cmd.webhook.url, timeout_s=cmd.webhook.timeout_s))

    if cmd.kafka is not None:
        from zephyr_ingest.destinations.kafka import KafkaDestination

        producer = _make_kafka_producer_or_exit(cmd.kafka.brokers)
        dests.append(
            KafkaDestination(
                topic=cmd.kafka.topic,
                producer=producer,
                flush_timeout_s=cmd.kafka.flush_timeout_s,
            )
        )

    weaviate_client: WeaviateClientProtocol | None = None
    if cmd.weaviate is not None:
        from zephyr_ingest.destinations.weaviate import (
            WeaviateCollectionProtocol,
            WeaviateDestination,
        )

        weaviate_client, collection = _make_weaviate_collection_or_exit(cfg=cmd.weaviate)
        dests.append(
            WeaviateDestination(
                collection_name=cmd.weaviate.collection,
                collection=cast(
                    WeaviateCollectionProtocol, collection
                ),  # runtime object, protocol-checked in _internal factory
                max_batch_errors=cmd.weaviate.max_batch_errors,
            )
        )

    if len(dests) == 1:
        return dests[0], weaviate_client

    from zephyr_ingest.destinations.fanout import FanoutDestination

    return FanoutDestination(destinations=tuple(dests)), weaviate_client


def _build_backend(*, cmd: RunCmd) -> object | None:
    if cmd.backend != "uns-api":
        return None

    return HttpUnsApiBackend(
        url=cmd.uns_api_url,
        api_key=cmd.uns_api_key,
        timeout_s=cmd.uns_api_timeout_s,
        transport=None,
    )


def _collect_documents(*, cmd: RunCmd) -> list[DocumentRef]:
    all_docs: list[DocumentRef] = []
    for p_str in cmd.paths:
        src = LocalFileSource(path=Path(p_str), glob=cmd.glob)
        all_docs.extend(list(src.iter_documents()))
    return all_docs


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    argv2 = sys.argv[1:] if argv is None else argv

    try:
        cmd = _parse_cmd(argv2)
    except ConfigError as e:
        logging.error("config error: %s", e)
        return 2

    if isinstance(cmd, ReplayDeliveryCmd):
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

    # RunCmd
    weaviate_client: WeaviateClientProtocol | None = None
    try:
        destination, weaviate_client = _build_destinations(cmd=cmd)
        backend_obj = _build_backend(cmd=cmd)

        ctx = RunContext.new(
            pipeline_version=cmd.pipeline_version or PIPELINE_VERSION,
            run_id=cmd.run_id,
            timestamp_utc=cmd.timestamp_utc,
        )

        docs = _collect_documents(cmd=cmd)

        cfg = RunnerConfig(
            out_root=Path(cmd.out),
            strategy=cmd.strategy,
            unique_element_ids=cmd.unique_element_ids,
            skip_unsupported=cmd.skip_unsupported,
            skip_existing=cmd.skip_existing,
            force=cmd.force,
            retry=cmd.retry,
            workers=cmd.workers,
            stale_lock_ttl_s=cmd.stale_lock_ttl_s,
            destination=destination,
            backend=backend_obj,
        )

        run_documents(docs=docs, cfg=cfg, ctx=ctx, destination=destination)
        return 0
    finally:
        if weaviate_client is not None:
            weaviate_client.close()


if __name__ == "__main__":
    raise SystemExit(main())
