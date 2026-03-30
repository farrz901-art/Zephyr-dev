from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

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

if TYPE_CHECKING:
    from zephyr_ingest.destinations.weaviate import WeaviateCollectionProtocol
from zephyr_ingest.config.argparse_extract import (
    get_bool,
    get_float,
    get_int,
    get_opt_int,
    get_opt_str,
    get_req_str,
    get_str_list,
)
from zephyr_ingest.config.cli_presence import any_flag_present, collect_present_flags
from zephyr_ingest.config.env_overlay import (
    first_env,
)
from zephyr_ingest.config.errors import ConfigError
from zephyr_ingest.config.file_toml_v1 import ConfigFileV1, load_config_file_v1
from zephyr_ingest.config.models import KafkaConfigV1, WeaviateConfigV1, WebhookConfigV1
from zephyr_ingest.config.snapshot_v1 import (
    CONFIG_SNAPSHOT_SCHEMA_VERSION,
    BackendSnapshotV1,
    ConfigSnapshotV1,
    ConfigSourcesV1,
    ConfigValueSource,
    DestinationsSnapshotV1,
)
from zephyr_ingest.destinations.base import Destination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.replay_delivery import replay_delivery_dlq
from zephyr_ingest.runner import RetryConfig, RunnerConfig, run_documents
from zephyr_ingest.sources.local_file import LocalFileSource
from zephyr_ingest.spec.argparse_render import add_specs_to_parser
from zephyr_ingest.spec.registry import get_spec, list_spec_ids
from zephyr_ingest.spec.types import ConnectorSpecV1, SpecFieldTypeV1


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
    config_sources: ConfigSourcesV1


@dataclass(frozen=True, slots=True)
class ReplayDeliveryCmd:
    out: str
    webhook_url: str
    webhook_timeout_s: float
    limit: int | None
    dry_run: bool
    move_done: bool


@dataclass(frozen=True, slots=True)
class ResolveConfigCmd:
    """
    Resolve/merge config without running ingest.
    """

    run_cmd: RunCmd
    config_path: str
    strict: bool


@dataclass(frozen=True, slots=True)
class InitConfigCmd:
    """
    Generate a starter TOML config file (schema_version=1).
    """

    out: str | None


@dataclass(frozen=True, slots=True)
class SpecListCmd:
    pass


@dataclass(frozen=True, slots=True)
class SpecShowCmd:
    spec_id: str
    fmt: str  # "zephyr" | "jsonschema"


def _add_runlike_args(*, p: argparse.ArgumentParser, paths_required: bool) -> None:
    # path: required for "run", optional for "config resolve"
    if paths_required:
        p.add_argument(
            "--path", nargs="+", dest="paths", required=True, help="One or more file paths"
        )
    else:
        p.add_argument(
            "--path",
            nargs="*",
            dest="paths",
            default=[],
            help="Optional paths (not required for resolve)",
        )

    p.add_argument("--glob", default="**/*", help="Glob pattern when --path is a directory")
    p.add_argument("--out", default=".cache/out", help="Output root directory")
    p.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional TOML config file (CLI explicit > ENV secrets > FILE > DEFAULT)",
    )

    p.add_argument(
        "--strategy",
        default="auto",
        choices=["auto", "fast", "hi_res", "ocr_only"],
        help="Partition strategy (mainly for pdf/image)",
    )
    # Spec-driven args (SSOT): backend.uns_api + destinations
    spec_ids = [
        "backend.uns_api.v1",
        "destination.webhook.v1",
        "destination.kafka.v1",
        "destination.weaviate.v1",
    ]
    specs: list[ConnectorSpecV1] = []
    for spec_id in spec_ids:
        s = get_spec(spec_id=spec_id)
        if s is None:
            raise RuntimeError(f"missing spec: {spec_id}")
        specs.append(s)
    add_specs_to_parser(p=p, specs=specs)

    p.add_argument("--skip-unsupported", action="store_true", default=True)
    p.add_argument("--skip-existing", action="store_true", default=True)
    p.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    p.add_argument("--force", action="store_true", default=False)
    p.add_argument("--unique-element-ids", action="store_true", default=True)
    p.add_argument("--no-unique-element-ids", dest="unique_element_ids", action="store_false")

    p.add_argument("--pipeline-version", default=None, help="Override pipeline version")
    p.add_argument("--run-id", default=None, help="Override run ID (UUID)")
    p.add_argument("--timestamp-utc", default=None, help="Override timestamp (ISO 8601)")

    p.add_argument("--no-retry", dest="retry_enabled", action="store_false", default=True)
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--base-backoff-ms", type=int, default=200)
    p.add_argument("--max-backoff-ms", type=int, default=5000)

    p.add_argument("--workers", type=int, default=1, help="Number of concurrent workers")
    p.add_argument(
        "--stale-lock-ttl-s",
        type=int,
        default=None,
        help="TTL in seconds to break stale file locks (default: None, disabled)",
    )

    # Destination configs
    # WebhookConfigV1.add_cli_args(p)
    # KafkaConfigV1.add_cli_args(p)
    # WeaviateConfigV1.add_cli_args(p)
    # Destination configs are spec-driven (see above).


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="zephyr-ingest")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run ingest pipeline over a local file path")
    _add_runlike_args(p=run, paths_required=True)

    replay = sub.add_parser("replay-delivery", help="Replay failed delivery DLQ records")
    replay.add_argument(
        "--out", default=".cache/out", help="Output root directory containing _dlq/"
    )
    replay.add_argument("--webhook-url", required=True, help="Webhook URL to resend payloads to")
    replay.add_argument("--webhook-timeout-s", type=float, default=10.0)
    replay.add_argument("--limit", type=int, default=None)
    replay.add_argument("--dry-run", action="store_true", default=False)
    replay.add_argument("--no-move-done", dest="move_done", action="store_false", default=True)

    cfg = sub.add_parser("config", help="Config utilities")
    cfg_sub = cfg.add_subparsers(dest="config_cmd", required=True)

    resolve = cfg_sub.add_parser("resolve", help="Resolve config file + overlays and print JSON")
    _add_runlike_args(p=resolve, paths_required=False)
    resolve.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Fail if config file does not declare schema_version",
    )
    # For resolve, --config should be required in practice.
    # We enforce it in parsing to keep error handling consistent (ConfigError -> exit 2).

    init = cfg_sub.add_parser("init", help="Print a starter TOML config to stdout or file")
    init.add_argument(
        "--out",
        type=str,
        default=None,
        help="Write generated config to this path (default: stdout)",
    )

    spec = sub.add_parser("spec", help="Connector spec utilities")
    spec_sub = spec.add_subparsers(dest="spec_cmd", required=True)

    spec_list = spec_sub.add_parser("list", help="List available spec IDs")
    _ = spec_list  # no args

    spec_show = spec_sub.add_parser("show", help="Show a spec by ID")
    spec_show.add_argument("--id", required=True, help="Spec ID (see `spec list`)")
    spec_show.add_argument(
        "--format",
        default="zephyr",
        choices=["zephyr", "jsonschema"],
        help="Output format: zephyr (raw) or jsonschema",
    )

    return p


def _parse_run_cmd(ns: argparse.Namespace, argv: Sequence[str]) -> RunCmd:
    present = collect_present_flags(argv)

    config_path = get_opt_str(ns, "config")
    file_cfg: ConfigFileV1 | None = None

    if config_path is not None:
        file_cfg = load_config_file_v1(path=Path(config_path))

    sources: ConfigSourcesV1 = {}

    def _src(flag: str, file_val: object | None) -> ConfigValueSource:
        if flag in present:
            return "cli"
        if file_val is not None:
            return "file"
        return "default"

    def _src_bool(flags: tuple[str, ...], file_val: object | None) -> ConfigValueSource:
        if any_flag_present(present, *flags):
            return "cli"
        if file_val is not None:
            return "file"
        return "default"

    # -------------------------
    # helpers: choose CLI vs FILE vs default
    # -------------------------

    def _choose_str(*, flag: str, cli_val: str, file_val: str | None) -> str:
        if flag in present:
            return cli_val

        return file_val if file_val is not None else cli_val

    def _choose_opt_str(*, flag: str, cli_val: str | None, file_val: str | None) -> str | None:
        if flag in present:
            return cli_val

        return file_val if file_val is not None else cli_val

    def _choose_float(*, flag: str, cli_val: float, file_val: float | None) -> float:
        if flag in present:
            return cli_val

        return file_val if file_val is not None else cli_val

    def _choose_int(*, flag: str, cli_val: int, file_val: int | None) -> int:
        if flag in present:
            return cli_val

        return file_val if file_val is not None else cli_val

    def _choose_bool(*, flags: tuple[str, ...], cli_val: bool, file_val: bool | None) -> bool:
        if any_flag_present(present, *flags):
            return cli_val

        return file_val if file_val is not None else cli_val

    run_file = file_cfg.run if file_cfg is not None else None
    retry_file = file_cfg.retry if file_cfg is not None else None
    dest_file = file_cfg.destinations if file_cfg is not None else None

    # -------------------------
    # strategy / backend
    # -------------------------
    strategy_str_cli = get_req_str(ns, "strategy")
    file_strategy = None if run_file is None else run_file.strategy
    strategy_str = _choose_str(
        flag="--strategy",
        cli_val=strategy_str_cli,
        file_val=file_strategy,
    )
    sources["runner.strategy"] = _src("--strategy", file_strategy)
    try:
        strategy = PartitionStrategy(strategy_str)
    except ValueError as e:
        raise ConfigError(f"Invalid --strategy: {strategy_str}") from e

    backend_cli = get_req_str(ns, "backend")
    file_backend = None if run_file is None else run_file.backend
    backend = _choose_str(
        flag="--backend",
        cli_val=backend_cli,
        file_val=file_backend,
    )
    sources["backend.kind"] = _src("--backend", file_backend)
    if backend not in ("local", "uns-api"):
        raise ConfigError(f"Invalid --backend: {backend}")

    # -------------------------
    # uns-api fields
    # -------------------------
    file_uns_api_url = None if run_file is None else run_file.uns_api_url
    uns_api_url = _choose_str(
        flag="--uns-api-url",
        cli_val=get_req_str(ns, "uns_api_url"),
        file_val=file_uns_api_url,
    )
    file_uns_api_timeout = None if run_file is None else run_file.uns_api_timeout_s
    uns_api_timeout_s = _choose_float(
        flag="--uns-api-timeout-s",
        cli_val=get_float(ns, "uns_api_timeout_s"),
        file_val=file_uns_api_timeout,
    )

    # uns_api_key precedence (secrets): CLI explicit > ENV > FILE > DEFAULT(None)
    file_uns_api_key = None if run_file is None else run_file.uns_api_key
    uns_api_key_base = _choose_opt_str(
        flag="--uns-api-key",
        cli_val=get_opt_str(ns, "uns_api_key"),
        file_val=file_uns_api_key,
    )
    uns_api_key_src: ConfigValueSource
    if backend == "uns-api" and "--uns-api-key" not in present:
        env_key = first_env("ZEPHYR_UNS_API_KEY", "UNS_API_KEY", "UNSTRUCTURED_API_KEY")
        if env_key is not None:
            uns_api_key_base = env_key
            uns_api_key_src = "env"
        elif file_uns_api_key is not None:
            uns_api_key_src = "file"
        else:
            uns_api_key_src = "default"
    elif backend == "uns-api" and "--uns-api-key" in present:
        uns_api_key_src = "cli"
    else:
        uns_api_key_src = "default"
    uns_api_key = uns_api_key_base if backend == "uns-api" else None
    if backend == "uns-api":
        sources["backend.url"] = _src("--uns-api-url", file_uns_api_url)
        sources["backend.timeout_s"] = _src("--uns-api-timeout-s", file_uns_api_timeout)
        sources["backend.api_key"] = uns_api_key_src

    # -------------------------
    # runner flags
    # -------------------------
    file_skip_existing = None if run_file is None else run_file.skip_existing
    skip_existing = _choose_bool(
        flags=("--skip-existing", "--no-skip-existing"),
        cli_val=get_bool(ns, "skip_existing"),
        file_val=file_skip_existing,
    )
    sources["runner.skip_existing"] = _src_bool(
        ("--skip-existing", "--no-skip-existing"), file_skip_existing
    )

    file_skip_unsupported = None if run_file is None else run_file.skip_unsupported
    skip_unsupported = _choose_bool(
        flags=("--skip-unsupported",),
        cli_val=get_bool(ns, "skip_unsupported"),
        file_val=file_skip_unsupported,
    )
    sources["runner.skip_unsupported"] = _src_bool(("--skip-unsupported",), file_skip_unsupported)

    file_force = None if run_file is None else run_file.force
    force = _choose_bool(
        flags=("--force",),
        cli_val=get_bool(ns, "force"),
        file_val=file_force,
    )
    sources["runner.force"] = _src_bool(("--force",), file_force)

    file_unique_ids = None if run_file is None else run_file.unique_element_ids
    unique_element_ids = _choose_bool(
        flags=("--unique-element-ids", "--no-unique-element-ids"),
        cli_val=get_bool(ns, "unique_element_ids"),
        file_val=file_unique_ids,
    )
    sources["runner.unique_element_ids"] = _src_bool(
        ("--unique-element-ids", "--no-unique-element-ids"), file_unique_ids
    )

    file_workers = None if run_file is None else run_file.workers
    workers = _choose_int(
        flag="--workers",
        cli_val=get_int(ns, "workers"),
        file_val=file_workers,
    )
    sources["runner.workers"] = _src("--workers", file_workers)

    file_stale_lock = None if run_file is None else run_file.stale_lock_ttl_s
    stale_lock_ttl_cli = get_opt_int(ns, "stale_lock_ttl_s")
    stale_lock_ttl_s = _choose_opt_str(
        flag="--stale-lock-ttl-s",
        cli_val=None if stale_lock_ttl_cli is None else str(stale_lock_ttl_cli),
        file_val=None if file_stale_lock is None else str(file_stale_lock),
    )
    stale_lock_ttl_s_i: int | None
    if stale_lock_ttl_s is None:
        stale_lock_ttl_s_i = None
    else:
        try:
            stale_lock_ttl_s_i = int(stale_lock_ttl_s)
        except ValueError as e:
            raise ConfigError("--stale-lock-ttl-s must be int") from e
    sources["runner.stale_lock_ttl_s"] = _src("--stale-lock-ttl-s", file_stale_lock)

    # -------------------------
    # retry (CLI explicit > FILE > DEFAULT)
    # Note: CLI has only --no-retry to explicitly override.
    # -------------------------
    retry_enabled_cli = get_bool(ns, "retry_enabled")
    retry_enabled = retry_enabled_cli
    file_retry_enabled = None if retry_file is None else retry_file.enabled
    if "--no-retry" not in present and file_retry_enabled is not None:
        retry_enabled = file_retry_enabled
    if "--no-retry" in present:
        sources["retry.enabled"] = "cli"
    elif file_retry_enabled is not None:
        sources["retry.enabled"] = "file"
    else:
        sources["retry.enabled"] = "default"

    file_max_attempts = None if retry_file is None else retry_file.max_attempts
    max_attempts = _choose_int(
        flag="--max-attempts",
        cli_val=get_int(ns, "max_attempts"),
        file_val=file_max_attempts,
    )
    sources["retry.max_attempts"] = _src("--max-attempts", file_max_attempts)

    file_base_backoff = None if retry_file is None else retry_file.base_backoff_ms
    base_backoff_ms = _choose_int(
        flag="--base-backoff-ms",
        cli_val=get_int(ns, "base_backoff_ms"),
        file_val=file_base_backoff,
    )
    sources["retry.base_backoff_ms"] = _src("--base-backoff-ms", file_base_backoff)

    file_max_backoff = None if retry_file is None else retry_file.max_backoff_ms
    max_backoff_ms = _choose_int(
        flag="--max-backoff-ms",
        cli_val=get_int(ns, "max_backoff_ms"),
        file_val=file_max_backoff,
    )
    sources["retry.max_backoff_ms"] = _src("--max-backoff-ms", file_max_backoff)

    retry = RetryConfig(
        enabled=retry_enabled,
        max_attempts=max_attempts,
        base_backoff_ms=base_backoff_ms,
        max_backoff_ms=max_backoff_ms,
    )

    # -------------------------
    # destinations: webhook (CLI explicit fields can override FILE-enabled destination)
    # -------------------------
    webhook_url_cli = get_opt_str(ns, "webhook_url")
    file_webhook = None if dest_file is None else dest_file.webhook
    webhook_url_file = None if file_webhook is None else file_webhook.url
    webhook_url = _choose_opt_str(
        flag="--webhook-url", cli_val=webhook_url_cli, file_val=webhook_url_file
    )

    webhook: WebhookConfigV1 | None = None

    if webhook_url is not None:
        if webhook_url.strip() == "":
            raise ConfigError("--webhook-url must not be empty")
        webhook_timeout_file = None if file_webhook is None else file_webhook.timeout_s
        webhook_timeout_s = _choose_float(
            flag="--webhook-timeout-s",
            cli_val=get_float(ns, "webhook_timeout_s"),
            file_val=webhook_timeout_file,
        )
        webhook = WebhookConfigV1(url=webhook_url, timeout_s=webhook_timeout_s)
        # sources
        sources["destinations.webhook.url"] = (
            "cli"
            if "--webhook-url" in present
            else ("file" if file_webhook is not None else "default")
        )
        sources["destinations.webhook.timeout_s"] = _src(
            "--webhook-timeout-s", webhook_timeout_file
        )

    # -------------------------
    # destinations: kafka
    # - If CLI specifies either topic/brokers, it must specify both.
    # - If FILE enables kafka, CLI can still override flush timeout.
    # -------------------------
    kafka_topic_cli = get_opt_str(ns, "kafka_topic")
    kafka_brokers_cli = get_opt_str(ns, "kafka_brokers")
    file_kafka = None if dest_file is None else dest_file.kafka
    kafka_topic_file = None if file_kafka is None else file_kafka.topic
    kafka_brokers_file = None if file_kafka is None else file_kafka.brokers

    kafka_cli_touched = any_flag_present(present, "--kafka-topic", "--kafka-brokers")
    kafka: KafkaConfigV1 | None = None

    if kafka_cli_touched:
        if kafka_topic_cli is None or kafka_brokers_cli is None:
            raise ConfigError("Both --kafka-topic and --kafka-brokers must be specified together")
        flush_timeout_file = None if file_kafka is None else file_kafka.flush_timeout_s
        flush_timeout_s = _choose_float(
            flag="--kafka-flush-timeout-s",
            cli_val=get_float(ns, "kafka_flush_timeout_s"),
            file_val=flush_timeout_file,
        )
        kafka = KafkaConfigV1(
            topic=kafka_topic_cli, brokers=kafka_brokers_cli, flush_timeout_s=flush_timeout_s
        )
        sources["destinations.kafka.topic"] = "cli"
        sources["destinations.kafka.brokers"] = "cli"
        sources["destinations.kafka.flush_timeout_s"] = _src(
            "--kafka-flush-timeout-s", flush_timeout_file
        )
    else:
        if kafka_topic_file is not None and kafka_brokers_file is not None:
            flush_timeout_file = None if file_kafka is None else file_kafka.flush_timeout_s
            flush_timeout_s = _choose_float(
                flag="--kafka-flush-timeout-s",
                cli_val=get_float(ns, "kafka_flush_timeout_s"),
                file_val=flush_timeout_file,
            )
            kafka = KafkaConfigV1(
                topic=kafka_topic_file, brokers=kafka_brokers_file, flush_timeout_s=flush_timeout_s
            )
            sources["destinations.kafka.topic"] = "file"
            sources["destinations.kafka.brokers"] = "file"
            sources["destinations.kafka.flush_timeout_s"] = _src(
                "--kafka-flush-timeout-s", flush_timeout_file
            )
    # -------------------------
    # destinations: weaviate
    # Enable if collection is provided either by CLI or FILE.
    # Secrets precedence: CLI explicit > ENV > FILE > DEFAULT
    # -------------------------
    weaviate_collection_cli = get_opt_str(ns, "weaviate_collection")
    file_weaviate = None if dest_file is None else dest_file.weaviate
    weaviate_collection_file = None if file_weaviate is None else file_weaviate.collection
    weaviate_collection = _choose_opt_str(
        flag="--weaviate-collection",
        cli_val=weaviate_collection_cli,
        file_val=weaviate_collection_file,
    )
    weaviate: WeaviateConfigV1 | None = None
    if weaviate_collection is not None:
        if weaviate_collection.strip() == "":
            raise ConfigError("--weaviate-collection must not be empty")
        wv_file = file_weaviate
        max_batch_errors = _choose_int(
            flag="--weaviate-max-batch-errors",
            cli_val=get_int(ns, "weaviate_max_batch_errors"),
            file_val=None if wv_file is None else wv_file.max_batch_errors,
        )
        http_host = _choose_str(
            flag="--weaviate-http-host",
            cli_val=get_req_str(ns, "weaviate_http_host"),
            file_val=None if wv_file is None else wv_file.http_host,
        )
        http_port = _choose_int(
            flag="--weaviate-http-port",
            cli_val=get_int(ns, "weaviate_http_port"),
            file_val=None if wv_file is None else wv_file.http_port,
        )
        http_secure = _choose_bool(
            flags=("--weaviate-http-secure",),
            cli_val=get_bool(ns, "weaviate_http_secure"),
            file_val=None if wv_file is None else wv_file.http_secure,
        )
        grpc_host = _choose_str(
            flag="--weaviate-grpc-host",
            cli_val=get_req_str(ns, "weaviate_grpc_host"),
            file_val=None if wv_file is None else wv_file.grpc_host,
        )
        grpc_port = _choose_int(
            flag="--weaviate-grpc-port",
            cli_val=get_int(ns, "weaviate_grpc_port"),
            file_val=None if wv_file is None else wv_file.grpc_port,
        )
        grpc_secure = _choose_bool(
            flags=("--weaviate-grpc-secure",),
            cli_val=get_bool(ns, "weaviate_grpc_secure"),
            file_val=None if wv_file is None else wv_file.grpc_secure,
        )
        # weaviate api key (secrets): CLI explicit > ENV > FILE > DEFAULT(None)
        api_key_base = _choose_opt_str(
            flag="--weaviate-api-key",
            cli_val=get_opt_str(ns, "weaviate_api_key"),
            file_val=None if wv_file is None else wv_file.api_key,
        )
        weaviate_api_key_src: ConfigValueSource
        if "--weaviate-api-key" not in present:
            env_wv = first_env("ZEPHYR_WEAVIATE_API_KEY", "WEAVIATE_API_KEY")
            if env_wv is not None:
                api_key_base = env_wv
                weaviate_api_key_src = "env"
            elif wv_file is not None and wv_file.api_key is not None:
                weaviate_api_key_src = "file"
            else:
                weaviate_api_key_src = "default"
        else:
            weaviate_api_key_src = "cli"

        skip_init_checks = _choose_bool(
            flags=("--weaviate-skip-init-checks",),
            cli_val=get_bool(ns, "weaviate_skip_init_checks"),
            file_val=None if wv_file is None else wv_file.skip_init_checks,
        )

        weaviate = WeaviateConfigV1(
            collection=weaviate_collection,
            max_batch_errors=max_batch_errors,
            http_host=http_host,
            http_port=http_port,
            http_secure=http_secure,
            grpc_host=grpc_host,
            grpc_port=grpc_port,
            grpc_secure=grpc_secure,
            api_key=api_key_base,
            skip_init_checks=skip_init_checks,
        )
        # sources
        sources["destinations.weaviate.collection"] = (
            "cli"
            if "--weaviate-collection" in present
            else ("file" if file_weaviate is not None else "default")
        )
        sources["destinations.weaviate.max_batch_errors"] = _src(
            "--weaviate-max-batch-errors", None if wv_file is None else wv_file.max_batch_errors
        )
        sources["destinations.weaviate.http_host"] = _src(
            "--weaviate-http-host", None if wv_file is None else wv_file.http_host
        )
        sources["destinations.weaviate.http_port"] = _src(
            "--weaviate-http-port", None if wv_file is None else wv_file.http_port
        )
        sources["destinations.weaviate.http_secure"] = _src_bool(
            ("--weaviate-http-secure",), None if wv_file is None else wv_file.http_secure
        )
        sources["destinations.weaviate.grpc_host"] = _src(
            "--weaviate-grpc-host", None if wv_file is None else wv_file.grpc_host
        )
        sources["destinations.weaviate.grpc_port"] = _src(
            "--weaviate-grpc-port", None if wv_file is None else wv_file.grpc_port
        )
        sources["destinations.weaviate.grpc_secure"] = _src_bool(
            ("--weaviate-grpc-secure",), None if wv_file is None else wv_file.grpc_secure
        )
        sources["destinations.weaviate.api_key"] = weaviate_api_key_src
        sources["destinations.weaviate.skip_init_checks"] = _src_bool(
            ("--weaviate-skip-init-checks",), None if wv_file is None else wv_file.skip_init_checks
        )

    return RunCmd(
        paths=get_str_list(ns, "paths"),
        glob=get_req_str(ns, "glob"),
        out=get_req_str(ns, "out"),
        strategy=strategy,
        backend=backend,
        # uns_api_url=get_req_str(ns, "uns_api_url"),
        uns_api_url=uns_api_url,
        uns_api_key=uns_api_key,
        # uns_api_timeout_s=get_float(ns, "uns_api_timeout_s"),
        # skip_unsupported=get_bool(ns, "skip_unsupported"),
        # skip_existing=get_bool(ns, "skip_existing"),
        # force=get_bool(ns, "force"),
        # unique_element_ids=get_bool(ns, "unique_element_ids"),
        uns_api_timeout_s=uns_api_timeout_s,
        skip_unsupported=skip_unsupported,
        skip_existing=skip_existing,
        force=force,
        unique_element_ids=unique_element_ids,
        pipeline_version=get_opt_str(ns, "pipeline_version"),
        run_id=get_opt_str(ns, "run_id"),
        timestamp_utc=get_opt_str(ns, "timestamp_utc"),
        retry=retry,
        # workers=get_int(ns, "workers"),
        # stale_lock_ttl_s=get_opt_int(ns, "stale_lock_ttl_s"),
        workers=workers,
        stale_lock_ttl_s=stale_lock_ttl_s_i,
        webhook=webhook,
        kafka=kafka,
        weaviate=weaviate,
        config_sources=sources,
    )


def _parse_cmd(
    argv: Sequence[str],
) -> RunCmd | ReplayDeliveryCmd | ResolveConfigCmd | InitConfigCmd | SpecListCmd | SpecShowCmd:
    p = _build_parser()
    ns = p.parse_args(list(argv))

    if ns.cmd == "run":
        return _parse_run_cmd(ns, argv)

    if ns.cmd == "config" and ns.config_cmd == "resolve":
        config_path = get_opt_str(ns, "config")
        if config_path is None:
            raise ConfigError("--config is required for `config resolve`")

        # Parse using the same resolver as run. This will load/merge the config file.
        run_cmd = _parse_run_cmd(ns, argv)
        strict = get_bool(ns, "strict")
        return ResolveConfigCmd(run_cmd=run_cmd, config_path=config_path, strict=strict)

    if ns.cmd == "config" and ns.config_cmd == "init":
        out = get_opt_str(ns, "out")
        return InitConfigCmd(out=out)

    if ns.cmd == "spec" and ns.spec_cmd == "list":
        return SpecListCmd()

    if ns.cmd == "spec" and ns.spec_cmd == "show":
        spec_id = get_req_str(ns, "id")
        fmt = get_req_str(ns, "format")
        return SpecShowCmd(spec_id=spec_id, fmt=fmt)

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


def _field_type_to_jsonschema_type(t: SpecFieldTypeV1) -> str:
    if t == "string":
        return "string"
    if t == "int":
        return "integer"
    if t == "float":
        return "number"
    # bool
    return "boolean"


def spec_to_jsonschema(*, spec: ConnectorSpecV1) -> dict[str, object]:
    """
    Convert ConnectorSpecV1 to a simple JSONSchema object.
    Notes:
    - This is a UI/docs schema, not a runtime validator.
    - Field `name` is used as the property name (flat dotted key).
    """
    props: dict[str, object] = {}
    required: list[str] = []

    for f in spec["fields"]:
        p: dict[str, object] = {
            "type": _field_type_to_jsonschema_type(f["type"]),
        }
        if "help" in f:
            p["description"] = f["help"]
        if "default" in f:
            p["default"] = f["default"]
        if "choices" in f:
            p["enum"] = f["choices"]
        if f.get("secret", False):
            p["writeOnly"] = True
        if "examples" in f:
            p["examples"] = f["examples"]
        props[f["name"]] = p
        if f["required"]:
            required.append(f["name"])

    schema: dict[str, object] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "title": spec["id"],
        "description": spec["description"],
        "properties": props,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema


def _default_config_toml() -> str:
    # Keep this TOML valid by default:
    # - Only include tables/keys that our parser accepts without required destination fields.
    # - Provide optional destinations as commented examples.
    return """\
schema_version = 1

[run]
# backend = "local"  # or "uns-api"
backend = "local"
strategy = "auto"   # auto/fast/hi_res/ocr_only

# If backend="uns-api", you typically also set:
# uns_api_url = "http://localhost:8001/general/v0/general"
# uns_api_timeout_s = 60.0
# uns_api_key = "..."  # Prefer ENV injection (ZEPHYR_UNS_API_KEY)

[retry]
enabled = true
max_attempts = 3
base_backoff_ms = 200
max_backoff_ms = 5000

# -------------------------------
# Optional destinations (examples)
# Uncomment a block to enable it.
# -------------------------------

# [destinations.webhook]
# url = "http://localhost:9000/ingest"
# timeout_s = 10.0

# [destinations.kafka]
# topic = "zephyr.delivery"
# brokers = "localhost:9092"
# flush_timeout_s = 10.0

# [destinations.weaviate]
# collection = "ZephyrDoc"
# max_batch_errors = 0
# http_host = "localhost"
# http_port = 8080
# http_secure = false
# grpc_host = "localhost"
# grpc_port = 50051
# grpc_secure = false
# skip_init_checks = true
# api_key = "..."  # Prefer ENV injection (ZEPHYR_WEAVIATE_API_KEY)
"""


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
) -> tuple[WeaviateClientProtocol, "WeaviateCollectionProtocol"]:
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


def _build_config_snapshot(*, cmd: RunCmd) -> ConfigSnapshotV1:
    destinations: DestinationsSnapshotV1 = {
        "filesystem": {"enabled": True},
    }
    if cmd.webhook is not None:
        destinations["webhook"] = cmd.webhook.to_snapshot_v1()
    if cmd.kafka is not None:
        destinations["kafka"] = cmd.kafka.to_snapshot_v1()
    if cmd.weaviate is not None:
        destinations["weaviate"] = cmd.weaviate.to_snapshot_v1()

    backend: BackendSnapshotV1
    if cmd.backend == "uns-api":
        backend = {
            "kind": "uns-api",
            "url": cmd.uns_api_url,
            "timeout_s": cmd.uns_api_timeout_s,
            "api_key": None if cmd.uns_api_key is None else "***",
        }
    else:
        backend = {"kind": "local"}

    return {
        "schema_version": CONFIG_SNAPSHOT_SCHEMA_VERSION,
        "input": {
            "paths_count": len(cmd.paths),
            "glob": cmd.glob,
            "source": "local_file",
        },
        "runner": {
            "out": cmd.out,
            "strategy": str(cmd.strategy),
            "skip_existing": cmd.skip_existing,
            "skip_unsupported": cmd.skip_unsupported,
            "force": cmd.force,
            "unique_element_ids": cmd.unique_element_ids,
            "workers": cmd.workers,
            "stale_lock_ttl_s": cmd.stale_lock_ttl_s,
        },
        "retry": {
            "enabled": cmd.retry.enabled,
            "max_attempts": cmd.retry.max_attempts,
            "base_backoff_ms": cmd.retry.base_backoff_ms,
            "max_backoff_ms": cmd.retry.max_backoff_ms,
        },
        "backend": backend,
        "destinations": destinations,
        "sources": cmd.config_sources,
    }


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
        from zephyr_ingest.destinations.weaviate import WeaviateDestination

        weaviate_client, collection = _make_weaviate_collection_or_exit(cfg=cmd.weaviate)
        dests.append(
            WeaviateDestination(
                collection_name=cmd.weaviate.collection,
                collection=collection,  # runtime object, protocol-checked in _internal factory
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

    if isinstance(cmd, ResolveConfigCmd):
        # Enforce schema_version presence in strict mode.
        file_cfg = load_config_file_v1(path=Path(cmd.config_path))
        if cmd.strict and not file_cfg.schema_version_present:
            logging.error("config strict mode: root.schema_version is required")
            return 2
        snap = _build_config_snapshot(cmd=cmd.run_cmd)
        out_obj = {
            "config_file": {
                "path": cmd.config_path,
                "schema_version": file_cfg.schema_version,
                "schema_version_source": "file" if file_cfg.schema_version_present else "default",
            },
            "config_snapshot": snap,
        }
        sys.stdout.write(json.dumps(out_obj, ensure_ascii=False, indent=2))
        sys.stdout.write("\n")
        return 0

    if isinstance(cmd, InitConfigCmd):
        text = _default_config_toml()
        if cmd.out is None:
            sys.stdout.write(text)
            return 0

        out_path = Path(cmd.out)
        if out_path.exists():
            logging.error("config init: output path already exists: %s", str(out_path))
            return 2
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        logging.info("wrote config: %s", str(out_path))
        return 0

    if isinstance(cmd, SpecListCmd):
        ids = list_spec_ids()
        sys.stdout.write(json.dumps({"spec_ids": ids}, ensure_ascii=False, indent=2))
        sys.stdout.write("\n")
        return 0

    if isinstance(cmd, SpecShowCmd):
        spec = get_spec(spec_id=cmd.spec_id)
        if spec is None:
            logging.error("unknown spec id: %s", cmd.spec_id)
            return 2
        if cmd.fmt == "zephyr":
            sys.stdout.write(json.dumps(spec, ensure_ascii=False, indent=2))
            sys.stdout.write("\n")
            return 0
        if cmd.fmt == "jsonschema":
            js = spec_to_jsonschema(spec=spec)
            sys.stdout.write(json.dumps(js, ensure_ascii=False, indent=2))
            sys.stdout.write("\n")
            return 0
        logging.error("unsupported format: %s", cmd.fmt)
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
        config_snapshot = _build_config_snapshot(cmd=cmd)
        run_documents(
            docs=docs,
            cfg=cfg,
            ctx=ctx,
            destination=destination,
            config_snapshot=config_snapshot,
        )

        return 0
    finally:
        if weaviate_client is not None:
            weaviate_client.close()


if __name__ == "__main__":
    raise SystemExit(main())
