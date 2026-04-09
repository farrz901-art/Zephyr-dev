from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from zephyr_ingest.config.errors import ConfigError

DEFAULT_FILE_SCHEMA_VERSION = 1


def _as_table(x: object, path: str) -> dict[str, object]:
    if not isinstance(x, dict):
        raise ConfigError(f"{path} must be a TOML table")

    d = cast(dict[Any, Any], x)

    if not all(isinstance(k, str) for k in d):
        raise ConfigError(f"{path} must be a TOML table")

    return cast(dict[str, object], d)


def _unknown_keys(tbl: dict[str, object], allowed: set[str], path: str) -> None:
    bad = set(tbl.keys()) - allowed
    if bad:
        raise ConfigError(f"Unknown keys at {path}: {', '.join(sorted(bad))}")


def _opt_str(tbl: dict[str, object], key: str, path: str) -> str | None:
    v = tbl.get(key)
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s == "" else s
    raise ConfigError(f"{path}.{key} must be a string")


def _opt_int(tbl: dict[str, object], key: str, path: str) -> int | None:
    v = tbl.get(key)
    if v is None:
        return None
    if isinstance(v, bool):
        raise ConfigError(f"{path}.{key} must be an int (not bool)")
    if isinstance(v, int):
        return v
    raise ConfigError(f"{path}.{key} must be an int")


def _opt_float(tbl: dict[str, object], key: str, path: str) -> float | None:
    v = tbl.get(key)
    if v is None:
        return None
    if isinstance(v, bool):
        raise ConfigError(f"{path}.{key} must be a float (not bool)")
    if isinstance(v, (int, float)):
        return float(v)
    raise ConfigError(f"{path}.{key} must be a float")


def _opt_bool(tbl: dict[str, object], key: str, path: str) -> bool | None:
    v = tbl.get(key)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    raise ConfigError(f"{path}.{key} must be a bool")


@dataclass(frozen=True, slots=True)
class RunFileV1:
    strategy: str | None = None
    backend: str | None = None
    uns_api_url: str | None = None
    uns_api_timeout_s: float | None = None
    uns_api_key: str | None = None

    skip_existing: bool | None = None
    skip_unsupported: bool | None = None
    force: bool | None = None
    unique_element_ids: bool | None = None
    workers: int | None = None
    stale_lock_ttl_s: int | None = None


@dataclass(frozen=True, slots=True)
class RetryFileV1:
    enabled: bool | None = None
    max_attempts: int | None = None
    base_backoff_ms: int | None = None
    max_backoff_ms: int | None = None


@dataclass(frozen=True, slots=True)
class WebhookDestFileV1:
    url: str
    timeout_s: float | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None


@dataclass(frozen=True, slots=True)
class KafkaDestFileV1:
    topic: str
    brokers: str
    flush_timeout_s: float | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None


@dataclass(frozen=True, slots=True)
class WeaviateDestFileV1:
    collection: str
    max_batch_errors: int | None = None
    timeout_s: float | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    http_host: str | None = None
    http_port: int | None = None
    http_secure: bool | None = None

    grpc_host: str | None = None
    grpc_port: int | None = None
    grpc_secure: bool | None = None

    api_key: str | None = None
    skip_init_checks: bool | None = None


@dataclass(frozen=True, slots=True)
class S3DestFileV1:
    bucket: str
    region: str
    access_key: str
    secret_key: str
    endpoint_url: str | None = None
    session_token: str | None = None
    prefix: str | None = None
    write_mode: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None


@dataclass(frozen=True, slots=True)
class OpenSearchDestFileV1:
    url: str
    index: str
    timeout_s: float | None = None
    skip_tls_verify: bool | None = None
    username: str | None = None
    password: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None


@dataclass(frozen=True, slots=True)
class ClickHouseDestFileV1:
    url: str
    table: str
    timeout_s: float | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None


@dataclass(frozen=True, slots=True)
class MongoDBDestFileV1:
    uri: str
    database: str
    collection: str
    timeout_s: float | None = None
    write_mode: str | None = None
    username: str | None = None
    password: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None


@dataclass(frozen=True, slots=True)
class LokiDestFileV1:
    url: str
    stream: str
    timeout_s: float | None = None
    tenant_id: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None


@dataclass(frozen=True, slots=True)
class DestinationsFileV1:
    webhook: WebhookDestFileV1 | None = None
    kafka: KafkaDestFileV1 | None = None
    weaviate: WeaviateDestFileV1 | None = None
    s3: S3DestFileV1 | None = None
    opensearch: OpenSearchDestFileV1 | None = None
    clickhouse: ClickHouseDestFileV1 | None = None
    mongodb: MongoDBDestFileV1 | None = None
    loki: LokiDestFileV1 | None = None


@dataclass(frozen=True, slots=True)
class ConfigFileV1:
    schema_version: int
    schema_version_present: bool
    run: RunFileV1
    retry: RetryFileV1
    destinations: DestinationsFileV1


def load_config_file_v1(*, path: Path) -> ConfigFileV1:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    top = _as_table(raw, "root")
    _unknown_keys(top, {"schema_version", "run", "retry", "destinations"}, "root")

    schema_version_present = "schema_version" in top
    schema_version = DEFAULT_FILE_SCHEMA_VERSION

    if schema_version_present:
        v = top.get("schema_version")
        if isinstance(v, bool) or not isinstance(v, int):
            raise ConfigError("root.schema_version must be an int")
        schema_version = v
    if schema_version != 1:
        raise ConfigError(f"Unsupported config schema_version: {schema_version} (expected 1)")

    run_tbl = _as_table(top.get("run", {}), "run")
    retry_tbl = _as_table(top.get("retry", {}), "retry")
    dest_tbl = _as_table(top.get("destinations", {}), "destinations")

    _unknown_keys(
        run_tbl,
        {
            "strategy",
            "backend",
            "uns_api_url",
            "uns_api_timeout_s",
            "uns_api_key",
            "skip_existing",
            "skip_unsupported",
            "force",
            "unique_element_ids",
            "workers",
            "stale_lock_ttl_s",
        },
        "run",
    )
    _unknown_keys(
        retry_tbl, {"enabled", "max_attempts", "base_backoff_ms", "max_backoff_ms"}, "retry"
    )
    _unknown_keys(
        dest_tbl,
        {"webhook", "kafka", "weaviate", "s3", "opensearch", "clickhouse", "mongodb", "loki"},
        "destinations",
    )

    webhook: WebhookDestFileV1 | None = None
    if "webhook" in dest_tbl:
        w = _as_table(dest_tbl["webhook"], "destinations.webhook")
        _unknown_keys(w, {"url", "timeout_s", "max_inflight", "rate_limit"}, "destinations.webhook")
        url = _opt_str(w, "url", "destinations.webhook")
        if url is None:
            raise ConfigError(
                "destinations.webhook.url is required when [destinations.webhook] is present"
            )
        webhook = WebhookDestFileV1(
            url=url,
            timeout_s=_opt_float(w, "timeout_s", "destinations.webhook"),
            max_inflight=_opt_int(w, "max_inflight", "destinations.webhook"),
            rate_limit=_opt_float(w, "rate_limit", "destinations.webhook"),
        )

    kafka: KafkaDestFileV1 | None = None
    if "kafka" in dest_tbl:
        k = _as_table(dest_tbl["kafka"], "destinations.kafka")
        _unknown_keys(
            k,
            {"topic", "brokers", "flush_timeout_s", "max_inflight", "rate_limit"},
            "destinations.kafka",
        )
        topic = _opt_str(k, "topic", "destinations.kafka")
        brokers = _opt_str(k, "brokers", "destinations.kafka")
        if topic is None or brokers is None:
            raise ConfigError(
                "destinations.kafka.topic and destinations.kafka.brokers "
                "are required when [destinations.kafka] is present"
            )
        kafka = KafkaDestFileV1(
            topic=topic,
            brokers=brokers,
            flush_timeout_s=_opt_float(k, "flush_timeout_s", "destinations.kafka"),
            max_inflight=_opt_int(k, "max_inflight", "destinations.kafka"),
            rate_limit=_opt_float(k, "rate_limit", "destinations.kafka"),
        )

    weaviate: WeaviateDestFileV1 | None = None
    if "weaviate" in dest_tbl:
        wv = _as_table(dest_tbl["weaviate"], "destinations.weaviate")
        _unknown_keys(
            wv,
            {
                "collection",
                "max_batch_errors",
                "timeout_s",
                "max_inflight",
                "rate_limit",
                "http_host",
                "http_port",
                "http_secure",
                "grpc_host",
                "grpc_port",
                "grpc_secure",
                "api_key",
                "skip_init_checks",
            },
            "destinations.weaviate",
        )
        collection = _opt_str(wv, "collection", "destinations.weaviate")
        if collection is None:
            raise ConfigError(
                "destinations.weaviate.collection is required "
                "when [destinations.weaviate] is present"
            )
        weaviate = WeaviateDestFileV1(
            collection=collection,
            max_batch_errors=_opt_int(wv, "max_batch_errors", "destinations.weaviate"),
            timeout_s=_opt_float(wv, "timeout_s", "destinations.weaviate"),
            max_inflight=_opt_int(wv, "max_inflight", "destinations.weaviate"),
            rate_limit=_opt_float(wv, "rate_limit", "destinations.weaviate"),
            http_host=_opt_str(wv, "http_host", "destinations.weaviate"),
            http_port=_opt_int(wv, "http_port", "destinations.weaviate"),
            http_secure=_opt_bool(wv, "http_secure", "destinations.weaviate"),
            grpc_host=_opt_str(wv, "grpc_host", "destinations.weaviate"),
            grpc_port=_opt_int(wv, "grpc_port", "destinations.weaviate"),
            grpc_secure=_opt_bool(wv, "grpc_secure", "destinations.weaviate"),
            api_key=_opt_str(wv, "api_key", "destinations.weaviate"),
            skip_init_checks=_opt_bool(wv, "skip_init_checks", "destinations.weaviate"),
        )

    s3: S3DestFileV1 | None = None
    if "s3" in dest_tbl:
        s3_tbl = _as_table(dest_tbl["s3"], "destinations.s3")
        _unknown_keys(
            s3_tbl,
            {
                "bucket",
                "region",
                "access_key",
                "secret_key",
                "endpoint_url",
                "session_token",
                "prefix",
                "write_mode",
                "max_inflight",
                "rate_limit",
            },
            "destinations.s3",
        )
        bucket = _opt_str(s3_tbl, "bucket", "destinations.s3")
        region = _opt_str(s3_tbl, "region", "destinations.s3")
        access_key = _opt_str(s3_tbl, "access_key", "destinations.s3")
        secret_key = _opt_str(s3_tbl, "secret_key", "destinations.s3")
        if bucket is None or region is None or access_key is None or secret_key is None:
            raise ConfigError(
                "destinations.s3.bucket, destinations.s3.region, "
                "destinations.s3.access_key, and destinations.s3.secret_key "
                "are required when [destinations.s3] is present"
            )
        s3 = S3DestFileV1(
            bucket=bucket,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=_opt_str(s3_tbl, "endpoint_url", "destinations.s3"),
            session_token=_opt_str(s3_tbl, "session_token", "destinations.s3"),
            prefix=_opt_str(s3_tbl, "prefix", "destinations.s3"),
            write_mode=_opt_str(s3_tbl, "write_mode", "destinations.s3"),
            max_inflight=_opt_int(s3_tbl, "max_inflight", "destinations.s3"),
            rate_limit=_opt_float(s3_tbl, "rate_limit", "destinations.s3"),
        )

    opensearch: OpenSearchDestFileV1 | None = None
    if "opensearch" in dest_tbl:
        os_tbl = _as_table(dest_tbl["opensearch"], "destinations.opensearch")
        _unknown_keys(
            os_tbl,
            {
                "url",
                "index",
                "timeout_s",
                "skip_tls_verify",
                "username",
                "password",
                "max_inflight",
                "rate_limit",
            },
            "destinations.opensearch",
        )
        url = _opt_str(os_tbl, "url", "destinations.opensearch")
        index = _opt_str(os_tbl, "index", "destinations.opensearch")
        if url is None or index is None:
            raise ConfigError(
                "destinations.opensearch.url and destinations.opensearch.index "
                "are required when [destinations.opensearch] is present"
            )
        opensearch = OpenSearchDestFileV1(
            url=url,
            index=index,
            timeout_s=_opt_float(os_tbl, "timeout_s", "destinations.opensearch"),
            skip_tls_verify=_opt_bool(os_tbl, "skip_tls_verify", "destinations.opensearch"),
            username=_opt_str(os_tbl, "username", "destinations.opensearch"),
            password=_opt_str(os_tbl, "password", "destinations.opensearch"),
            max_inflight=_opt_int(os_tbl, "max_inflight", "destinations.opensearch"),
            rate_limit=_opt_float(os_tbl, "rate_limit", "destinations.opensearch"),
        )

    clickhouse: ClickHouseDestFileV1 | None = None
    if "clickhouse" in dest_tbl:
        ch_tbl = _as_table(dest_tbl["clickhouse"], "destinations.clickhouse")
        _unknown_keys(
            ch_tbl,
            {
                "url",
                "table",
                "timeout_s",
                "database",
                "username",
                "password",
                "max_inflight",
                "rate_limit",
            },
            "destinations.clickhouse",
        )
        url = _opt_str(ch_tbl, "url", "destinations.clickhouse")
        table = _opt_str(ch_tbl, "table", "destinations.clickhouse")
        if url is None or table is None:
            raise ConfigError(
                "destinations.clickhouse.url and destinations.clickhouse.table "
                "are required when [destinations.clickhouse] is present"
            )
        clickhouse = ClickHouseDestFileV1(
            url=url,
            table=table,
            timeout_s=_opt_float(ch_tbl, "timeout_s", "destinations.clickhouse"),
            database=_opt_str(ch_tbl, "database", "destinations.clickhouse"),
            username=_opt_str(ch_tbl, "username", "destinations.clickhouse"),
            password=_opt_str(ch_tbl, "password", "destinations.clickhouse"),
            max_inflight=_opt_int(ch_tbl, "max_inflight", "destinations.clickhouse"),
            rate_limit=_opt_float(ch_tbl, "rate_limit", "destinations.clickhouse"),
        )

    mongodb: MongoDBDestFileV1 | None = None
    if "mongodb" in dest_tbl:
        mongo_tbl = _as_table(dest_tbl["mongodb"], "destinations.mongodb")
        _unknown_keys(
            mongo_tbl,
            {
                "uri",
                "database",
                "collection",
                "timeout_s",
                "write_mode",
                "username",
                "password",
                "max_inflight",
                "rate_limit",
            },
            "destinations.mongodb",
        )
        uri = _opt_str(mongo_tbl, "uri", "destinations.mongodb")
        database = _opt_str(mongo_tbl, "database", "destinations.mongodb")
        collection = _opt_str(mongo_tbl, "collection", "destinations.mongodb")
        if uri is None or database is None or collection is None:
            raise ConfigError(
                "destinations.mongodb.uri, destinations.mongodb.database, and "
                "destinations.mongodb.collection are required when "
                "[destinations.mongodb] is present"
            )
        mongodb = MongoDBDestFileV1(
            uri=uri,
            database=database,
            collection=collection,
            timeout_s=_opt_float(mongo_tbl, "timeout_s", "destinations.mongodb"),
            write_mode=_opt_str(mongo_tbl, "write_mode", "destinations.mongodb"),
            username=_opt_str(mongo_tbl, "username", "destinations.mongodb"),
            password=_opt_str(mongo_tbl, "password", "destinations.mongodb"),
            max_inflight=_opt_int(mongo_tbl, "max_inflight", "destinations.mongodb"),
            rate_limit=_opt_float(mongo_tbl, "rate_limit", "destinations.mongodb"),
        )

    loki: LokiDestFileV1 | None = None
    if "loki" in dest_tbl:
        loki_tbl = _as_table(dest_tbl["loki"], "destinations.loki")
        _unknown_keys(
            loki_tbl,
            {
                "url",
                "stream",
                "timeout_s",
                "tenant_id",
                "max_inflight",
                "rate_limit",
            },
            "destinations.loki",
        )
        url = _opt_str(loki_tbl, "url", "destinations.loki")
        stream = _opt_str(loki_tbl, "stream", "destinations.loki")
        if url is None or stream is None:
            raise ConfigError(
                "destinations.loki.url and destinations.loki.stream are required "
                "when [destinations.loki] is present"
            )
        loki = LokiDestFileV1(
            url=url,
            stream=stream,
            timeout_s=_opt_float(loki_tbl, "timeout_s", "destinations.loki"),
            tenant_id=_opt_str(loki_tbl, "tenant_id", "destinations.loki"),
            max_inflight=_opt_int(loki_tbl, "max_inflight", "destinations.loki"),
            rate_limit=_opt_float(loki_tbl, "rate_limit", "destinations.loki"),
        )

    return ConfigFileV1(
        schema_version=schema_version,
        schema_version_present=schema_version_present,
        run=RunFileV1(
            strategy=_opt_str(run_tbl, "strategy", "run"),
            backend=_opt_str(run_tbl, "backend", "run"),
            uns_api_url=_opt_str(run_tbl, "uns_api_url", "run"),
            uns_api_timeout_s=_opt_float(run_tbl, "uns_api_timeout_s", "run"),
            uns_api_key=_opt_str(run_tbl, "uns_api_key", "run"),
            skip_existing=_opt_bool(run_tbl, "skip_existing", "run"),
            skip_unsupported=_opt_bool(run_tbl, "skip_unsupported", "run"),
            force=_opt_bool(run_tbl, "force", "run"),
            unique_element_ids=_opt_bool(run_tbl, "unique_element_ids", "run"),
            workers=_opt_int(run_tbl, "workers", "run"),
            stale_lock_ttl_s=_opt_int(run_tbl, "stale_lock_ttl_s", "run"),
        ),
        retry=RetryFileV1(
            enabled=_opt_bool(retry_tbl, "enabled", "retry"),
            max_attempts=_opt_int(retry_tbl, "max_attempts", "retry"),
            base_backoff_ms=_opt_int(retry_tbl, "base_backoff_ms", "retry"),
            max_backoff_ms=_opt_int(retry_tbl, "max_backoff_ms", "retry"),
        ),
        destinations=DestinationsFileV1(
            webhook=webhook,
            kafka=kafka,
            weaviate=weaviate,
            s3=s3,
            opensearch=opensearch,
            clickhouse=clickhouse,
            mongodb=mongodb,
            loki=loki,
        ),
    )
