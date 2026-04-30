from __future__ import annotations

import argparse
from dataclasses import dataclass

from zephyr_ingest.config.argparse_extract import (
    get_bool,
    get_float,
    get_int,
    get_opt_float,
    get_opt_int,
    get_opt_str,
    get_req_str,
)
from zephyr_ingest.config.errors import ConfigError
from zephyr_ingest.config.snapshot_v1 import (
    ClickHouseDestinationSnapshotV1,
    KafkaDestinationSnapshotV1,
    LokiDestinationSnapshotV1,
    MongoDBDestinationSnapshotV1,
    OpenSearchDestinationSnapshotV1,
    S3DestinationSnapshotV1,
    SqliteDestinationSnapshotV1,
    WeaviateDestinationSnapshotV1,
    WebhookDestinationSnapshotV1,
)


@dataclass(frozen=True, slots=True)
class WebhookConfigV1:
    url: str
    timeout_s: float
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def add_cli_args(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--webhook-url",
            type=str,
            default=None,
            help="Webhook URL (enables destination).",
        )
        p.add_argument(
            "--webhook-timeout-s",
            type=float,
            default=10.0,
            help="Webhook request timeout (seconds).",
        )
        p.add_argument(
            "--webhook-max-inflight",
            type=int,
            default=None,
            help="Webhook max inflight deliveries.",
        )
        p.add_argument(
            "--webhook-rate-limit",
            type=float,
            default=None,
            help="Webhook delivery rate limit.",
        )

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> WebhookConfigV1 | None:
        url = get_opt_str(ns, "webhook_url")
        if url is None:
            return None
        timeout_s = get_float(ns, "webhook_timeout_s")
        return WebhookConfigV1(
            url=url,
            timeout_s=timeout_s,
            max_inflight=get_opt_int(ns, "webhook_max_inflight"),
            rate_limit=get_opt_float(ns, "webhook_rate_limit"),
        )

    def to_snapshot_v1(self) -> WebhookDestinationSnapshotV1:
        snapshot: WebhookDestinationSnapshotV1 = {"url": self.url, "timeout_s": self.timeout_s}
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot

    def redacted_dict(self) -> dict[str, object]:
        # Keep older call sites stable (dict[str, object])
        # while the typed snapshot is the source of truth.
        return dict(self.to_snapshot_v1())

    # def redacted_dict(self) -> dict[str, object]:
    #     return {"url": self.url, "timeout_s": self.timeout_s}


@dataclass(frozen=True, slots=True)
class KafkaConfigV1:
    topic: str
    brokers: str
    flush_timeout_s: float
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def add_cli_args(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--kafka-topic",
            type=str,
            default=None,
            help="Kafka topic name (requires --kafka-brokers).",
        )
        p.add_argument(
            "--kafka-brokers",
            type=str,
            default=None,
            help="Kafka broker addresses, comma-separated (e.g. localhost:9092)",
        )
        p.add_argument(
            "--kafka-flush-timeout-s",
            type=float,
            default=10.0,
            help="Kafka producer flush timeout in seconds.",
        )
        p.add_argument(
            "--kafka-max-inflight",
            type=int,
            default=None,
            help="Kafka max inflight deliveries.",
        )
        p.add_argument(
            "--kafka-rate-limit",
            type=float,
            default=None,
            help="Kafka delivery rate limit.",
        )

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> KafkaConfigV1 | None:
        topic = get_opt_str(ns, "kafka_topic")
        brokers = get_opt_str(ns, "kafka_brokers")

        if topic is None and brokers is None:
            return None

        if topic is None or brokers is None:
            raise ConfigError("Both --kafka-topic and --kafka-brokers must be specified together")

        flush_timeout_s = get_float(ns, "kafka_flush_timeout_s")
        return KafkaConfigV1(
            topic=topic,
            brokers=brokers,
            flush_timeout_s=flush_timeout_s,
            max_inflight=get_opt_int(ns, "kafka_max_inflight"),
            rate_limit=get_opt_float(ns, "kafka_rate_limit"),
        )

    def to_snapshot_v1(self) -> KafkaDestinationSnapshotV1:
        snapshot: KafkaDestinationSnapshotV1 = {
            "topic": self.topic,
            "brokers": self.brokers,
            "flush_timeout_s": self.flush_timeout_s,
        }
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot

    def redacted_dict(self) -> dict[str, object]:
        """兼容旧接口，复用快照逻辑"""
        return dict(self.to_snapshot_v1())

    # def redacted_dict(self) -> dict[str, object]:
    #     return {
    #         "topic": self.topic,
    #         "brokers": self.brokers,
    #         "flush_timeout_s": self.flush_timeout_s,
    #     }


@dataclass(frozen=True, slots=True)
class WeaviateConfigV1:
    collection: str
    max_batch_errors: int

    http_host: str
    http_port: int
    http_secure: bool

    grpc_host: str
    grpc_port: int
    grpc_secure: bool

    api_key: str | None
    skip_init_checks: bool
    timeout_s: float | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def add_cli_args(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--weaviate-collection",
            type=str,
            default=None,
            help="Weaviate collection name (enables destination).",
        )
        p.add_argument(
            "--weaviate-max-batch-errors",
            type=int,
            default=0,
            help="Max tolerated batch errors; 0 means any error fails delivery (default: 0)",
        )
        p.add_argument(
            "--weaviate-timeout-s",
            type=float,
            default=None,
            help="Weaviate request timeout in seconds.",
        )
        p.add_argument(
            "--weaviate-max-inflight",
            type=int,
            default=None,
            help="Weaviate max inflight deliveries.",
        )
        p.add_argument(
            "--weaviate-rate-limit",
            type=float,
            default=None,
            help="Weaviate delivery rate limit.",
        )

        p.add_argument("--weaviate-http-host", type=str, default="localhost")
        p.add_argument("--weaviate-http-port", type=int, default=8080)
        p.add_argument("--weaviate-http-secure", action="store_true", default=False)

        p.add_argument("--weaviate-grpc-host", type=str, default="localhost")
        p.add_argument("--weaviate-grpc-port", type=int, default=50051)
        p.add_argument("--weaviate-grpc-secure", action="store_true", default=False)

        p.add_argument(
            "--weaviate-api-key",
            type=str,
            default=None,
            help="Weaviate API key. Prefer ENV injection.",
        )
        p.add_argument(
            "--weaviate-skip-init-checks",
            action="store_true",
            default=False,
            help="Skip Weaviate client init checks",
        )

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> WeaviateConfigV1 | None:
        collection = get_opt_str(ns, "weaviate_collection")
        if collection is None:
            return None

        return WeaviateConfigV1(
            collection=collection,
            max_batch_errors=get_int(ns, "weaviate_max_batch_errors"),
            timeout_s=get_opt_float(ns, "weaviate_timeout_s"),
            max_inflight=get_opt_int(ns, "weaviate_max_inflight"),
            rate_limit=get_opt_float(ns, "weaviate_rate_limit"),
            http_host=get_req_str(ns, "weaviate_http_host"),
            http_port=get_int(ns, "weaviate_http_port"),
            http_secure=get_bool(ns, "weaviate_http_secure"),
            grpc_host=get_req_str(ns, "weaviate_grpc_host"),
            grpc_port=get_int(ns, "weaviate_grpc_port"),
            grpc_secure=get_bool(ns, "weaviate_grpc_secure"),
            api_key=get_opt_str(ns, "weaviate_api_key"),
            skip_init_checks=get_bool(ns, "weaviate_skip_init_checks"),
        )

    def to_snapshot_v1(self) -> WeaviateDestinationSnapshotV1:
        """生成强类型 Weaviate 快照，包含脱敏和格式化"""
        snapshot: WeaviateDestinationSnapshotV1 = {
            "collection": self.collection,
            "max_batch_errors": self.max_batch_errors,
            "http": f"{self.http_host}:{self.http_port} (secure={self.http_secure})",
            "grpc": f"{self.grpc_host}:{self.grpc_port} (secure={self.grpc_secure})",
            "api_key": "***" if self.api_key else None,
            "skip_init_checks": self.skip_init_checks,
        }
        if self.timeout_s is not None:
            snapshot["timeout_s"] = self.timeout_s
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot

    def redacted_dict(self) -> dict[str, object]:
        """兼容旧接口，确保 API Key 已屏蔽"""
        return dict(self.to_snapshot_v1())

    # def redacted_dict(self) -> dict[str, object]:
    #     return {
    #         "collection": self.collection,
    #         "max_batch_errors": self.max_batch_errors,
    #         "http": f"{self.http_host}:{self.http_port} secure={self.http_secure}",
    #         "grpc": f"{self.grpc_host}:{self.grpc_port} secure={self.grpc_secure}",
    #         "api_key": None if self.api_key is None else "***",
    #         "skip_init_checks": self.skip_init_checks,
    #     }


@dataclass(frozen=True, slots=True)
class S3ConfigV1:
    bucket: str
    region: str
    access_key: str
    secret_key: str
    endpoint_url: str | None = None
    session_token: str | None = None
    prefix: str = ""
    write_mode: str = "overwrite"
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> S3ConfigV1 | None:
        bucket = get_opt_str(ns, "s3_bucket")
        if bucket is None:
            return None

        region = get_req_str(ns, "s3_region")
        access_key = get_req_str(ns, "s3_access_key")
        secret_key = get_req_str(ns, "s3_secret_key")
        return S3ConfigV1(
            bucket=bucket,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=get_opt_str(ns, "s3_endpoint_url"),
            session_token=get_opt_str(ns, "s3_session_token"),
            prefix=get_opt_str(ns, "s3_prefix") or "",
            write_mode=get_req_str(ns, "s3_write_mode"),
            max_inflight=get_opt_int(ns, "s3_max_inflight"),
            rate_limit=get_opt_float(ns, "s3_rate_limit"),
        )

    def to_snapshot_v1(self) -> S3DestinationSnapshotV1:
        snapshot: S3DestinationSnapshotV1 = {
            "bucket": self.bucket,
            "region": self.region,
            "prefix": self.prefix,
            "write_mode": self.write_mode,
            "access_key": "***",
            "secret_key": "***",
            "session_token": None if self.session_token is None else "***",
        }
        if self.endpoint_url is not None:
            snapshot["endpoint_url"] = self.endpoint_url
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot


@dataclass(frozen=True, slots=True)
class OpenSearchConfigV1:
    url: str
    index: str
    timeout_s: float
    skip_tls_verify: bool = False
    username: str | None = None
    password: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> OpenSearchConfigV1 | None:
        url = get_opt_str(ns, "opensearch_url")
        if url is None:
            return None
        username = get_opt_str(ns, "opensearch_username")
        password = get_opt_str(ns, "opensearch_password")
        if (username is None) != (password is None):
            raise ConfigError(
                "Both --opensearch-username and --opensearch-password must be specified together"
            )
        return OpenSearchConfigV1(
            url=url,
            index=get_req_str(ns, "opensearch_index"),
            timeout_s=get_float(ns, "opensearch_timeout_s"),
            skip_tls_verify=get_bool(ns, "opensearch_skip_tls_verify"),
            username=username,
            password=password,
            max_inflight=get_opt_int(ns, "opensearch_max_inflight"),
            rate_limit=get_opt_float(ns, "opensearch_rate_limit"),
        )

    def to_snapshot_v1(self) -> OpenSearchDestinationSnapshotV1:
        snapshot: OpenSearchDestinationSnapshotV1 = {
            "url": self.url,
            "index": self.index,
            "timeout_s": self.timeout_s,
            "verify_tls": not self.skip_tls_verify,
            "username": None if self.username is None else "***",
            "password": None if self.password is None else "***",
        }
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot


@dataclass(frozen=True, slots=True)
class ClickHouseConfigV1:
    url: str
    table: str
    timeout_s: float
    database: str | None = None
    username: str | None = None
    password: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> ClickHouseConfigV1 | None:
        url = get_opt_str(ns, "clickhouse_url")
        if url is None:
            return None
        username = get_opt_str(ns, "clickhouse_username")
        password = get_opt_str(ns, "clickhouse_password")
        if (username is None) != (password is None):
            raise ConfigError(
                "Both --clickhouse-username and --clickhouse-password must be specified together"
            )
        return ClickHouseConfigV1(
            url=url,
            table=get_req_str(ns, "clickhouse_table"),
            timeout_s=get_float(ns, "clickhouse_timeout_s"),
            database=get_opt_str(ns, "clickhouse_database"),
            username=username,
            password=password,
            max_inflight=get_opt_int(ns, "clickhouse_max_inflight"),
            rate_limit=get_opt_float(ns, "clickhouse_rate_limit"),
        )

    def to_snapshot_v1(self) -> ClickHouseDestinationSnapshotV1:
        snapshot: ClickHouseDestinationSnapshotV1 = {
            "url": self.url,
            "table": self.table,
            "timeout_s": self.timeout_s,
            "username": None if self.username is None else "***",
            "password": None if self.password is None else "***",
        }
        if self.database is not None:
            snapshot["database"] = self.database
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot


@dataclass(frozen=True, slots=True)
class MongoDBConfigV1:
    uri: str
    database: str
    collection: str
    timeout_s: float
    write_mode: str = "replace_upsert"
    username: str | None = None
    password: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> MongoDBConfigV1 | None:
        uri = get_opt_str(ns, "mongodb_uri")
        if uri is None:
            return None
        username = get_opt_str(ns, "mongodb_username")
        password = get_opt_str(ns, "mongodb_password")
        if (username is None) != (password is None):
            raise ConfigError(
                "Both --mongodb-username and --mongodb-password must be specified together"
            )
        return MongoDBConfigV1(
            uri=uri,
            database=get_req_str(ns, "mongodb_database"),
            collection=get_req_str(ns, "mongodb_collection"),
            timeout_s=get_float(ns, "mongodb_timeout_s"),
            write_mode=get_req_str(ns, "mongodb_write_mode"),
            username=username,
            password=password,
            max_inflight=get_opt_int(ns, "mongodb_max_inflight"),
            rate_limit=get_opt_float(ns, "mongodb_rate_limit"),
        )

    def to_snapshot_v1(self) -> MongoDBDestinationSnapshotV1:
        snapshot: MongoDBDestinationSnapshotV1 = {
            "uri": self.uri,
            "database": self.database,
            "collection": self.collection,
            "timeout_s": self.timeout_s,
            "write_mode": self.write_mode,
            "username": None if self.username is None else "***",
            "password": None if self.password is None else "***",
        }
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot


@dataclass(frozen=True, slots=True)
class LokiConfigV1:
    url: str
    stream: str
    timeout_s: float
    tenant_id: str | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> LokiConfigV1 | None:
        url = get_opt_str(ns, "loki_url")
        if url is None:
            return None
        return LokiConfigV1(
            url=url,
            stream=get_req_str(ns, "loki_stream"),
            timeout_s=get_float(ns, "loki_timeout_s"),
            tenant_id=get_opt_str(ns, "loki_tenant_id"),
            max_inflight=get_opt_int(ns, "loki_max_inflight"),
            rate_limit=get_opt_float(ns, "loki_rate_limit"),
        )

    def to_snapshot_v1(self) -> LokiDestinationSnapshotV1:
        snapshot: LokiDestinationSnapshotV1 = {
            "url": self.url,
            "stream": self.stream,
            "timeout_s": self.timeout_s,
        }
        if self.tenant_id is not None:
            snapshot["tenant_id"] = self.tenant_id
        if self.max_inflight is not None:
            snapshot["max_inflight"] = self.max_inflight
        if self.rate_limit is not None:
            snapshot["rate_limit"] = self.rate_limit
        return snapshot


@dataclass(frozen=True, slots=True)
class SqliteConfigV1:
    file_path: str
    table_name: str = "zephyr_delivery_records"
    timeout_s: float = 5.0
    mode: str = "replace_upsert"

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> SqliteConfigV1 | None:
        file_path = get_opt_str(ns, "sqlite_file_path")
        if file_path is None:
            return None
        return SqliteConfigV1(
            file_path=file_path,
            table_name=get_req_str(ns, "sqlite_table_name"),
            timeout_s=get_float(ns, "sqlite_timeout_s"),
            mode=get_req_str(ns, "sqlite_mode"),
        )

    def to_snapshot_v1(self) -> SqliteDestinationSnapshotV1:
        return {
            "file_path": self.file_path,
            "table_name": self.table_name,
            "timeout_s": self.timeout_s,
            "mode": self.mode,
        }
