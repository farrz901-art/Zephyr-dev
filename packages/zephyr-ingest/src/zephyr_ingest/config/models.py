from __future__ import annotations

import argparse
from dataclasses import dataclass

from zephyr_ingest.config.argparse_extract import (
    get_bool,
    get_float,
    get_int,
    get_opt_str,
    get_req_str,
)
from zephyr_ingest.config.errors import ConfigError
from zephyr_ingest.config.snapshot_v1 import (
    KafkaDestinationSnapshotV1,
    WeaviateDestinationSnapshotV1,
    WebhookDestinationSnapshotV1,
)


@dataclass(frozen=True, slots=True)
class WebhookConfigV1:
    url: str
    timeout_s: float

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

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> WebhookConfigV1 | None:
        url = get_opt_str(ns, "webhook_url")
        if url is None:
            return None
        timeout_s = get_float(ns, "webhook_timeout_s")
        return WebhookConfigV1(url=url, timeout_s=timeout_s)

    def to_snapshot_v1(self) -> WebhookDestinationSnapshotV1:
        return {"url": self.url, "timeout_s": self.timeout_s}

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

    @staticmethod
    def from_namespace(ns: argparse.Namespace) -> KafkaConfigV1 | None:
        topic = get_opt_str(ns, "kafka_topic")
        brokers = get_opt_str(ns, "kafka_brokers")

        if topic is None and brokers is None:
            return None

        if topic is None or brokers is None:
            raise ConfigError("Both --kafka-topic and --kafka-brokers must be specified together")

        flush_timeout_s = get_float(ns, "kafka_flush_timeout_s")
        return KafkaConfigV1(topic=topic, brokers=brokers, flush_timeout_s=flush_timeout_s)

    def to_snapshot_v1(self) -> KafkaDestinationSnapshotV1:
        return {
            "topic": self.topic,
            "brokers": self.brokers,
            "flush_timeout_s": self.flush_timeout_s,
        }

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
        return {
            "collection": self.collection,
            "max_batch_errors": self.max_batch_errors,
            "http": f"{self.http_host}:{self.http_port} (secure={self.http_secure})",
            "grpc": f"{self.grpc_host}:{self.grpc_port} (secure={self.grpc_secure})",
            "api_key": "***" if self.api_key else None,
            "skip_init_checks": self.skip_init_checks,
        }

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
