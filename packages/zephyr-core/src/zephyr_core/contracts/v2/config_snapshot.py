from __future__ import annotations

from typing import Literal, NotRequired, TypeAlias, TypedDict

CONFIG_SNAPSHOT_SCHEMA_VERSION: Literal[1] = 1


class InputSnapshotV1(TypedDict):
    paths_count: int
    glob: str
    source: Literal["local_file"]


class RunnerSnapshotV1(TypedDict):
    out: str
    strategy: str
    skip_existing: bool
    skip_unsupported: bool
    force: bool
    unique_element_ids: bool
    workers: int
    stale_lock_ttl_s: int | None


class RetrySnapshotV1(TypedDict):
    enabled: bool
    max_attempts: int
    base_backoff_ms: int
    max_backoff_ms: int


class LocalBackendSnapshotV1(TypedDict):
    kind: Literal["local"]


class UnsApiBackendSnapshotV1(TypedDict):
    kind: Literal["uns-api"]
    url: str
    timeout_s: float
    api_key: str | None  # redacted: "***" or None


BackendSnapshotV1: TypeAlias = LocalBackendSnapshotV1 | UnsApiBackendSnapshotV1


class FilesystemDestinationSnapshotV1(TypedDict):
    enabled: bool


class WebhookDestinationSnapshotV1(TypedDict):
    url: str
    timeout_s: float
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class KafkaDestinationSnapshotV1(TypedDict):
    topic: str
    brokers: str
    flush_timeout_s: float
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class WeaviateDestinationSnapshotV1(TypedDict):
    collection: str
    max_batch_errors: int
    http: str
    grpc: str
    api_key: str | None  # redacted: "***" or None
    skip_init_checks: bool
    timeout_s: NotRequired[float | None]
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class S3DestinationSnapshotV1(TypedDict):
    bucket: str
    region: str
    prefix: str
    write_mode: str
    endpoint_url: NotRequired[str | None]
    access_key: str | None
    secret_key: str | None
    session_token: str | None
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class OpenSearchDestinationSnapshotV1(TypedDict):
    url: str
    index: str
    timeout_s: float
    verify_tls: bool
    username: str | None
    password: str | None
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class ClickHouseDestinationSnapshotV1(TypedDict):
    url: str
    table: str
    timeout_s: float
    database: NotRequired[str | None]
    username: str | None
    password: str | None
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class MongoDBDestinationSnapshotV1(TypedDict):
    uri: str
    database: str
    collection: str
    timeout_s: float
    write_mode: str
    username: str | None
    password: str | None
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class LokiDestinationSnapshotV1(TypedDict):
    url: str
    stream: str
    timeout_s: float
    tenant_id: NotRequired[str | None]
    max_inflight: NotRequired[int | None]
    rate_limit: NotRequired[float | None]


class SqliteDestinationSnapshotV1(TypedDict):
    file_path: str
    table_name: str
    timeout_s: float
    mode: str


class DestinationsSnapshotV1(TypedDict):
    filesystem: FilesystemDestinationSnapshotV1
    webhook: NotRequired[WebhookDestinationSnapshotV1]
    kafka: NotRequired[KafkaDestinationSnapshotV1]
    weaviate: NotRequired[WeaviateDestinationSnapshotV1]
    s3: NotRequired[S3DestinationSnapshotV1]
    opensearch: NotRequired[OpenSearchDestinationSnapshotV1]
    clickhouse: NotRequired[ClickHouseDestinationSnapshotV1]
    mongodb: NotRequired[MongoDBDestinationSnapshotV1]
    loki: NotRequired[LokiDestinationSnapshotV1]
    sqlite: NotRequired[SqliteDestinationSnapshotV1]


ConfigValueSource: TypeAlias = Literal["cli", "env", "file", "default"]
ConfigSourcesV1: TypeAlias = dict[str, ConfigValueSource]


class ConfigSnapshotV1(TypedDict):
    schema_version: Literal[1]
    input: InputSnapshotV1
    runner: RunnerSnapshotV1
    retry: RetrySnapshotV1
    backend: BackendSnapshotV1
    destinations: DestinationsSnapshotV1
    sources: NotRequired[ConfigSourcesV1]
