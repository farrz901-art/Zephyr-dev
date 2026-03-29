from __future__ import annotations

from typing import Literal, NotRequired, Required, TypeAlias, TypedDict

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
    # redacted: "***" or None
    api_key: str | None


BackendSnapshotV1: TypeAlias = LocalBackendSnapshotV1 | UnsApiBackendSnapshotV1


ConfigValueSource: TypeAlias = Literal["cli", "env", "file", "default"]
ConfigSourcesV1: TypeAlias = dict[str, ConfigValueSource]


class FilesystemDestinationSnapshotV1(TypedDict):
    enabled: bool


class WebhookDestinationSnapshotV1(TypedDict):
    url: str
    timeout_s: float


class KafkaDestinationSnapshotV1(TypedDict):
    topic: str
    brokers: str
    flush_timeout_s: float


class WeaviateDestinationSnapshotV1(TypedDict):
    collection: str
    max_batch_errors: int
    http: str
    grpc: str
    # redacted: "***" or None
    api_key: str | None
    skip_init_checks: bool


class DestinationsSnapshotV1(TypedDict):
    filesystem: Required[FilesystemDestinationSnapshotV1]
    webhook: NotRequired[WebhookDestinationSnapshotV1]
    kafka: NotRequired[KafkaDestinationSnapshotV1]
    weaviate: NotRequired[WeaviateDestinationSnapshotV1]


class ConfigSnapshotV1(TypedDict):
    schema_version: Literal[1]
    input: InputSnapshotV1
    runner: RunnerSnapshotV1
    retry: RetrySnapshotV1
    backend: BackendSnapshotV1
    destinations: DestinationsSnapshotV1
    sources: NotRequired[ConfigSourcesV1]
