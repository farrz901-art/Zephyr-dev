from __future__ import annotations

from typing import Literal, NotRequired, TypeAlias, TypedDict

from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1

BATCH_REPORT_SCHEMA_VERSION: Literal[1] = 1


class DurationStatsV1(TypedDict):
    min: int | None
    max: int | None
    avg: int | None
    p95: int | None


class CountsV1(TypedDict):
    total: int
    success: int
    failed: int
    skipped_unsupported: int
    skipped_existing: int


class DeliveryCountersV1(TypedDict):
    total: int
    ok: int
    failed: int


DeliveryByDestinationV1: TypeAlias = dict[str, DeliveryCountersV1]


class DeliveryV1(TypedDict):
    total: int
    ok: int
    failed: int
    failed_retryable: int
    failed_non_retryable: int
    failed_unknown: int
    dlq_written_total: int
    dlq_dir: str
    by_destination: DeliveryByDestinationV1
    fanout_children_by_destination: DeliveryByDestinationV1


class RetryV1(TypedDict):
    enabled: bool
    max_attempts: int
    base_backoff_ms: int
    max_backoff_ms: int
    retry_attempts_total: int
    retried_success: int
    retryable_failed: int


class MetricsV1(TypedDict):
    # Wall-clock runtime of run_documents (ms)
    run_wall_ms: int
    # Derived throughput (docs/min). None when wall time is 0.
    docs_per_min: float | None

    # Prometheus-like totals (redundant but stable for ops)
    docs_total: int
    docs_success_total: int
    docs_failed_total: int
    docs_skipped_total: int

    delivery_total: int
    delivery_ok_total: int
    delivery_failed_total: int
    delivery_failed_retryable_total: int
    delivery_failed_non_retryable_total: int
    delivery_failed_unknown_total: int
    dlq_written_total: int


class BatchReportV1(TypedDict):
    schema_version: Literal[1]
    run_id: str
    pipeline_version: str
    timestamp_utc: str
    strategy: str

    counts: CountsV1
    delivery: DeliveryV1
    counts_by_extension: dict[str, int]
    counts_by_error_code: dict[str, int]
    retry: RetryV1
    durations_ms: DurationStatsV1

    generated_at_utc: str
    workers: int
    executor: Literal["serial", "thread"]

    config_snapshot: NotRequired[ConfigSnapshotV1]
    metrics: NotRequired[MetricsV1]
