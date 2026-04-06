from __future__ import annotations

from typing import Literal, NotRequired, TypeAlias, TypedDict

from zephyr_core.contracts.v2.config_snapshot import ConfigSnapshotV1

BATCH_REPORT_SCHEMA_VERSION: Literal[1] = 1


class DurationStatsV1(TypedDict):
    min: int | None
    max: int | None
    avg: int | None
    p95: int | None


class StageDurationsV1(TypedDict):
    hash_ms: DurationStatsV1
    partition_ms: DurationStatsV1
    delivery_ms: DurationStatsV1


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
DeliveryFailureKindsV1: TypeAlias = dict[str, int]
DeliveryFailureKindsByDestinationV1: TypeAlias = dict[str, DeliveryFailureKindsV1]


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
    failure_kinds_by_destination: NotRequired[DeliveryFailureKindsByDestinationV1]


class RetryV1(TypedDict):
    enabled: bool
    max_attempts: int
    base_backoff_ms: int
    max_backoff_ms: int
    retry_attempts_total: int
    retried_success: int
    retryable_failed: int


class MetricsV1(TypedDict):
    run_wall_ms: int
    docs_per_min: float | None

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
    stage_durations_ms: NotRequired[StageDurationsV1]

    generated_at_utc: str
    workers: int
    executor: Literal["serial", "thread"]

    config_snapshot: NotRequired[ConfigSnapshotV1]
    metrics: NotRequired[MetricsV1]
