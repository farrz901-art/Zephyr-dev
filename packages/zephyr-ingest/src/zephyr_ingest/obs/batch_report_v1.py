"""
Compat shim: keep old import paths stable, but make zephyr-core/contracts/v2 the SSOT.
"""

from __future__ import annotations

from zephyr_core.contracts.v2.batch_report import (  # noqa: F401
    BATCH_REPORT_SCHEMA_VERSION,
    BatchReportV1,
    CountsV1,
    DeliveryByDestinationV1,
    DeliveryCountersV1,
    DeliveryFailureKindsByDestinationV1,
    DeliveryFailureKindsV1,
    DeliveryV1,
    DurationStatsV1,
    MetricsV1,
    RetryV1,
    StageDurationsV1,
)

__all__ = [
    "BATCH_REPORT_SCHEMA_VERSION",
    "DurationStatsV1",
    "StageDurationsV1",
    "CountsV1",
    "DeliveryCountersV1",
    "DeliveryByDestinationV1",
    "DeliveryFailureKindsV1",
    "DeliveryFailureKindsByDestinationV1",
    "DeliveryV1",
    "RetryV1",
    "MetricsV1",
    "BatchReportV1",
]
