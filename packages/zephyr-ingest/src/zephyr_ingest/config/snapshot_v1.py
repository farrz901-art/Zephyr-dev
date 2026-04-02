"""
Compat shim: keep old import paths stable, but make zephyr-core/contracts/v2 the SSOT.
"""

from __future__ import annotations

from zephyr_core.contracts.v2.config_snapshot import (  # noqa: F401
    CONFIG_SNAPSHOT_SCHEMA_VERSION,
    BackendSnapshotV1,
    ConfigSnapshotV1,
    ConfigSourcesV1,
    ConfigValueSource,
    DestinationsSnapshotV1,
    InputSnapshotV1,
    KafkaDestinationSnapshotV1,
    LocalBackendSnapshotV1,
    RetrySnapshotV1,
    RunnerSnapshotV1,
    UnsApiBackendSnapshotV1,
    WeaviateDestinationSnapshotV1,
    WebhookDestinationSnapshotV1,
)

__all__ = [
    "CONFIG_SNAPSHOT_SCHEMA_VERSION",
    "InputSnapshotV1",
    "RunnerSnapshotV1",
    "RetrySnapshotV1",
    "LocalBackendSnapshotV1",
    "UnsApiBackendSnapshotV1",
    "BackendSnapshotV1",
    "WebhookDestinationSnapshotV1",
    "KafkaDestinationSnapshotV1",
    "WeaviateDestinationSnapshotV1",
    "DestinationsSnapshotV1",
    "ConfigValueSource",
    "ConfigSourcesV1",
    "ConfigSnapshotV1",
]
