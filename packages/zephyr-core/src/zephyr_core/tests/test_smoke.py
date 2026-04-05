"""Smoke tests for zephyr-core package."""

import zephyr_core


def test_package_exists() -> None:
    assert zephyr_core is not None


def test_version_exists() -> None:
    assert hasattr(zephyr_core, "__version__")
    assert isinstance(zephyr_core.__version__, str)


def test_contract_exports_exist() -> None:
    assert hasattr(zephyr_core, "PartitionResult")
    assert hasattr(zephyr_core, "ZephyrElement")
    assert hasattr(zephyr_core, "DocumentMetadata")
    assert hasattr(zephyr_core, "EngineInfo")
    assert hasattr(zephyr_core, "PartitionStrategy")
    assert hasattr(zephyr_core, "HealthCheckKind")
    assert hasattr(zephyr_core, "HealthCheckResult")
    assert hasattr(zephyr_core, "HealthCheckProvider")
    assert hasattr(zephyr_core, "WorkerPhase")
    assert hasattr(zephyr_core, "Lifecycle")


def test_error_exports_exist() -> None:
    assert hasattr(zephyr_core, "ErrorCode")
    assert hasattr(zephyr_core, "ZephyrError")
