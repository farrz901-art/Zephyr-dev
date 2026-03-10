"""Smoke tests for zephyr-core package."""

import zephyr_core


def test_package_exists():
    """Test that the core package can be imported."""
    assert zephyr_core is not None


def test_version_exists():
    """Test that the package has a version attribute."""
    assert hasattr(zephyr_core, "__version__")
    assert isinstance(zephyr_core.__version__, str)
