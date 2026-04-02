"""
Compat shim: keep old import paths stable, but make zephyr-core/contracts/v2 the SSOT.
"""

from __future__ import annotations

from zephyr_core.contracts.v2.spec import (  # noqa: F401
    ConnectorSpecV1,
    SpecFieldTypeV1,
    SpecFieldV1,
)

__all__ = [
    "SpecFieldTypeV1",
    "SpecFieldV1",
    "ConnectorSpecV1",
]
