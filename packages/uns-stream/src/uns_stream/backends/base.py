from __future__ import annotations

from typing import Any, Protocol

from zephyr_core import PartitionStrategy, ZephyrElement


class PartitionBackend(Protocol):
    """Backend interface.

    Phase1 default: local unstructured (in-process).
    Phase2/3: can add HTTP backend (unstructured-api) without changing callers.
    """

    name: str  # e.g. "unstructured"
    backend: str  # e.g. "local" / "http"
    version: str  # e.g. unstructured.__version__

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]: ...
