from __future__ import annotations

from typing import Final

# SSOT: env var names for secrets (used by env overlay + spec registry)
UNS_API_KEY_ENV_NAMES: Final[tuple[str, ...]] = (
    "ZEPHYR_UNS_API_KEY",
    "UNS_API_KEY",
    "UNSTRUCTURED_API_KEY",
)

WEAVIATE_API_KEY_ENV_NAMES: Final[tuple[str, ...]] = ("ZEPHYR_WEAVIATE_API_KEY", "WEAVIATE_API_KEY")
