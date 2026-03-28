from __future__ import annotations

import os
from dataclasses import replace

from zephyr_ingest.config.models import WeaviateConfigV1


def _normalize_secret(s: str | None) -> str | None:
    if s is None:
        return None
    s2 = s.strip()
    return None if s2 == "" else s2


def first_env(*names: str) -> str | None:
    """
    Return the first non-empty env value among given names.
    """
    for name in names:
        v = _normalize_secret(os.environ.get(name))
        if v is not None:
            return v
    return None


def overlay_uns_api_key(*, backend: str, uns_api_key: str | None) -> str | None:
    """
    Overlay Unstructured API key from env only when:
    - backend == "uns-api"
    - and uns_api_key not provided via CLI/config.

    ENV precedence:
      ZEPHYR_UNS_API_KEY -> UNS_API_KEY -> UNSTRUCTURED_API_KEY
    """
    if backend != "uns-api":
        return uns_api_key
    if _normalize_secret(uns_api_key) is not None:
        return uns_api_key

    return first_env("ZEPHYR_UNS_API_KEY", "UNS_API_KEY", "UNSTRUCTURED_API_KEY")


def overlay_weaviate_api_key(*, weaviate: WeaviateConfigV1 | None) -> WeaviateConfigV1 | None:
    """
    Overlay Weaviate API key from env only when:
    - Weaviate is enabled (weaviate config exists)
    - and api_key not provided via CLI/config.

    ENV precedence:
      ZEPHYR_WEAVIATE_API_KEY -> WEAVIATE_API_KEY
    """
    if weaviate is None:
        return None

    if _normalize_secret(weaviate.api_key) is not None:
        return weaviate

    env_key = first_env("ZEPHYR_WEAVIATE_API_KEY", "WEAVIATE_API_KEY")
    if env_key is None:
        return weaviate

    return replace(weaviate, api_key=env_key)
