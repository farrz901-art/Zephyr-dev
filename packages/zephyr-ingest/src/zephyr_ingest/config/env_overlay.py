from __future__ import annotations

import os
from dataclasses import replace

from zephyr_ingest.config.constants import UNS_API_KEY_ENV_NAMES, WEAVIATE_API_KEY_ENV_NAMES
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


def overlay_uns_api_key(
    *, backend: str, uns_api_key: str | None, allow_override: bool = False
) -> str | None:
    if backend != "uns-api":
        return uns_api_key

    env_key = first_env(*UNS_API_KEY_ENV_NAMES)
    if env_key is None:
        return uns_api_key

    if allow_override:
        return env_key

    if _normalize_secret(uns_api_key) is None:
        return env_key

    return uns_api_key


def overlay_weaviate_api_key(
    *, weaviate: WeaviateConfigV1 | None, allow_override: bool = False
) -> WeaviateConfigV1 | None:
    if weaviate is None:
        return None

    env_key = first_env(*WEAVIATE_API_KEY_ENV_NAMES)
    if env_key is None:
        return weaviate

    if allow_override:
        return replace(weaviate, api_key=env_key)

    if _normalize_secret(weaviate.api_key) is None:
        return replace(weaviate, api_key=env_key)

    return weaviate
