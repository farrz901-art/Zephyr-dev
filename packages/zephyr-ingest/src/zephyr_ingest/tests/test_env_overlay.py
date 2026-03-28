from __future__ import annotations

from dataclasses import replace

import pytest

from zephyr_ingest.config.env_overlay import (
    first_env,
    overlay_uns_api_key,
    overlay_weaviate_api_key,
)
from zephyr_ingest.config.models import WeaviateConfigV1


def test_first_env_returns_first_non_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("A", raising=False)
    monkeypatch.delenv("B", raising=False)
    assert first_env("A", "B") is None

    monkeypatch.setenv("A", "")
    monkeypatch.setenv("B", "  x  ")
    assert first_env("A", "B") == "x"


def test_overlay_uns_api_key_only_when_backend_uns_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZEPHYR_UNS_API_KEY", "k1")

    # local backend: do not overlay
    assert overlay_uns_api_key(backend="local", uns_api_key=None) is None

    # uns-api backend: overlay
    assert overlay_uns_api_key(backend="uns-api", uns_api_key=None) == "k1"

    # CLI provided value wins
    assert overlay_uns_api_key(backend="uns-api", uns_api_key="cli") == "cli"


def test_overlay_weaviate_api_key_only_when_weaviate_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEAVIATE_API_KEY", "w1")

    # Not enabled => do nothing
    assert overlay_weaviate_api_key(weaviate=None) is None

    base = WeaviateConfigV1(
        collection="C",
        max_batch_errors=0,
        http_host="localhost",
        http_port=8080,
        http_secure=False,
        grpc_host="localhost",
        grpc_port=50051,
        grpc_secure=False,
        api_key=None,
        skip_init_checks=False,
    )

    w = overlay_weaviate_api_key(weaviate=base)
    assert w is not None
    assert w.api_key == "w1"

    # CLI/config wins
    base2 = replace(base, api_key="cli")
    w2 = overlay_weaviate_api_key(weaviate=base2)
    assert w2 is not None
    assert w2.api_key == "cli"
