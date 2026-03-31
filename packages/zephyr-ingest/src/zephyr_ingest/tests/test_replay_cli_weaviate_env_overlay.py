from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, cast

import pytest

from zephyr_ingest import cli


@dataclass
class DummyClient:
    closed: bool = False

    def close(self) -> None:
        self.closed = True


@dataclass
class DummyBatch:
    number_errors: int = 0

    def add_object(self, properties: dict[str, Any], uuid: str | None = None) -> str:
        return uuid or "x"


@dataclass
class DummyBatchManager:
    failed_objects: list[Any]

    @contextmanager
    def dynamic(self) -> Iterator[DummyBatch]:
        yield DummyBatch()


@dataclass
class DummyCollection:
    batch: DummyBatchManager


def test_replay_weaviate_api_key_env_overlay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    out_root = tmp_path / "out"
    (out_root / "_dlq" / "delivery").mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("ZEPHYR_WEAVIATE_API_KEY", "env-key")

    captured: dict[str, Any] = {}

    def fake_connect(*, params: Any, collection_name: str) -> tuple[Any, Any]:
        captured["api_key"] = params.api_key
        client = DummyClient()
        captured["client"] = client
        return client, DummyCollection(batch=DummyBatchManager(failed_objects=[]))

    monkeypatch.setattr(cli, "connect_weaviate_and_get_collection", cast(Any, fake_connect))

    rc = cli.main(
        [
            "replay-delivery",
            "--out",
            str(out_root),
            "--dest",
            "weaviate",
            "--weaviate-collection",
            "ZephyrDoc",
            "--dry-run",
        ]
    )
    assert rc == 0
    assert captured["api_key"] == "env-key"
    assert captured["client"].closed is True
