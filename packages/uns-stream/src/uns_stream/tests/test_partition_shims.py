from __future__ import annotations

from pathlib import Path
from typing import Any

from uns_stream.partition.auto import partition as auto_partition
from uns_stream.partition.text import partition_text
from zephyr_core import PartitionStrategy, ZephyrElement


class DummyBackend:
    name = "dummy"
    backend = "test"
    version = "0.0.0"

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]:
        return [
            ZephyrElement(element_id="1", type="Title", text=f"kind={kind}", metadata={}),
        ]


def test_partition_text_uses_backend(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    res = partition_text(filename=str(f), backend=DummyBackend())
    assert res.engine.name == "dummy"
    assert res.elements[0].text == "kind=text"


def test_auto_routes_txt(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    res = auto_partition(filename=str(f), backend=DummyBackend())
    assert res.elements[0].text == "kind=text"


def test_auto_routes_md(tmp_path: Path) -> None:
    f = tmp_path / "a.md"
    f.write_text("# hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    assert res.elements[0].text == "kind=md"


def test_auto_routes_json(tmp_path: Path) -> None:
    f = tmp_path / "a.json"
    f.write_text('{"a": 1}', encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    assert res.elements[0].text == "kind=json"
