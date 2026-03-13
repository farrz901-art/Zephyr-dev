from __future__ import annotations

from pathlib import Path
from typing import Any

from uns_stream.service import partition_file
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
            ZephyrElement(element_id="1", type="Title", text="Hello", metadata={"kind": kind}),
            ZephyrElement(element_id="2", type="NarrativeText", text="World", metadata={}),
        ]


def test_partition_file_builds_contract(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    res = partition_file(filename=str(f), kind="text", backend=DummyBackend())

    assert res.document.filename == "a.txt"
    assert res.engine.name == "dummy"
    assert len(res.elements) == 2
    assert "Hello" in res.normalized_text
