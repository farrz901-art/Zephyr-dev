from __future__ import annotations

from pathlib import Path
from typing import Any

from uns_stream.partition.auto import partition as auto_partition
from uns_stream.partition.image import partition_image
from uns_stream.partition.pdf import partition_pdf
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
            ZephyrElement(
                element_id="1", type="Title", text=f"kind={kind}|strategy={strategy}", metadata={}
            ),
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


def test_partition_pdf_passes_kind_and_strategy(tmp_path: Path) -> None:
    f = tmp_path / "a.pdf"
    f.write_bytes(b"%PDF-1.4 fake")  # 不会被真正解析，因为用 DummyBackend
    res = partition_pdf(filename=str(f), strategy=PartitionStrategy.HI_RES, backend=DummyBackend())
    assert res.elements[0].text == "kind=pdf|strategy=hi_res"


def test_partition_image_passes_kind_and_strategy(tmp_path: Path) -> None:
    f = tmp_path / "a.png"
    f.write_bytes(b"\x89PNG\r\n\x1a\n")  # fake header
    res = partition_image(
        filename=str(f), strategy=PartitionStrategy.OCR_ONLY, backend=DummyBackend()
    )
    assert res.elements[0].text == "kind=image|strategy=ocr_only"
