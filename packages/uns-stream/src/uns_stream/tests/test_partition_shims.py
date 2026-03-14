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
    # assert res.elements[0].text == "kind=text"
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "text"
    assert kv["strategy"] == "auto"


def test_auto_routes_txt(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    res = auto_partition(filename=str(f), backend=DummyBackend())
    # assert res.elements[0].text == "kind=text"
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "text"
    assert kv["strategy"] == "auto"


def test_auto_routes_md(tmp_path: Path) -> None:
    f = tmp_path / "a.md"
    f.write_text("# hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    # assert res.elements[0].text == "kind=md"
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "md"
    assert kv["strategy"] == "auto"


def test_auto_routes_json(tmp_path: Path) -> None:
    f = tmp_path / "a.json"
    f.write_text('{"a": 1}', encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    # assert res.elements[0].text == "kind=json"
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "json"
    assert kv["strategy"] == "auto"


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


def test_auto_routes_csv(tmp_path: Path) -> None:
    """测试 .csv 文件路由到 kind=csv"""
    f = tmp_path / "data.csv"
    f.write_text("name,age\nAlice,30", encoding="utf-8")

    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "csv"
    assert kv["strategy"] == "auto"


def test_auto_routes_xlsx(tmp_path: Path) -> None:
    """测试 .xlsx 文件路由到 kind=xlsx"""
    f = tmp_path / "data.xlsx"
    f.write_bytes(b"fake xlsx content")  # DummyBackend 不需要真实内容

    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "xlsx"
    assert kv["strategy"] == "auto"


def test_auto_routes_jsonl(tmp_path: Path) -> None:
    """测试 .jsonl 文件路由到 kind=ndjson"""
    f = tmp_path / "data.jsonl"
    f.write_text('{"id":1,"text":"hello"}\n{"id":2,"text":"world"}', encoding="utf-8")

    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "ndjson"  # 注意：Unstructured 中 .jsonl 通常走 ndjson
    assert kv["strategy"] == "auto"


def _kv(text: str) -> dict[str, str]:
    # "kind=text|strategy=auto" -> {"kind": "text", "strategy": "auto"}
    out: dict[str, str] = {}
    for part in text.split("|"):
        k, v = part.split("=", 1)
        out[k] = v
    return out
