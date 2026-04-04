from __future__ import annotations

from pathlib import Path
from typing import Any

from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionStrategy, ZephyrElement


def _kv(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in text.split("|"):
        k, v = part.split("=", 1)
        out[k] = v
    return out


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
                element_id="1",
                type="Title",
                text=f"kind={kind}|strategy={strategy.value}",
                metadata={},
            ),
        ]


def test_auto_routes_doc(tmp_path: Path) -> None:
    f = tmp_path / "a.doc"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "doc"
    assert kv["strategy"] == "auto"


def test_auto_routes_docx(tmp_path: Path) -> None:
    f = tmp_path / "a.docx"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "docx"
    assert kv["strategy"] == "auto"


def test_auto_routes_ppt(tmp_path: Path) -> None:
    f = tmp_path / "a.ppt"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "ppt"
    assert kv["strategy"] == "auto"


def test_auto_routes_pptx(tmp_path: Path) -> None:
    f = tmp_path / "a.pptx"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "pptx"
    assert kv["strategy"] == "auto"


def test_auto_routes_rtf(tmp_path: Path) -> None:
    f = tmp_path / "a.rtf"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "rtf"
    assert kv["strategy"] == "auto"


def test_auto_routes_rst(tmp_path: Path) -> None:
    f = tmp_path / "a.rst"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "rst"
    assert kv["strategy"] == "auto"


def test_auto_routes_epub(tmp_path: Path) -> None:
    f = tmp_path / "a.epub"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "epub"
    assert kv["strategy"] == "auto"


def test_auto_routes_odt(tmp_path: Path) -> None:
    f = tmp_path / "a.odt"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "odt"
    assert kv["strategy"] == "auto"


def test_auto_routes_org(tmp_path: Path) -> None:
    f = tmp_path / "a.org"
    f.write_text("hello", encoding="utf-8")
    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)
    assert kv["kind"] == "org"
    assert kv["strategy"] == "auto"
