# 确保 .txt/.log 路由能走通
from __future__ import annotations

# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownParameterType=false
from pathlib import Path
from typing import Any

import pytest

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
        # PartitionStrategy is a StrEnum, so f"{strategy}" == "auto"/"hi_res"/...
        return [
            ZephyrElement(
                element_id="1",
                type="Title",
                text=f"kind={kind}|strategy={strategy}",
                metadata={"filename": filename},
            ),
        ]


@pytest.mark.parametrize(
    ("name", "writer", "expected_kind"),
    [
        ("a.txt", lambda p: p.write_text("x", encoding="utf-8"), "text"),
        ("a.md", lambda p: p.write_text("# x", encoding="utf-8"), "md"),
        ("a.json", lambda p: p.write_text('{"a": 1}', encoding="utf-8"), "json"),
        ("a.csv", lambda p: p.write_text("a,b\n1,2", encoding="utf-8"), "csv"),
        ("a.tsv", lambda p: p.write_text("a\tb\n1\t2", encoding="utf-8"), "tsv"),
        ("a.jsonl", lambda p: p.write_text('{"a":1}\n', encoding="utf-8"), "ndjson"),
        ("a.docx", lambda p: p.write_bytes(b"fake"), "docx"),
        ("a.pptx", lambda p: p.write_bytes(b"fake"), "pptx"),
        ("a.rtf", lambda p: p.write_text(r"{\rtf1 hello}", encoding="utf-8"), "rtf"),
        ("a.epub", lambda p: p.write_bytes(b"fake"), "epub"),
        ("a.odt", lambda p: p.write_bytes(b"fake"), "odt"),
        ("a.org", lambda p: p.write_text("* title", encoding="utf-8"), "org"),
        ("a.pdf", lambda p: p.write_bytes(b"%PDF-1.4 fake"), "pdf"),
        ("a.png", lambda p: p.write_bytes(b"\x89PNG\r\n\x1a\n"), "image"),
    ],
)
def test_auto_routes_extensions(tmp_path: Path, name: str, writer: Any, expected_kind: str) -> None:
    f = tmp_path / name
    writer(f)

    res = auto_partition(filename=str(f), backend=DummyBackend())
    kv = _kv(res.elements[0].text)

    assert kv["kind"] == expected_kind
    assert kv["strategy"] == "auto"
