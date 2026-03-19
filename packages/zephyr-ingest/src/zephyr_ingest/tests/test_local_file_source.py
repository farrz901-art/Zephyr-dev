from __future__ import annotations

from pathlib import Path

from zephyr_ingest.sources.local_file import LocalFileSource


def test_local_file_source_enumerates_sorted(tmp_path: Path) -> None:
    (tmp_path / "b.txt").write_text("b", encoding="utf-8")
    (tmp_path / "a.txt").write_text("a", encoding="utf-8")

    src = LocalFileSource(path=tmp_path, glob="*.txt")
    docs = list(src.iter_documents())

    assert [d.filename for d in docs] == ["a.txt", "b.txt"]
    assert all(d.source == "local_file" for d in docs)
    assert all(d.extension == ".txt" for d in docs)
