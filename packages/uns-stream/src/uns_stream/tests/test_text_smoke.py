# 用临时 txt 文件跑 partition_text(filename=...)
from __future__ import annotations

from pathlib import Path

from uns_stream.partition.text import partition_text


def test_partition_text_real_unstructured(tmp_path: Path) -> None:
    """
    Real smoke test using LocalUnstructuredBackend (in-process).
    This should work with base `unstructured` install (no extras).
    """
    f = tmp_path / "sample.txt"
    f.write_text("Zephyr smoke test\nSecond line", encoding="utf-8")

    res = partition_text(filename=str(f))  # no DummyBackend -> real unstructured

    # Contract sanity
    assert res.document.filename == "sample.txt"
    assert res.engine.name == "unstructured"
    assert res.engine.backend == "local"
    assert len(res.elements) > 0
    assert "Zephyr" in res.normalized_text

    # Element sanity
    assert all(e.element_id for e in res.elements)
    assert all(e.type for e in res.elements)
