from __future__ import annotations

# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownParameterType=false
import importlib
from pathlib import Path
from typing import Any

import pytest

from uns_stream.partition.docx import partition_docx
from uns_stream.partition.pdf import partition_pdf
from uns_stream.partition.pptx import partition_pptx
from uns_stream.partition.xlsx import partition_xlsx
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


def _module_importable(mod: str) -> bool:
    try:
        importlib.import_module(mod)
        return True
    except ModuleNotFoundError:
        return False


@pytest.mark.parametrize(
    ("kind", "mod", "call", "expected_extra"),
    [
        (
            "pdf",
            "unstructured.partition.pdf",
            lambda f: partition_pdf(filename=str(f), strategy=PartitionStrategy.FAST),
            "pdf",
        ),
        (
            "docx",
            "unstructured.partition.docx",
            lambda f: partition_docx(filename=str(f)),
            "docx",
        ),
        (
            "pptx",
            "unstructured.partition.pptx",
            lambda f: partition_pptx(filename=str(f)),
            "pptx",
        ),
        (
            "xlsx",
            "unstructured.partition.xlsx",
            lambda f: partition_xlsx(filename=str(f)),
            "xlsx",
        ),
    ],
)
def test_missing_extra_raises_clear_error(
    tmp_path: Path,
    kind: str,
    mod: str,
    call: Any,
    expected_extra: str,
) -> None:
    # 如果模块已经能 import，说明对应 extra（或 all-docs）已装，
    # 这个测试只验证“缺 extra 的行为”，所以直接跳过。
    if _module_importable(mod):
        pytest.skip(f"{kind} appears installed; skipping missing-extra contract test.")

    f = tmp_path / f"a.{kind}"
    f.write_bytes(b"fake")

    with pytest.raises(ZephyrError) as ei:
        call(f)

    # 使用 getattr 绕过 pyright 对异常属性的严格检查
    err = ei.value
    assert getattr(err, "code") == ErrorCode.UNS_EXTRA_MISSING
    details = getattr(err, "details")
    assert details is not None
    assert details.get("extra") == expected_extra

    # err = ei.value
    # assert err.code == ErrorCode.UNS_EXTRA_MISSING
    # assert err.details is not None
    # assert err.details.get("extra") == expected_extra
