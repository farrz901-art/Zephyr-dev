from __future__ import annotations

from pathlib import Path
from typing import Callable

from uns_stream.partition.text import partition_text
from zephyr_core import ErrorCode, PartitionResult, ZephyrError

# 后续逐步补：pdf/docx/md/xlsx/...（先 stub 也行）


_ROUTER: dict[str, Callable[[str], PartitionResult]] = {
    ".txt": lambda fn: partition_text(filename=fn),
    ".text": lambda fn: partition_text(filename=fn),
    ".log": lambda fn: partition_text(filename=fn),
    # ".html": ...
    # ".xml": ...
    # ".eml": ...
    # ".msg": ...
}


def partition(filename: str) -> PartitionResult:
    ext = Path(filename).suffix.lower()
    fn = _ROUTER.get(ext)
    if not fn:
        raise ZephyrError(
            code=ErrorCode.UNS_UNSUPPORTED_TYPE,
            message=f"Unsupported file extension: {ext}",
            details={"filename": filename, "ext": ext},
        )
    return fn(filename)
