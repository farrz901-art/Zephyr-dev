from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from uns_stream.backends.base import PartitionBackend
from uns_stream.partition.csv import partition_csv
from uns_stream.partition.email import partition_email
from uns_stream.partition.html import partition_html
from uns_stream.partition.image import partition_image
from uns_stream.partition.json import partition_json
from uns_stream.partition.md import partition_md
from uns_stream.partition.msg import partition_msg
from uns_stream.partition.ndjson import partition_ndjson
from uns_stream.partition.pdf import partition_pdf
from uns_stream.partition.text import partition_text
from uns_stream.partition.tsv import partition_tsv
from uns_stream.partition.xlsx import partition_xlsx
from uns_stream.partition.xml import partition_xml
from zephyr_core import ErrorCode, PartitionResult, PartitionStrategy, ZephyrError

_ROUTER: dict[str, Callable[..., PartitionResult]] = {
    ".txt": partition_text,
    ".text": partition_text,
    ".log": partition_text,
    ".html": partition_html,
    ".htm": partition_html,
    ".xml": partition_xml,
    ".eml": partition_email,
    ".msg": partition_msg,
    ".json": partition_json,
    ".md": partition_md,
    ".markdown": partition_md,
    ".pdf": partition_pdf,
    ".png": partition_image,
    ".jpg": partition_image,
    ".jpeg": partition_image,
    ".tiff": partition_image,
    ".tif": partition_image,
    ".bmp": partition_image,
    ".heic": partition_image,
    ".csv": partition_csv,
    ".tsv": partition_tsv,
    ".xlsx": partition_xlsx,
    ".xls": partition_xlsx,
    ".ndjson": partition_ndjson,
    ".jsonl": partition_ndjson,
    # 后续继续添加其他格式
}


def partition(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
) -> PartitionResult:
    ext = Path(filename).suffix.lower()
    fn = _ROUTER.get(ext)
    if fn is None:
        raise ZephyrError(
            code=ErrorCode.UNS_UNSUPPORTED_TYPE,
            message=f"Unsupported file extension: {ext}",
            details={"filename": filename, "ext": ext},
        )

    # mypy/pyright: _ROUTER values are callables (kept as object to avoid Protocol variance noise)
    return fn(
        filename=filename,
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
    )
