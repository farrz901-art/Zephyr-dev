from __future__ import annotations

from pathlib import Path

from uns_stream.backends.base import PartitionBackend
from uns_stream.service import partition_file
from zephyr_core import ErrorCode, PartitionResult, PartitionStrategy, ZephyrError

# _ROUTER: dict[str, Callable[..., PartitionResult]] = {
#     ".txt": partition_text,
#     ".text": partition_text,
#     ".log": partition_text,
#     ".html": partition_html,
#     ".htm": partition_html,
#     ".xml": partition_xml,
#     ".eml": partition_email,
#     ".msg": partition_msg,
#     ".json": partition_json,
#     ".md": partition_md,
#     ".markdown": partition_md,
#     ".pdf": partition_pdf,
#     ".png": partition_image,
#     ".jpg": partition_image,
#     ".jpeg": partition_image,
#     ".tiff": partition_image,
#     ".tif": partition_image,
#     ".bmp": partition_image,
#     ".heic": partition_image,
#     ".csv": partition_csv,
#     ".tsv": partition_tsv,
#     ".xlsx": partition_xlsx,
#     ".xls": partition_xlsx,
#     ".ndjson": partition_ndjson,
#     ".jsonl": partition_ndjson,
#     ".doc": partition_doc,
#     ".docx": partition_docx,
#     ".ppt": partition_ppt,
#     ".pptx": partition_pptx,
#     ".rtf": partition_rtf,
#     ".rst": partition_rst,
#     ".epub": partition_epub,
#     ".odt": partition_odt,
#     ".org": partition_org,
#     # 后续继续添加其他格式
# }

_KIND_BY_EXT: dict[str, str] = {
    ".txt": "text",
    ".text": "text",
    ".log": "text",
    ".html": "html",
    ".htm": "html",
    ".xml": "xml",
    ".eml": "email",
    ".msg": "msg",
    ".json": "json",
    ".md": "md",
    ".markdown": "md",
    ".pdf": "pdf",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".tiff": "image",
    ".tif": "image",
    ".bmp": "image",
    ".heic": "image",
    ".csv": "csv",
    ".tsv": "tsv",
    ".xlsx": "xlsx",
    ".xls": "xlsx",
    ".ndjson": "ndjson",
    ".jsonl": "ndjson",
    ".doc": "doc",
    ".docx": "docx",
    ".ppt": "ppt",
    ".pptx": "pptx",
    ".rtf": "rtf",
    ".rst": "rst",
    ".epub": "epub",
    ".odt": "odt",
    ".org": "org",
}


def partition(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> PartitionResult:
    ext = Path(filename).suffix.lower()
    kind = _KIND_BY_EXT.get(ext)
    if kind is None:
        raise ZephyrError(
            code=ErrorCode.UNS_UNSUPPORTED_TYPE,
            message=f"Unsupported file extension: {ext}",
            details={"filename": filename, "ext": ext},
        )

    # mypy/pyright: _ROUTER values are callables (kept as object to avoid Protocol variance noise)
    return partition_file(
        filename=filename,
        kind=kind,
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
        run_id=run_id,
        pipeline_version=pipeline_version,
        sha256=sha256,
        size_bytes=size_bytes,
    )
