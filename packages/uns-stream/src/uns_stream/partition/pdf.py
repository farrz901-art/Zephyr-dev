from __future__ import annotations

from typing import Any

from uns_stream.backends.base import PartitionBackend
from uns_stream.service import partition_file
from zephyr_core import PartitionResult, PartitionStrategy


def partition_pdf(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    infer_table_structure: bool = False,
    languages: list[str] | None = None,
    backend: PartitionBackend | None = None,
    **kwargs: Any,
) -> PartitionResult:
    # Note: Unstructured docs recommend infer_table_structure + hi_res for tables. <!--citation:3-->
    return partition_file(
        filename=filename,
        kind="pdf",
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
        infer_table_structure=infer_table_structure,
        languages=languages,
        **kwargs,
    )
