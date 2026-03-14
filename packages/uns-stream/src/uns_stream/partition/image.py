from __future__ import annotations

from typing import Any

from uns_stream.backends.base import PartitionBackend
from uns_stream.service import partition_file
from zephyr_core import PartitionResult, PartitionStrategy


def partition_image(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    languages: list[str] | None = None,
    backend: PartitionBackend | None = None,
    **kwargs: Any,
) -> PartitionResult:
    # Unstructured docs: images support auto/hi_res/ocr_only. <!--citation:2-->
    return partition_file(
        filename=filename,
        kind="image",
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
        languages=languages,
        **kwargs,
    )
