from __future__ import annotations

from uns_stream.backends.base import PartitionBackend
from uns_stream.service import partition_file
from zephyr_core import PartitionResult, PartitionStrategy


def partition_ndjson(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
) -> PartitionResult:
    return partition_file(
        filename=filename,
        kind="ndjson",
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
    )
