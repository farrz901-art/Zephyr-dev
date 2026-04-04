from __future__ import annotations

from pathlib import Path

from zephyr_core import PartitionResult, RunMetaV1
from zephyr_ingest._internal.artifacts import dump_partition_artifacts
from zephyr_ingest.destinations.base import DeliveryReceipt


class FilesystemDestination:
    name = "filesystem"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        out_dir = dump_partition_artifacts(
            out_root=out_root, sha256=sha256, meta=meta, result=result
        )
        return DeliveryReceipt(destination=self.name, ok=True, details={"out_dir": str(out_dir)})
