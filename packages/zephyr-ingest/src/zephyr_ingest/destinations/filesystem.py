from __future__ import annotations

from pathlib import Path

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_ingest._internal.artifacts import dump_partition_artifacts
from zephyr_ingest.destinations.base import DeliveryReceipt


def _filesystem_failure_kind(*, exc: OSError) -> str:
    lowered = str(exc).lower()
    if any(marker in lowered for marker in ("used by another process", "resource busy", "busy")):
        return "locked"
    return "operational"


def _filesystem_retryable(*, exc: OSError) -> bool:
    return _filesystem_failure_kind(exc=exc) == "locked"


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
        try:
            out_dir = dump_partition_artifacts(
                out_root=out_root,
                sha256=sha256,
                meta=meta,
                result=result,
            )
        except OSError as exc:
            return DeliveryReceipt(
                destination=self.name,
                ok=False,
                details={
                    "out_root": str(out_root.expanduser().resolve()),
                    "sha256": sha256,
                    "exc_type": type(exc).__name__,
                    "exc": str(exc),
                    "retryable": _filesystem_retryable(exc=exc),
                    "failure_kind": _filesystem_failure_kind(exc=exc),
                    "error_code": str(ErrorCode.DELIVERY_FAILED),
                },
            )
        return DeliveryReceipt(destination=self.name, ok=True, details={"out_dir": str(out_dir)})
