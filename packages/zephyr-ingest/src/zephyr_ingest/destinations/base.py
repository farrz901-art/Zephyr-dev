from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from zephyr_core import PartitionResult, RunMetaV1


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    destination: str
    ok: bool
    details: dict[str, Any] | None = None


class Destination(Protocol):
    name: str

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt: ...
