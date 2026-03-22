from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from zephyr_core import PartitionResult, RunMetaV1
from zephyr_ingest.destinations.base import DeliveryReceipt, Destination


@dataclass(frozen=True, slots=True)
class FanoutDestination:
    destinations: tuple[Destination, ...]
    name: str = "fanout"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        receipts: list[dict[str, Any]] = []
        all_ok = True

        for dest in self.destinations:
            r = dest(out_root=out_root, sha256=sha256, meta=meta, result=result)
            receipts.append({"destination": r.destination, "ok": r.ok, "details": r.details})
            if not r.ok:
                all_ok = False

        return DeliveryReceipt(destination=self.name, ok=all_ok, details={"receipts": receipts})
