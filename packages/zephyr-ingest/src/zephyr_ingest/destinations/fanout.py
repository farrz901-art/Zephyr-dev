from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from zephyr_core import PartitionResult, RunMetaV1
from zephyr_ingest.destinations.base import DeliveryReceipt, Destination


def _aggregate_failed_child_details(*, receipts: tuple[DeliveryReceipt, ...]) -> dict[str, Any]:
    details: dict[str, Any] = {
        "receipts": [
            {"destination": receipt.destination, "ok": receipt.ok, "details": receipt.details}
            for receipt in receipts
        ]
    }
    if not receipts:
        return details

    details["attempts"] = max(receipt.attempt_count for receipt in receipts)
    failed_receipts = tuple(receipt for receipt in receipts if not receipt.ok)
    if not failed_receipts:
        return details

    retryability_values = {
        receipt.failure_retryability
        for receipt in failed_receipts
        if receipt.failure_retryability in {"retryable", "non_retryable"}
    }
    if len(retryability_values) == 1:
        details["retryable"] = retryability_values.pop() == "retryable"

    failure_kind_values = {
        receipt.shared_failure_kind
        for receipt in failed_receipts
        if receipt.shared_failure_kind not in {"not_failed", "unknown"}
    }
    if len(failure_kind_values) == 1:
        details["failure_kind"] = failure_kind_values.pop()

    error_code_values = {
        receipt.shared_error_code
        for receipt in failed_receipts
        if receipt.shared_error_code not in {"not_failed", "unknown"}
    }
    if len(error_code_values) == 1:
        details["error_code"] = error_code_values.pop()

    return details


@dataclass(frozen=True, slots=True)
class FanoutDestination:
    destinations: tuple[Destination, ...]
    # name: str = "fanout"
    name: str = field(default="fanout", init=False)

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        child_receipts: list[DeliveryReceipt] = []
        all_ok = True

        for dest in self.destinations:
            r = dest(out_root=out_root, sha256=sha256, meta=meta, result=result)
            child_receipts.append(r)
            if not r.ok:
                all_ok = False

        return DeliveryReceipt(
            destination=self.name,
            ok=all_ok,
            details=_aggregate_failed_child_details(receipts=tuple(child_receipts)),
        )
