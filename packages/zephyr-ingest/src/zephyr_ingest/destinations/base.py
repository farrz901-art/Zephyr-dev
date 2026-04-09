from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict

from zephyr_core import PartitionResult, RunMetaV1

DeliveryFailureRetryability = Literal["retryable", "non_retryable", "not_failed", "unknown"]
DeliverySharedFailureKind = Literal[
    "client_error",
    "server_error",
    "rate_limited",
    "timeout",
    "connection",
    "protocol",
    "locked",
    "constraint",
    "operational",
    "database_error",
    "not_failed",
    "unknown",
]
DeliverySharedErrorCode = Literal[
    "ZE-DELIVERY-FAILED",
    "ZE-DELIVERY-HTTP-FAILED",
    "ZE-DELIVERY-KAFKA-FAILED",
    "ZE-DELIVERY-WEAVIATE-FAILED",
    "ZE-DELIVERY-INVALID-PAYLOAD",
    "not_failed",
    "unknown",
]
DeliveryOutcomeSummary = Literal["delivered", "failed"]


class DeliveryReceiptSummaryV1(TypedDict):
    delivery_outcome: DeliveryOutcomeSummary
    failure_retryability: DeliveryFailureRetryability
    failure_kind: DeliverySharedFailureKind
    error_code: DeliverySharedErrorCode
    attempt_count: int
    payload_count: Literal[1]


_SHARED_FAILURE_KINDS: tuple[DeliverySharedFailureKind, ...] = (
    "client_error",
    "server_error",
    "rate_limited",
    "timeout",
    "connection",
    "protocol",
    "locked",
    "constraint",
    "operational",
    "database_error",
)
_SHARED_ERROR_CODES: tuple[DeliverySharedErrorCode, ...] = (
    "ZE-DELIVERY-FAILED",
    "ZE-DELIVERY-HTTP-FAILED",
    "ZE-DELIVERY-KAFKA-FAILED",
    "ZE-DELIVERY-WEAVIATE-FAILED",
    "ZE-DELIVERY-INVALID-PAYLOAD",
)
_SHARED_FAILURE_KIND_MAP: dict[str, DeliverySharedFailureKind] = {
    failure_kind: failure_kind for failure_kind in _SHARED_FAILURE_KINDS
}
_SHARED_ERROR_CODE_MAP: dict[str, DeliverySharedErrorCode] = {
    error_code: error_code for error_code in _SHARED_ERROR_CODES
}


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    destination: str
    ok: bool
    details: dict[str, Any] | None = None

    @property
    def attempt_count(self) -> int:
        if self.details is None:
            return 1
        attempts = self.details.get("attempts")
        if isinstance(attempts, int) and attempts >= 1:
            return attempts
        return 1

    @property
    def failure_retryability(self) -> DeliveryFailureRetryability:
        if self.ok:
            return "not_failed"
        if self.details is None:
            return "unknown"
        retryable = self.details.get("retryable")
        if retryable is True:
            return "retryable"
        if retryable is False:
            return "non_retryable"
        return "unknown"

    @property
    def shared_failure_kind(self) -> DeliverySharedFailureKind:
        if self.ok:
            return "not_failed"
        if self.details is None:
            return "unknown"
        failure_kind = self.details.get("failure_kind")
        if isinstance(failure_kind, str):
            normalized = _SHARED_FAILURE_KIND_MAP.get(failure_kind)
            if normalized is not None:
                return normalized
        return "unknown"

    @property
    def shared_error_code(self) -> DeliverySharedErrorCode:
        if self.ok:
            return "not_failed"
        if self.details is None:
            return "unknown"
        error_code = self.details.get("error_code")
        if isinstance(error_code, str):
            normalized = _SHARED_ERROR_CODE_MAP.get(error_code)
            if normalized is not None:
                return normalized
        return "unknown"

    @property
    def shared_summary(self) -> DeliveryReceiptSummaryV1:
        return {
            "delivery_outcome": "delivered" if self.ok else "failed",
            "failure_retryability": self.failure_retryability,
            "failure_kind": self.shared_failure_kind,
            "error_code": self.shared_error_code,
            "attempt_count": self.attempt_count,
            "payload_count": 1,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "destination": self.destination,
            "ok": self.ok,
            "details": self.details,
            "summary": self.shared_summary,
        }


class Destination(Protocol):
    @property
    # name: str
    def name(self) -> str: ...

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt: ...
