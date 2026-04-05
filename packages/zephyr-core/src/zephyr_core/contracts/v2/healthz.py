from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable


class HealthCheckKind(StrEnum):
    LIVENESS = "liveness"
    READINESS = "readiness"
    STARTUP = "startup"


def _empty_details() -> dict[str, object]:
    return {}


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    kind: HealthCheckKind
    healthy: bool
    reason: str | None = None
    details: dict[str, object] = field(default_factory=_empty_details)


@runtime_checkable
class HealthCheckProvider(Protocol):
    def check(self, kind: HealthCheckKind) -> HealthCheckResult: ...
