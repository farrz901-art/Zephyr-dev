from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from zephyr_core.errors.codes import ErrorCode


@dataclass(frozen=True, slots=True)
class ZephyrError(Exception):
    code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"
