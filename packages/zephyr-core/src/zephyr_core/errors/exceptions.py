from __future__ import annotations

from typing import Any

from zephyr_core.errors.codes import ErrorCode


class ZephyrError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"
