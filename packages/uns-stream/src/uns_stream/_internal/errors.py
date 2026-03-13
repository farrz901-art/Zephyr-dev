from __future__ import annotations

from zephyr_core import ErrorCode, ZephyrError


def missing_extra(*, extra: str, detail: str | None = None) -> ZephyrError:
    msg = (
        f"Missing optional dependency extra '{extra}'. "
        f"Install: uv sync --extra {extra} (or install uns-stream[{extra}])."
    )
    if detail:
        msg += f" Detail: {detail}"
    return ZephyrError(code=ErrorCode.UNS_EXTRA_MISSING, message=msg, details={"extra": extra})
