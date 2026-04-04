from __future__ import annotations

import socket
from typing import cast

from zephyr_core import ErrorCode, ZephyrError


def is_retryable_exception(e: Exception) -> bool:
    if isinstance(e, ZephyrError):
        code = getattr(e, "code", None)
        if code in {ErrorCode.UNS_EXTRA_MISSING, ErrorCode.UNS_UNSUPPORTED_TYPE}:
            return False

        details_obj = getattr(e, "details", None)
        if isinstance(details_obj, dict):
            typed_details = cast("dict[str, object]", details_obj)
            val = typed_details.get("retryable")
            if val is True or val == "true":
                return True
            if val is False or val == "false":
                return False

    transient_errors = (
        TimeoutError,
        ConnectionError,
        socket.timeout,
        InterruptedError,
    )
    if isinstance(e, transient_errors):
        return True

    exc_msg = str(e).lower()
    return "rate limit" in exc_msg or "too many requests" in exc_msg
