from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    IO_READ_FAILED = "ZE-IO-READ-FAILED"

    UNS_EXTRA_MISSING = "ZE-UNS-EXTRA-MISSING"
    UNS_PARTITION_FAILED = "ZE-UNS-PARTITION-FAILED"
    UNS_UNSUPPORTED_TYPE = "ZE-UNS-UNSUPPORTED-TYPE"
