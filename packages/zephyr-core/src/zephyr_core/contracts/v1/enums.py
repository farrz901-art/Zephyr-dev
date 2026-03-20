from __future__ import annotations

from enum import StrEnum


class PartitionStrategy(StrEnum):
    AUTO = "auto"
    FAST = "fast"
    HI_RES = "hi_res"
    OCR_ONLY = "ocr_only"


class RunOutcome(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED_UNSUPPORTED = "skipped_unsupported"
    SKIPPED_EXISTING = "skipped_existing"
