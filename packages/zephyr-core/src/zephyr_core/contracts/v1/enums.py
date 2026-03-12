from __future__ import annotations

from enum import StrEnum


class PartitionStrategy(StrEnum):
    AUTO = "auto"
    FAST = "fast"
    HI_RES = "hi_res"
    OCR_ONLY = "ocr_only"
