from __future__ import annotations

from typing import Final

from zephyr_core import ErrorCode, ZephyrError

OCR_AGENT_PADDLE_QNAME: Final = "unstructured.partition.utils.ocr_models.paddle_ocr.OCRAgentPaddle"
OCR_AGENT_TESSERACT_QNAME: Final = (
    "unstructured.partition.utils.ocr_models.tesseract_ocr.OCRAgentTesseract"
)

OCR_AGENT_ALIASES: Final[dict[str, str]] = {
    "paddle": OCR_AGENT_PADDLE_QNAME,
    "tesseract": OCR_AGENT_TESSERACT_QNAME,
}
_ACCEPTED_QNAME_PREFIX: Final = "unstructured.partition.utils.ocr_models."


def resolve_ocr_agent_alias(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        raise ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED,
            message="OCR agent must be a non-empty alias or qualified name",
            details={"retryable": False, "ocr_agent": value},
        )

    alias = OCR_AGENT_ALIASES.get(normalized.lower())
    if alias is not None:
        return alias

    if normalized.startswith(_ACCEPTED_QNAME_PREFIX) and ".OCRAgent" in normalized:
        return normalized

    raise ZephyrError(
        code=ErrorCode.UNS_PARTITION_FAILED,
        message=f"Unsupported OCR agent: {normalized}",
        details={
            "retryable": False,
            "ocr_agent": normalized,
            "supported_aliases": sorted(OCR_AGENT_ALIASES),
        },
    )
