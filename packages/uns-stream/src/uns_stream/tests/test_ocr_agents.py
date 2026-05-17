from __future__ import annotations

import pytest

from uns_stream._internal.ocr_agents import (
    OCR_AGENT_PADDLE_QNAME,
    OCR_AGENT_TESSERACT_QNAME,
    resolve_ocr_agent_alias,
)
from zephyr_core import ErrorCode, ZephyrError

pytestmark = [pytest.mark.uns, pytest.mark.unit]


def test_resolve_ocr_agent_alias_supports_expected_values() -> None:
    assert resolve_ocr_agent_alias(None) is None
    assert resolve_ocr_agent_alias("paddle") == OCR_AGENT_PADDLE_QNAME
    assert resolve_ocr_agent_alias("PADDLE") == OCR_AGENT_PADDLE_QNAME
    assert resolve_ocr_agent_alias("tesseract") == OCR_AGENT_TESSERACT_QNAME
    assert resolve_ocr_agent_alias(OCR_AGENT_PADDLE_QNAME) == OCR_AGENT_PADDLE_QNAME


def test_resolve_ocr_agent_alias_rejects_invalid_values() -> None:
    with pytest.raises(ZephyrError) as excinfo:
        resolve_ocr_agent_alias("google-vision")

    assert excinfo.value.code == ErrorCode.UNS_PARTITION_FAILED
    assert excinfo.value.details is not None
    assert excinfo.value.details["ocr_agent"] == "google-vision"
