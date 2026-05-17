from __future__ import annotations

import pytest

import uns_stream._internal.paddleocr_runtime as runtime_mod
from uns_stream._internal.ocr_agents import (
    OCR_AGENT_PADDLE_QNAME,
    OCR_AGENT_TESSERACT_QNAME,
)

pytestmark = [pytest.mark.uns, pytest.mark.unit]


def test_partition_call_uses_paddle_ocr_detects_supported_fields() -> None:
    assert runtime_mod.partition_call_uses_paddle_ocr({"ocr_agent": OCR_AGENT_PADDLE_QNAME}) is True
    assert (
        runtime_mod.partition_call_uses_paddle_ocr({"table_ocr_agent": OCR_AGENT_PADDLE_QNAME})
        is True
    )
    assert (
        runtime_mod.partition_call_uses_paddle_ocr(
            {
                "ocr_agent": OCR_AGENT_TESSERACT_QNAME,
                "table_ocr_agent": OCR_AGENT_PADDLE_QNAME,
            }
        )
        is True
    )


def test_partition_call_uses_paddle_ocr_rejects_non_paddle_cases() -> None:
    assert (
        runtime_mod.partition_call_uses_paddle_ocr(
            {
                "ocr_agent": OCR_AGENT_TESSERACT_QNAME,
                "table_ocr_agent": OCR_AGENT_TESSERACT_QNAME,
            }
        )
        is False
    )
    assert runtime_mod.partition_call_uses_paddle_ocr({}) is False


def test_preload_torch_for_paddleocr_success_path(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeTorch:
        __version__ = "2.7.0"

    def fake_import_module(name: str) -> object:
        assert name == "torch"
        return FakeTorch()

    monkeypatch.setattr(runtime_mod, "import_module", fake_import_module)

    observed = runtime_mod.preload_torch_for_paddleocr()

    assert observed == {
        "torch_preload_applied": True,
        "torch_import_ok": True,
        "torch_version": "2.7.0",
        "torch_error_type": None,
        "torch_error": None,
    }


def test_preload_torch_for_paddleocr_failure_path(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import_module(name: str) -> object:
        assert name == "torch"
        raise OSError("shm.dll missing")

    monkeypatch.setattr(runtime_mod, "import_module", fake_import_module)

    observed = runtime_mod.preload_torch_for_paddleocr()

    assert observed["torch_preload_applied"] is True
    assert observed["torch_import_ok"] is False
    assert observed["torch_version"] is None
    assert observed["torch_error_type"] == "OSError"
    assert observed["torch_error"] == "shm.dll missing"


def test_apply_paddle_language_normalization_only_on_paddle_pdf_image_path() -> None:
    updated, note = runtime_mod.apply_paddle_language_normalization(
        kind="pdf",
        call_kwargs={
            "ocr_agent": OCR_AGENT_PADDLE_QNAME,
            "table_ocr_agent": OCR_AGENT_PADDLE_QNAME,
            "languages": ["zho", "eng"],
        },
    )

    assert updated["languages"] == ["ch"]
    assert note is not None
    assert note["paddle_languages_before"] == ["zho", "eng"]
    assert note["paddle_languages_after"] == ["ch"]


def test_apply_paddle_language_normalization_skips_non_paddle_paths() -> None:
    updated, note = runtime_mod.apply_paddle_language_normalization(
        kind="text",
        call_kwargs={
            "ocr_agent": OCR_AGENT_PADDLE_QNAME,
            "languages": ["zho", "eng"],
        },
    )

    assert updated["languages"] == ["zho", "eng"]
    assert note is None


def test_is_known_paddle_runtime_failure_uses_traceback_context() -> None:
    with pytest.raises(RuntimeError) as excinfo:
        raise RuntimeError("unstructured_paddleocr language code failure")

    assert runtime_mod.is_known_paddle_runtime_failure(excinfo.value) is True

    with pytest.raises(ValueError) as excinfo2:
        raise ValueError("bad filename")

    assert runtime_mod.is_known_paddle_runtime_failure(excinfo2.value) is False
