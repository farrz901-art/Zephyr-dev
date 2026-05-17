from __future__ import annotations

from typing import Any, Callable

import pytest

import uns_stream.backends.local_unstructured as local_mod
from uns_stream._internal.ocr_agents import OCR_AGENT_PADDLE_QNAME, OCR_AGENT_TESSERACT_QNAME
from uns_stream.backends.local_unstructured import LocalUnstructuredBackend
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError

pytestmark = [pytest.mark.uns, pytest.mark.unit]


class FakeElement:
    """Minimal fake unstructured element compatible with to_zephyr_elements()."""

    def __init__(self, *, element_id: str = "1", type_: str = "Title", text: str = "Hello") -> None:
        self._d = {
            "element_id": element_id,
            "type": type_,
            "text": text,
            "metadata": {"fake": True},
        }

    def to_dict(self) -> dict[str, Any]:
        return dict(self._d)


def _install_fake_loader(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Monkeypatch _load_partition_fn to capture kwargs passed to unstructured partition_* calls."""
    calls: list[dict[str, Any]] = []

    def fake_loader(kind: str) -> Callable[..., Any]:
        def fake_partition_fn(**kwargs: Any) -> list[FakeElement]:
            calls.append({"kind": kind, "kwargs": dict(kwargs)})
            return [FakeElement(text=f"kind={kind}")]

        return fake_partition_fn

    monkeypatch.setattr(local_mod, "_load_partition_fn", fake_loader)
    return calls


def _install_runtime_recorder(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def fake_preload_torch_for_paddleocr() -> dict[str, object]:
        calls.append("preload")
        return {
            "torch_preload_applied": True,
            "torch_import_ok": True,
            "torch_version": "fake",
            "torch_error_type": None,
            "torch_error": None,
        }

    def fake_ensure_paddleocr_base_dir() -> str:
        calls.append("ensure")
        return "tmp"

    monkeypatch.setattr(local_mod, "preload_torch_for_paddleocr", fake_preload_torch_for_paddleocr)
    monkeypatch.setattr(local_mod, "ensure_paddleocr_base_dir", fake_ensure_paddleocr_base_dir)
    return calls


def test_strategy_is_passed_only_for_pdf_and_image(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fake_loader(monkeypatch)
    runtime_calls = _install_runtime_recorder(monkeypatch)
    backend = LocalUnstructuredBackend()

    # pdf: should pass strategy through as a string
    out_pdf = backend.partition_elements(
        filename="x.pdf",
        kind="pdf",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
        infer_table_structure=True,
    )
    assert isinstance(out_pdf, list)
    assert isinstance(out_pdf[0], ZephyrElement)
    assert calls[-1]["kind"] == "pdf"
    assert calls[-1]["kwargs"]["filename"] == "x.pdf"
    assert calls[-1]["kwargs"]["unique_element_ids"] is True
    assert calls[-1]["kwargs"]["infer_table_structure"] is True
    assert calls[-1]["kwargs"]["strategy"] == "hi_res"
    assert calls[-1]["kwargs"]["ocr_agent"] == OCR_AGENT_PADDLE_QNAME
    assert calls[-1]["kwargs"]["table_ocr_agent"] == OCR_AGENT_PADDLE_QNAME
    assert runtime_calls == ["preload", "ensure"]

    # image: should pass strategy through (FAST should be normalized elsewhere; see next test)
    backend.partition_elements(
        filename="x.png",
        kind="image",
        strategy=PartitionStrategy.OCR_ONLY,
        unique_element_ids=False,
        languages=["eng"],
    )
    assert calls[-1]["kind"] == "image"
    assert calls[-1]["kwargs"]["strategy"] == "ocr_only"
    assert calls[-1]["kwargs"]["languages"] == ["en"]
    assert calls[-1]["kwargs"]["ocr_agent"] == OCR_AGENT_PADDLE_QNAME
    assert calls[-1]["kwargs"]["table_ocr_agent"] == OCR_AGENT_PADDLE_QNAME
    assert runtime_calls == ["preload", "ensure", "preload", "ensure"]

    # text: should NOT pass strategy at all (avoid TypeError for non-pdf/image partition fns)
    backend.partition_elements(
        filename="x.txt",
        kind="text",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
    )
    assert calls[-1]["kind"] == "text"
    assert "strategy" not in calls[-1]["kwargs"]
    assert "ocr_agent" not in calls[-1]["kwargs"]
    assert "table_ocr_agent" not in calls[-1]["kwargs"]
    assert runtime_calls == ["preload", "ensure", "preload", "ensure"]


def test_image_fast_strategy_maps_to_auto(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fake_loader(monkeypatch)
    runtime_calls = _install_runtime_recorder(monkeypatch)
    backend = LocalUnstructuredBackend()

    backend.partition_elements(
        filename="x.jpg",
        kind="image",
        strategy=PartitionStrategy.FAST,
        unique_element_ids=True,
    )
    assert calls[-1]["kind"] == "image"
    # We keep ZephyrStrategy uniform; for image FAST is not a supported unstructured strategy,
    # so we normalize it to "auto".
    assert calls[-1]["kwargs"]["strategy"] == "auto"
    assert calls[-1]["kwargs"]["ocr_agent"] == OCR_AGENT_PADDLE_QNAME
    assert calls[-1]["kwargs"]["table_ocr_agent"] == OCR_AGENT_PADDLE_QNAME
    assert runtime_calls == ["preload", "ensure"]


@pytest.mark.parametrize("kind", ["text", "docx", "xlsx", "csv", "html", "md"])
def test_non_ocr_kinds_do_not_receive_default_ocr_agents(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    calls = _install_fake_loader(monkeypatch)
    runtime_calls = _install_runtime_recorder(monkeypatch)
    backend = LocalUnstructuredBackend()

    backend.partition_elements(
        filename=f"x.{kind}",
        kind=kind,
        strategy=PartitionStrategy.AUTO,
        unique_element_ids=True,
    )

    assert calls[-1]["kind"] == kind
    assert "ocr_agent" not in calls[-1]["kwargs"]
    assert "table_ocr_agent" not in calls[-1]["kwargs"]
    assert runtime_calls == []


def test_explicit_tesseract_override_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fake_loader(monkeypatch)
    runtime_calls = _install_runtime_recorder(monkeypatch)
    backend = LocalUnstructuredBackend()

    backend.partition_elements(
        filename="x.pdf",
        kind="pdf",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
        ocr_agent=OCR_AGENT_TESSERACT_QNAME,
        table_ocr_agent=OCR_AGENT_TESSERACT_QNAME,
    )

    assert calls[-1]["kwargs"]["ocr_agent"] == OCR_AGENT_TESSERACT_QNAME
    assert calls[-1]["kwargs"]["table_ocr_agent"] == OCR_AGENT_TESSERACT_QNAME
    assert runtime_calls == []


def test_pdf_paddle_path_normalizes_mixed_chinese_languages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _install_fake_loader(monkeypatch)
    _install_runtime_recorder(monkeypatch)
    backend = LocalUnstructuredBackend()

    backend.partition_elements(
        filename="x.pdf",
        kind="pdf",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
        languages=["zho", "eng"],
    )

    assert calls[-1]["kwargs"]["languages"] == ["ch"]
    notes = backend.consume_runtime_notes()
    normalization_note = next(
        note for note in notes if note.get("event") == "paddle_language_normalization"
    )
    assert normalization_note["paddle_languages_before"] == ["zho", "eng"]
    assert normalization_note["paddle_languages_after"] == ["ch"]


def test_tesseract_path_is_not_language_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fake_loader(monkeypatch)
    _install_runtime_recorder(monkeypatch)
    backend = LocalUnstructuredBackend()

    backend.partition_elements(
        filename="x.pdf",
        kind="pdf",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
        ocr_agent=OCR_AGENT_TESSERACT_QNAME,
        table_ocr_agent=OCR_AGENT_TESSERACT_QNAME,
        languages=["zho", "eng"],
    )

    assert calls[-1]["kwargs"]["languages"] == ["zho", "eng"]
    assert backend.consume_runtime_notes() == []


def test_known_paddle_failure_triggers_tesseract_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _install_runtime_recorder(monkeypatch)

    def fake_loader(kind: str) -> Callable[..., Any]:
        def fake_partition_fn(**kwargs: Any) -> list[FakeElement]:
            calls.append({"kind": kind, "kwargs": dict(kwargs)})
            if kwargs["ocr_agent"] == OCR_AGENT_PADDLE_QNAME:
                raise RuntimeError("paddle_ocr runtime failed")
            return [FakeElement(text="fallback-ok")]

        return fake_partition_fn

    monkeypatch.setattr(local_mod, "_load_partition_fn", fake_loader)
    backend = LocalUnstructuredBackend()

    out = backend.partition_elements(
        filename="x.pdf",
        kind="pdf",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
        languages=["zho", "eng"],
    )

    assert isinstance(out, list)
    assert len(calls) == 2
    assert calls[0]["kwargs"]["ocr_agent"] == OCR_AGENT_PADDLE_QNAME
    assert calls[0]["kwargs"]["languages"] == ["ch"]
    assert calls[1]["kwargs"]["ocr_agent"] == OCR_AGENT_TESSERACT_QNAME
    assert calls[1]["kwargs"]["table_ocr_agent"] == OCR_AGENT_TESSERACT_QNAME
    assert calls[1]["kwargs"]["languages"] == ["zho", "eng"]
    notes = backend.consume_runtime_notes()
    fallback_note = next(note for note in notes if note.get("event") == "paddle_fallback")
    assert fallback_note["paddle_fallback_applied"] is True
    assert fallback_note["fallback_status"] == "ok"


def test_fallback_failure_reports_both_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_runtime_recorder(monkeypatch)

    def fake_loader(kind: str) -> Callable[..., Any]:
        def fake_partition_fn(**kwargs: Any) -> list[FakeElement]:
            if kwargs["ocr_agent"] == OCR_AGENT_PADDLE_QNAME:
                raise RuntimeError("paddle_ocr runtime failed")
            raise RuntimeError("tesseract fallback failed")

        return fake_partition_fn

    monkeypatch.setattr(local_mod, "_load_partition_fn", fake_loader)
    backend = LocalUnstructuredBackend()

    with pytest.raises(ZephyrError) as excinfo:
        backend.partition_elements(
            filename="x.pdf",
            kind="pdf",
            strategy=PartitionStrategy.HI_RES,
            unique_element_ids=True,
            languages=["zho", "eng"],
        )

    assert excinfo.value.details is not None
    assert excinfo.value.details["paddle_original_error_type"] == "RuntimeError"
    assert excinfo.value.details["fallback_error_type"] == "RuntimeError"
    notes = backend.consume_runtime_notes()
    fallback_note = next(note for note in notes if note.get("event") == "paddle_fallback")
    assert fallback_note["fallback_status"] == "error"


def test_non_paddle_error_does_not_trigger_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _install_runtime_recorder(monkeypatch)

    def fake_loader(kind: str) -> Callable[..., Any]:
        def fake_partition_fn(**kwargs: Any) -> list[FakeElement]:
            calls.append({"kind": kind, "kwargs": dict(kwargs)})
            raise ValueError("bad filename")

        return fake_partition_fn

    monkeypatch.setattr(local_mod, "_load_partition_fn", fake_loader)
    backend = LocalUnstructuredBackend()

    with pytest.raises(ValueError):
        backend.partition_elements(
            filename="x.pdf",
            kind="pdf",
            strategy=PartitionStrategy.HI_RES,
            unique_element_ids=True,
        )

    assert len(calls) == 1


def test_local_backend_rejects_unsupported_enhanced_kwargs_for_strict_partition_fn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalUnstructuredBackend()

    def fake_loader(kind: str) -> Callable[..., Any]:
        del kind

        def fake_partition_fn(
            *,
            filename: str,
            unique_element_ids: bool = True,
        ) -> list[FakeElement]:
            del filename, unique_element_ids
            return [FakeElement(text="strict")]

        return fake_partition_fn

    monkeypatch.setattr(local_mod, "_load_partition_fn", fake_loader)

    with pytest.raises(ZephyrError) as excinfo:
        backend.partition_elements(
            filename="x.txt",
            kind="text",
            strategy=PartitionStrategy.AUTO,
            unique_element_ids=True,
            extract_image_block_to_payload=True,
        )

    assert excinfo.value.code == ErrorCode.UNS_PARTITION_FAILED
    assert excinfo.value.details is not None
    assert excinfo.value.details["unsupported_partition_kwargs"] == [
        "extract_image_block_to_payload"
    ]
