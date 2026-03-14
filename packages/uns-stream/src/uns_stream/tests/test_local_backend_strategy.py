from __future__ import annotations

from typing import Any, Callable

import pytest

import uns_stream.backends.local_unstructured as local_mod
from uns_stream.backends.local_unstructured import LocalUnstructuredBackend
from zephyr_core import PartitionStrategy, ZephyrElement


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


def test_strategy_is_passed_only_for_pdf_and_image(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fake_loader(monkeypatch)
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
    assert calls[-1]["kwargs"]["languages"] == ["eng"]

    # text: should NOT pass strategy at all (avoid TypeError for non-pdf/image partition fns)
    backend.partition_elements(
        filename="x.txt",
        kind="text",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
    )
    assert calls[-1]["kind"] == "text"
    assert "strategy" not in calls[-1]["kwargs"]


def test_image_fast_strategy_maps_to_auto(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fake_loader(monkeypatch)
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
