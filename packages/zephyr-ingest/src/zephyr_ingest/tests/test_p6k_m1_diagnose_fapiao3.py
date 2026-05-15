from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image
from tools import p6k_m1_diagnose_fapiao3 as diagnosis_mod

from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)


def test_diagnosis_report_marks_missing_input(tmp_path: Path) -> None:
    report = diagnosis_mod.build_report(
        input_path=tmp_path / "missing.jpg",
        profile="invoice",
        strategy=PartitionStrategy.HI_RES,
    )

    assert report["status"] == "missing_input"
    assert report["missing_input"] is True
    assert report["file_exists"] is False
    assert report["exception_type"] is None


def test_diagnosis_report_captures_image_shape_and_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = tmp_path / "fapiao3.jpg"
    image = Image.new("P", (12, 10))
    image.save(sample, format="GIF")

    def _raise_partition_error(**_: object) -> PartitionResult:
        raise RuntimeError("cannot write mode P as JPEG")

    monkeypatch.setattr(diagnosis_mod, "auto_partition", _raise_partition_error)
    report = diagnosis_mod.build_report(
        input_path=sample,
        profile="invoice",
        strategy=PartitionStrategy.HI_RES,
    )

    assert report["status"] == "error"
    assert report["file_exists"] is True
    assert report["image_format"] == "GIF"
    assert report["image_mode"] == "P"
    assert report["image_dimensions"] == {"width": 12, "height": 10}
    assert report["failure_stage"] == "jpeg_save"
    assert report["exception_type"] == "RuntimeError"
    assert report["exception_chain_summary"]
    assert report["traceback_tail"]


def test_diagnosis_report_marks_success_when_partition_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = tmp_path / "fapiao3.jpg"
    image = Image.new("RGB", (10, 10), color="white")
    image.save(sample, format="JPEG")

    def _ok_partition(**_: object) -> PartitionResult:
        return PartitionResult(
            document=DocumentMetadata(
                filename=sample.name,
                mime_type="image/jpeg",
                sha256="sha",
                size_bytes=sample.stat().st_size,
                created_at_utc="2026-05-15T00:00:00Z",
            ),
            engine=EngineInfo(
                name="unstructured",
                backend="local",
                version="0",
                strategy=PartitionStrategy.HI_RES,
            ),
            elements=[
                ZephyrElement(
                    element_id="e1",
                    type="Image",
                    text="ok",
                    metadata={"page_number": 1},
                )
            ],
            normalized_text="ok",
            warnings=[],
        )

    monkeypatch.setattr(diagnosis_mod, "auto_partition", _ok_partition)
    report = diagnosis_mod.build_report(
        input_path=sample,
        profile="invoice",
        strategy=PartitionStrategy.HI_RES,
    )

    assert report["status"] == "ok"
    assert report["failure_stage"] is None
    assert report["elements_count"] == 1
