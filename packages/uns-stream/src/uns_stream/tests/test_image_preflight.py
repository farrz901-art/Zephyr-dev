from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from uns_stream._internal.image_preflight import image_preflight_warning_kind
from uns_stream.service import partition_file
from zephyr_core import PartitionStrategy, ZephyrElement


class RecordingImageBackend:
    name = "dummy"
    backend = "test"
    version = "0.0.0"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]:
        with Image.open(filename) as image:
            mode = image.mode
        self.calls.append(
            {
                "filename": filename,
                "kind": kind,
                "strategy": strategy,
                "unique_element_ids": unique_element_ids,
                "mode": mode,
                "metadata_filename": kwargs.get("metadata_filename"),
                "kwargs": dict(kwargs),
            }
        )
        if mode != "RGB":
            raise IndexError("list index out of range")
        return [
            ZephyrElement(
                element_id="e1",
                type="Image",
                text="invoice image",
                metadata={"page_number": 1, "filename": kwargs.get("metadata_filename")},
            )
        ]


def _write_palette_gif_with_jpg_name(path: Path) -> None:
    image = Image.new("P", (16, 16))
    palette: list[int] = []
    for value in range(256):
        palette.extend((value, 0, 0))
    image.putpalette(palette)
    image.putdata([0] * (16 * 16))
    image.save(path, format="GIF")


def test_partition_file_applies_image_preflight_for_palette_invoice_images(tmp_path: Path) -> None:
    image_path = tmp_path / "fapiao3.jpg"
    _write_palette_gif_with_jpg_name(image_path)
    backend = RecordingImageBackend()

    result = partition_file(
        filename=str(image_path),
        kind="image",
        backend=backend,
        profile="invoice",
        strategy=PartitionStrategy.HI_RES,
    )

    assert len(backend.calls) == 1
    call = backend.calls[0]
    assert call["mode"] == "RGB"
    assert Path(call["filename"]).name != image_path.name
    assert call["metadata_filename"] == image_path.name
    assert result.warnings
    warning_payload = json.loads(result.warnings[0])
    assert warning_payload["kind"] == image_preflight_warning_kind()
    assert warning_payload["image_preflight_applied"] is True
    assert warning_payload["image_preflight_reason"] == "palette_mode_jpeg_save_risk"
    assert warning_payload["original_image_mode"] == "P"
    assert warning_payload["normalized_image_mode"] == "RGB"


def test_partition_file_does_not_swallow_non_preflight_image_failure(tmp_path: Path) -> None:
    image_path = tmp_path / "broken.jpg"
    _write_palette_gif_with_jpg_name(image_path)

    class AlwaysFailingBackend(RecordingImageBackend):
        def partition_elements(self, **kwargs: Any) -> list[ZephyrElement]:
            raise RuntimeError("still broken after preflight")

    backend = AlwaysFailingBackend()

    with pytest.raises(Exception, match="Partition failed"):
        partition_file(
            filename=str(image_path),
            kind="image",
            backend=backend,
            profile="invoice",
            strategy=PartitionStrategy.HI_RES,
        )
