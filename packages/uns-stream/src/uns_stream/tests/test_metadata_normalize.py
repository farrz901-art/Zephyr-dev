from __future__ import annotations

# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
from typing import Any

from uns_stream._internal.normalize import normalize_unstructured_metadata


def test_normalize_removes_file_directory_and_derives_bbox_and_bool() -> None:
    meta: dict[str, Any] = {
        "file_directory": r"packages\uns-stream\src\uns_stream\tests\fixtures\example-docs\pdf",
        "is_extracted": "true",
        "coordinates": {
            "points": [
                [1146.5523, 450.4698],
                [1146.5523, 495.2769],
                [2341.0949, 495.2769],
                [2341.0949, 450.4698],
            ],
            "system": "PixelSpace",
            "layout_width": 2975,
            "layout_height": 3850,
        },
    }

    # 关键修复：使用元组解包 (out, _)
    out, _ = normalize_unstructured_metadata(meta)

    assert "file_directory" not in out
    assert out["is_extracted"] is True

    # 现在的 out 是真正的字典，可以安全调用 .get()
    bbox = out.get("bbox")
    assert isinstance(bbox, dict)
    assert bbox["x_min"] == 1146.5523
    assert bbox["y_min"] == 450.4698
    assert bbox["x_max"] == 2341.0949
    assert bbox["y_max"] == 495.2769


def test_normalize_is_extracted_false() -> None:
    # 关键修复：使用元组解包 (out, _)
    out, _ = normalize_unstructured_metadata({"is_extracted": "false"})
    assert out["is_extracted"] is False
