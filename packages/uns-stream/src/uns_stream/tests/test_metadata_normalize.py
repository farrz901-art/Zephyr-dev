from __future__ import annotations

# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
from typing import Any

from uns_stream._internal.normalize import normalize_unstructured_metadata
from uns_stream._internal.serde import to_zephyr_elements


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


def test_normalize_preserves_enhanced_metadata_fields() -> None:
    meta: dict[str, Any] = {
        "text_as_html": "<table><tr><td>1</td></tr></table>",
        "image_base64": "ZmFrZS1pbWFnZQ==",
        "image_mime_type": "image/png",
        "parent_id": "parent-1",
        "is_extracted": True,
        "detection_class_prob": 0.98,
        "coordinates": {
            "points": [[1.0, 2.0], [1.0, 8.0], [9.0, 8.0], [9.0, 2.0]],
            "system": "PixelSpace",
            "layout_width": 1200,
            "layout_height": 1600,
        },
        "page_number": 7,
        "filetype": "application/pdf",
        "filename": "invoice.pdf",
        "languages": ["zho", "eng"],
        "data_source": {"source_id": "src-1"},
        "file_directory": r"C:\secret",
    }

    out, warnings = normalize_unstructured_metadata(meta)

    assert warnings == []
    assert out["text_as_html"] == "<table><tr><td>1</td></tr></table>"
    assert out["image_base64"] == "ZmFrZS1pbWFnZQ=="
    assert out["image_mime_type"] == "image/png"
    assert out["parent_id"] == "parent-1"
    assert out["is_extracted"] is True
    assert out["detection_class_prob"] == 0.98
    assert out["coordinates"]["system"] == "PixelSpace"
    assert out["coordinates"]["layout_width"] == 1200
    assert out["coordinates"]["layout_height"] == 1600
    assert out["page_number"] == 7
    assert out["filetype"] == "application/pdf"
    assert out["filename"] == "invoice.pdf"
    assert out["languages"] == ["zho", "eng"]
    assert out["data_source"] == {"source_id": "src-1"}
    assert "file_directory" not in out


class _FakeElement:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def to_dict(self) -> dict[str, Any]:
        return dict(self._payload)


def test_to_zephyr_elements_preserves_enhanced_metadata_and_redaction() -> None:
    elements = to_zephyr_elements(
        [
            _FakeElement(
                {
                    "element_id": "e1",
                    "type": "Table",
                    "text": "row",
                    "metadata": {
                        "text_as_html": "<table><tr><td>row</td></tr></table>",
                        "image_base64": "YmFzZTY0",
                        "image_mime_type": "image/jpeg",
                        "parent_id": "parent-1",
                        "is_extracted": "true",
                        "detection_class_prob": 0.91,
                        "coordinates": {
                            "points": [[0.0, 0.0], [0.0, 4.0], [8.0, 4.0], [8.0, 0.0]],
                            "system": "PixelSpace",
                            "layout_width": 800,
                            "layout_height": 600,
                        },
                        "page_number": 2,
                        "filetype": "application/pdf",
                        "filename": "contract.pdf",
                        "languages": ["eng", "zho"],
                        "data_source": {"system": "drive"},
                        "file_directory": r"C:\should-hide",
                    },
                }
            )
        ]
    )

    assert len(elements) == 1
    metadata = elements[0].metadata
    assert metadata["text_as_html"] == "<table><tr><td>row</td></tr></table>"
    assert metadata["image_base64"] == "YmFzZTY0"
    assert metadata["image_mime_type"] == "image/jpeg"
    assert metadata["parent_id"] == "parent-1"
    assert metadata["is_extracted"] is True
    assert metadata["detection_class_prob"] == 0.91
    assert metadata["coordinates"]["system"] == "PixelSpace"
    assert metadata["coordinates"]["layout_width"] == 800
    assert metadata["coordinates"]["layout_height"] == 600
    assert metadata["page_number"] == 2
    assert metadata["filetype"] == "application/pdf"
    assert metadata["filename"] == "contract.pdf"
    assert metadata["languages"] == ["eng", "zho"]
    assert metadata["data_source"] == {"system": "drive"}
    assert "file_directory" not in metadata
