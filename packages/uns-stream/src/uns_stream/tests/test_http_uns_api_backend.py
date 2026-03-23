from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from uns_stream.backends.http_uns_api import HttpUnsApiBackend
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


def test_http_uns_api_backend_converts_elements_and_normalizes_metadata(tmp_path: Path) -> None:
    f = tmp_path / "a.pdf"
    f.write_bytes(b"%PDF-1.4 fake")

    def handler(request: httpx.Request) -> httpx.Response:
        # ensure api key header is passed through
        assert request.headers.get("unstructured-api-key") == "k"

        return httpx.Response(
            200,
            json=[
                {
                    "type": "Title",
                    "element_id": "e1",
                    "text": "Hello",
                    "metadata": {
                        "file_directory": r"C:\secret\path",
                        "is_extracted": "true",
                        "coordinates": {"points": [[0.0, 0.0], [0.0, 1.0], [2.0, 1.0], [2.0, 0.0]]},
                    },
                }
            ],
        )

    transport = httpx.MockTransport(handler)

    b = HttpUnsApiBackend(
        url="https://example.test/general/v0/general",
        api_key="k",
        timeout_s=5.0,
        transport=transport,
    )

    els = b.partition_elements(
        filename=str(f),
        kind="pdf",
        strategy=PartitionStrategy.HI_RES,
        unique_element_ids=True,
        infer_table_structure=True,
        coordinates=True,
        languages=["eng"],
    )

    assert len(els) == 1
    assert els[0].element_id == "e1"
    assert els[0].metadata.get("file_directory") is None  # removed by normalize
    assert els[0].metadata.get("is_extracted") is True  # normalized to bool
    assert "bbox" in els[0].metadata


def test_http_uns_api_backend_raises_retryable_on_5xx(tmp_path: Path) -> None:
    f = tmp_path / "a.pdf"
    f.write_bytes(b"%PDF-1.4 fake")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    transport = httpx.MockTransport(handler)

    b = HttpUnsApiBackend(
        url="https://example.test/general/v0/general",
        api_key=None,
        timeout_s=5.0,
        transport=transport,
    )

    with pytest.raises(ZephyrError) as ei:
        b.partition_elements(
            filename=str(f),
            kind="pdf",
            strategy=PartitionStrategy.FAST,
            unique_element_ids=True,
        )

    err = ei.value
    assert err.code == ErrorCode.UNS_PARTITION_FAILED
    assert err.details is not None
    assert err.details.get("retryable") is True
