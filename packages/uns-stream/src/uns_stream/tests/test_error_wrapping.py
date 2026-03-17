from __future__ import annotations

# pyright: reportAttributeAccessIssue=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownParameterType=false
# pyright: reportUnknownVariableType=false
from pathlib import Path
from typing import Any

import pytest

from uns_stream.service import partition_file
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError


class ExplodingBackend:
    name = "dummy"
    backend = "test"
    version = "0.0.0"

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]:
        raise ValueError("boom")


def test_partition_wraps_unknown_exception(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    with pytest.raises(ZephyrError) as ei:
        partition_file(
            filename=str(f),
            kind="text",
            backend=ExplodingBackend(),
        )

    err = ei.value
    assert err.code == ErrorCode.UNS_PARTITION_FAILED
    assert err.details is not None
    assert err.details["retryable"] is False
    assert err.details["kind"] == "text"
    assert err.details["strategy"] == "auto"
    assert err.details["engine"]["name"] == "dummy"
    assert err.details["exc_type"] == "ValueError"
    assert "boom" in err.details["exc"]
