from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest

from uns_stream.service import partition_file
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError


class DummyOKBackend:
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
        return [
            ZephyrElement(element_id="1", type="Title", text="Hello", metadata={}),
            ZephyrElement(element_id="2", type="NarrativeText", text="World", metadata={}),
        ]


class DummyExplodeBackend:
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


class DummyZephyrErrorBackend:
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
        raise ZephyrError(
            code=ErrorCode.UNS_UNSUPPORTED_TYPE, message="nope", details={"retryable": False}
        )


def _messages(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(r.getMessage() for r in caplog.records)


def test_logs_partition_done_on_success(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    caplog.set_level(logging.INFO)

    partition_file(filename=str(f), kind="text", backend=DummyOKBackend())

    msgs = _messages(caplog)
    assert "partition_start" in msgs
    assert "partition_done" in msgs


def test_logs_partition_failed_on_unknown_exception(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    caplog.set_level(logging.INFO)

    with pytest.raises(ZephyrError) as ei:
        partition_file(filename=str(f), kind="text", backend=DummyExplodeBackend())

    assert getattr(ei.value, "code") == ErrorCode.UNS_PARTITION_FAILED

    msgs = _messages(caplog)
    assert "partition_start" in msgs
    assert "partition_failed" in msgs


def test_logs_partition_error_on_zephyrerror(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")

    caplog.set_level(logging.INFO)

    with pytest.raises(ZephyrError) as ei:
        partition_file(filename=str(f), kind="text", backend=DummyZephyrErrorBackend())

    assert getattr(ei.value, "code") == ErrorCode.UNS_UNSUPPORTED_TYPE

    msgs = _messages(caplog)
    assert "partition_start" in msgs
    assert "partition_error" in msgs
