from __future__ import annotations

import logging
from pathlib import Path

import pytest

from uns_stream.service import partition_file
from zephyr_core import PartitionStrategy, ZephyrElement


class DummyBackend:
    name = "unstructured"
    backend = "dummy"
    version = "0"

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: object,
    ) -> list[ZephyrElement]:
        return []


def test_partition_logs_include_run_fields(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    p = tmp_path / "a.txt"
    p.write_text("x", encoding="utf-8")

    partition_file(
        filename=str(p),
        kind="text",
        strategy=PartitionStrategy.AUTO,
        backend=DummyBackend(),
        run_id="r1",
        pipeline_version="p1",
    )

    msgs = [r.getMessage() for r in caplog.records]
    assert any(
        m.startswith("partition_start ")
        and "run_id=r1" in m
        and "pipeline_version=p1" in m
        and "sha256=" in m
        for m in msgs
    )
    assert any(
        m.startswith("partition_done ") and "run_id=r1" in m and "pipeline_version=p1" in m
        for m in msgs
    )
