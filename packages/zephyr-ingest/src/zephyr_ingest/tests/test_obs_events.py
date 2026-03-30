from __future__ import annotations

import logging
from pathlib import Path

import pytest

from zephyr_ingest.obs.events import log_event


def test_log_event_sorts_keys_and_formats_line(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("zephyr_ingest.tests.obs")

    log_event(
        logger,
        level=logging.INFO,
        event="run_start",
        run_id="r1",
        pipeline_version="p1",
        out_root=Path("/tmp/out"),
    )

    assert len(caplog.records) == 1
    msg = caplog.records[0].getMessage()

    # event must be prefix
    assert msg.startswith("run_start ")
    # keys are sorted: out_root, pipeline_version, run_id
    expected_path = str(Path("/tmp/out"))
    assert f"out_root={expected_path}" in msg
    assert "pipeline_version=p1" in msg
    assert "run_id=r1" in msg
