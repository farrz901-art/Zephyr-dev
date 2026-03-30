from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from zephyr_ingest.replay_delivery import replay_delivery_dlq


def test_replay_emits_start_attempt_result_done(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    out_root = tmp_path / "out"
    dlq_dir = out_root / "_dlq" / "delivery"
    dlq_dir.mkdir(parents=True, exist_ok=True)

    # valid record
    (dlq_dir / "ok.json").write_text(
        json.dumps({"sha256": "abc", "run_id": "r1", "run_meta": {"schema_version": 3}}),
        encoding="utf-8",
    )
    # invalid record
    (dlq_dir / "bad.json").write_text(json.dumps({"sha256": 123}), encoding="utf-8")

    stats = replay_delivery_dlq(
        out_root=out_root,
        webhook_url="http://example.test/hook",
        dry_run=True,
        move_done=False,
    )

    assert stats.total == 2
    assert stats.attempted == 2
    assert stats.succeeded == 0  # dry_run does not count as succeeded
    assert stats.failed == 1  # invalid record

    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("replay_start ") for m in msgs)
    assert any(m.startswith("replay_attempt ") for m in msgs)
    assert any(m.startswith("replay_result ") for m in msgs)
    assert any(m.startswith("replay_done ") for m in msgs)
