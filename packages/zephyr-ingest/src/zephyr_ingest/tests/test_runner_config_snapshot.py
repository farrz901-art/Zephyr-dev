from __future__ import annotations

import json
from pathlib import Path

from zephyr_core import RunContext
from zephyr_ingest.runner import RunnerConfig, run_documents


def test_runner_writes_config_snapshot(tmp_path: Path) -> None:
    ctx = RunContext.new()
    cfg = RunnerConfig(out_root=tmp_path / "out")

    snap: dict[str, object] = {
        "runner": {"workers": 1},
        "backend": {"kind": "local"},
        "destinations": {"filesystem": {"enabled": True}},
    }

    run_documents(docs=[], cfg=cfg, ctx=ctx, config_snapshot=snap)

    report_path = (cfg.out_root / "batch_report.json").resolve()
    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["config_snapshot"] == snap
