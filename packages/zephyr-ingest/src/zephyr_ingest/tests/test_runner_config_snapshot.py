from __future__ import annotations

import json
from pathlib import Path

from zephyr_core import RunContext
from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1
from zephyr_ingest.runner import RunnerConfig, run_documents


def test_runner_writes_config_snapshot(tmp_path: Path) -> None:
    ctx = RunContext.new()
    cfg = RunnerConfig(out_root=tmp_path / "out")

    snap: ConfigSnapshotV1 = {
        "schema_version": 1,
        "input": {"paths_count": 0, "glob": "**/*", "source": "local_file"},
        "runner": {
            "out": str(cfg.out_root),
            "strategy": "auto",
            "skip_existing": True,
            "skip_unsupported": True,
            "force": False,
            "unique_element_ids": True,
            "workers": 1,
            "stale_lock_ttl_s": None,
        },
        "retry": {
            "enabled": True,
            "max_attempts": 3,
            "base_backoff_ms": 200,
            "max_backoff_ms": 5000,
        },
        "backend": {"kind": "local"},
        "destinations": {"filesystem": {"enabled": True}},
    }

    run_documents(docs=[], cfg=cfg, ctx=ctx, config_snapshot=snap)

    report_path = (cfg.out_root / "batch_report.json").resolve()
    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["config_snapshot"] == snap
