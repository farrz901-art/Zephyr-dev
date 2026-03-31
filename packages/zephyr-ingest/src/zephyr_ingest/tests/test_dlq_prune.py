from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from zephyr_ingest import cli
from zephyr_ingest.dlq_prune import prune_delivery_dlq


def _touch_with_mtime(path: Path, *, mtime_epoch_s: int) -> None:
    path.write_text("{}", encoding="utf-8")
    os.utime(path, (mtime_epoch_s, mtime_epoch_s))


def test_prune_dry_run_only_done_by_default(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    pending = out_root / "_dlq" / "delivery"
    done = out_root / "_dlq" / "delivery_done"
    pending.mkdir(parents=True, exist_ok=True)
    done.mkdir(parents=True, exist_ok=True)

    now = 2_000_000_000
    old = now - (10 * 86400)
    new = now - (1 * 86400)

    _touch_with_mtime(pending / "p_old.json", mtime_epoch_s=old)
    _touch_with_mtime(done / "d_old.json", mtime_epoch_s=old)
    _touch_with_mtime(done / "d_new.json", mtime_epoch_s=new)

    stats = prune_delivery_dlq(
        out_root=out_root,
        older_than_days=7,
        include_pending=False,
        include_done=True,
        apply=False,
        move_to=Path("_dlq/delivery_pruned"),
        now_epoch_s=now,
    )

    assert stats.scanned == 2  # only done dir
    assert stats.selected == 1
    assert stats.moved == 0
    assert (done / "d_old.json").exists()
    assert (pending / "p_old.json").exists()  # pending untouched


def test_prune_apply_includes_pending_when_requested(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    pending = out_root / "_dlq" / "delivery"
    done = out_root / "_dlq" / "delivery_done"
    pending.mkdir(parents=True, exist_ok=True)
    done.mkdir(parents=True, exist_ok=True)

    now = 2_000_000_000
    old = now - (10 * 86400)

    _touch_with_mtime(pending / "p_old.json", mtime_epoch_s=old)
    _touch_with_mtime(done / "d_old.json", mtime_epoch_s=old)

    stats = prune_delivery_dlq(
        out_root=out_root,
        older_than_days=7,
        include_pending=True,
        include_done=True,
        apply=True,
        move_to=Path("_dlq/delivery_pruned"),
        now_epoch_s=now,
    )

    pruned_pending = out_root / "_dlq" / "delivery_pruned" / "delivery" / "p_old.json"
    pruned_done = out_root / "_dlq" / "delivery_pruned" / "delivery_done" / "d_old.json"

    # assert stats.errors == 0, (
    #     f"Prune failed. Selected: {stats.selected}, "
    #     f"Moved: {stats.moved}. Check for exceptions in prune_delivery_dlq."
    # )
    assert stats.selected == 2
    assert stats.moved == 2

    assert pruned_pending.exists()
    assert pruned_done.exists()
    assert not (pending / "p_old.json").exists()
    assert not (done / "d_old.json").exists()


def test_cli_dlq_prune_outputs_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out_root = tmp_path / "out"
    done = out_root / "_dlq" / "delivery_done"
    done.mkdir(parents=True, exist_ok=True)

    now = int(time.time())
    old = now - (10 * 86400)
    _touch_with_mtime(done / "d_old.json", mtime_epoch_s=old)

    rc = cli.main(
        [
            "dlq",
            "prune",
            "--out",
            str(out_root),
            "--older-than-days",
            "7",
        ]
    )
    assert rc == 0
    obj = json.loads(capsys.readouterr().out)
    assert obj["older_than_days"] == 7
    assert obj["apply"] is False
    assert obj["include_done"] is True
