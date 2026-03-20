from __future__ import annotations

from pathlib import Path
from typing import Any

from uns_stream._internal.utils import sha256_file
from zephyr_core import DocumentRef, RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_ingest.runner import RunnerConfig, run_documents


def test_runner_skip_existing_short_circuits(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    f = inbox / "a.txt"
    f.write_text("hello", encoding="utf-8")

    # Pre-create run_meta.json at the expected location so runner should skip it
    out_root = tmp_path / "out"
    sha = sha256_file(f)
    pre_dir = out_root / sha
    pre_dir.mkdir(parents=True, exist_ok=True)
    (pre_dir / "run_meta.json").write_text("{}", encoding="utf-8")

    docs = [
        DocumentRef(
            uri=str(f),
            source="local_file",
            discovered_at_utc="2026-03-20T00:00:00Z",
            filename="a.txt",
            extension=".txt",
            size_bytes=f.stat().st_size,
        )
    ]

    def exploding_partition_fn(**kwargs: Any):
        raise AssertionError("partition_fn should NOT be called when skip_existing is enabled")

    ctx = RunContext.new(
        pipeline_version="p1", timestamp_utc="2026-03-20T00:00:00Z", run_id="r-skip"
    )
    cfg = RunnerConfig(
        out_root=out_root,
        strategy=PartitionStrategy.AUTO,
        skip_existing=True,
        force=False,
    )

    stats = run_documents(docs=docs, cfg=cfg, ctx=ctx, partition_fn=exploding_partition_fn)

    assert stats.total == 1
    assert stats.skipped_existing == 1
    assert stats.success == 0
    assert stats.failed == 0
    assert (out_root / "batch_report.json").exists()
