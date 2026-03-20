from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol, cast

from uns_stream._internal.artifacts import dump_partition_artifacts
from uns_stream._internal.utils import sha256_file
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import (
    DocumentRef,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    RunMetaV1,
    ZephyrError,
)
from zephyr_core.contracts.v1.run_meta import EngineMetaV1, ErrorInfoV1, MetricsV1

logger = logging.getLogger(__name__)


# 1. 严格匹配 auto_partition 的签名
class PartitionFn(Protocol):
    def __call__(
        self,
        *,
        filename: str,
        strategy: PartitionStrategy = PartitionStrategy.AUTO,
        unique_element_ids: bool = True,
        backend: Any | None = None,
    ) -> PartitionResult: ...


class ArtifactsWriter(Protocol):
    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> Path: ...


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    out_root: Path
    strategy: PartitionStrategy = PartitionStrategy.AUTO
    unique_element_ids: bool = True
    skip_unsupported: bool = True


@dataclass(frozen=True, slots=True)
class RunStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped_unsupported: int = 0


def run_documents(
    *,
    docs: Iterable[DocumentRef],
    cfg: RunnerConfig,
    ctx: RunContext,
    partition_fn: PartitionFn = auto_partition,
    artifacts_writer: ArtifactsWriter = dump_partition_artifacts,
) -> RunStats:
    out_root = cfg.out_root.expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    total, success, failed, skipped = 0, 0, 0, 0

    for d in docs:
        total += 1
        p = Path(d.uri)
        sha = sha256_file(p)
        t0 = time.perf_counter()

        try:
            res = partition_fn(
                filename=str(p),
                strategy=cfg.strategy,
                unique_element_ids=cfg.unique_element_ids,
            )
            duration_ms = int((time.perf_counter() - t0) * 1000)

            meta = RunMetaV1(
                run_id=ctx.run_id,
                pipeline_version=ctx.pipeline_version,
                timestamp_utc=ctx.timestamp_utc,
                schema_version=ctx.run_meta_schema_version,
                document=res.document,
                engine=EngineMetaV1(
                    name=res.engine.name,
                    backend=res.engine.backend,
                    version=res.engine.version,
                    strategy=str(res.engine.strategy),
                ),
                metrics=MetricsV1(
                    duration_ms=duration_ms,
                    elements_count=len(res.elements),
                    normalized_text_len=len(res.normalized_text),
                ),
                warnings=list(res.warnings),
            )

            artifacts_writer(out_root=out_root, sha256=sha, meta=meta, result=res)
            success += 1

        except ZephyrError as e:
            duration_ms = int((time.perf_counter() - t0) * 1000)

            # 使用 getattr 确保在所有环境下属性访问都安全
            e_code = str(getattr(e, "code", ErrorCode.UNS_PARTITION_FAILED))
            e_msg = str(getattr(e, "message", "Unknown error"))
            e_details = cast("dict[str, Any] | None", getattr(e, "details", None))

            if cfg.skip_unsupported and e_code == "ZE-UNS-UNSUPPORTED-TYPE":
                skipped += 1
            else:
                failed += 1

            meta = RunMetaV1(
                run_id=ctx.run_id,
                pipeline_version=ctx.pipeline_version,
                timestamp_utc=ctx.timestamp_utc,
                schema_version=ctx.run_meta_schema_version,
                metrics=MetricsV1(duration_ms=duration_ms),
                error=ErrorInfoV1(code=e_code, message=e_msg, details=e_details),
            )
            artifacts_writer(out_root=out_root, sha256=sha, meta=meta, result=None)

    stats = RunStats(total=total, success=success, failed=failed, skipped_unsupported=skipped)

    batch_report = {
        "run_id": ctx.run_id,
        "pipeline_version": ctx.pipeline_version,
        "timestamp_utc": ctx.timestamp_utc,
        "strategy": str(cfg.strategy),
        "counts": {
            "total": stats.total,
            "success": stats.success,
            "failed": stats.failed,
            "skipped_unsupported": stats.skipped_unsupported,
        },
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    (out_root / "batch_report.json").write_text(
        json.dumps(batch_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return stats
