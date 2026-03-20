from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol, cast

from uns_stream._internal.artifacts import dump_partition_artifacts
from uns_stream._internal.retry_policy import is_retryable_exception
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
from zephyr_core.contracts.v1.enums import RunOutcome
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
class RetryConfig:
    enabled: bool = True
    max_attempts: int = 3
    base_backoff_ms: int = 200
    max_backoff_ms: int = 5_000


def _default_retry() -> RetryConfig:
    return RetryConfig()


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    out_root: Path
    strategy: PartitionStrategy = PartitionStrategy.AUTO
    unique_element_ids: bool = True
    skip_unsupported: bool = True
    skip_existing: bool = True
    force: bool = False
    retry: RetryConfig = field(default_factory=_default_retry)


@dataclass(frozen=True, slots=True)
class RunStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped_unsupported: int = 0
    skipped_existing: int = 0


def _p95_int(values: list[int]) -> int | None:
    if not values:
        return None
    vals = sorted(values)
    # ceil(0.95*n)-1, pure integer math
    idx = (len(vals) * 95 + 99) // 100 - 1
    if idx < 0:
        idx = 0
    return vals[idx]


def _duration_stats(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"min": None, "max": None, "avg": None, "p95": None}
    return {
        "min": min(values),
        "max": max(values),
        "avg": sum(values) // len(values),
        "p95": _p95_int(values),
    }


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

    total, success, failed, skipped_unsupported, skipped_existing = 0, 0, 0, 0, 0

    counts_by_extension: dict[str, int] = {}
    counts_by_error_code: dict[str, int] = {}

    durations_ms_list: list[int] = []

    retry_attempts_total: int = 0
    retried_success: int = 0
    retryable_failed: int = 0

    for d in docs:
        total += 1

        ext = d.extension or Path(d.uri).suffix.lower()
        counts_by_extension[ext] = counts_by_extension.get(ext, 0) + 1

        p = Path(d.uri)
        sha = sha256_file(p)

        out_dir = out_root / sha
        meta_path = out_dir / "run_meta.json"
        if cfg.skip_existing and meta_path.exists() and not cfg.force:
            skipped_existing += 1
            continue

        t0 = time.perf_counter()

        attempts = 0

        while True:
            attempts += 1

            try:
                res = partition_fn(
                    filename=str(p),
                    strategy=cfg.strategy,
                    unique_element_ids=cfg.unique_element_ids,
                )
                duration_ms = int((time.perf_counter() - t0) * 1000)

                durations_ms_list.append(duration_ms)

                if attempts > 1:
                    retry_attempts_total += attempts - 1
                    retried_success += 1

                meta = RunMetaV1(
                    run_id=ctx.run_id,
                    pipeline_version=ctx.pipeline_version,
                    timestamp_utc=ctx.timestamp_utc,
                    schema_version=ctx.run_meta_schema_version,
                    outcome=RunOutcome.SUCCESS,
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
                        attempts=attempts,
                    ),
                    warnings=list(res.warnings),
                )

                artifacts_writer(out_root=out_root, sha256=sha, meta=meta, result=res)
                success += 1
                break

            except ZephyrError as e:
                duration_ms = int((time.perf_counter() - t0) * 1000)

                # 是否值得重试：统一用 retry_policy
                retryable = is_retryable_exception(e)

                if cfg.retry.enabled and retryable and attempts < cfg.retry.max_attempts:
                    backoff_ms = min(
                        cfg.retry.max_backoff_ms,
                        cfg.retry.base_backoff_ms * (2 ** (attempts - 1)),
                    )
                    if backoff_ms > 0:
                        time.sleep(backoff_ms / 1000.0)
                    continue

                # 使用 getattr 确保在所有环境下属性访问都安全
                e_code = str(getattr(e, "code", ErrorCode.UNS_PARTITION_FAILED))
                e_msg = str(getattr(e, "message", "Unknown error"))

                is_unsupported = e_code == str(ErrorCode.UNS_UNSUPPORTED_TYPE)

                # 决定 Outcome (B2)
                if cfg.skip_unsupported and is_unsupported:
                    current_outcome = RunOutcome.SKIPPED_UNSUPPORTED
                    skipped_unsupported += 1
                else:
                    current_outcome = RunOutcome.FAILED
                    failed += 1

                durations_ms_list.append(duration_ms)

                counts_by_error_code[e_code] = counts_by_error_code.get(e_code, 0) + 1

                if attempts > 1:
                    retry_attempts_total += attempts - 1

                # 只有最终失败时才统计 retryable_failed（unsupported 也可以统计，看你偏好）
                if retryable:
                    retryable_failed += 1

                e_details = cast("dict[str, Any] | None", getattr(e, "details", None))
                merged_details = dict(e_details) if e_details else {}
                if "retryable" not in merged_details:
                    merged_details["retryable"] = retryable

                # if cfg.skip_unsupported and e_code == str(ErrorCode.UNS_UNSUPPORTED_TYPE):
                #     skipped_unsupported += 1
                # else:
                #     failed += 1

                meta = RunMetaV1(
                    run_id=ctx.run_id,
                    pipeline_version=ctx.pipeline_version,
                    timestamp_utc=ctx.timestamp_utc,
                    schema_version=ctx.run_meta_schema_version,
                    metrics=MetricsV1(duration_ms=duration_ms, attempts=attempts),
                    outcome=current_outcome,
                    error=ErrorInfoV1(code=e_code, message=e_msg, details=merged_details),
                )
                artifacts_writer(out_root=out_root, sha256=sha, meta=meta, result=None)
                break

    stats = RunStats(
        total=total,
        success=success,
        failed=failed,
        skipped_unsupported=skipped_unsupported,
        skipped_existing=skipped_existing,
    )

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
            "skipped_existing": stats.skipped_existing,
        },
        "counts_by_extension": counts_by_extension,
        "counts_by_error_code": counts_by_error_code,
        "retry": {
            "enabled": cfg.retry.enabled,
            "max_attempts": cfg.retry.max_attempts,
            "base_backoff_ms": cfg.retry.base_backoff_ms,
            "max_backoff_ms": cfg.retry.max_backoff_ms,
            "retry_attempts_total": retry_attempts_total,
            "retried_success": retried_success,
            "retryable_failed": retryable_failed,
        },
        "durations_ms": _duration_stats(durations_ms_list),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    (out_root / "batch_report.json").write_text(
        json.dumps(batch_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return stats
