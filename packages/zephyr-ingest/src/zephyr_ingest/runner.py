from __future__ import annotations

import contextlib
import json
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol, cast

# from uns_stream._internal.artifacts import dump_partition_artifacts
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
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1
from zephyr_ingest.destinations.base import DeliveryReceipt, Destination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.obs.batch_report_v1 import (
    BATCH_REPORT_SCHEMA_VERSION,
    BatchReportV1,
    DeliveryCountersV1,
    DurationStatsV1,
)
from zephyr_ingest.obs.batch_report_v1 import (
    MetricsV1 as BatchMetricsV1,
)
from zephyr_ingest.obs.events import log_event

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


# class ArtifactsWriter(Protocol):
#     def __call__(
#         self,
#         *,
#         out_root: Path,
#         sha256: str,
#         meta: RunMetaV1,
#         result: PartitionResult | None = None,
#     ) -> Path: ...


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
    backend: object | None = None
    skip_unsupported: bool = True
    skip_existing: bool = True
    force: bool = False
    retry: RetryConfig = field(default_factory=_default_retry)
    workers: int = 1  # 默认单线程，保持行为稳定
    destination: Destination | None = None
    # If set, treat a lock file older than TTL seconds as stale and break it.
    stale_lock_ttl_s: int | None = None


@dataclass(frozen=True, slots=True)
class DocProcessResult:
    sha256: str
    extension: str
    outcome: RunOutcome
    duration_ms: int | None
    attempts: int
    retryable: bool | None
    error_code: str | None
    skipped_existing: bool
    delivery_receipt: DeliveryReceipt | None
    dlq_written: bool


@dataclass(frozen=True, slots=True)
class RunStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped_unsupported: int = 0
    skipped_existing: int = 0


def _write_delivery_receipt(out_dir: Path, receipt: DeliveryReceipt) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "delivery_receipt.json").write_text(
        json.dumps(asdict(receipt), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _bump_delivery_counter(
    counters: dict[str, DeliveryCountersV1],
    *,
    destination: str,
    ok: bool,
) -> None:
    bucket = counters.get(destination)
    if bucket is None:
        new_bucket: DeliveryCountersV1 = {"total": 0, "ok": 0, "failed": 0}
        counters[destination] = new_bucket
        bucket = new_bucket
    bucket["total"] += 1
    if ok:
        bucket["ok"] += 1
    else:
        bucket["failed"] += 1


def _p95_int(values: list[int]) -> int | None:
    if not values:
        return None
    vals = sorted(values)
    # ceil(0.95*n)-1, pure integer math
    idx = (len(vals) * 95 + 99) // 100 - 1
    if idx < 0:
        idx = 0
    return vals[idx]


def _duration_stats(values: list[int]) -> DurationStatsV1:
    if not values:
        return {"min": None, "max": None, "avg": None, "p95": None}
    return {
        "min": min(values),
        "max": max(values),
        "avg": sum(values) // len(values),
        "p95": _p95_int(values),
    }


def _process_one(
    *,
    doc: DocumentRef,
    cfg: RunnerConfig,
    ctx: RunContext,
    out_root: Path,
    partition_fn: PartitionFn,
    # artifacts_writer: ArtifactsWriter,
    destination: Destination,
) -> DocProcessResult:
    p = Path(doc.uri)
    sha = sha256_file(p)

    log_event(
        logger,
        level=logging.INFO,
        event="doc_start",
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        sha256=sha,
        uri=doc.uri,
        extension=doc.extension,
    )

    ext = doc.extension or p.suffix.lower()

    out_dir = out_root / sha
    meta_path = out_dir / "run_meta.json"

    if cfg.skip_existing and meta_path.exists() and not cfg.force:
        return DocProcessResult(
            sha256=sha,
            extension=ext,
            outcome=RunOutcome.SKIPPED_EXISTING,
            duration_ms=None,
            attempts=0,
            retryable=None,
            error_code=None,
            skipped_existing=True,
            delivery_receipt=None,
            dlq_written=False,
        )

    # 并发竞争锁 (Write-only)
    out_dir.mkdir(parents=True, exist_ok=True)
    lock_path = out_dir / ".zephyr.lock"
    lock_acquired = False

    try:
        try:
            with lock_path.open("x", encoding="utf-8") as f:
                f.write(ctx.run_id)
            lock_acquired = True
        except FileExistsError:
            ttl = cfg.stale_lock_ttl_s
            if ttl is not None:
                try:
                    # 检查文件修改时间
                    stat = lock_path.stat()
                    age_s = time.time() - stat.st_mtime

                    if age_s >= ttl:
                        # 尝试读取原持有者 ID 用于日志审计
                        owner = None
                        try:
                            with contextlib.suppress(Exception):
                                owner = lock_path.read_text(encoding="utf-8").strip() or None
                        except Exception:
                            pass

                        logger.warning(
                            "stale_lock_detected_and_removing "
                            "sha=%s age_s=%s owner_run_id=%s path=%s",
                            sha,
                            int(age_s),
                            owner,
                            str(lock_path),
                        )

                        # 强制删除并尝试重新获取
                        lock_path.unlink(missing_ok=True)
                        try:
                            with lock_path.open("x", encoding="utf-8") as f:
                                f.write(ctx.run_id)
                            lock_acquired = True
                        except FileExistsError:
                            # 如果此时又被别的线程抢走了，老老实实跳过
                            pass
                except FileNotFoundError:
                    # 竞态：锁在 stat() 或 unlink() 期间被原持有者正常释放了
                    try:
                        with lock_path.open("x", encoding="utf-8") as f:
                            f.write(ctx.run_id)
                        lock_acquired = True
                    except FileExistsError:
                        pass
                except Exception as e:
                    logger.error("failed_to_handle_stale_lock sha=%s exc=%s", sha, str(e))

            # 另一个线程正在处理相同内容
            if not lock_acquired:
                return DocProcessResult(
                    sha256=sha,
                    extension=ext,
                    outcome=RunOutcome.SKIPPED_EXISTING,
                    duration_ms=None,
                    attempts=0,
                    retryable=None,
                    error_code=None,
                    skipped_existing=True,
                    delivery_receipt=None,
                    dlq_written=False,
                )
        t0 = time.perf_counter()
        attempts = 0

        while True:
            attempts += 1
            try:
                res = partition_fn(
                    filename=str(p),
                    strategy=cfg.strategy,
                    unique_element_ids=cfg.unique_element_ids,
                    backend=cfg.backend,
                )
                duration_ms = int((time.perf_counter() - t0) * 1000)
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
                    error=None,
                )

                log_event(
                    logger,
                    level=logging.INFO,
                    event="delivery_start",
                    run_id=ctx.run_id,
                    pipeline_version=ctx.pipeline_version,
                    sha256=sha,
                    destination=getattr(destination, "name", "unknown"),
                    outcome=None if meta.outcome is None else str(meta.outcome),
                )

                try:
                    receipt = destination(out_root=out_root, sha256=sha, meta=meta, result=res)
                except Exception as de:
                    receipt = DeliveryReceipt(
                        destination=destination.name,
                        ok=False,
                        details={"exc_type": type(de).__name__, "exc": str(de)},
                    )

                log_event(
                    logger,
                    level=logging.INFO,
                    event="delivery_done",
                    run_id=ctx.run_id,
                    pipeline_version=ctx.pipeline_version,
                    sha256=sha,
                    destination=receipt.destination,
                    ok=receipt.ok,
                )

                _write_delivery_receipt(out_dir, receipt)

                dlq_written = False
                if not receipt.ok:
                    retryable = None
                    if isinstance(receipt.details, dict):
                        r = receipt.details.get("retryable")
                        if isinstance(r, bool):
                            retryable = r

                    log_event(
                        logger,
                        level=logging.WARNING,
                        event="delivery_failed",
                        run_id=ctx.run_id,
                        pipeline_version=ctx.pipeline_version,
                        sha256=sha,
                        destination=receipt.destination,
                        retryable=retryable,
                    )

                    try:
                        dlq_path = write_delivery_dlq(
                            out_root=out_root, sha256=sha, meta=meta, receipt=receipt
                        )
                        dlq_written = True

                        log_event(
                            logger,
                            level=logging.WARNING,
                            event="dlq_written",
                            run_id=ctx.run_id,
                            pipeline_version=ctx.pipeline_version,
                            sha256=sha,
                            destination=receipt.destination,
                            dlq_path=dlq_path,
                        )

                    except Exception as dlqe:
                        logger.warning("delivery_dlq_write_failed sha=%s exc=%s", sha, str(dlqe))

                log_event(
                    logger,
                    level=logging.INFO,
                    event="doc_done",
                    run_id=ctx.run_id,
                    pipeline_version=ctx.pipeline_version,
                    sha256=sha,
                    outcome=None if meta.outcome is None else str(meta.outcome),
                )

                # artifacts_writer(out_root=out_root, sha256=sha, meta=meta, result=res)
                # destination(out_root=out_root, sha256=sha, meta=meta, result=res)
                return DocProcessResult(
                    sha256=sha,
                    extension=ext,
                    outcome=RunOutcome.SUCCESS,
                    duration_ms=duration_ms,
                    attempts=attempts,
                    retryable=None,
                    error_code=None,
                    skipped_existing=False,
                    delivery_receipt=receipt,
                    dlq_written=dlq_written,
                )
            except ZephyrError as e:
                duration_ms = int((time.perf_counter() - t0) * 1000)
                retryable = is_retryable_exception(e)
                if cfg.retry.enabled and retryable and attempts < cfg.retry.max_attempts:
                    backoff_ms = min(
                        cfg.retry.max_backoff_ms,
                        cfg.retry.base_backoff_ms * (2 ** (attempts - 1)),
                    )
                    if backoff_ms > 0:
                        time.sleep(backoff_ms / 1000.0)
                    continue
                e_code = str(e.code)
                merged_details: dict[str, Any] = {}
                if isinstance(e.details, dict):
                    merged_details = dict(e.details)
                if "retryable" not in merged_details:
                    merged_details["retryable"] = retryable
                if cfg.skip_unsupported and e_code == str(ErrorCode.UNS_UNSUPPORTED_TYPE):
                    outcome = RunOutcome.SKIPPED_UNSUPPORTED
                else:
                    outcome = RunOutcome.FAILED

                log_event(
                    logger,
                    level=logging.WARNING,
                    event="delivery_start",
                    run_id=ctx.run_id,
                    sha256=sha,
                    destination=getattr(destination, "name", "unknown"),
                    outcome=str(outcome),  # 告知系统这是在处理失败件的交付
                )

                meta = RunMetaV1(
                    run_id=ctx.run_id,
                    pipeline_version=ctx.pipeline_version,
                    timestamp_utc=ctx.timestamp_utc,
                    schema_version=ctx.run_meta_schema_version,
                    outcome=outcome,
                    metrics=MetricsV1(duration_ms=duration_ms, attempts=attempts),
                    error=ErrorInfoV1(code=e_code, message=e.message, details=merged_details),
                )

                try:
                    receipt = destination(out_root=out_root, sha256=sha, meta=meta, result=None)
                except Exception as de:
                    receipt = DeliveryReceipt(
                        destination=destination.name,
                        ok=False,
                        details={"exc_type": type(de).__name__, "exc": str(de)},
                    )

                log_event(
                    logger,
                    level=logging.WARNING,
                    event="delivery_done",
                    run_id=ctx.run_id,
                    sha256=sha,
                    destination=receipt.destination,
                    ok=receipt.ok,
                )

                _write_delivery_receipt(out_dir, receipt)

                dlq_written = False
                if not receipt.ok:
                    try:
                        write_delivery_dlq(
                            out_root=out_root, sha256=sha, meta=meta, receipt=receipt
                        )
                        dlq_written = True
                    except Exception as dlqe:
                        logger.warning("delivery_dlq_write_failed sha=%s exc=%s", sha, str(dlqe))

                log_event(
                    logger,
                    level=logging.INFO,
                    event="doc_done",
                    run_id=ctx.run_id,
                    sha256=sha,
                    outcome=str(outcome),
                )

                # artifacts_writer(out_root=out_root, sha256=sha, meta=meta, result=None)
                # destination(out_root=out_root, sha256=sha, meta=meta, result=None)
                return DocProcessResult(
                    sha256=sha,
                    extension=ext,
                    outcome=outcome,
                    duration_ms=duration_ms,
                    attempts=attempts,
                    retryable=retryable,
                    error_code=e_code,
                    skipped_existing=False,
                    delivery_receipt=receipt,
                    dlq_written=dlq_written,
                )
    finally:
        if lock_acquired:
            lock_path.unlink(missing_ok=True)


def run_documents(
    *,
    docs: Iterable[DocumentRef],
    cfg: RunnerConfig,
    ctx: RunContext,
    partition_fn: PartitionFn = auto_partition,
    # artifacts_writer: ArtifactsWriter = dump_partition_artifacts,
    destination: Destination | None = None,
    config_snapshot: ConfigSnapshotV1 | None = None,
) -> RunStats:
    destination = destination or cfg.destination or FilesystemDestination()

    out_root = cfg.out_root.expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    wall_start = time.perf_counter()

    log_event(
        logger,
        level=logging.INFO,
        event="run_start",
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        out_root=out_root,
        workers=cfg.workers,
    )

    total, success, failed, skipped_unsupported, skipped_existing = 0, 0, 0, 0, 0

    delivery_total, delivery_ok, delivery_failed = 0, 0, 0
    delivery_by_destination: dict[str, DeliveryCountersV1] = {}
    fanout_children_by_destination: dict[str, DeliveryCountersV1] = {}

    counts_by_extension: dict[str, int] = {}
    counts_by_error_code: dict[str, int] = {}

    durations_ms_list: list[int] = []

    retry_attempts_total: int = 0
    retried_success: int = 0
    retryable_failed: int = 0

    delivery_dlq_written_total = 0

    def consume_result(r: DocProcessResult) -> None:
        nonlocal success, failed, skipped_unsupported, skipped_existing
        nonlocal retry_attempts_total, retried_success, retryable_failed
        nonlocal delivery_total, delivery_ok, delivery_failed
        nonlocal delivery_dlq_written_total

        if r.outcome == RunOutcome.SUCCESS:
            success += 1
            if r.duration_ms is not None:
                durations_ms_list.append(r.duration_ms)
            if r.attempts > 1:
                retry_attempts_total += r.attempts - 1
                retried_success += 1
        elif r.outcome == RunOutcome.SKIPPED_EXISTING:
            skipped_existing += 1
        elif r.outcome == RunOutcome.SKIPPED_UNSUPPORTED:
            skipped_unsupported += 1
            if r.duration_ms is not None:
                durations_ms_list.append(r.duration_ms)
            if r.error_code:
                counts_by_error_code[r.error_code] = counts_by_error_code.get(r.error_code, 0) + 1
            if r.attempts > 1:
                retry_attempts_total += r.attempts - 1
        else:
            failed += 1
            if r.duration_ms is not None:
                durations_ms_list.append(r.duration_ms)
            if r.error_code:
                counts_by_error_code[r.error_code] = counts_by_error_code.get(r.error_code, 0) + 1
            if r.attempts > 1:
                retry_attempts_total += r.attempts - 1
            if r.retryable:
                retryable_failed += 1

        if r.dlq_written:
            delivery_dlq_written_total += 1

        if r.delivery_receipt is None:
            return

        delivery_total += 1
        if r.delivery_receipt.ok:
            delivery_ok += 1
        else:
            delivery_failed += 1

        _bump_delivery_counter(
            delivery_by_destination,
            destination=r.delivery_receipt.destination,
            ok=r.delivery_receipt.ok,
        )

        details_obj: object = r.delivery_receipt.details

        # 深入统计 fanout 内部情况
        if r.delivery_receipt.destination == "fanout" and isinstance(details_obj, dict):
            # 2. 只有确认是字典后，再尝试获取 receipts 列表
            receipts_obj: object = details_obj.get("receipts")

            if isinstance(receipts_obj, list):
                typed_receipts = cast("list[object]", receipts_obj)

                # 3. 迭代列表中的每一个项
                for item_obj in typed_receipts:
                    # 4. 必须确认每一项也是字典
                    if not isinstance(item_obj, dict):
                        continue

                    # 5. 此时使用 cast 安全转换，Pyright 不再报错
                    item = cast("dict[str, Any]", item_obj)
                    d_name = item.get("destination")
                    d_ok = item.get("ok")

                    # 6. 最终验证字段类型，确保调用 _bump_delivery_counter 时参数合法
                    if isinstance(d_name, str) and isinstance(d_ok, bool):
                        _bump_delivery_counter(
                            fanout_children_by_destination,
                            destination=d_name,
                            ok=d_ok,
                        )

    # 执行分支
    if cfg.workers <= 1:
        for d in docs:
            total += 1
            ext = d.extension or Path(d.uri).suffix.lower()
            counts_by_extension[ext] = counts_by_extension.get(ext, 0) + 1
            res = _process_one(
                doc=d,
                cfg=cfg,
                ctx=ctx,
                out_root=out_root,
                partition_fn=partition_fn,
                destination=destination,
            )
            consume_result(res)
    else:
        futures: list[Future[DocProcessResult]] = []
        with ThreadPoolExecutor(max_workers=cfg.workers) as executor:
            for d in docs:
                total += 1
                ext = d.extension or Path(d.uri).suffix.lower()
                counts_by_extension[ext] = counts_by_extension.get(ext, 0) + 1
                futures.append(
                    executor.submit(
                        _process_one,
                        doc=d,
                        cfg=cfg,
                        ctx=ctx,
                        out_root=out_root,
                        partition_fn=partition_fn,
                        destination=destination,
                    )
                )

            for fut in as_completed(futures):
                consume_result(fut.result())

    stats = RunStats(
        total=total,
        success=success,
        failed=failed,
        skipped_unsupported=skipped_unsupported,
        skipped_existing=skipped_existing,
    )

    batch_report: BatchReportV1 = {
        "schema_version": BATCH_REPORT_SCHEMA_VERSION,
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
        "delivery": {
            "total": delivery_total,
            "ok": delivery_ok,
            "failed": delivery_failed,
            "dlq_written_total": delivery_dlq_written_total,
            "dlq_dir": str((out_root / "_dlq" / "delivery").resolve()),
            "by_destination": delivery_by_destination,
            "fanout_children_by_destination": fanout_children_by_destination,
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
        "workers": cfg.workers,
        "executor": "serial" if cfg.workers <= 1 else "thread",
    }

    wall_ms = int((time.perf_counter() - wall_start) * 1000)
    docs_per_min: float | None = None
    if wall_ms > 0:
        docs_per_min = (float(total) / float(wall_ms)) * 60000.0

    metrics: BatchMetricsV1 = {
        "run_wall_ms": wall_ms,
        "docs_per_min": docs_per_min,
        "docs_total": total,
        "docs_success_total": success,
        "docs_failed_total": failed,
        "docs_skipped_total": skipped_existing + skipped_unsupported,
        "delivery_total": delivery_total,
        "delivery_ok_total": delivery_ok,
        "delivery_failed_total": delivery_failed,
        "dlq_written_total": delivery_dlq_written_total,
    }
    batch_report["metrics"] = metrics

    if config_snapshot is not None:
        batch_report["config_snapshot"] = config_snapshot

    (out_root / "batch_report.json").write_text(
        json.dumps(batch_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log_event(
        logger,
        level=logging.INFO,
        event="run_done",
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        out_root=out_root,
        docs_total=total,
        docs_ok=success,
        docs_failed=failed,
        docs_skipped=skipped_unsupported + skipped_existing,
        delivery_ok=delivery_ok,
        delivery_failed=delivery_failed,
        dlq_written_total=delivery_dlq_written_total,
    )

    return stats
