from __future__ import annotations

import contextlib
import json
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, cast

from zephyr_core import (
    DocumentRef,
    ErrorCode,
    PartitionStrategy,
    RunContext,
    RunMetaV1,
    ZephyrError,
)
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.run_meta import (
    EngineMetaV1,
    ErrorInfoV1,
    ExecutionModeV1,
    MetricsV1,
    RunProvenanceV1,
)
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest._internal.retry_policy import is_retryable_exception
from zephyr_ingest._internal.utils import sha256_file
from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1
from zephyr_ingest.destinations.base import DeliveryReceipt, Destination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.flow_processor import (
    DEFAULT_FLOW_KIND,
    CallableFlowProcessor,
    FlowKind,
    FlowProcessor,
    PartitionFn,
    build_processor_for_flow_kind,
    normalize_flow_input_identity_sha,
)
from zephyr_ingest.obs.batch_report_v1 import (
    BATCH_REPORT_SCHEMA_VERSION,
    BatchReportV1,
    DeliveryCountersV1,
    DurationStatsV1,
    RuntimeBoundaryV1,
    StageDurationsV1,
)
from zephyr_ingest.obs.batch_report_v1 import (
    MetricsV1 as BatchMetricsV1,
)
from zephyr_ingest.obs.events import log_event
from zephyr_ingest.source_contracts import normalize_source_contract_id
from zephyr_ingest.task_idempotency import normalize_task_idempotency_key
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskKind,
    TaskV1,
)
from zephyr_ingest.usage_record import write_batch_usage_record_v1, write_usage_record_v1

logger = logging.getLogger(__name__)


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
    delivery_retryable: bool | None
    dlq_written: bool
    # stage timings (ms); None means stage not executed (e.g. skipped)
    hash_ms: int | None
    partition_ms: int | None
    delivery_ms: int | None


@dataclass(frozen=True, slots=True)
class RunStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped_unsupported: int = 0
    skipped_existing: int = 0


def _resolve_flow_processor(
    *,
    flow_kind: FlowKind | str,
    processor: FlowProcessor | None,
    partition_fn: PartitionFn | None,
    backend: object | None,
) -> FlowProcessor:
    # `processor` is the primary orchestration boundary.
    if processor is not None:
        return processor

    # `partition_fn` remains supported only as a legacy compatibility adapter path.
    if partition_fn is not None:
        return CallableFlowProcessor(partition_fn=partition_fn, backend=backend)

    return build_processor_for_flow_kind(flow_kind=flow_kind, backend=backend)


def _write_delivery_receipt(out_dir: Path, receipt: DeliveryReceipt) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "delivery_receipt.json").write_text(
        json.dumps(receipt.to_dict(), ensure_ascii=False, indent=2, default=str),
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


def _bump_failure_kind_counter(
    counters: dict[str, dict[str, int]],
    *,
    destination: str,
    failure_kind: str,
) -> None:
    bucket = counters.setdefault(destination, {})
    bucket[failure_kind] = bucket.get(failure_kind, 0) + 1


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


def _new_processor_cache() -> dict[FlowKind, FlowProcessor]:
    return {}


def _build_task_run_provenance(
    *,
    task: TaskV1,
    execution_mode: ExecutionModeV1,
) -> RunProvenanceV1:
    task_identity_key = None
    if task.identity is not None:
        task_identity_key = normalize_task_idempotency_key(task)

    return RunProvenanceV1(
        run_origin="intake",
        delivery_origin="primary",
        execution_mode=execution_mode,
        task_id=task.task_id,
        task_identity_key=task_identity_key,
    )


def _build_task_for_document(
    *,
    doc: DocumentRef,
    flow_kind: FlowKind | str,
    cfg: RunnerConfig,
    ctx: RunContext,
    sha256: str,
) -> TaskV1:
    task_kind: TaskKind
    if flow_kind == "uns":
        task_kind = "uns"
    elif flow_kind == "it":
        task_kind = "it"
    else:
        raise ValueError(f"Unsupported task flow kind: {flow_kind!r}")

    return TaskV1(
        task_id=sha256,
        kind=task_kind,
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1.from_document_ref(
                doc,
                source_contract_id=normalize_source_contract_id(
                    task_kind=task_kind,
                    task_document_source=doc.source,
                ),
            )
        ),
        execution=TaskExecutionV1(
            strategy=cfg.strategy,
            unique_element_ids=cfg.unique_element_ids,
        ),
        identity=TaskIdentityV1(
            pipeline_version=ctx.pipeline_version,
            sha256=sha256,
        ),
    )


def process_task(
    *,
    task: TaskV1,
    cfg: RunnerConfig,
    ctx: RunContext,
    processor: FlowProcessor,
    destination: Destination,
    out_root: Path | None = None,
    hash_ms: int | None = None,
    execution_mode: ExecutionModeV1 = "batch",
) -> DocProcessResult:
    doc = task.inputs.document.to_document_ref()
    p = Path(doc.uri)
    sha = task.identity.sha256 if task.identity is not None else sha256_file(p)
    ext = doc.extension or p.suffix.lower()
    resolved_out_root = (out_root or cfg.out_root).expanduser().resolve()
    resolved_out_root.mkdir(parents=True, exist_ok=True)
    out_dir = resolved_out_root / sha

    t0 = time.perf_counter()
    attempts = 0
    partition_ms_total = 0
    delivery_ms: int | None = None
    t_part0 = time.perf_counter()

    while True:
        attempts += 1
        try:
            t_part0 = time.perf_counter()
            res = processor.process(
                doc=doc,
                strategy=task.execution.strategy,
                unique_element_ids=task.execution.unique_element_ids,
                run_id=ctx.run_id,
                pipeline_version=ctx.pipeline_version,
                sha256=sha,
            )
            partition_ms_total += int((time.perf_counter() - t_part0) * 1000)
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
                provenance=_build_task_run_provenance(
                    task=task,
                    execution_mode=execution_mode,
                ),
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

            t_del0 = time.perf_counter()
            try:
                receipt = destination(out_root=resolved_out_root, sha256=sha, meta=meta, result=res)
            except Exception as de:
                receipt = DeliveryReceipt(
                    destination=destination.name,
                    ok=False,
                    details={"exc_type": type(de).__name__, "exc": str(de)},
                )
            delivery_ms = int((time.perf_counter() - t_del0) * 1000)

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
            write_usage_record_v1(out_dir=out_dir, task=task, meta=meta, receipt=receipt)

            dlq_written = False
            delivery_retryable: bool | None = None
            if not receipt.ok:
                retryable = None
                if isinstance(receipt.details, dict):
                    r = receipt.details.get("retryable")
                    if isinstance(r, bool):
                        retryable = r
                delivery_retryable = retryable

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
                        out_root=resolved_out_root, sha256=sha, meta=meta, receipt=receipt
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
                hash_ms=hash_ms,
                partition_ms=partition_ms_total,
                delivery_ms=delivery_ms,
            )

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
                delivery_retryable=delivery_retryable,
                dlq_written=dlq_written,
                hash_ms=hash_ms,
                partition_ms=partition_ms_total,
                delivery_ms=delivery_ms,
            )
        except ZephyrError as e:
            duration_ms = int((time.perf_counter() - t0) * 1000)
            partition_ms_total += int((time.perf_counter() - t_part0) * 1000)
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
                outcome=str(outcome),
            )

            meta = RunMetaV1(
                run_id=ctx.run_id,
                pipeline_version=ctx.pipeline_version,
                timestamp_utc=ctx.timestamp_utc,
                schema_version=ctx.run_meta_schema_version,
                outcome=outcome,
                metrics=MetricsV1(duration_ms=duration_ms, attempts=attempts),
                error=ErrorInfoV1(code=e_code, message=e.message, details=merged_details),
                provenance=_build_task_run_provenance(
                    task=task,
                    execution_mode=execution_mode,
                ),
            )

            t_del_fail0 = time.perf_counter()
            try:
                receipt = destination(
                    out_root=resolved_out_root,
                    sha256=sha,
                    meta=meta,
                    result=None,
                )
            except Exception as de:
                receipt = DeliveryReceipt(
                    destination=destination.name,
                    ok=False,
                    details={"exc_type": type(de).__name__, "exc": str(de)},
                )

            delivery_ms = int((time.perf_counter() - t_del_fail0) * 1000)
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
            write_usage_record_v1(out_dir=out_dir, task=task, meta=meta, receipt=receipt)

            dlq_written = False

            delivery_retryable2: bool | None = None
            if not receipt.ok and isinstance(receipt.details, dict):
                r2 = receipt.details.get("retryable")
                if isinstance(r2, bool):
                    delivery_retryable2 = r2

            if not receipt.ok:
                try:
                    write_delivery_dlq(
                        out_root=resolved_out_root, sha256=sha, meta=meta, receipt=receipt
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
                delivery_retryable=delivery_retryable2,
                dlq_written=dlq_written,
                hash_ms=hash_ms,
                partition_ms=partition_ms_total,
                delivery_ms=delivery_ms,
            )


@dataclass(slots=True)
class TaskExecutionHandler:
    cfg: RunnerConfig
    ctx: RunContext
    flow_kind: FlowKind | None = None
    processor: FlowProcessor | None = None
    partition_fn: PartitionFn | None = None
    destination: Destination | None = None
    _processor_cache: dict[FlowKind, FlowProcessor] = field(
        default_factory=_new_processor_cache,
        init=False,
    )

    def __call__(self, task: TaskV1) -> None:
        selected_flow_kind = self.flow_kind or task.kind
        active_processor = self.processor
        if active_processor is None:
            active_processor = self._processor_cache.get(selected_flow_kind)
            if active_processor is None:
                active_processor = _resolve_flow_processor(
                    flow_kind=selected_flow_kind,
                    processor=None,
                    partition_fn=self.partition_fn,
                    backend=self.cfg.backend,
                )
                self._processor_cache[selected_flow_kind] = active_processor

        resolved_destination = self.destination or self.cfg.destination or FilesystemDestination()
        resolved_out_root = self.cfg.out_root.expanduser().resolve()
        resolved_out_root.mkdir(parents=True, exist_ok=True)
        process_task(
            task=task,
            cfg=self.cfg,
            ctx=self.ctx,
            processor=active_processor,
            destination=resolved_destination,
            out_root=resolved_out_root,
            execution_mode="worker",
        )


def build_task_execution_handler(
    *,
    cfg: RunnerConfig,
    ctx: RunContext,
    flow_kind: FlowKind | None = None,
    processor: FlowProcessor | None = None,
    partition_fn: PartitionFn | None = None,
    destination: Destination | None = None,
) -> TaskExecutionHandler:
    return TaskExecutionHandler(
        cfg=cfg,
        ctx=ctx,
        flow_kind=flow_kind,
        processor=processor,
        partition_fn=partition_fn,
        destination=destination,
    )


def _process_one(
    *,
    doc: DocumentRef,
    cfg: RunnerConfig,
    ctx: RunContext,
    out_root: Path,
    flow_kind: FlowKind | str,
    processor: FlowProcessor,
    # artifacts_writer: ArtifactsWriter,
    destination: Destination,
) -> DocProcessResult:
    p = Path(doc.uri)
    t_hash0 = time.perf_counter()
    sha = sha256_file(p)
    sha = normalize_flow_input_identity_sha(
        flow_kind=flow_kind,
        filename=doc.uri,
        default_sha=sha,
    )
    hash_ms = int((time.perf_counter() - t_hash0) * 1000)

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
            delivery_retryable=None,
            dlq_written=False,
            hash_ms=hash_ms,
            partition_ms=None,
            delivery_ms=None,
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
                    delivery_retryable=None,
                    dlq_written=False,
                    hash_ms=hash_ms,
                    partition_ms=None,
                    delivery_ms=None,
                )
        task = _build_task_for_document(
            doc=doc,
            flow_kind=flow_kind,
            cfg=cfg,
            ctx=ctx,
            sha256=sha,
        )
        return process_task(
            task=task,
            cfg=cfg,
            ctx=ctx,
            processor=processor,
            destination=destination,
            out_root=out_root,
            hash_ms=hash_ms,
            execution_mode="batch",
        )
    finally:
        if lock_acquired:
            lock_path.unlink(missing_ok=True)


def run_documents(
    *,
    docs: Iterable[DocumentRef],
    cfg: RunnerConfig,
    ctx: RunContext,
    flow_kind: FlowKind | str = DEFAULT_FLOW_KIND,
    processor: FlowProcessor | None = None,
    partition_fn: PartitionFn | None = None,
    # artifacts_writer: ArtifactsWriter = dump_partition_artifacts,
    destination: Destination | None = None,
    config_snapshot: ConfigSnapshotV1 | None = None,
) -> RunStats:
    """Run documents through the primary processor path or the legacy partition_fn adapter."""

    destination = destination or cfg.destination or FilesystemDestination()
    active_processor = _resolve_flow_processor(
        flow_kind=flow_kind,
        processor=processor,
        partition_fn=partition_fn,
        backend=cfg.backend,
    )

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

    delivery_failed_retryable = 0
    delivery_failed_non_retryable = 0
    delivery_failed_unknown = 0

    delivery_by_destination: dict[str, DeliveryCountersV1] = {}
    fanout_children_by_destination: dict[str, DeliveryCountersV1] = {}

    counts_by_extension: dict[str, int] = {}
    counts_by_error_code: dict[str, int] = {}

    durations_ms_list: list[int] = []
    hash_durations_ms: list[int] = []
    partition_durations_ms: list[int] = []
    delivery_durations_ms: list[int] = []

    retry_attempts_total: int = 0
    retried_success: int = 0
    retryable_failed: int = 0

    delivery_dlq_written_total = 0
    delivery_failure_kinds_by_destination: dict[str, dict[str, int]] = {}

    def consume_result(r: DocProcessResult) -> None:
        nonlocal success, failed, skipped_unsupported, skipped_existing
        nonlocal retry_attempts_total, retried_success, retryable_failed
        nonlocal delivery_total, delivery_ok, delivery_failed
        nonlocal delivery_dlq_written_total
        nonlocal delivery_failed_retryable, delivery_failed_non_retryable, delivery_failed_unknown

        if r.outcome == RunOutcome.SUCCESS:
            success += 1
            if r.duration_ms is not None:
                durations_ms_list.append(r.duration_ms)
            if r.hash_ms is not None:
                hash_durations_ms.append(r.hash_ms)
            if r.partition_ms is not None:
                partition_durations_ms.append(r.partition_ms)
            if r.delivery_ms is not None:
                delivery_durations_ms.append(r.delivery_ms)
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
            delivery_retryability = r.delivery_receipt.failure_retryability
            if delivery_retryability == "retryable":
                delivery_failed_retryable += 1
            elif delivery_retryability == "non_retryable":
                delivery_failed_non_retryable += 1
            else:
                delivery_failed_unknown += 1

        _bump_delivery_counter(
            delivery_by_destination,
            destination=r.delivery_receipt.destination,
            ok=r.delivery_receipt.ok,
        )

        shared_error_code = r.delivery_receipt.shared_error_code
        if shared_error_code not in {"not_failed", "unknown"}:
            counts_by_error_code[shared_error_code] = (
                counts_by_error_code.get(shared_error_code, 0) + 1
            )
        shared_failure_kind = r.delivery_receipt.shared_failure_kind
        if shared_failure_kind not in {"not_failed", "unknown"}:
            _bump_failure_kind_counter(
                delivery_failure_kinds_by_destination,
                destination=r.delivery_receipt.destination,
                failure_kind=shared_failure_kind,
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
                        child_details_obj = item.get("details")
                        if not d_ok and isinstance(child_details_obj, dict):
                            child_details = cast("dict[str, object]", child_details_obj)
                            failure_kind_obj = child_details.get("failure_kind")
                            if isinstance(failure_kind_obj, str) and failure_kind_obj:
                                _bump_failure_kind_counter(
                                    delivery_failure_kinds_by_destination,
                                    destination=d_name,
                                    failure_kind=failure_kind_obj,
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
                flow_kind=flow_kind,
                processor=active_processor,
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
                        flow_kind=flow_kind,
                        processor=active_processor,
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
            "failed_retryable": delivery_failed_retryable,
            "failed_non_retryable": delivery_failed_non_retryable,
            "failed_unknown": delivery_failed_unknown,
            "dlq_written_total": delivery_dlq_written_total,
            "dlq_dir": str((out_root / "_dlq" / "delivery").resolve()),
            "by_destination": delivery_by_destination,
            "fanout_children_by_destination": fanout_children_by_destination,
            "failure_kinds_by_destination": delivery_failure_kinds_by_destination,
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
    runtime_boundary: RuntimeBoundaryV1 = {
        "execution_scope": "bounded_local_same_host",
        "executor": "serial" if cfg.workers <= 1 else "thread",
        "worker_count": cfg.workers,
        "threadpool_semantics": "not_used" if cfg.workers <= 1 else "local_threads_only",
        "local_lock_scope": "out_root_file_lock",
        "artifact_storage_scope": "local_out_root",
        "skip_existing": cfg.skip_existing,
        "skip_unsupported": cfg.skip_unsupported,
        "stale_lock_ttl_s": cfg.stale_lock_ttl_s,
        "effective_destination": getattr(destination, "name", "unknown"),
    }
    batch_report["runtime_boundary"] = runtime_boundary

    stage_durations: StageDurationsV1 = {
        "hash_ms": _duration_stats(hash_durations_ms),
        "partition_ms": _duration_stats(partition_durations_ms),
        "delivery_ms": _duration_stats(delivery_durations_ms),
    }
    batch_report["stage_durations_ms"] = stage_durations

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
        "delivery_failed_retryable_total": delivery_failed_retryable,
        "delivery_failed_non_retryable_total": delivery_failed_non_retryable,
        "delivery_failed_unknown_total": delivery_failed_unknown,
        "dlq_written_total": delivery_dlq_written_total,
    }
    batch_report["metrics"] = metrics

    if config_snapshot is not None:
        batch_report["config_snapshot"] = config_snapshot

    (out_root / "batch_report.json").write_text(
        json.dumps(batch_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_batch_usage_record_v1(
        out_root=out_root,
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        docs_total=total,
        docs_success=success,
        docs_failed=failed,
        docs_skipped=skipped_existing + skipped_unsupported,
        delivery_total=delivery_total,
        delivery_ok=delivery_ok,
        delivery_failed=delivery_failed,
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
