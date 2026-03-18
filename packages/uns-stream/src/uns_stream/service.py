from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from uns_stream._internal.retry_policy import is_retryable_exception
from uns_stream._internal.utils import sha256_file
from uns_stream.backends.base import PartitionBackend
from uns_stream.backends.local_unstructured import LocalUnstructuredBackend
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    ErrorCode,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
    ZephyrError,
)

logger = logging.getLogger(__name__)


def partition_file(
    *,
    filename: str,
    kind: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    **partition_kwargs: object,
) -> PartitionResult:
    t0 = time.perf_counter()
    p = Path(filename)

    # IO metadata (log/audit-friendly)
    size_bytes = p.stat().st_size
    # sha = _sha256_file(p)
    sha = sha256_file(p)

    # === only construct backend once ===
    b: PartitionBackend = backend or LocalUnstructuredBackend()

    logger.info(
        "partition_start file=%s kind=%s strategy=%s "
        "engine=%s backend=%s bytes=%s sha256=%s kwargs=%s",
        p.name,
        kind,
        str(strategy),
        b.name,
        b.backend,
        size_bytes,
        sha,
        sorted(partition_kwargs.keys()),
    )

    try:
        elements: list[ZephyrElement] = b.partition_elements(
            filename=str(p),
            kind=kind,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            **partition_kwargs,
        )

    except ZephyrError as ze:
        duration_ms = int((time.perf_counter() - t0) * 1000)

        # 1. 拿到原始属性
        raw_details = getattr(ze, "details", None)
        raw_code = getattr(ze, "code", ErrorCode.UNS_PARTITION_FAILED)

        retryable: bool | None = None

        # 2. 核心修复：通过 isinstance 检查后，立即用 cast 锁定类型为 dict[str, Any]
        # 这样 Pyright 就能识别出 .get() 方法，且不会报 Unknown 错误
        if isinstance(raw_details, dict):
            typed_details = cast("dict[str, Any]", raw_details)
            val = typed_details.get("retryable")

            # 3. 这里的比较逻辑依然保持最稳健的写法
            if val is True or val == "true":
                retryable = True
            elif val is False or val == "false":
                retryable = False

        # 使用 f-string 确保 code 转换为字符串时不会触发 Unknown 报错
        code_str = f"{raw_code}"

        logger.warning(
            "partition_error file=%s kind=%s strategy=%s ms=%s code=%s retryable=%s",
            p.name,
            kind,
            str(strategy),
            duration_ms,
            code_str,
            retryable,
        )
        raise

    except Exception as e:
        duration_ms = int((time.perf_counter() - t0) * 1000)

        # # 统一 wrap 为可治理的 ZephyrError
        # is_retryable = False
        # --- 调用中央策略进行分诊 ---
        is_retryable = is_retryable_exception(e)

        wrapped = ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED,
            message="Partition failed",
            details={
                "filename": str(p),
                "kind": kind,
                "strategy": str(strategy),
                "engine": {"name": b.name, "backend": b.backend, "version": b.version},
                "sha256": sha,
                "size_bytes": size_bytes,
                "duration_ms": duration_ms,
                "extra_kwargs": {k: str(v) for k, v in partition_kwargs.items()},
                "exc_type": type(e).__name__,
                "exc": str(e),
                "retryable": is_retryable,
            },
        )

        logger.error(
            "partition_failed file=%s kind=%s strategy=%s"
            "engine=%s backend=%s ms=%s exc_type=%s retryable=%s",
            p.name,
            kind,
            str(strategy),
            b.name,
            b.backend,
            duration_ms,
            type(e).__name__,
            is_retryable,
        )
        raise wrapped from e

    normalized_text = "\n\n".join([e.text for e in elements if e.text])

    duration_ms = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "partition_done file=%s kind=%s strategy=%s engine=%s"
        "backend=%s ms=%s elements=%s text_len=%s",
        p.name,
        kind,
        str(strategy),
        b.name,
        b.backend,
        duration_ms,
        len(elements),
        len(normalized_text),
    )

    doc = DocumentMetadata(
        filename=p.name,
        mime_type=None,
        sha256=sha,
        size_bytes=size_bytes,
        created_at_utc=datetime.now(timezone.utc).isoformat(),
    )

    engine = EngineInfo(name=b.name, backend=b.backend, version=b.version, strategy=strategy)

    return PartitionResult(
        document=doc,
        engine=engine,
        elements=elements,
        normalized_text=normalized_text,
        warnings=[],
    )
