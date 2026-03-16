from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

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


def _sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def partition_file(
    *,
    filename: str,
    kind: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    **partition_kwargs: object,
) -> PartitionResult:
    p = Path(filename)

    # === 关键修改：只构造一次 backend ===
    b: PartitionBackend = backend or LocalUnstructuredBackend()

    try:
        elements: list[ZephyrElement] = b.partition_elements(
            filename=str(p),
            kind=kind,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            **partition_kwargs,
        )
    except ZephyrError:
        # 保留已标准化的错误语义（比如 missing extra / unsupported type）
        raise
    except Exception as e:
        # 统一 wrap 为可治理的 ZephyrError
        raise ZephyrError(
            code=ErrorCode.UNS_PARTITION_FAILED,
            message="Partition failed",
            details={
                "filename": str(p),
                "kind": kind,
                "strategy": str(strategy),  # StrEnum -> "auto"/"hi_res"/...
                "engine": {"name": b.name, "backend": b.backend, "version": b.version},
                # 保证 JSON-serializable：统一转字符串
                "extra_kwargs": {k: str(v) for k, v in partition_kwargs.items()},
                "exc_type": type(e).__name__,
                "exc": str(e),
            },
        ) from e

    normalized_text = "\n\n".join([e.text for e in elements if e.text])

    doc = DocumentMetadata(
        filename=p.name,
        mime_type=None,
        sha256=_sha256_file(p),
        size_bytes=p.stat().st_size,
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
