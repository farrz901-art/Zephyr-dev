from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from zephyr_core import ErrorCode, PartitionResult, ZephyrError


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def dump_partition_success(
    *,
    out_root: Path,
    result: PartitionResult,
    run_id: str,
    pipeline_version: str,
    timestamp_utc: str,
    duration_ms: int | None = None,
) -> Path:
    """Write artifacts for a successful partition run."""
    out_root.mkdir(parents=True, exist_ok=True)

    # 显式转换 document 属性以解决 Pyright 对 slots dataclass 的识别问题
    doc_meta = cast(Any, result.document)
    out_dir = out_root / str(getattr(doc_meta, "sha256", "unknown"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # 使用列表推导式和 cast 转换每一个 element
    _write_json(out_dir / "elements.json", [asdict(cast(Any, e)) for e in result.elements])
    _write_text(out_dir / "normalized.txt", result.normalized_text)

    # 构造标准化的 Engine 信息
    engine_info = result.engine
    engine_payload = {
        "name": str(getattr(engine_info, "name", "unknown")),
        "backend": str(getattr(engine_info, "backend", "unknown")),
        "version": str(getattr(engine_info, "version", "unknown")),
        "strategy": str(getattr(engine_info, "strategy", "auto")),
    }

    _write_json(
        out_dir / "run_meta.json",
        {
            "run_id": run_id,
            "pipeline_version": pipeline_version,
            "timestamp_utc": timestamp_utc,
            "document": asdict(cast(Any, result.document)),
            "engine": engine_payload,
            "metrics": {
                "duration_ms": duration_ms,
                "elements_count": len(result.elements),
                "normalized_text_len": len(result.normalized_text),
            },
            "warnings": list(result.warnings),
        },
    )

    return out_dir


def dump_partition_error(
    *,
    out_root: Path,
    sha256: str,
    error: ZephyrError,
    run_id: str,
    pipeline_version: str,
    timestamp_utc: str,
    duration_ms: int | None = None,
) -> Path:
    """Write artifacts for a failed partition run (run_meta.json only)."""
    out_root.mkdir(parents=True, exist_ok=True)
    out_dir = out_root / sha256
    out_dir.mkdir(parents=True, exist_ok=True)

    # 健壮地提取异常属性，解决 Pyright 的 UnknownMemberType 错误
    # 使用 f-string 转换 code，使用 str() 转换 message，使用 cast 转换 details
    err_code = getattr(error, "code", ErrorCode.UNS_PARTITION_FAILED)
    err_msg = getattr(error, "message", str(error))
    err_details = getattr(error, "details", None)

    _write_json(
        out_dir / "run_meta.json",
        {
            "run_id": run_id,
            "pipeline_version": pipeline_version,
            "timestamp_utc": timestamp_utc,
            "metrics": {"duration_ms": duration_ms},
            "error": {
                "code": f"{err_code}",
                "message": str(err_msg),
                "details": cast("dict[str, Any] | None", err_details),
            },
        },
    )
    return out_dir
