from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

# 1. 导入新的 RunMetaV1 契约
from zephyr_core import PartitionResult
from zephyr_core.contracts.v1.run_meta import RunMetaV1


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def dump_partition_artifacts(
    *,
    out_root: Path,
    sha256: str,
    meta: RunMetaV1,
    result: PartitionResult | None = None,
) -> Path:
    """
    统一的落盘逻辑。
    如果提供了 result，则写入 elements.json 和 normalized.txt；
    无论成功失败，都写入 run_meta.json。
    """
    out_root.mkdir(parents=True, exist_ok=True)
    out_dir = out_root / sha256
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. 如果成功（有 result），写入数据文件
    if result:
        _write_json(out_dir / "elements.json", [asdict(cast(Any, e)) for e in result.elements])
        _write_text(out_dir / "normalized.txt", result.normalized_text)

    # 2. 无论成功失败，根据 RunMetaV1 契约写入元数据
    _write_json(out_dir / "run_meta.json", meta.to_dict())

    return out_dir
