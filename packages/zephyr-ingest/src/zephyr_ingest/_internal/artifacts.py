from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from it_stream.artifacts import dump_it_artifacts
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
    out_root.mkdir(parents=True, exist_ok=True)
    out_dir = out_root / sha256
    out_dir.mkdir(parents=True, exist_ok=True)

    if result is not None:
        _write_json(out_dir / "elements.json", [asdict(cast(Any, e)) for e in result.elements])
        _write_text(out_dir / "normalized.txt", result.normalized_text)
        if result.engine.name == "it-stream":
            dump_it_artifacts(out_dir=out_dir, result=result)

    _write_json(out_dir / "run_meta.json", meta.to_dict())

    return out_dir
