from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from it_stream.artifacts import dump_it_artifacts
from zephyr_core import PartitionResult
from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_ingest._internal.package_manifest import write_package_manifest_for_run


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

    manifest_run_meta = meta.to_dict()

    if result is not None:
        _write_json(out_dir / "elements.json", [asdict(cast(Any, e)) for e in result.elements])
        _write_text(out_dir / "normalized.txt", result.normalized_text)
        if manifest_run_meta.get("document") is None:
            manifest_run_meta["document"] = {
                "filename": result.document.filename,
                "mime_type": result.document.mime_type,
                "sha256": result.document.sha256,
                "size_bytes": result.document.size_bytes,
                "created_at_utc": result.document.created_at_utc,
            }
        if manifest_run_meta.get("engine") is None:
            manifest_run_meta["engine"] = {
                "name": result.engine.name,
                "backend": result.engine.backend,
                "version": result.engine.version,
                "strategy": str(result.engine.strategy),
            }
        if result.engine.name == "it-stream":
            dump_it_artifacts(
                out_dir=out_dir,
                result=result,
                pipeline_version=meta.pipeline_version,
                run_provenance=meta.provenance,
            )

    _write_json(out_dir / "run_meta.json", meta.to_dict())
    if result is not None:
        write_package_manifest_for_run(
            out_dir=out_dir,
            source_sha256=sha256,
            run_meta=manifest_run_meta,
        )

    return out_dir
