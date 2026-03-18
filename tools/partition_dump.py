from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from dataclasses import asdict
from pathlib import Path
from typing import Any

from uns_stream._internal.utils import sha256_file
from zephyr_core.contracts.v1.run_meta import RunMetaV1, EngineMetaV1, MetricsV1, ErrorInfoV1
from uns_stream._internal.artifacts import dump_partition_artifacts
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionStrategy, ZephyrError


# def _write_text(path: Path, text: str) -> None:
#     path.write_text(text, encoding="utf-8")
#
#
# def _write_json(path: Path, obj: Any) -> None:
#     path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dump Zephyr partition outputs (elements.json / normalized.txt / run_meta.json).",
    )
    parser.add_argument("--file", required=True, help="Input file path")
    parser.add_argument(
        "--strategy",
        default="auto",
        choices=["auto", "fast", "hi_res", "ocr_only"],
        help="Partition strategy (mainly for pdf/image).",
    )
    parser.add_argument("--out", default=".cache/out", help="Output directory root")
    parser.add_argument("--unique-element-ids", action="store_true", default=True)
    parser.add_argument("--no-unique-element-ids", dest="unique_element_ids", action="store_false")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("partition_dump")

    in_path = Path(args.file).resolve()
    out_root = Path(args.out).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    strategy = PartitionStrategy(args.strategy)


    run_id = str(uuid.uuid4())
    timestamp_iso = datetime.now(timezone.utc).isoformat()
    pipeline_version = "p1"
    t0 = time.perf_counter()

    try:
        res = auto_partition(
            filename=str(in_path),
            strategy=strategy,
            unique_element_ids=bool(args.unique_element_ids),
        )
        duration_ms = int((time.perf_counter() - t0) * 1000)

        # 1. 【成功时】构造强类型契约对象
        meta = RunMetaV1(
            run_id=run_id,
            pipeline_version=pipeline_version,
            timestamp_utc=timestamp_iso,
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

        out_dir = dump_partition_artifacts(
            out_root=out_root,
            sha256=res.document.sha256,
            meta=meta,
            result=res
        )
        logger.info("Success. Wrote to: %s", out_dir)

    except ZephyrError as e:
        import hashlib

        duration_ms = int((time.perf_counter() - t0) * 1000)
        # h = hashlib.sha256(in_path.read_bytes()).hexdigest()
        h = sha256_file(in_path)

        # 2. 【失败时】构造包含 ErrorInfo 的契约对象
        meta = RunMetaV1(
            run_id=run_id,
            pipeline_version=pipeline_version,
            timestamp_utc=timestamp_iso,
            metrics=MetricsV1(duration_ms=duration_ms),
            error=ErrorInfoV1(
                code=str(e.code),
                message=e.message,
                details=getattr(e, "details", None),
            ),
        )

        out_dir = dump_partition_artifacts(
            out_root=out_root,
            sha256=h,
            meta=meta
        )
        logger.error("Failed. Error meta wrote to: %s", out_dir)
        raise


if __name__ == "__main__":
    main()
