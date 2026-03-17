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

from uns_stream._internal.artifacts import dump_partition_error, dump_partition_success
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionStrategy, ZephyrError


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


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

    # 记录起始时间
    t0 = time.perf_counter()

    try:
        res = auto_partition(
            filename=str(in_path),
            strategy=strategy,
            unique_element_ids=bool(args.unique_element_ids),
        )

        # 计算耗时
        duration_ms = int((time.perf_counter() - t0) * 1000)

        # out_dir = out_root / res.document.sha256
        # out_dir.mkdir(parents=True, exist_ok=True)
        #
        # _write_json(out_dir / "elements.json", [asdict(e) for e in res.elements])
        # _write_text(out_dir / "normalized.txt", res.normalized_text)
        # _write_json(
        #     out_dir / "run_meta.json",
        #     {
        #         "run_id": run_id,
        #         "pipeline_version": pipeline_version,
        #         "timestamp_utc": timestamp_iso,
        #         "document": asdict(res.document),
        #         "engine": {
        #             "name": res.engine.name,
        #             "backend": res.engine.backend,
        #             "version": res.engine.version,
        #             "strategy": str(res.engine.strategy),
        #         },
        #         "metrics": {
        #             "elements_count": len(res.elements),
        #             "normalized_text_len": len(res.normalized_text),
        #         },
        #         "warnings": list(res.warnings),
        #     },
        # )
        #
        # logger.info("Wrote artifacts to: %s", out_dir)

        out_dir = dump_partition_success(
            out_root=out_root,
            result=res,
            run_id=run_id,
            pipeline_version=pipeline_version,
            timestamp_utc=timestamp_iso,
            duration_ms=duration_ms,
        )

        logger.info("Wrote artifacts to: %s (RunID: %s)", out_dir, run_id)

    except ZephyrError as e:
        # 失败也写一份 run_meta.json 便于回放/排障
        import hashlib

        duration_ms = int((time.perf_counter() - t0) * 1000)

        h = hashlib.sha256(in_path.read_bytes()).hexdigest()
        # out_dir = out_root / h
        # out_dir.mkdir(parents=True, exist_ok=True)
        #
        # _write_json(
        #     out_dir / "run_meta.json",
        #     {
        #         "error": {
        #             "code": str(e.code),
        #             "message": e.message,
        #             "details": e.details,
        #         }
        #     },
        # )
        # logger.error("Partition failed. Wrote error meta to: %s", out_dir)

        out_dir = dump_partition_error(
            out_root=out_root,
            sha256=h,
            error=e,
            run_id=run_id,
            pipeline_version=pipeline_version,
            timestamp_utc=timestamp_iso,
            duration_ms=duration_ms,
        )

        logger.error("Partition failed. Wrote error meta to: %s (RunID: %s)", out_dir, run_id)

        raise


if __name__ == "__main__":
    main()
