from __future__ import annotations

import argparse
import json
import traceback
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from uns_stream._internal.enhanced_partition import resolve_partition_options
from uns_stream._internal.image_preflight import image_extraction_enabled, probe_image
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionStrategy


def _package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _failure_stage(*, exception_message: str, traceback_text: str) -> str:
    text = f"{exception_message}\n{traceback_text}".lower()
    if "cannot identify image file" in text:
        return "image_open"
    if "exif" in text:
        return "image_metadata"
    if "cannot write mode" in text or "jpeg" in text:
        return "jpeg_save"
    if (
        "save_elements" in text
        or "image_paths[page_index]" in text
        or "list index out of range" in text
    ):
        return "image_block_extraction"
    if "base64" in text or "payload" in text:
        return "payload_encoding"
    if "partition" in text:
        return "partition_call"
    return "unknown"


def _tail_lines(text: str, *, max_lines: int = 25) -> list[str]:
    lines = [line for line in text.splitlines() if line.strip()]
    return lines[-max_lines:]


def build_report(
    *, input_path: Path, profile: str, strategy: PartitionStrategy | None
) -> dict[str, Any]:
    probe = probe_image(input_path)
    resolved = resolve_partition_options(profile=profile, strategy=strategy)
    resolved_partition_kwargs = resolved.merged_backend_kwargs()
    report: dict[str, Any] = {
        "repository": "Zephyr-dev",
        "report_id": "zephyr.dev.p6k.m1.fix.fapiao3.diagnosis.v1",
        "input_file": input_path.name,
        "input_path": str(input_path),
        "missing_input": not input_path.exists(),
        "file_exists": probe.file_exists,
        "file_size": probe.file_size,
        "image_format": probe.image_format,
        "image_mode": probe.image_mode,
        "image_dimensions": None
        if probe.image_dimensions is None
        else {"width": probe.image_dimensions[0], "height": probe.image_dimensions[1]},
        "exif_orientation": probe.exif_orientation,
        "unstructured_version": _package_version("unstructured"),
        "pillow_version": _package_version("Pillow"),
        "profile": profile,
        "strategy": None if strategy is None else str(strategy),
        "resolved_partition_kwargs": resolved_partition_kwargs,
        "extract_image_block_types": resolved_partition_kwargs.get("extract_image_block_types"),
        "extract_image_block_to_payload": resolved_partition_kwargs.get(
            "extract_image_block_to_payload"
        ),
        "extract_image_block_output_dir": resolved_partition_kwargs.get(
            "extract_image_block_output_dir"
        ),
        "image_extraction_enabled": image_extraction_enabled(resolved_partition_kwargs),
    }

    if not input_path.exists():
        report.update(
            {
                "exception_type": None,
                "exception_message": None,
                "exception_chain_summary": [],
                "traceback_tail": [],
                "failure_stage": None,
                "status": "missing_input",
            }
        )
        return report

    try:
        result = auto_partition(
            filename=str(input_path),
            profile=profile,
            strategy=strategy,
        )
        report.update(
            {
                "status": "ok",
                "exception_type": None,
                "exception_message": None,
                "exception_chain_summary": [],
                "traceback_tail": [],
                "failure_stage": None,
                "warnings": result.warnings,
                "elements_count": len(result.elements),
            }
        )
        return report
    except Exception as exc:
        traceback_text = traceback.format_exc()
        chain_summary: list[str] = []
        current: BaseException | None = exc
        while current is not None and len(chain_summary) < 8:
            chain_summary.append(f"{type(current).__name__}: {current}")
            current = current.__cause__

        report.update(
            {
                "status": "error",
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "exception_chain_summary": chain_summary,
                "traceback_tail": _tail_lines(traceback_text),
                "failure_stage": _failure_stage(
                    exception_message=str(exc),
                    traceback_text=traceback_text,
                ),
            }
        )
        return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="p6k_m1_diagnose_fapiao3")
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--profile", default="invoice")
    parser.add_argument(
        "--strategy",
        default="hi_res",
        choices=["auto", "fast", "hi_res", "ocr_only"],
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ns = _parse_args(argv)
    strategy = PartitionStrategy(ns.strategy)
    report = build_report(
        input_path=Path(str(ns.input)).expanduser().resolve(strict=False),
        profile=str(ns.profile),
        strategy=strategy,
    )
    out_path = Path(str(ns.out)).expanduser().resolve(strict=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
