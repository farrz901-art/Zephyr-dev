from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any

from uns_stream._internal.ocr_agents import OCR_AGENT_PADDLE_QNAME
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionStrategy, ZephyrElement

REPOSITORY = "Zephyr-dev"
PLANNED_INPUTS = ["fapiao.jpeg", "fapiao3.jpg", "hetong2-jiashuiyin.pdf"]


@dataclass(frozen=True, slots=True)
class BenchmarkMode:
    mode: str
    ocr_agent: str | None
    table_ocr_agent: str | None


BENCHMARK_MODES = (
    BenchmarkMode(mode="default_paddleocr", ocr_agent=None, table_ocr_agent=None),
    BenchmarkMode(
        mode="explicit_tesseract",
        ocr_agent="tesseract",
        table_ocr_agent="tesseract",
    ),
)


@dataclass(frozen=True, slots=True)
class BenchmarkArgs:
    input_dir: Path
    out_dir: Path
    profile: str
    strategy: PartitionStrategy | None
    json_out: bool
    markdown_out: bool
    fail_on_missing_inputs: bool


def _safe_name(value: str) -> str:
    return value.replace(" ", "_")


def _count_chinese_characters(text: str) -> int:
    return sum(1 for char in text if "\u4e00" <= char <= "\u9fff")


def _element_dicts(elements: list[ZephyrElement]) -> list[dict[str, Any]]:
    return [
        {
            "element_id": element.element_id,
            "type": element.type,
            "text": element.text,
            "metadata": dict(element.metadata),
        }
        for element in elements
    ]


def _metadata_presence(elements: list[dict[str, Any]]) -> dict[str, bool]:
    flags = {
        "text_as_html_present": False,
        "image_base64_present": False,
        "image_mime_type_present": False,
    }
    for element in elements:
        metadata_obj = element.get("metadata")
        if not isinstance(metadata_obj, dict):
            continue
        metadata = metadata_obj
        if "text_as_html" in metadata:
            flags["text_as_html_present"] = True
        if "image_base64" in metadata:
            flags["image_base64_present"] = True
        if "image_mime_type" in metadata:
            flags["image_mime_type_present"] = True
    return flags


def _status_summary(results: list[dict[str, Any]]) -> dict[str, int]:
    ok = sum(1 for item in results if item["status"] == "ok")
    missing = sum(1 for item in results if item["status"] == "missing_input")
    failed = sum(1 for item in results if item["status"] == "error")
    return {"ok": ok, "missing_input": missing, "error": failed}


def generate_report(args: BenchmarkArgs) -> dict[str, Any]:
    resolved_input_dir = args.input_dir.expanduser().resolve(strict=False)
    resolved_out_dir = args.out_dir.expanduser().resolve(strict=False)
    resolved_out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    missing_inputs: list[str] = []

    for planned_input in PLANNED_INPUTS:
        input_path = resolved_input_dir / planned_input
        if not input_path.exists():
            missing_inputs.append(planned_input)
            for mode in BENCHMARK_MODES:
                results.append(
                    {
                        "input": planned_input,
                        "mode": mode.mode,
                        "profile": args.profile,
                        "ocr_agent": mode.ocr_agent or OCR_AGENT_PADDLE_QNAME,
                        "table_ocr_agent": mode.table_ocr_agent or OCR_AGENT_PADDLE_QNAME,
                        "status": "missing_input",
                        "duration_ms": None,
                        "elements_count": 0,
                        "normalized_text_len": 0,
                        "table_present": False,
                        "text_as_html_present": False,
                        "image_base64_present": False,
                        "image_mime_type_present": False,
                        "chinese_char_count": 0,
                        "warnings": [f"missing input: {input_path}"],
                        "errors": [],
                        "normalized_output_path": None,
                        "elements_output_path": None,
                    }
                )
            continue

        for mode in BENCHMARK_MODES:
            mode_root = resolved_out_dir / _safe_name(Path(planned_input).stem) / mode.mode
            mode_root.mkdir(parents=True, exist_ok=True)
            t0 = time.perf_counter()
            try:
                result = auto_partition(
                    filename=str(input_path),
                    profile=args.profile,
                    strategy=args.strategy,
                    ocr_agent=mode.ocr_agent,
                    table_ocr_agent=mode.table_ocr_agent,
                )
                duration_ms = int((time.perf_counter() - t0) * 1000)
                elements_payload = _element_dicts(result.elements)
                metadata_presence = _metadata_presence(elements_payload)
                normalized_output_path = mode_root / "normalized_text.txt"
                elements_output_path = mode_root / "elements.json"
                normalized_output_path.write_text(result.normalized_text, encoding="utf-8")
                elements_output_path.write_text(
                    json.dumps(elements_payload, ensure_ascii=False, indent=2, sort_keys=True)
                    + "\n",
                    encoding="utf-8",
                )
                results.append(
                    {
                        "input": planned_input,
                        "mode": mode.mode,
                        "profile": args.profile,
                        "ocr_agent": mode.ocr_agent or OCR_AGENT_PADDLE_QNAME,
                        "table_ocr_agent": mode.table_ocr_agent or OCR_AGENT_PADDLE_QNAME,
                        "status": "ok",
                        "duration_ms": duration_ms,
                        "elements_count": len(result.elements),
                        "normalized_text_len": len(result.normalized_text),
                        "table_present": any(
                            element.type == "Table" for element in result.elements
                        ),
                        "warnings": list(result.warnings),
                        "errors": [],
                        "normalized_output_path": str(normalized_output_path),
                        "elements_output_path": str(elements_output_path),
                        "chinese_char_count": _count_chinese_characters(result.normalized_text),
                        **metadata_presence,
                    }
                )
            except Exception as exc:
                duration_ms = int((time.perf_counter() - t0) * 1000)
                results.append(
                    {
                        "input": planned_input,
                        "mode": mode.mode,
                        "profile": args.profile,
                        "ocr_agent": mode.ocr_agent or OCR_AGENT_PADDLE_QNAME,
                        "table_ocr_agent": mode.table_ocr_agent or OCR_AGENT_PADDLE_QNAME,
                        "status": "error",
                        "duration_ms": duration_ms,
                        "elements_count": 0,
                        "normalized_text_len": 0,
                        "table_present": False,
                        "text_as_html_present": False,
                        "image_base64_present": False,
                        "image_mime_type_present": False,
                        "chinese_char_count": 0,
                        "warnings": [],
                        "errors": [f"{type(exc).__name__}: {exc}"],
                        "normalized_output_path": None,
                        "elements_output_path": None,
                    }
                )

    summary = _status_summary(results)
    return {
        "repository": REPOSITORY,
        "report_id": "zephyr.dev.p6k.ocr1.paddle_vs_tesseract_benchmark.v1",
        "unstructured_version": version("unstructured"),
        "input_dir": str(resolved_input_dir),
        "out_dir": str(resolved_out_dir),
        "profile": args.profile,
        "strategy": None if args.strategy is None else str(args.strategy),
        "planned_inputs": list(PLANNED_INPUTS),
        "modes": [mode.mode for mode in BENCHMARK_MODES],
        "results": results,
        "summary": {
            **summary,
            "missing_inputs": missing_inputs,
            "manual_benchmark_pending": len(missing_inputs) > 0,
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# P6K OCR1 Paddle vs Tesseract Benchmark",
        "",
        f"- Repository: {report['repository']}",
        f"- Unstructured version: {report['unstructured_version']}",
        f"- Profile: {report['profile']}",
        f"- Strategy: {report['strategy']}",
        f"- Manual benchmark pending: {report['summary']['manual_benchmark_pending']}",
        "",
    ]
    for result in report["results"]:
        lines.append(f"## {result['input']} / {result['mode']}")
        lines.append("")
        lines.append(f"- Status: {result['status']}")
        lines.append(f"- OCR agent: {result['ocr_agent']}")
        lines.append(f"- Table OCR agent: {result['table_ocr_agent']}")
        lines.append(f"- Duration ms: {result['duration_ms']}")
        lines.append(f"- Elements count: {result['elements_count']}")
        lines.append(f"- Normalized text len: {result['normalized_text_len']}")
        lines.append(f"- Table present: {result['table_present']}")
        lines.append(f"- Chinese char count: {result['chinese_char_count']}")
        lines.append(f"- Warnings: {result['warnings']}")
        lines.append(f"- Errors: {result['errors']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _parse_args(argv: list[str] | None = None) -> BenchmarkArgs:
    parser = argparse.ArgumentParser(prog="p6k_ocr1_paddle_vs_tesseract_benchmark")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--profile", default="invoice_paddle")
    parser.add_argument(
        "--strategy",
        default=None,
        choices=["auto", "fast", "hi_res", "ocr_only"],
    )
    parser.add_argument("--json", action="store_true", dest="json_out", default=False)
    parser.add_argument("--markdown", action="store_true", dest="markdown_out", default=False)
    parser.add_argument("--fail-on-missing-inputs", action="store_true", default=False)
    ns = parser.parse_args(argv)
    strategy = None if ns.strategy is None else PartitionStrategy(ns.strategy)
    return BenchmarkArgs(
        input_dir=Path(ns.input_dir),
        out_dir=Path(ns.out_dir),
        profile=str(ns.profile),
        strategy=strategy,
        json_out=bool(ns.json_out),
        markdown_out=bool(ns.markdown_out),
        fail_on_missing_inputs=bool(ns.fail_on_missing_inputs),
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report = generate_report(args)
    if args.markdown_out:
        print(render_markdown(report), end="")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if args.fail_on_missing_inputs and report["summary"]["missing_inputs"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
