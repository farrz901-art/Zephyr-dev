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
from zephyr_core import PartitionStrategy, ZephyrElement, ZephyrError

REPOSITORY = "Zephyr-dev"
PLANNED_INPUTS = ["fapiao.jpeg", "fapiao3.jpg", "hetong2-jiashuiyin.pdf"]
RUNTIME_NOTE_PREFIX = "zephyr_runtime_note:"


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
    ok = sum(
        1
        for item in results
        if item["status"] in {"paddle_ok", "paddle_failed_fallback_ok", "explicit_tesseract_ok"}
    )
    missing = sum(1 for item in results if item["status"] == "missing_input")
    failed = sum(
        1
        for item in results
        if item["status"] in {"error", "paddle_failed_fallback_failed", "explicit_tesseract_failed"}
    )
    return {"ok": ok, "missing_input": missing, "error": failed}


def _parse_runtime_notes(warnings: list[str]) -> tuple[list[str], list[dict[str, object]]]:
    plain_warnings: list[str] = []
    runtime_notes: list[dict[str, object]] = []
    for warning in warnings:
        if warning.startswith(RUNTIME_NOTE_PREFIX):
            raw_payload = warning.removeprefix(RUNTIME_NOTE_PREFIX)
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                plain_warnings.append(warning)
                continue
            if isinstance(payload, dict):
                runtime_notes.append(payload)
                continue
        plain_warnings.append(warning)
    return plain_warnings, runtime_notes


def _find_runtime_note(
    runtime_notes: list[dict[str, object]], *, event: str
) -> dict[str, object] | None:
    for note in runtime_notes:
        if note.get("event") == event:
            return note
    return None


def _success_result(
    *,
    planned_input: str,
    mode: BenchmarkMode,
    args: BenchmarkArgs,
    duration_ms: int,
    elements_payload: list[dict[str, Any]],
    normalized_output_path: Path,
    elements_output_path: Path,
    normalized_text: str,
    warnings: list[str],
) -> dict[str, Any]:
    metadata_presence = _metadata_presence(elements_payload)
    plain_warnings, runtime_notes = _parse_runtime_notes(warnings)
    fallback_note = _find_runtime_note(runtime_notes, event="paddle_fallback")
    normalization_note = _find_runtime_note(runtime_notes, event="paddle_language_normalization")

    default_path_final_status: str | None = None
    default_path_paddle_status: str | None = None
    default_path_fallback_status: str | None = None
    explicit_tesseract_status: str | None = None
    status = "ok"

    if mode.mode == "default_paddleocr":
        default_path_final_status = "ok"
        if fallback_note is not None:
            status = "paddle_failed_fallback_ok"
            default_path_paddle_status = str(fallback_note.get("paddle_status", "error"))
            default_path_fallback_status = str(fallback_note.get("fallback_status", "ok"))
        else:
            status = "paddle_ok"
            default_path_paddle_status = "ok"
    else:
        status = "explicit_tesseract_ok"
        explicit_tesseract_status = "ok"

    return {
        "input": planned_input,
        "mode": mode.mode,
        "profile": args.profile,
        "ocr_agent": mode.ocr_agent or OCR_AGENT_PADDLE_QNAME,
        "table_ocr_agent": mode.table_ocr_agent or OCR_AGENT_PADDLE_QNAME,
        "status": status,
        "duration_ms": duration_ms,
        "elements_count": len(elements_payload),
        "normalized_text_len": len(normalized_text),
        "table_present": any(element["type"] == "Table" for element in elements_payload),
        "warnings": plain_warnings,
        "errors": [],
        "normalized_output_path": str(normalized_output_path),
        "elements_output_path": str(elements_output_path),
        "chinese_char_count": _count_chinese_characters(normalized_text),
        "default_path_final_status": default_path_final_status,
        "default_path_paddle_status": default_path_paddle_status,
        "default_path_fallback_status": default_path_fallback_status,
        "paddle_fallback_applied": fallback_note is not None,
        "paddle_language_before": None
        if normalization_note is None
        else normalization_note.get("paddle_languages_before"),
        "paddle_language_after": None
        if normalization_note is None
        else normalization_note.get("paddle_languages_after"),
        "explicit_tesseract_status": explicit_tesseract_status,
        **metadata_presence,
    }


def _error_result(
    *,
    planned_input: str,
    mode: BenchmarkMode,
    args: BenchmarkArgs,
    duration_ms: int,
    exc: Exception,
) -> dict[str, Any]:
    fallback_applied = False
    default_path_final_status: str | None = None
    default_path_paddle_status: str | None = None
    default_path_fallback_status: str | None = None
    explicit_tesseract_status: str | None = None
    paddle_language_before: object = None
    paddle_language_after: object = None
    warnings: list[str] = []
    errors = [f"{type(exc).__name__}: {exc}"]
    status = "error"

    if mode.mode == "default_paddleocr":
        default_path_final_status = "error"
        if isinstance(exc, ZephyrError) and isinstance(exc.details, dict):
            details = exc.details
            if "paddle_original_error_type" in details:
                fallback_applied = True
                status = "paddle_failed_fallback_failed"
                default_path_paddle_status = "error"
                default_path_fallback_status = "error"
                warnings.append(
                    f"paddle fallback failed: {details.get('paddle_original_error_type')}: "
                    f"{details.get('paddle_original_error_message')}"
                )
            else:
                status = "error"
                default_path_paddle_status = "error"
        else:
            default_path_paddle_status = "error"
    else:
        status = "explicit_tesseract_failed"
        explicit_tesseract_status = "error"

    return {
        "input": planned_input,
        "mode": mode.mode,
        "profile": args.profile,
        "ocr_agent": mode.ocr_agent or OCR_AGENT_PADDLE_QNAME,
        "table_ocr_agent": mode.table_ocr_agent or OCR_AGENT_PADDLE_QNAME,
        "status": status,
        "duration_ms": duration_ms,
        "elements_count": 0,
        "normalized_text_len": 0,
        "table_present": False,
        "text_as_html_present": False,
        "image_base64_present": False,
        "image_mime_type_present": False,
        "chinese_char_count": 0,
        "warnings": warnings,
        "errors": errors,
        "normalized_output_path": None,
        "elements_output_path": None,
        "default_path_final_status": default_path_final_status,
        "default_path_paddle_status": default_path_paddle_status,
        "default_path_fallback_status": default_path_fallback_status,
        "paddle_fallback_applied": fallback_applied,
        "paddle_language_before": paddle_language_before,
        "paddle_language_after": paddle_language_after,
        "explicit_tesseract_status": explicit_tesseract_status,
    }


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
                        "default_path_final_status": None,
                        "default_path_paddle_status": None,
                        "default_path_fallback_status": None,
                        "paddle_fallback_applied": False,
                        "paddle_language_before": None,
                        "paddle_language_after": None,
                        "explicit_tesseract_status": None,
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
                normalized_output_path = mode_root / "normalized_text.txt"
                elements_output_path = mode_root / "elements.json"
                normalized_output_path.write_text(result.normalized_text, encoding="utf-8")
                elements_output_path.write_text(
                    json.dumps(elements_payload, ensure_ascii=False, indent=2, sort_keys=True)
                    + "\n",
                    encoding="utf-8",
                )
                results.append(
                    _success_result(
                        planned_input=planned_input,
                        mode=mode,
                        args=args,
                        duration_ms=duration_ms,
                        elements_payload=elements_payload,
                        normalized_output_path=normalized_output_path,
                        elements_output_path=elements_output_path,
                        normalized_text=result.normalized_text,
                        warnings=list(result.warnings),
                    )
                )
            except Exception as exc:
                duration_ms = int((time.perf_counter() - t0) * 1000)
                results.append(
                    _error_result(
                        planned_input=planned_input,
                        mode=mode,
                        args=args,
                        duration_ms=duration_ms,
                        exc=exc,
                    )
                )

    summary = _status_summary(results)
    default_path_results = [item for item in results if item["mode"] == "default_paddleocr"]
    explicit_tesseract_results = [item for item in results if item["mode"] == "explicit_tesseract"]
    default_path_final_ok = sum(
        1 for item in default_path_results if item["default_path_final_status"] == "ok"
    )
    paddle_native_ok = sum(
        1 for item in default_path_results if item["status"] == "paddle_ok"
    )
    paddle_fallback_ok = sum(
        1 for item in default_path_results if item["status"] == "paddle_failed_fallback_ok"
    )
    explicit_tesseract_ok = sum(
        1 for item in explicit_tesseract_results if item["status"] == "explicit_tesseract_ok"
    )
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
            "default_path_final_ok": default_path_final_ok,
            "default_path_total": len(default_path_results),
            "paddle_native_ok": paddle_native_ok,
            "paddle_fallback_ok": paddle_fallback_ok,
            "explicit_tesseract_ok": explicit_tesseract_ok,
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
        lines.append(f"- Default final status: {result['default_path_final_status']}")
        lines.append(f"- Paddle status: {result['default_path_paddle_status']}")
        lines.append(f"- Fallback status: {result['default_path_fallback_status']}")
        lines.append(f"- Paddle fallback applied: {result['paddle_fallback_applied']}")
        lines.append(f"- Paddle language before: {result['paddle_language_before']}")
        lines.append(f"- Paddle language after: {result['paddle_language_after']}")
        lines.append(f"- Explicit Tesseract status: {result['explicit_tesseract_status']}")
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
