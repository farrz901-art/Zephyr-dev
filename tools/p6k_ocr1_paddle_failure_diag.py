from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from uns_stream._internal.enhanced_partition import resolve_partition_options
from uns_stream._internal.ocr_agents import OCR_AGENT_PADDLE_QNAME
from uns_stream._internal.paddleocr_runtime import (
    apply_paddle_language_normalization,
    format_traceback_tail,
)
from uns_stream.backends.local_unstructured import LocalUnstructuredBackend
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionStrategy

PLANNED_INPUTS = ["fapiao.jpeg", "fapiao3.jpg", "hetong2-jiashuiyin.pdf"]
RUNTIME_NOTE_PREFIX = "zephyr_runtime_note:"


def _kind_for_input(path: Path) -> str:
    if path.suffix.lower() == ".pdf":
        return "pdf"
    return "image"


def _parse_runtime_notes(warnings: list[str]) -> tuple[list[str], list[dict[str, object]]]:
    plain_warnings: list[str] = []
    runtime_notes: list[dict[str, object]] = []
    for warning in warnings:
        if warning.startswith(RUNTIME_NOTE_PREFIX):
            raw = warning.removeprefix(RUNTIME_NOTE_PREFIX)
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                plain_warnings.append(warning)
                continue
            if isinstance(payload, dict):
                runtime_notes.append(payload)
                continue
        plain_warnings.append(warning)
    return plain_warnings, runtime_notes


def _find_note(notes: list[dict[str, object]], event: str) -> dict[str, object] | None:
    for note in notes:
        if note.get("event") == event:
            return note
    return None


def _build_call_kwargs(
    path: Path,
    *,
    profile: str,
    strategy: PartitionStrategy | None,
) -> dict[str, object]:
    kind = _kind_for_input(path)
    resolved = resolve_partition_options(profile=profile, strategy=strategy)
    effective_strategy = resolved.strategy or PartitionStrategy.AUTO
    call_kwargs: dict[str, object] = {
        "filename": str(path),
        "unique_element_ids": True,
    }
    if kind == "image" and effective_strategy == PartitionStrategy.FAST:
        call_kwargs["strategy"] = "auto"
    else:
        call_kwargs["strategy"] = effective_strategy.value
    call_kwargs["ocr_agent"] = OCR_AGENT_PADDLE_QNAME
    call_kwargs["table_ocr_agent"] = OCR_AGENT_PADDLE_QNAME
    call_kwargs.update(resolved.merged_backend_kwargs())
    return call_kwargs


def _sample_report(
    *,
    input_path: Path,
    profile: str,
    strategy: PartitionStrategy | None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "input": input_path.name,
        "file_exists": input_path.exists(),
        "profile": profile,
        "resolved_strategy": None if strategy is None else strategy.value,
        "resolved_languages_before_paddle_normalization": None,
        "resolved_languages_after_paddle_normalization": None,
        "resolved_ocr_agent": OCR_AGENT_PADDLE_QNAME,
        "resolved_table_ocr_agent": OCR_AGENT_PADDLE_QNAME,
        "call_kwargs_before_paddle_normalization": None,
        "call_kwargs_after_paddle_normalization": None,
        "paddle_attempted": False,
        "paddle_status": None,
        "paddle_exception_type": None,
        "paddle_exception_message": None,
        "paddle_traceback_tail": None,
        "captured_warnings": [],
        "fallback_attempted": False,
        "fallback_status": None,
        "fallback_exception_type": None,
        "fallback_exception_message": None,
        "fallback_traceback_tail": None,
        "final_status": "missing_input",
    }
    if not input_path.exists():
        return record

    call_kwargs_before = _build_call_kwargs(input_path, profile=profile, strategy=strategy)
    call_kwargs_after, normalization_note = apply_paddle_language_normalization(
        kind=_kind_for_input(input_path),
        call_kwargs=call_kwargs_before,
    )
    record["call_kwargs_before_paddle_normalization"] = call_kwargs_before
    record["call_kwargs_after_paddle_normalization"] = call_kwargs_after
    record["resolved_languages_before_paddle_normalization"] = call_kwargs_before.get("languages")
    record["resolved_languages_after_paddle_normalization"] = call_kwargs_after.get("languages")
    record["paddle_attempted"] = True

    backend = LocalUnstructuredBackend()
    try:
        result = auto_partition(
            filename=str(input_path),
            backend=backend,
            profile=profile,
            strategy=strategy,
        )
        plain_warnings, runtime_notes = _parse_runtime_notes(list(result.warnings))
        normalization_runtime_note = _find_note(runtime_notes, "paddle_language_normalization")
        fallback_note = _find_note(runtime_notes, "paddle_fallback")
        record["captured_warnings"] = plain_warnings
        if normalization_runtime_note is not None:
            record["resolved_languages_before_paddle_normalization"] = (
                normalization_runtime_note.get("paddle_languages_before")
            )
            record["resolved_languages_after_paddle_normalization"] = (
                normalization_runtime_note.get("paddle_languages_after")
            )
        if fallback_note is not None:
            record["paddle_status"] = fallback_note.get("paddle_status")
            record["paddle_exception_type"] = fallback_note.get("paddle_original_error_type")
            record["paddle_exception_message"] = fallback_note.get("paddle_original_error_message")
            record["paddle_traceback_tail"] = fallback_note.get("paddle_traceback_tail")
            record["fallback_attempted"] = True
            record["fallback_status"] = fallback_note.get("fallback_status")
            record["final_status"] = (
                "ok" if fallback_note.get("fallback_status") == "ok" else "error"
            )
        else:
            record["paddle_status"] = "ok"
            record["final_status"] = "ok"
    except Exception as exc:
        runtime_notes = backend.consume_runtime_notes()
        fallback_note = _find_note(runtime_notes, "paddle_fallback")
        if fallback_note is not None:
            record["paddle_status"] = fallback_note.get("paddle_status")
            record["paddle_exception_type"] = fallback_note.get("paddle_original_error_type")
            record["paddle_exception_message"] = fallback_note.get("paddle_original_error_message")
            record["paddle_traceback_tail"] = fallback_note.get("paddle_traceback_tail")
            record["fallback_attempted"] = True
            record["fallback_status"] = fallback_note.get("fallback_status")
            record["fallback_exception_type"] = fallback_note.get("fallback_exception_type")
            record["fallback_exception_message"] = fallback_note.get("fallback_exception_message")
            record["fallback_traceback_tail"] = fallback_note.get("fallback_traceback_tail")
        else:
            record["paddle_status"] = "error"
            record["paddle_exception_type"] = type(exc).__name__
            record["paddle_exception_message"] = str(exc)
            record["paddle_traceback_tail"] = format_traceback_tail(exc)
        record["final_status"] = "error"
    return record


def generate_report(
    *,
    input_dir: Path,
    profile: str,
    strategy: PartitionStrategy | None,
) -> dict[str, Any]:
    resolved_input_dir = input_dir.expanduser().resolve(strict=False)
    results = [
        _sample_report(
            input_path=resolved_input_dir / planned_input,
            profile=profile,
            strategy=strategy,
        )
        for planned_input in PLANNED_INPUTS
    ]
    missing_inputs = [item["input"] for item in results if item["file_exists"] is False]
    return {
        "repository": "Zephyr-dev",
        "report_id": "zephyr.dev.p6k.ocr1.fix2.paddle_runtime_errors.v1",
        "input_dir": str(resolved_input_dir),
        "planned_inputs": list(PLANNED_INPUTS),
        "profile": profile,
        "strategy": None if strategy is None else strategy.value,
        "missing_inputs": missing_inputs,
        "results": results,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="p6k_ocr1_paddle_failure_diag")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--profile", default="invoice_paddle")
    parser.add_argument(
        "--strategy",
        default=None,
        choices=["auto", "fast", "hi_res", "ocr_only"],
    )
    parser.add_argument("--json", action="store_true", default=False)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    strategy = None if args.strategy is None else PartitionStrategy(args.strategy)
    report = generate_report(
        input_dir=Path(args.input_dir),
        profile=str(args.profile),
        strategy=strategy,
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    out_path.write_text(serialized, encoding="utf-8")
    print(serialized, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
