from __future__ import annotations

import os
import sys
import traceback
from importlib import import_module
from pathlib import Path
from typing import Mapping, cast

from uns_stream._internal.ocr_agents import (
    OCR_AGENT_PADDLE_QNAME,
    OCR_AGENT_TESSERACT_QNAME,
)
from uns_stream._internal.paddleocr_languages import normalize_languages_for_paddleocr


def _languages_before_list(languages: object) -> list[str]:
    if isinstance(languages, str):
        return [languages]
    if isinstance(languages, list):
        list_values = cast("list[object]", languages)
        return [item for item in list_values if isinstance(item, str)]
    if isinstance(languages, tuple):
        tuple_values = cast("tuple[object, ...]", languages)
        return [item for item in tuple_values if isinstance(item, str)]
    if isinstance(languages, (set, frozenset)):
        set_values = cast("set[object] | frozenset[object]", languages)
        return [item for item in set_values if isinstance(item, str)]
    return [str(languages)]


def partition_call_uses_paddle_ocr(call_kwargs: Mapping[str, object]) -> bool:
    return any(
        call_kwargs.get(field_name) == OCR_AGENT_PADDLE_QNAME
        for field_name in ("ocr_agent", "table_ocr_agent")
    )


def preload_torch_for_paddleocr() -> dict[str, object]:
    result: dict[str, object] = {
        "torch_preload_applied": True,
        "torch_import_ok": False,
        "torch_version": None,
        "torch_error_type": None,
        "torch_error": None,
    }
    try:
        torch_module = import_module("torch")
    except Exception as exc:
        result["torch_error_type"] = type(exc).__name__
        result["torch_error"] = str(exc)
        return result

    result["torch_import_ok"] = True
    version_value = getattr(torch_module, "__version__", None)
    result["torch_version"] = str(version_value) if version_value is not None else None
    return result


def apply_paddle_language_normalization(
    *, kind: str, call_kwargs: Mapping[str, object]
) -> tuple[dict[str, object], dict[str, object] | None]:
    updated = dict(call_kwargs)
    if kind not in {"pdf", "image"} or not partition_call_uses_paddle_ocr(updated):
        return updated, None

    languages = updated.get("languages")
    if languages is None:
        return updated, None

    normalized = normalize_languages_for_paddleocr(languages)
    if normalized is None:
        return updated, None

    updated["languages"] = normalized
    note: dict[str, object] = {
        "event": "paddle_language_normalization",
        "paddle_language_normalization_applied": True,
        "paddle_languages_before": _languages_before_list(languages),
        "paddle_languages_after": list(normalized),
    }
    return updated, note


def format_traceback_tail(exc: BaseException, *, line_count: int = 12) -> str:
    formatted = traceback.format_exception(type(exc), exc, exc.__traceback__)
    tail = "".join(formatted[-line_count:]).strip()
    return tail


def is_known_paddle_runtime_failure(exc: BaseException) -> bool:
    haystack = f"{type(exc).__name__}: {exc}\n{format_traceback_tail(exc, line_count=24)}".lower()
    markers = (
        "unstructured_paddleocr",
        "paddle_ocr",
        "language code",
        "unsupported language",
    )
    return any(marker in haystack for marker in markers)


def build_tesseract_fallback_kwargs(call_kwargs: Mapping[str, object]) -> dict[str, object]:
    fallback_kwargs = dict(call_kwargs)
    fallback_kwargs["ocr_agent"] = OCR_AGENT_TESSERACT_QNAME
    fallback_kwargs["table_ocr_agent"] = OCR_AGENT_TESSERACT_QNAME
    return fallback_kwargs


def ensure_paddleocr_base_dir() -> str:
    base_dir = (
        os.environ.get("ZEPHYR_PADDLE_OCR_BASE_DIR")
        or os.environ.get("PADDLE_OCR_BASE_DIR")
        or str((Path.cwd() / ".tmp" / "paddleocr").resolve())
    )
    resolved = Path(base_dir).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    os.environ["PADDLE_OCR_BASE_DIR"] = str(resolved)

    loaded_module = sys.modules.get("unstructured_paddleocr.unstructured_paddleocr")
    if loaded_module is not None and hasattr(loaded_module, "BASE_DIR"):
        setattr(loaded_module, "BASE_DIR", str(resolved))

    return str(resolved)
