from __future__ import annotations

import os
import sys
from importlib import import_module
from pathlib import Path
from typing import Mapping

from uns_stream._internal.ocr_agents import OCR_AGENT_PADDLE_QNAME


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
