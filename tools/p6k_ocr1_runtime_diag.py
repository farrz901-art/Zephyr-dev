from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from uns_stream._internal.paddleocr_runtime import ensure_paddleocr_base_dir

DEFAULT_OUT = Path("validation/p6k_ocr1_runtime_diag.json")


def _safe_version(package_name: str) -> str | None:
    try:
        return version(package_name)
    except PackageNotFoundError:
        return None


def _run_import_probe(code: str, *, timeout_seconds: int = 120) -> dict[str, object]:
    try:
        completed = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=dict(os.environ),
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "error": f"TimeoutExpired: {exc}",
        }
    if completed.returncode == 0:
        return {"ok": True, "error": None}
    error_text = (
        completed.stderr.strip()
        or completed.stdout.strip()
        or f"exit {completed.returncode}"
    )
    return {"ok": False, "error": error_text}


def _diagnose(report: dict[str, object]) -> tuple[str, str]:
    if (
        report["torch_import_ok"] is True
        and report["paddle_import_ok"] is True
        and report["paddle_then_torch_ok"] is False
        and (
            report["torch_then_paddle_ok"] is True
            or report["torch_then_unstructured_paddleocr_ok"] is True
        )
    ):
        return (
            "windows_paddle_torch_import_order_conflict",
            "Preload torch before PaddleOCR runtime initialization on Windows.",
        )
    if report["unstructured_paddleocr_import_ok"] is True:
        return ("runtime_ok", "No PaddleOCR import-order remediation is required.")
    return (
        "runtime_investigation_required",
        "Investigate the local PaddleOCR runtime environment before enabling real OCR benchmarks.",
    )


def build_report() -> dict[str, object]:
    paddle_ocr_base_dir = ensure_paddleocr_base_dir()
    probes = {
        "paddle_import": _run_import_probe("import paddle"),
        "torch_import": _run_import_probe("import torch"),
        "paddle_then_torch": _run_import_probe("import paddle\nimport torch"),
        "torch_then_paddle": _run_import_probe("import torch\nimport paddle"),
        "unstructured_paddleocr_import": _run_import_probe("import unstructured_paddleocr"),
        "torch_then_unstructured_paddleocr": _run_import_probe(
            "import torch\nimport unstructured_paddleocr"
        ),
        "ocr_agent_paddle_import": _run_import_probe(
            "from unstructured.partition.utils.ocr_models.paddle_ocr import OCRAgentPaddle"
        ),
    }

    report: dict[str, object] = {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "executable": sys.executable,
        "unstructured_version": _safe_version("unstructured"),
        "unstructured_paddleocr_version": _safe_version("unstructured-paddleocr"),
        "paddlepaddle_version": _safe_version("paddlepaddle"),
        "torch_version": _safe_version("torch"),
        "torchvision_version": _safe_version("torchvision"),
        "paddle_import_ok": probes["paddle_import"]["ok"],
        "paddle_import_error": probes["paddle_import"]["error"],
        "torch_import_ok": probes["torch_import"]["ok"],
        "torch_import_error": probes["torch_import"]["error"],
        "paddle_then_torch_ok": probes["paddle_then_torch"]["ok"],
        "paddle_then_torch_error": probes["paddle_then_torch"]["error"],
        "torch_then_paddle_ok": probes["torch_then_paddle"]["ok"],
        "torch_then_paddle_error": probes["torch_then_paddle"]["error"],
        "unstructured_paddleocr_import_ok": probes["unstructured_paddleocr_import"]["ok"],
        "unstructured_paddleocr_import_error": probes["unstructured_paddleocr_import"]["error"],
        "torch_then_unstructured_paddleocr_ok": probes["torch_then_unstructured_paddleocr"]["ok"],
        "torch_then_unstructured_paddleocr_error": probes["torch_then_unstructured_paddleocr"][
            "error"
        ],
        "ocr_agent_paddle_import_ok": probes["ocr_agent_paddle_import"]["ok"],
        "ocr_agent_paddle_import_error": probes["ocr_agent_paddle_import"]["error"],
        "paddle_ocr_base_dir": os.environ.get("PADDLE_OCR_BASE_DIR"),
        "zephyr_paddle_ocr_base_dir": paddle_ocr_base_dir,
    }
    diagnosis, recommended_action = _diagnose(report)
    report["diagnosis"] = diagnosis
    report["recommended_action"] = recommended_action
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="p6k_ocr1_runtime_diag")
    parser.add_argument("--json", action="store_true", default=False)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report = build_report()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    out_path.write_text(serialized, encoding="utf-8")
    print(serialized, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
