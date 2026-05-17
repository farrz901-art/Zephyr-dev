from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools import p6k_ocr1_runtime_diag as diag_mod


def test_build_report_classifies_known_windows_import_order_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe_results = {
        "import paddle": {"ok": True, "error": None},
        "import torch": {"ok": True, "error": None},
        "import paddle\nimport torch": {"ok": False, "error": "WinError 127"},
        "import torch\nimport paddle": {"ok": True, "error": None},
        "import unstructured_paddleocr": {"ok": False, "error": "WinError 127"},
        "import torch\nimport unstructured_paddleocr": {"ok": True, "error": None},
        "from unstructured.partition.utils.ocr_models.paddle_ocr import OCRAgentPaddle": {
            "ok": True,
            "error": None,
        },
    }

    monkeypatch.setattr(diag_mod, "ensure_paddleocr_base_dir", lambda: "E:/tmp/paddleocr")
    versions = {
        "unstructured": "0.22.28",
        "unstructured-paddleocr": "2.10.0",
        "paddlepaddle": "3.3.0",
        "torch": "2.8.0",
        "torchvision": "0.23.0",
    }

    def fake_safe_version(package_name: str) -> str | None:
        return versions.get(package_name)

    def fake_run_import_probe(code: str, timeout_seconds: int = 120) -> dict[str, object]:
        del timeout_seconds
        return cast(dict[str, object], probe_results[code])

    monkeypatch.setattr(
        diag_mod,
        "_safe_version",
        fake_safe_version,
    )
    monkeypatch.setattr(diag_mod, "_run_import_probe", fake_run_import_probe)

    report = diag_mod.build_report()

    assert report["diagnosis"] == "windows_paddle_torch_import_order_conflict"
    assert report["recommended_action"] == (
        "Preload torch before PaddleOCR runtime initialization on Windows."
    )
    assert report["torch_then_unstructured_paddleocr_ok"] is True
    assert report["zephyr_paddle_ocr_base_dir"] == "E:/tmp/paddleocr"


def test_runtime_diag_main_writes_default_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "diagnosis": "runtime_ok",
        "recommended_action": "No PaddleOCR import-order remediation is required.",
    }
    out_path = tmp_path / "runtime_diag.json"

    monkeypatch.setattr(diag_mod, "build_report", lambda: payload)

    rc = diag_mod.main(["--json", "--out", str(out_path)])

    assert rc == 0
    assert json.loads(out_path.read_text(encoding="utf-8")) == payload
    assert json.loads(capsys.readouterr().out) == payload
