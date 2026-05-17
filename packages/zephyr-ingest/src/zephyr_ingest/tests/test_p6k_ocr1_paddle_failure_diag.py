from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools import p6k_ocr1_paddle_failure_diag as diag_mod


def test_paddle_failure_diag_reports_missing_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out_path = tmp_path / "diag.json"

    rc = diag_mod.main(
        [
            "--input-dir",
            str(tmp_path / "missing"),
            "--out",
            str(out_path),
            "--profile",
            "invoice_paddle",
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["missing_inputs"] == diag_mod.PLANNED_INPUTS
    assert all(item["final_status"] == "missing_input" for item in payload["results"])
    assert json.loads(out_path.read_text(encoding="utf-8")) == payload


def test_paddle_failure_diag_captures_fallback_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sample = tmp_path / "fapiao.jpeg"
    sample.write_bytes(b"fake-image")
    monkeypatch.setattr(diag_mod, "PLANNED_INPUTS", ["fapiao.jpeg"])

    class _Result:
        def __init__(self) -> None:
            self.warnings = [
                diag_mod.RUNTIME_NOTE_PREFIX
                + json.dumps(
                    {
                        "event": "paddle_language_normalization",
                        "paddle_languages_before": ["zho", "eng"],
                        "paddle_languages_after": ["ch"],
                    }
                ),
                diag_mod.RUNTIME_NOTE_PREFIX
                + json.dumps(
                    {
                        "event": "paddle_fallback",
                        "paddle_status": "error",
                        "paddle_original_error_type": "RuntimeError",
                        "paddle_original_error_message": "paddle failed",
                        "paddle_traceback_tail": "tail",
                        "fallback_status": "ok",
                    }
                ),
            ]

    def fake_auto_partition(**kwargs: object) -> _Result:
        del kwargs
        return _Result()

    monkeypatch.setattr(diag_mod, "auto_partition", fake_auto_partition)

    report = diag_mod.generate_report(
        input_dir=tmp_path,
        profile="invoice_paddle",
        strategy=None,
    )

    sample_result = report["results"][0]
    assert sample_result["paddle_status"] == "error"
    assert sample_result["fallback_attempted"] is True
    assert sample_result["fallback_status"] == "ok"
    assert sample_result["final_status"] == "ok"
    assert sample_result["resolved_languages_after_paddle_normalization"] == ["ch"]
