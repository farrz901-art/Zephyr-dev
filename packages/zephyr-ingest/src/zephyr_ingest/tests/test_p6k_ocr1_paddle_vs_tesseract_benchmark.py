from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools import p6k_ocr1_paddle_vs_tesseract_benchmark as benchmark_mod


def test_ocr1_benchmark_skips_missing_inputs_by_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = benchmark_mod.main(
        [
            "--input-dir",
            str(tmp_path / "missing"),
            "--out-dir",
            str(tmp_path / "out"),
            "--profile",
            "invoice_paddle",
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["manual_benchmark_pending"] is True
    assert payload["summary"]["missing_input"] == len(benchmark_mod.PLANNED_INPUTS) * 2
    assert all(item["status"] == "missing_input" for item in payload["results"])


def test_ocr1_benchmark_can_fail_on_missing_inputs(tmp_path: Path) -> None:
    rc = benchmark_mod.main(
        [
            "--input-dir",
            str(tmp_path / "missing"),
            "--out-dir",
            str(tmp_path / "out"),
            "--fail-on-missing-inputs",
        ]
    )

    assert rc == 2


def test_ocr1_benchmark_reports_modes_and_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sample = tmp_path / "fapiao.jpeg"
    sample.write_bytes(b"fake-image")
    monkeypatch.setattr(benchmark_mod, "PLANNED_INPUTS", ["fapiao.jpeg"])

    class _Result:
        def __init__(self) -> None:
            self.elements: list[object] = []
            self.normalized_text = "测试invoice"
            self.warnings = ["warn"]

    captured: list[dict[str, object]] = []

    def _fake_partition(**kwargs: object) -> _Result:
        captured.append(dict(kwargs))
        return _Result()

    monkeypatch.setattr(benchmark_mod, "auto_partition", _fake_partition)

    report = benchmark_mod.generate_report(
        benchmark_mod.BenchmarkArgs(
            input_dir=tmp_path,
            out_dir=tmp_path / "out",
            profile="invoice_paddle",
            strategy=None,
            json_out=True,
            markdown_out=False,
            fail_on_missing_inputs=False,
        )
    )

    assert report["summary"]["ok"] == 2
    assert report["summary"]["manual_benchmark_pending"] is False
    assert len(captured) == 2
    assert captured[0]["ocr_agent"] is None
    assert captured[1]["ocr_agent"] == "tesseract"
    assert report["results"][0]["normalized_output_path"] is not None
