from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools import p6k_m1_enhanced_partition_benchmark as benchmark_mod


def test_benchmark_tool_skips_missing_inputs_by_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = benchmark_mod.main(
        [
            "--input-dir",
            str(tmp_path / "missing-inputs"),
            "--out-dir",
            str(tmp_path / "out"),
            "--profile",
            "invoice",
            "--json",
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["profile"] == "invoice"
    assert payload["summary"]["missing_count"] == 4
    assert payload["summary"]["manual_benchmark_pending"] is True
    assert all(item["status"] == "missing_input" for item in payload["results"])
    assert all(item["image_preflight_applied"] is False for item in payload["results"])


def test_benchmark_tool_can_fail_on_missing_inputs(tmp_path: Path) -> None:
    rc = benchmark_mod.main(
        [
            "--input-dir",
            str(tmp_path / "missing-inputs"),
            "--out-dir",
            str(tmp_path / "out"),
            "--profile",
            "contract",
            "--fail-on-missing-inputs",
        ]
    )

    assert rc == 2


def test_benchmark_tool_marks_product_grade_not_ready_on_runtime_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = tmp_path / "fapiao.jpeg"
    sample.write_bytes(b"fake-image")
    monkeypatch.setattr(benchmark_mod, "PLANNED_INPUTS", ["fapiao.jpeg"])

    def _raise_partition_error(**_: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(benchmark_mod, "auto_partition", _raise_partition_error)
    report = benchmark_mod.generate_report(
        benchmark_mod.BenchmarkArgs(
            input_dir=tmp_path,
            out_dir=tmp_path / "out",
            profile="invoice",
            strategy=None,
            json_out=True,
            markdown_out=False,
            fail_on_missing_inputs=False,
        )
    )

    assert report["summary"]["missing_count"] == 0
    assert report["summary"]["error_count"] == 1
    assert report["summary"]["manual_benchmark_pending"] is False
    assert report["summary"]["product_grade_ready"] is False
    assert report["results"][0]["normalized_image_used"] is False
    assert report["results"][0]["partition_retry_applied"] is False
