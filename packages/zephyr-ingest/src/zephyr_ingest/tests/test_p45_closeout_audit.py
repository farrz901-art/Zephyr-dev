from __future__ import annotations

import json
from pathlib import Path

import pytest

from zephyr_ingest.testing.p45_matrix import load_p45_truth_matrix

REPO_ROOT = Path(__file__).resolve().parents[5]
EXIT_MATRIX_PATH = REPO_ROOT / "validation" / "p45_closeout_exit_matrix.json"
EXIT_REPORT_PATH = REPO_ROOT / "validation" / "P4_5_CLOSEOUT_AUDIT.md"


@pytest.mark.auth_contract
def test_p45_closeout_exit_matrix_covers_full_retained_surface() -> None:
    truth_matrix = load_p45_truth_matrix()
    exit_matrix = json.loads(EXIT_MATRIX_PATH.read_text(encoding="utf-8"))

    retained_destinations = {
        surface["name"]
        for surface in truth_matrix["surfaces"]
        if surface["retained"] and surface["kind"] == "destination"
    }
    retained_sources = {
        surface["name"]
        for surface in truth_matrix["surfaces"]
        if surface["retained"] and surface["kind"] == "source"
    }
    retained_input_paths = {
        surface["name"]
        for surface in truth_matrix["surfaces"]
        if surface["retained"] and surface["kind"] == "input-path"
    }

    assert exit_matrix["phase"] == "P4.5 closeout audit"
    assert exit_matrix["exit_judgment"] in {"P4.5 exit approved", "P4.5 exit blocked"}
    assert {item["name"] for item in exit_matrix["destinations"]} == retained_destinations
    assert {item["name"] for item in exit_matrix["sources"]} == retained_sources
    assert {item["name"] for item in exit_matrix["preserved_input_paths"]} == retained_input_paths

    for section_name in ("destinations", "sources", "preserved_input_paths"):
        for item in exit_matrix[section_name]:
            assert item["evidence_refs"]
            for ref in item["evidence_refs"]:
                assert (REPO_ROOT / ref).exists(), ref


@pytest.mark.auth_contract
def test_p45_closeout_exit_report_and_matrix_align() -> None:
    exit_matrix = json.loads(EXIT_MATRIX_PATH.read_text(encoding="utf-8"))
    report = EXIT_REPORT_PATH.read_text(encoding="utf-8")

    assert "P4.5 Closeout Audit" in report
    assert exit_matrix["exit_judgment"] in report
    if exit_matrix["exit_judgment"] == "P4.5 exit approved":
        assert exit_matrix["p5_may_begin"] is True
        assert exit_matrix["parity"]["dual_standard_behavior_remaining"] is False
        assert all(not item["blocking"] for item in exit_matrix["destinations"])
        assert all(not item["blocking"] for item in exit_matrix["sources"])
        assert all(not item["blocking"] for item in exit_matrix["preserved_input_paths"])
        assert all(not item["blocking"] for item in exit_matrix["cross_cutting"])
