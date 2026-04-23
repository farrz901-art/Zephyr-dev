from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m5_validation_report import main as p5_m5_validation_main

from zephyr_ingest.testing.p5_capability_domains import (
    DESTINATION_CONNECTORS,
    IT_STREAM_SOURCE_CONNECTORS,
    UNS_STREAM_SOURCE_CONNECTORS,
)
from zephyr_ingest.testing.p5_m5_validation import (
    M5_EXCLUDED_SCOPE,
    M5_MUST_VALIDATE_CHAINS,
    P5_M5_DEEP_ANCHOR_SELECTION_PATH,
    P5_M5_RETAINED_SURFACE_INVENTORY_PATH,
    P5_M5_VALIDATION_CHARTER_PATH,
    P5_M5_VALIDATION_MATRIX_PATH,
    build_p5_m5_deep_anchor_selection,
    build_p5_m5_retained_surface_inventory,
    build_p5_m5_validation_matrix,
    format_p5_m5_validation_results,
    validate_p5_m5_validation_artifacts,
)
from zephyr_ingest.testing.p5_product_cuts import DESTINATION_COMPOSITION_SURFACES


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_m5_validation_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_M5_VALIDATION_MATRIX_PATH) == build_p5_m5_validation_matrix()
    assert _load_json_object(P5_M5_RETAINED_SURFACE_INVENTORY_PATH) == (
        build_p5_m5_retained_surface_inventory()
    )
    assert _load_json_object(P5_M5_DEEP_ANCHOR_SELECTION_PATH) == (
        build_p5_m5_deep_anchor_selection()
    )
    assert P5_M5_VALIDATION_CHARTER_PATH.exists()


@pytest.mark.auth_contract
def test_p5_m5_validation_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_m5_validation_artifacts()

    assert all(check.ok for check in checks), format_p5_m5_validation_results(checks)


@pytest.mark.auth_contract
def test_p5_m5_retained_inventory_freezes_sources_destinations_and_fanout() -> None:
    inventory = _load_json_object(P5_M5_RETAINED_SURFACE_INVENTORY_PATH)
    retained_sources = cast(dict[str, object], inventory["retained_sources"])
    uns_sources = cast(list[dict[str, object]], retained_sources["uns_stream"])
    it_sources = cast(list[dict[str, object]], retained_sources["it_stream"])
    destinations = cast(dict[str, object], inventory["retained_destinations"])
    sinks = cast(list[dict[str, object]], destinations["single_sink_destinations"])
    compositions = cast(list[dict[str, object]], destinations["composition_orchestration_surfaces"])

    assert [entry["source_id"] for entry in uns_sources] == list(UNS_STREAM_SOURCE_CONNECTORS)
    assert [entry["source_id"] for entry in it_sources] == list(IT_STREAM_SOURCE_CONNECTORS)
    assert [entry["destination"] for entry in sinks] == list(DESTINATION_CONNECTORS)
    assert [entry["destination"] for entry in compositions] == list(
        DESTINATION_COMPOSITION_SURFACES
    )
    assert "fanout" not in [entry["destination"] for entry in sinks]
    assert compositions[0]["surface_type"] == "destination_composition_orchestration"
    assert compositions[0]["single_sink_connector"] is False


@pytest.mark.auth_contract
def test_p5_m5_matrix_stays_layered_not_flat_execution() -> None:
    matrix = _load_json_object(P5_M5_VALIDATION_MATRIX_PATH)
    layers = cast(dict[str, object], matrix["layers"])
    layer_1 = cast(dict[str, object], layers["layer_1_baseline_coverage"])
    layer_2 = cast(dict[str, object], layers["layer_2_representative_deep_validation"])
    layer_3 = cast(dict[str, object], layers["layer_3_repeatability_soak_cleanup"])
    composition = cast(dict[str, object], layer_1["composition"])

    assert matrix["matrix_status"] == "charter_only_not_executed"
    assert matrix["strategy"] == "layered_matrix_not_flat_all_pairs"
    assert composition["not"] == "single sink connector proof"
    assert layer_2["must_validate_chains"] == list(M5_MUST_VALIDATE_CHAINS)
    assert layer_3["status"] == "future_m5_stage_not_s0_execution"
    assert matrix["explicit_non_claims"] == list(M5_EXCLUDED_SCOPE)


@pytest.mark.auth_contract
def test_p5_m5_deep_anchor_selection_records_representative_rationale() -> None:
    anchor_selection = _load_json_object(P5_M5_DEEP_ANCHOR_SELECTION_PATH)
    anchors = cast(dict[str, object], anchor_selection["anchors"])

    for anchor in (
        "filesystem",
        "opensearch",
        "webhook",
        "kafka",
        "spool_queue",
        "sqlite_queue",
        "google_drive_document_v1",
        "confluence_document_v1",
        "kafka_partition_offset_v1",
        "fanout",
    ):
        assert anchor in anchors
        anchor_entry = cast(dict[str, object], anchors[anchor])
        assert isinstance(anchor_entry["why"], str)
        assert anchor_entry["why"]

    fanout = cast(dict[str, object], anchors["fanout"])
    assert fanout["anchor_type"] == "destination_composition_orchestration"
    assert fanout["single_sink_connector"] is False
    assert anchor_selection["selection_strategy"] == (
        "representative_layered_anchors_not_equal_depth_all_pairs"
    )


@pytest.mark.auth_contract
def test_p5_m5_validation_charter_preserves_scope_boundaries() -> None:
    charter = P5_M5_VALIDATION_CHARTER_PATH.read_text(encoding="utf-8")

    assert "M5-S0 does not reopen M4 debt" in charter
    assert "layered, not flat all-pairs" in charter
    assert "fanout is composition/orchestration, not a single sink" in charter
    assert "distributed runtime" in charter
    assert "cloud or Kubernetes packaging" in charter
    assert "billing pricing entitlement" in charter
    assert "enterprise connector implementation" in charter


@pytest.mark.auth_contract
def test_p5_m5_validation_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_m5_validation_main(["--print-matrix-path"])
    matrix_output = capsys.readouterr()
    assert exit_code == 0
    printed_matrix = Path(matrix_output.out.strip())
    assert printed_matrix.parts[-2:] == ("validation", "p5_m5_validation_matrix.json")

    exit_code = p5_m5_validation_main(["--print-charter-path"])
    charter_output = capsys.readouterr()
    assert exit_code == 0
    printed_charter = Path(charter_output.out.strip())
    assert printed_charter.parts[-2:] == ("validation", "P5_M5_VALIDATION_CHARTER.md")

    exit_code = p5_m5_validation_main(["--print-inventory-path"])
    inventory_output = capsys.readouterr()
    assert exit_code == 0
    printed_inventory = Path(inventory_output.out.strip())
    assert printed_inventory.parts[-2:] == (
        "validation",
        "p5_m5_retained_surface_inventory.json",
    )

    exit_code = p5_m5_validation_main(["--print-deep-anchor-path"])
    anchor_output = capsys.readouterr()
    assert exit_code == 0
    printed_anchor = Path(anchor_output.out.strip())
    assert printed_anchor.parts[-2:] == ("validation", "p5_m5_deep_anchor_selection.json")

    exit_code = p5_m5_validation_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "m5_validation_matrix_matches_helper -> ok" in check_output.out
    assert "fanout_is_composition_not_sink_in_m5_inventory -> ok" in check_output.out

    exit_code = p5_m5_validation_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "validation_matrix" in rendered
    assert "retained_surface_inventory" in rendered
    assert "deep_anchor_selection" in rendered

    exit_code = p5_m5_validation_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "charter and matrix freeze only" in summary_output.out
    assert "fanout is composition/orchestration" in summary_output.out
