from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_product_cut_report import main as p5_product_cut_main

from zephyr_ingest.testing.p5_capability_domains import (
    DESTINATION_CONNECTORS,
    EXTRA_RESERVED_CAPABILITIES,
    IT_STREAM_SOURCE_CONNECTORS,
    UNS_STREAM_PRO_EXTRAS,
    UNS_STREAM_SOURCE_CONNECTORS,
)
from zephyr_ingest.testing.p5_product_cuts import (
    DESTINATION_COMPOSITION_SURFACES,
    P5_DEPENDENCY_SLICE_MANIFEST_PATH,
    P5_DESTINATION_SURFACE_CLASSIFICATION_PATH,
    P5_PACKAGING_OUTPUT_MANIFEST_PATH,
    P5_PACKAGING_SKELETON_REPORT_PATH,
    P5_PRODUCT_CUT_MANIFEST_PATH,
    build_p5_dependency_slice_manifest,
    build_p5_destination_surface_classification,
    build_p5_packaging_output_manifest,
    build_p5_product_cut_manifest,
    format_p5_product_cut_results,
    validate_p5_product_cut_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_product_cut_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_PRODUCT_CUT_MANIFEST_PATH) == build_p5_product_cut_manifest()
    assert _load_json_object(P5_DEPENDENCY_SLICE_MANIFEST_PATH) == (
        build_p5_dependency_slice_manifest()
    )
    assert _load_json_object(P5_DESTINATION_SURFACE_CLASSIFICATION_PATH) == (
        build_p5_destination_surface_classification()
    )
    assert _load_json_object(P5_PACKAGING_OUTPUT_MANIFEST_PATH) == (
        build_p5_packaging_output_manifest()
    )
    assert P5_PACKAGING_SKELETON_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_product_cut_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_product_cut_artifacts()

    assert all(check.ok for check in checks), format_p5_product_cut_results(checks)


@pytest.mark.auth_contract
def test_p5_product_cut_manifest_models_base_pro_extra_without_installable_claims() -> None:
    product_cut = _load_json_object(P5_PRODUCT_CUT_MANIFEST_PATH)
    cuts = cast(dict[str, object], product_cut["cuts"])
    base = cast(dict[str, object], cuts["base"])
    pro = cast(dict[str, object], cuts["pro"])
    extra = cast(dict[str, object], cuts["extra"])
    base_contains = cast(dict[str, object], base["intended_contains"])
    source_connectors = cast(dict[str, object], base_contains["source_connectors"])
    pro_additions = cast(dict[str, object], pro["incremental_additions"])

    assert product_cut["skeleton_status"] == "groundwork_only_not_installable"
    assert base["directly_buildable_today"] is False
    assert pro["directly_buildable_today"] is False
    assert extra["directly_buildable_today"] is False
    assert source_connectors["it_stream"] == list(IT_STREAM_SOURCE_CONNECTORS)
    assert source_connectors["uns_stream"] == list(UNS_STREAM_SOURCE_CONNECTORS)
    assert base_contains["single_sink_destinations"] == list(DESTINATION_CONNECTORS)
    assert base_contains["destination_composition_surfaces"] == list(
        DESTINATION_COMPOSITION_SURFACES
    )
    assert pro["inherits"] == "base"
    assert pro["not_modeled_as"] == "whole_uns_stream_package"
    assert pro_additions["uns_stream_optional_extras"] == list(UNS_STREAM_PRO_EXTRAS)
    assert extra["default_packaged"] is False
    assert extra["incubating_reserved_capabilities"] == list(EXTRA_RESERVED_CAPABILITIES)


@pytest.mark.auth_contract
def test_p5_dependency_slice_manifest_preserves_dev_vs_product_cut_truth() -> None:
    dependency_slice = _load_json_object(P5_DEPENDENCY_SLICE_MANIFEST_PATH)
    root_dev = cast(dict[str, object], dependency_slice["root_dev_convenience"])
    slices = cast(dict[str, object], dependency_slice["slices"])
    base = cast(dict[str, object], slices["base"])
    pro = cast(dict[str, object], slices["pro_incremental"])
    extra = cast(dict[str, object], slices["extra"])
    uns_mode = cast(dict[str, object], base["uns_stream_dependency_mode"])

    assert "uns-stream[all-docs]" in cast(list[object], root_dev["includes"])
    assert "not product-cut truth" in cast(str, root_dev["meaning"])
    assert "not evidence that uns-stream is whole-package Pro" in cast(str, root_dev["meaning"])
    assert base["product_cut_ready"] is False
    assert uns_mode["direct_dependency_basis"] == "unstructured"
    assert uns_mode["exclude_optional_extras_by_default"] is True
    assert pro["requires_base"] is True
    assert pro["not_modeled_as"] == "whole_uns_stream_package"
    assert pro["uns_stream_optional_extras"] == list(UNS_STREAM_PRO_EXTRAS)
    assert extra["default_included"] is False
    assert extra["reserved_capabilities"] == list(EXTRA_RESERVED_CAPABILITIES)


@pytest.mark.auth_contract
def test_p5_destination_surface_classification_keeps_fanout_composition_not_sink() -> None:
    classification = _load_json_object(P5_DESTINATION_SURFACE_CLASSIFICATION_PATH)
    sinks = cast(list[dict[str, object]], classification["single_sink_destinations"])
    compositions = cast(list[dict[str, object]], classification["composition_surfaces"])
    abstracts = cast(list[dict[str, object]], classification["abstract_surfaces"])

    sink_names = [cast(str, entry["name"]) for entry in sinks]
    composition_names = [cast(str, entry["name"]) for entry in compositions]

    assert tuple(sink_names) == DESTINATION_CONNECTORS
    assert "fanout" not in sink_names
    assert composition_names == ["fanout"]
    assert compositions[0]["surface_type"] == "destination_composition_orchestration"
    assert compositions[0]["single_sink_connector"] is False
    assert abstracts[0]["surface_type"] == "abstract_destination_base_not_connector"


@pytest.mark.auth_contract
def test_p5_packaging_output_manifest_stays_skeleton_only_and_extra_non_default() -> None:
    packaging = _load_json_object(P5_PACKAGING_OUTPUT_MANIFEST_PATH)
    future_outputs = cast(dict[str, object], packaging["future_outputs"])
    base = cast(dict[str, object], future_outputs["base"])
    pro = cast(dict[str, object], future_outputs["pro"])
    extra = cast(dict[str, object], future_outputs["extra"])

    assert packaging["output_status"] == "validation_artifacts_only_not_product_packages"
    assert base["installable_today"] is False
    assert pro["installable_today"] is False
    assert extra["installable_today"] is False
    assert extra["default_packaged"] is False
    assert "uns-stream optional extras" in cast(list[object], base["must_not_assume"])
    assert "whole uns-stream package is Pro" in cast(list[object], pro["must_not_assume"])
    assert "Extra is ready for default packaging" in cast(list[object], extra["must_not_assume"])


@pytest.mark.auth_contract
def test_p5_product_cut_report_cli_prints_paths_json_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_product_cut_main(["--print-product-cut-path"])
    product_cut_output = capsys.readouterr()
    assert exit_code == 0
    printed_product_cut = Path(product_cut_output.out.strip())
    assert printed_product_cut.parts[-2:] == ("validation", "p5_product_cut_manifest.json")

    exit_code = p5_product_cut_main(["--print-destination-classification-path"])
    classification_output = capsys.readouterr()
    assert exit_code == 0
    printed_classification = Path(classification_output.out.strip())
    assert printed_classification.parts[-2:] == (
        "validation",
        "p5_destination_surface_classification.json",
    )

    exit_code = p5_product_cut_main(["--print-packaging-output-path"])
    packaging_output = capsys.readouterr()
    assert exit_code == 0
    printed_packaging = Path(packaging_output.out.strip())
    assert printed_packaging.parts[-2:] == ("validation", "p5_packaging_output_manifest.json")

    exit_code = p5_product_cut_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "product_cut_manifest_matches_helper -> ok" in check_output.out
    assert "fanout_is_composition_not_sink -> ok" in check_output.out

    exit_code = p5_product_cut_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "product_cut_manifest" in rendered
    assert "destination_surface_classification" in rendered
    assert "packaging_output_manifest" in rendered

    exit_code = p5_product_cut_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "skeleton only; not installable product packages" in summary_output.out
    assert "fanout is composition/orchestration" in summary_output.out
