from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_capability_domain_report import main as p5_domain_main

from zephyr_ingest.testing.p5_capability_domains import (
    DESTINATION_CONNECTORS,
    EXTRA_RESERVED_CAPABILITIES,
    IT_STREAM_SOURCE_CONNECTORS,
    P5_BUILD_CUT_MANIFEST_PATH,
    P5_CAPABILITY_DOMAIN_MATRIX_PATH,
    P5_CAPABILITY_DOMAIN_REPORT_PATH,
    P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH,
    P5_OUTPUT_BOUNDARY_REPORT_PATH,
    UNS_STREAM_PRO_EXTRAS,
    UNS_STREAM_SOURCE_CONNECTORS,
    build_p5_build_cut_manifest,
    build_p5_capability_domain_matrix,
    build_p5_dependency_boundary_manifest,
    format_p5_capability_domain_results,
    validate_p5_capability_domain_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_capability_domain_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_CAPABILITY_DOMAIN_MATRIX_PATH) == (
        build_p5_capability_domain_matrix()
    )
    assert _load_json_object(P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH) == (
        build_p5_dependency_boundary_manifest()
    )
    assert _load_json_object(P5_BUILD_CUT_MANIFEST_PATH) == build_p5_build_cut_manifest()
    assert P5_CAPABILITY_DOMAIN_REPORT_PATH.exists()
    assert P5_OUTPUT_BOUNDARY_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_capability_domain_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_capability_domain_artifacts()

    assert all(check.ok for check in checks), format_p5_capability_domain_results(checks)


@pytest.mark.auth_contract
def test_p5_capability_domain_matrix_keeps_current_connectors_base_non_enterprise() -> None:
    matrix = _load_json_object(P5_CAPABILITY_DOMAIN_MATRIX_PATH)
    connectors = cast(dict[str, object], matrix["connectors"])
    it_sources = cast(list[dict[str, object]], connectors["it_stream_sources"])
    uns_sources = cast(list[dict[str, object]], connectors["uns_stream_sources"])
    destinations = cast(list[dict[str, object]], connectors["destinations"])

    assert tuple(entry["name"] for entry in it_sources) == IT_STREAM_SOURCE_CONNECTORS
    assert tuple(entry["name"] for entry in uns_sources) == UNS_STREAM_SOURCE_CONNECTORS
    assert tuple(entry["name"] for entry in destinations) == DESTINATION_CONNECTORS

    for entry in [*it_sources, *uns_sources, *destinations]:
        assert entry["domain"] == "base"
        assert entry["enterprise_grade_today"] is False


@pytest.mark.auth_contract
def test_p5_capability_domain_matrix_models_uns_stream_as_mixed_package() -> None:
    matrix = _load_json_object(P5_CAPABILITY_DOMAIN_MATRIX_PATH)
    uns_surface = cast(dict[str, object], matrix["uns_stream_capability_surface"])
    base_surface = cast(dict[str, object], uns_surface["base_surface"])
    pro_extra_entries = cast(list[dict[str, object]], uns_surface["pro_optional_extras"])
    extra_reserved_entries = cast(list[dict[str, object]], uns_surface["extra_reserved"])

    assert uns_surface["package_domain_model"] == "mixed_base_plus_pro_optional_extras"
    assert uns_surface["not_modeled_as"] == "whole_package_pro"
    assert base_surface["domain"] == "base"
    assert base_surface["dependency_basis"] == ["unstructured"]
    assert tuple(entry["extra"] for entry in pro_extra_entries) == UNS_STREAM_PRO_EXTRAS
    assert {entry["domain"] for entry in pro_extra_entries} == {"pro"}
    assert tuple(entry["capability"] for entry in extra_reserved_entries) == (
        EXTRA_RESERVED_CAPABILITIES
    )
    assert {entry["domain"] for entry in extra_reserved_entries} == {"extra"}


@pytest.mark.auth_contract
def test_p5_capability_domain_matrix_keeps_domains_non_commercial() -> None:
    matrix = _load_json_object(P5_CAPABILITY_DOMAIN_MATRIX_PATH)
    domain_model = cast(dict[str, object], matrix["domain_model"])
    not_claimed = cast(list[object], matrix["not_claimed"])
    promotion = cast(dict[str, object], matrix["extra_to_pro_promotion"])

    assert domain_model["kind"] == "technical_capability_domain"
    for prohibited in ("pricing tier", "subscription tier", "license tier", "entitlement domain"):
        assert prohibited in cast(list[object], domain_model["not"])
    for prohibited in (
        "commercial pricing tier",
        "license enforcement",
        "subscription entitlement",
    ):
        assert prohibited in not_claimed
    assert promotion["current_extra_capabilities"] == list(EXTRA_RESERVED_CAPABILITIES)
    assert promotion["requires_explicit_promotion"] is True


@pytest.mark.auth_contract
def test_p5_dependency_and_build_manifests_keep_cut_readiness_bounded() -> None:
    dependency_manifest = _load_json_object(P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH)
    build_manifest = _load_json_object(P5_BUILD_CUT_MANIFEST_PATH)
    workspace_reality = cast(dict[str, object], dependency_manifest["workspace_reality"])
    build_cuts = cast(dict[str, object], build_manifest["build_cuts"])

    assert workspace_reality["workspace_model"] == "single_body_workspace"
    assert "uns-stream[all-docs]" in cast(list[object], workspace_reality["root_dev_includes"])
    assert "not evidence that the entire uns-stream package is Pro" in cast(
        str,
        workspace_reality["root_dev_all_docs_meaning"],
    )

    for cut_name in ("base", "pro", "extra"):
        cut = cast(dict[str, object], build_cuts[cut_name])
        assert cut["directly_buildable_today"] is False


@pytest.mark.auth_contract
def test_p5_capability_domain_report_cli_prints_paths_json_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_domain_main(["--print-matrix-path"])
    matrix_output = capsys.readouterr()
    assert exit_code == 0
    printed_matrix = Path(matrix_output.out.strip())
    assert printed_matrix.parts[-2:] == ("validation", "p5_capability_domain_matrix.json")

    exit_code = p5_domain_main(["--print-dependency-manifest-path"])
    dependency_output = capsys.readouterr()
    assert exit_code == 0
    printed_dependency = Path(dependency_output.out.strip())
    assert printed_dependency.parts[-2:] == (
        "validation",
        "p5_dependency_boundary_manifest.json",
    )

    exit_code = p5_domain_main(["--print-build-cut-manifest-path"])
    build_cut_output = capsys.readouterr()
    assert exit_code == 0
    printed_build_cut = Path(build_cut_output.out.strip())
    assert printed_build_cut.parts[-2:] == ("validation", "p5_build_cut_manifest.json")

    exit_code = p5_domain_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "capability_domain_matrix_matches_helper -> ok" in check_output.out

    exit_code = p5_domain_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "capability_domain_matrix" in rendered
    assert "dependency_boundary_manifest" in rendered
    assert "build_cut_manifest" in rendered

    exit_code = p5_domain_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "technical capability domain" in summary_output.out
    assert "uns-stream: mixed Base direct surface plus Pro optional extras" in summary_output.out
