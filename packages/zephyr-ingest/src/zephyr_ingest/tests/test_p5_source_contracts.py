from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_source_contract_report import main as p5_source_main

from zephyr_ingest.testing.p5_capability_domains import (
    IT_STREAM_SOURCE_CONNECTORS,
    UNS_STREAM_SOURCE_CONNECTORS,
)
from zephyr_ingest.testing.p5_source_contracts import (
    P5_SOURCE_CONTRACT_MATRIX_PATH,
    P5_SOURCE_CONTRACT_REPORT_PATH,
    P5_SOURCE_SURFACE_CLASSIFICATION_PATH,
    P5_SOURCE_USAGE_LINKAGE_PATH,
    P5_USAGE_RUNTIME_CONTRACT_PATH,
    P5_USAGE_RUNTIME_OUTPUT_SURFACE_PATH,
    P5_USAGE_RUNTIME_REPORT_PATH,
    build_p5_source_contract_matrix,
    build_p5_source_surface_classification,
    build_p5_source_usage_linkage,
    build_p5_usage_runtime_contract,
    build_p5_usage_runtime_output_surface,
    format_p5_source_contract_results,
    validate_p5_source_contract_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_source_contract_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_SOURCE_CONTRACT_MATRIX_PATH) == build_p5_source_contract_matrix()
    assert _load_json_object(P5_SOURCE_SURFACE_CLASSIFICATION_PATH) == (
        build_p5_source_surface_classification()
    )
    assert _load_json_object(P5_USAGE_RUNTIME_CONTRACT_PATH) == build_p5_usage_runtime_contract()
    assert _load_json_object(P5_USAGE_RUNTIME_OUTPUT_SURFACE_PATH) == (
        build_p5_usage_runtime_output_surface()
    )
    assert _load_json_object(P5_SOURCE_USAGE_LINKAGE_PATH) == build_p5_source_usage_linkage()
    assert P5_SOURCE_CONTRACT_REPORT_PATH.exists()
    assert P5_USAGE_RUNTIME_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_source_contract_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_source_contract_artifacts()

    assert all(check.ok for check in checks), format_p5_source_contract_results(checks)


@pytest.mark.auth_contract
def test_p5_source_contract_matrix_keeps_source_boundaries_and_domains() -> None:
    matrix = _load_json_object(P5_SOURCE_CONTRACT_MATRIX_PATH)
    uns_sources = cast(list[dict[str, object]], matrix["uns_stream_sources"])
    it_sources = cast(list[dict[str, object]], matrix["it_stream_sources"])
    asymmetry = cast(dict[str, object], matrix["source_model_asymmetry"])

    assert {source["source_id"] for source in uns_sources} == set(UNS_STREAM_SOURCE_CONNECTORS)
    assert {source["source_id"] for source in it_sources} == set(IT_STREAM_SOURCE_CONNECTORS)
    assert all(source["technical_capability_domain"] == "base" for source in uns_sources)
    assert all(source["technical_capability_domain"] == "base" for source in it_sources)
    assert all(source["enterprise_grade_today"] is False for source in uns_sources + it_sources)
    assert "document acquisition" in cast(str, asymmetry["uns"])
    assert "structured cursor" in cast(str, asymmetry["it"])
    assert "fully symmetric" in cast(str, asymmetry["intentionally_not_claimed"])


@pytest.mark.auth_contract
def test_p5_source_contract_matrix_exposes_auth_raw_unit_and_boundedness() -> None:
    matrix = _load_json_object(P5_SOURCE_CONTRACT_MATRIX_PATH)
    all_sources = cast(list[dict[str, object]], matrix["uns_stream_sources"]) + cast(
        list[dict[str, object]],
        matrix["it_stream_sources"],
    )

    for source in all_sources:
        assert isinstance(source["auth_surface"], str)
        assert source["auth_surface"]
        raw_unit = cast(dict[str, object], source["raw_unit_relation"])
        assert raw_unit["primary_raw_unit"] in {"document", "record_or_emitted_item"}
        boundedness = cast(list[object], source["boundedness"])
        assert boundedness
        assert "enterprise connector" in cast(list[object], source["not_claimed"])


@pytest.mark.auth_contract
def test_p5_usage_runtime_contract_keeps_non_billing_runtime_emission_truth() -> None:
    contract = _load_json_object(P5_USAGE_RUNTIME_CONTRACT_PATH)
    usage_record = cast(dict[str, object], contract["usage_record"])
    domains = cast(dict[str, object], contract["capability_domains_used"])
    raw_units = cast(dict[str, object], contract["raw_units"])
    recovery = cast(dict[str, object], contract["recovery_symmetry"])

    assert usage_record["artifact"] == "usage_record.json"
    assert usage_record["authoritative_usage_truth"] is True
    assert usage_record["record_kinds"] == [
        "raw_technical_usage_fact_v1",
        "raw_technical_usage_batch_summary_v1",
    ]
    for prohibited in ("billing record", "pricing input", "license entitlement"):
        assert prohibited in cast(list[object], usage_record["not"])
    assert domains["runtime_emitted"] is True
    assert domains["current_default_for_retained_sources"] == {
        "base": True,
        "pro": False,
        "extra": False,
    }
    assert "not marked Pro" in cast(str, domains["mixed_uns_stream_rule"])
    uns = cast(dict[str, object], raw_units["uns"])
    it = cast(dict[str, object], raw_units["it"])
    assert uns["primary_raw_unit"] == "document"
    assert "page" in cast(list[object], uns["not_primary"])
    assert it["primary_raw_unit"] == "record_or_emitted_item"
    assert recovery["replay"] == "recovery_kind=replay via delivery_origin=replay"
    assert "not collapsed" in cast(str, recovery["redrive"])


@pytest.mark.auth_contract
def test_p5_source_usage_linkage_covers_sources_and_keeps_extra_non_default() -> None:
    linkage = _load_json_object(P5_SOURCE_USAGE_LINKAGE_PATH)
    source_linkages = cast(list[dict[str, object]], linkage["source_linkages"])

    assert len(source_linkages) == len(UNS_STREAM_SOURCE_CONNECTORS) + len(
        IT_STREAM_SOURCE_CONNECTORS
    )
    for item in source_linkages:
        marker = cast(dict[str, object], item["default_capability_domains_used"])
        assert marker == {"base": True, "pro": False, "extra": False}
        assert item["usage_raw_unit"] in {"document", "record_or_emitted_item"}
        assert "usage_record.source.source_contract_id" in cast(
            list[object],
            item["runtime_linkage_fields"],
        )


@pytest.mark.auth_contract
def test_p5_source_contract_report_cli_prints_paths_list_sources_json_and_checks(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_source_main(["--print-matrix-path"])
    matrix_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(matrix_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_source_contract_matrix.json",
    )

    exit_code = p5_source_main(["--print-usage-runtime-contract-path"])
    usage_output = capsys.readouterr()
    assert exit_code == 0
    assert Path(usage_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_usage_runtime_contract.json",
    )

    exit_code = p5_source_main(["--list-sources"])
    source_output = capsys.readouterr()
    assert exit_code == 0
    assert "http_document_v1" in source_output.out
    assert "kafka_partition_offset_v1" in source_output.out

    exit_code = p5_source_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "source_contract_matrix" in rendered
    assert "usage_runtime_contract" in rendered
    assert "source_usage_linkage" in rendered

    exit_code = p5_source_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "source_contract_matrix_matches_helper -> ok" in check_output.out
    assert "source_usage_linkage_covers_all_current_sources -> ok" in check_output.out

    exit_code = p5_source_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "technical, non-entitlement truth" in summary_output.out
    assert "usage_record.json" in summary_output.out
