from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_usage_report import main as p5_usage_main

from zephyr_ingest.testing.p5_capability_domains import EXTRA_RESERVED_CAPABILITIES
from zephyr_ingest.testing.p5_usage_facts import (
    P5_USAGE_FACT_MANIFEST_PATH,
    P5_USAGE_FACT_REPORT_PATH,
    P5_USAGE_LINKAGE_CONTRACT_PATH,
    P5_USAGE_OUTPUT_SURFACE_PATH,
    P5_USAGE_SUCCESS_FAILURE_CLASSIFICATION_PATH,
    build_p5_usage_fact_manifest,
    build_p5_usage_linkage_contract,
    build_p5_usage_output_surface,
    build_p5_usage_success_failure_classification,
    format_p5_usage_fact_results,
    validate_p5_usage_fact_artifacts,
)


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


@pytest.mark.auth_contract
def test_p5_usage_fact_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_USAGE_FACT_MANIFEST_PATH) == build_p5_usage_fact_manifest()
    assert _load_json_object(P5_USAGE_OUTPUT_SURFACE_PATH) == build_p5_usage_output_surface()
    assert _load_json_object(P5_USAGE_LINKAGE_CONTRACT_PATH) == (build_p5_usage_linkage_contract())
    assert _load_json_object(P5_USAGE_SUCCESS_FAILURE_CLASSIFICATION_PATH) == (
        build_p5_usage_success_failure_classification()
    )
    assert P5_USAGE_FACT_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_usage_fact_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_usage_fact_artifacts()

    assert all(check.ok for check in checks), format_p5_usage_fact_results(checks)


@pytest.mark.auth_contract
def test_p5_usage_manifest_keeps_non_billing_boundary_and_record_shape() -> None:
    manifest = _load_json_object(P5_USAGE_FACT_MANIFEST_PATH)
    not_claimed = cast(list[object], manifest["not"])
    record_shape = cast(dict[str, object], manifest["usage_record_shape"])
    minimum_linkage = cast(list[object], record_shape["minimum_linkage"])

    for prohibited in (
        "billing record",
        "pricing input",
        "license entitlement",
        "subscription allowance",
        "quota decision",
    ):
        assert prohibited in not_claimed
    assert record_shape["record_kind"] == "raw_usage_fact_v1_skeleton"
    assert record_shape["runtime_emitted_today"] is False
    assert "task.task_id" in minimum_linkage
    assert "run_meta.run_id" in minimum_linkage
    assert "run_meta.provenance.task_id" in minimum_linkage
    assert record_shape["capability_domains_used_marker"] == "capability_domains_used"


@pytest.mark.auth_contract
def test_p5_usage_manifest_keeps_raw_units_conservative() -> None:
    manifest = _load_json_object(P5_USAGE_FACT_MANIFEST_PATH)
    raw_units = cast(dict[str, object], manifest["raw_processing_units"])
    uns = cast(dict[str, object], raw_units["uns"])
    it = cast(dict[str, object], raw_units["it"])

    assert uns["primary_raw_unit"] == "document"
    assert uns["stable_today"] is True
    for derivative_or_unstable in (
        "page",
        "elements_count",
        "normalized_text_len",
        "delivery_success",
    ):
        assert derivative_or_unstable in cast(list[object], uns["not_primary_raw_units"])
    assert it["primary_raw_unit"] == "record_or_emitted_item"
    assert it["stable_today"] == "bounded"
    assert it["not_fully_first_class_today"] is True


@pytest.mark.auth_contract
def test_p5_usage_manifest_keeps_capability_domains_used_non_entitlement() -> None:
    manifest = _load_json_object(P5_USAGE_FACT_MANIFEST_PATH)
    marker = cast(dict[str, object], manifest["capability_domains_used"])
    shape = cast(dict[str, object], marker["shape"])
    default_marker = cast(dict[str, object], marker["default_marker_for_current_base_path"])

    assert marker["marker_kind"] == "technical_capability_domain_used_not_entitlement"
    assert shape == {"base": "bool", "pro": "bool", "extra": "bool", "evidence": "list[str]"}
    assert default_marker == {"base": True, "pro": False, "extra": False}
    assert "must not be marked Pro" in cast(str, marker["mixed_uns_stream_rule"])
    assert "must not mark runtime usage as Pro" in cast(str, marker["root_dev_all_docs_rule"])
    assert marker["extra_reserved_capabilities"] == list(EXTRA_RESERVED_CAPABILITIES)


@pytest.mark.auth_contract
def test_p5_usage_linkage_contract_keeps_task_kind_and_recovery_boundaries() -> None:
    linkage = _load_json_object(P5_USAGE_LINKAGE_CONTRACT_PATH)
    task_contract = cast(dict[str, object], linkage["task_contract"])
    run_contract = cast(dict[str, object], linkage["run_meta_contract"])
    recovery = cast(dict[str, object], linkage["recovery_linkage"])
    replay = cast(dict[str, object], recovery["replay"])
    requeue = cast(dict[str, object], recovery["requeue"])

    assert task_contract["kind_values"] == ["uns", "it"]
    blocked_runtime_kinds = cast(
        list[object],
        task_contract["do_not_add_runtime_observation_kinds"],
    )
    for invalid_kind in ("replay", "delivery", "batch", "worker_task"):
        assert invalid_kind in blocked_runtime_kinds
    assert "run_id" in cast(list[object], run_contract["stable_linkage_fields"])
    assert "usage_record" in cast(list[object], run_contract["not_first_class_today"])
    assert "capability_domains_used" in cast(list[object], run_contract["not_first_class_today"])
    assert replay["delivery_origin"] == "replay"
    assert requeue["run_origin"] == "requeue"


@pytest.mark.auth_contract
def test_p5_usage_success_failure_classification_is_symmetric_and_non_billing() -> None:
    classification = _load_json_object(P5_USAGE_SUCCESS_FAILURE_CLASSIFICATION_PATH)
    success = cast(dict[str, object], classification["success_surfaces"])
    failure = cast(dict[str, object], classification["failure_surfaces"])
    recovery = cast(dict[str, object], classification["recovery_surfaces"])
    not_claimed = cast(list[object], classification["not_claimed"])

    assert {"run_success", "delivery_success", "batch_success_or_partial"}.issubset(success)
    assert {"run_failure", "delivery_failure", "batch_failure_or_partial"}.issubset(failure)
    delivery_success = cast(dict[str, object], success["delivery_success"])
    delivery_failure = cast(dict[str, object], failure["delivery_failure"])
    assert delivery_success["not_raw_unit"] is True
    assert "details.retryable" in cast(list[object], delivery_failure["required_facts"])
    assert "resume" in recovery
    assert "replay" in recovery
    assert "requeue" in recovery
    assert "retry billing" in not_claimed
    assert "entitlement decision" in not_claimed


@pytest.mark.auth_contract
def test_p5_usage_output_surface_keeps_runtime_emission_bounded() -> None:
    output_surface = _load_json_object(P5_USAGE_OUTPUT_SURFACE_PATH)
    surfaces = cast(dict[str, object], output_surface["current_usage_like_surfaces"])
    run_meta = cast(dict[str, object], surfaces["run_meta"])
    future_surface = cast(dict[str, object], output_surface["future_usage_record_surface"])

    assert run_meta["usage_record_field_present_today"] is False
    assert run_meta["capability_domains_used_field_present_today"] is False
    assert "metrics.duration_ms" in cast(list[object], run_meta["fields"])
    assert future_surface["status"] == "manifested_not_runtime_emitted"
    assert future_surface["must_remain_redaction_safe"] is True
    assert "billing price" in cast(list[object], future_surface["must_not_include"])


@pytest.mark.auth_contract
def test_p5_usage_report_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = p5_usage_main(["--print-manifest-path"])
    manifest_output = capsys.readouterr()
    assert exit_code == 0
    printed_manifest = Path(manifest_output.out.strip())
    assert printed_manifest.parts[-2:] == ("validation", "p5_usage_fact_manifest.json")

    exit_code = p5_usage_main(["--print-linkage-contract-path"])
    linkage_output = capsys.readouterr()
    assert exit_code == 0
    printed_linkage = Path(linkage_output.out.strip())
    assert printed_linkage.parts[-2:] == ("validation", "p5_usage_linkage_contract.json")

    exit_code = p5_usage_main(["--print-success-failure-path"])
    classification_output = capsys.readouterr()
    assert exit_code == 0
    printed_classification = Path(classification_output.out.strip())
    assert printed_classification.parts[-2:] == (
        "validation",
        "p5_usage_success_failure_classification.json",
    )

    exit_code = p5_usage_main(["--check-artifacts"])
    check_output = capsys.readouterr()
    assert exit_code == 0
    assert "usage_fact_manifest_matches_helper -> ok" in check_output.out
    assert "usage_success_failure_symmetry_present -> ok" in check_output.out

    exit_code = p5_usage_main(["--json"])
    json_output = capsys.readouterr()
    assert exit_code == 0
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "usage_fact_manifest" in rendered
    assert "usage_linkage_contract" in rendered
    assert "usage_success_failure_classification" in rendered

    exit_code = p5_usage_main([])
    summary_output = capsys.readouterr()
    assert exit_code == 0
    assert "raw technical usage facts" in summary_output.out
    assert "not billing/pricing/entitlement" in summary_output.out
