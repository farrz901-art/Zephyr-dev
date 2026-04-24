from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p5_m5_s2 import (
    build_p5_m5_s2_deep_closures,
)
from zephyr_ingest.testing.p45 import repo_root

M5S3Severity = Literal["error", "warning", "info"]

P5_M5_S3_EXECUTION_PLAN_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s3_execution_plan.json"
)
P5_M5_S3_STABILITY_MATRIX_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_S3_STABILITY_MATRIX.md"
)
P5_M5_S3_REPEATABILITY_CARRIERS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s3_repeatability_carriers.json"
)
P5_M5_S3_CLEANUP_REPETITION_RULES_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s3_cleanup_repetition_rules.json"
)
P5_M5_S3_STABILITY_SIGNALS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s3_stability_signals.json"
)

_DELIVERY_BUNDLE: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_kafka_destination.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_opensearch_destination.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_kafka_sink.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
)
_QUEUE_BUNDLE: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
    "test_requeue_poison_task_moves_to_pending_and_preserves_governance",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
    "test_recover_stale_inflight_moves_task_back_to_pending",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
    "test_recover_stale_inflight_transitions_to_poison_at_orphan_threshold",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
    "test_requeue_inflight_task_moves_to_pending_and_preserves_governance",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
    "test_inspect_queue_bucket_list_surfaces_governance_and_identity_metadata",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_spool_queue.py::"
    "test_inspect_queue_is_read_only",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
    "test_sqlite_queue_participates_in_shared_inspection_and_requeue_subset",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_queue_backend.py::"
    "test_sqlite_queue_backend_work_source_handles_lock_contention_and_stale_inflight",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
)
_SOURCE_BUNDLE: Final[tuple[str, ...]] = (
    "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py",
    "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py",
    "packages/it-stream/src/it_stream/tests/test_kafka_source.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_replay_delivery.py",
)
_FANOUT_BUNDLE: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::test_fanout_all_ok",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
    "test_fanout_any_fail",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py::"
    "test_fanout_surfaces_shared_failure_summary_when_failed_children_agree",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
)


def _repeatability_entry(
    *,
    closure_name: str,
    family_name: str,
    run_1: tuple[str, ...],
    run_2: tuple[str, ...],
    optional_run_3: tuple[str, ...],
    stable_signals: list[str],
    drift_risks: list[str],
) -> dict[str, object]:
    return {
        "closure_name": closure_name,
        "family_name": family_name,
        "rounds": {
            "run_1": {
                "carrier_set": list(run_1),
                "scope": "initial representative deep proof run",
            },
            "run_2": {
                "carrier_set": list(run_2),
                "scope": "fresh basetemp/out_root/receipt/checkpoint scope rerun",
                "cleanup_required_before_run": True,
            },
            "optional_run_3": {
                "carrier_set": list(optional_run_3),
                "scope": "bounded same-host soak repetition",
                "required": False,
            },
        },
        "stable_signals": stable_signals,
        "drift_risks": drift_risks,
    }


def build_p5_m5_s3_repeatability_carriers() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S3 repeatability carriers",
        "repeatability_families": [
            _repeatability_entry(
                closure_name="failure_replay_verify_governance_receipt",
                family_name="delivery_replay_verify",
                run_1=_DELIVERY_BUNDLE,
                run_2=_DELIVERY_BUNDLE,
                optional_run_3=_DELIVERY_BUNDLE,
                stable_signals=[
                    "replay_verify_governance_closure_stability",
                    "governance_receipt_path_and_field_stability",
                    "usage_provenance_linkage_stability",
                    "cleanup_prevents_delivery_family_pollution",
                ],
                drift_risks=[
                    "residual_dlq_or_delivery_done_state",
                    "replay_db_reuse",
                    "governance_receipt_scope_reuse",
                    "residual_broker_or_service_target_state",
                ],
            ),
            _repeatability_entry(
                closure_name="poison_orphan_requeue_inspect_verify",
                family_name="queue_governance",
                run_1=_QUEUE_BUNDLE,
                run_2=_QUEUE_BUNDLE,
                optional_run_3=_QUEUE_BUNDLE,
                stable_signals=[
                    "inspect_read_only_stability",
                    "spool_sqlite_support_difference_stability",
                    "queue_state_and_provenance_stability",
                    "governance_receipt_stability",
                ],
                drift_risks=[
                    "spool_root_reuse",
                    "sqlite_queue_root_reuse",
                    "receipt_scope_reuse",
                    "queue_state_pollution_between_runs",
                ],
            ),
            _repeatability_entry(
                closure_name="source_contract_id_usage_governance_provenance",
                family_name="source_deep",
                run_1=_SOURCE_BUNDLE,
                run_2=_SOURCE_BUNDLE,
                optional_run_3=_SOURCE_BUNDLE,
                stable_signals=[
                    "source_contract_id_stability",
                    "usage_linkage_stability",
                    "provenance_linkage_stability",
                    "governance_visibility_stability",
                ],
                drift_risks=[
                    "remote_state_residue",
                    "checkpoint_or_progress_reuse",
                    "shared_output_scope_reuse",
                    "source_case_identity_collision",
                ],
            ),
            _repeatability_entry(
                closure_name="fanout_partial_failure_shared_summary_governance_usage",
                family_name="fanout_composition",
                run_1=_FANOUT_BUNDLE,
                run_2=_FANOUT_BUNDLE,
                optional_run_3=_FANOUT_BUNDLE,
                stable_signals=[
                    "fanout_composition_truth_stability",
                    "shared_failure_summary_stability",
                    "child_and_aggregate_visibility_stability",
                    "usage_governance_visibility_stability",
                ],
                drift_risks=[
                    "child_output_scope_mixing",
                    "aggregate_output_scope_mixing",
                    "filesystem_control_scope_reuse",
                    "fanout_case_identity_collision",
                ],
            ),
        ],
    }


def build_p5_m5_s3_cleanup_repetition_rules() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S3 cleanup repetition rules",
        "anchor_rules": {
            "webhook": {
                "run_carrier_bundle": list(_DELIVERY_BUNDLE),
                "cleanup_step": (
                    "fresh out_root plus fresh _dlq/delivery, delivery_done, and receipt scope"
                ),
                "rerun_carrier_bundle": list(_DELIVERY_BUNDLE),
                "compare_artifacts": [
                    "delivery receipt paths",
                    "governance action receipt fields",
                    "usage linkage paths",
                ],
                "dirty_state_pollution": [
                    "residual delivery_done artifacts",
                    "reused governance action scope",
                ],
            },
            "kafka": {
                "run_carrier_bundle": list(_DELIVERY_BUNDLE),
                "cleanup_step": "fresh topic/run identity, fresh replay.db, fresh receipt scope",
                "rerun_carrier_bundle": list(_DELIVERY_BUNDLE),
                "compare_artifacts": [
                    "replay.db location",
                    "delivery/governance receipt linkage",
                    "usage linkage fields",
                ],
                "dirty_state_pollution": [
                    "residual broker progress",
                    "reused replay.db",
                    "reused governance receipt scope",
                ],
            },
            "opensearch": {
                "run_carrier_bundle": list(_DELIVERY_BUNDLE),
                "cleanup_step": "fresh index namespace or unique isolation before rerun",
                "rerun_carrier_bundle": list(_DELIVERY_BUNDLE),
                "compare_artifacts": [
                    "target namespace stability",
                    "governance receipt linkage",
                ],
                "dirty_state_pollution": [
                    "residual index state",
                    "namespace reuse across reruns",
                ],
            },
            "spool_queue": {
                "run_carrier_bundle": list(_QUEUE_BUNDLE),
                "cleanup_step": "fresh spool root and fresh receipt scope",
                "rerun_carrier_bundle": list(_QUEUE_BUNDLE),
                "compare_artifacts": [
                    "spool bucket contents",
                    "inspect output fields",
                    "governance receipt support level",
                ],
                "dirty_state_pollution": [
                    "bucket reuse across runs",
                    "residual poison or inflight files",
                ],
            },
            "sqlite_queue": {
                "run_carrier_bundle": list(_QUEUE_BUNDLE),
                "cleanup_step": "fresh queue.sqlite3 or queue root before rerun",
                "rerun_carrier_bundle": list(_QUEUE_BUNDLE),
                "compare_artifacts": [
                    "queue.sqlite3 location",
                    "inspect output fields",
                    "result_only support truth",
                ],
                "dirty_state_pollution": [
                    "sqlite queue root reuse",
                    "residual inflight rows",
                ],
            },
            "replay_db": {
                "run_carrier_bundle": list(_DELIVERY_BUNDLE),
                "cleanup_step": "fresh replay.db path per rerun",
                "rerun_carrier_bundle": list(_DELIVERY_BUNDLE),
                "compare_artifacts": ["replay.db path", "replay governance receipt linkage"],
                "dirty_state_pollution": ["shared replay.db across reruns"],
            },
            "governance_receipts": {
                "run_carrier_bundle": list(_DELIVERY_BUNDLE + _QUEUE_BUNDLE + _FANOUT_BUNDLE),
                "cleanup_step": "fresh _governance/actions scope before rerun",
                "rerun_carrier_bundle": list(_DELIVERY_BUNDLE + _QUEUE_BUNDLE + _FANOUT_BUNDLE),
                "compare_artifacts": ["receipt path stability", "receipt field stability"],
                "dirty_state_pollution": ["action receipt scope reuse"],
            },
            "google_drive_document_v1": {
                "run_carrier_bundle": list(_SOURCE_BUNDLE),
                "cleanup_step": "fresh case identity and fresh local output scope before rerun",
                "rerun_carrier_bundle": list(_SOURCE_BUNDLE),
                "compare_artifacts": [
                    "source contract id linkage",
                    "usage linkage fields",
                    "provenance linkage fields",
                ],
                "dirty_state_pollution": [
                    "remote selection residue",
                    "shared output scope reuse",
                ],
            },
            "confluence_document_v1": {
                "run_carrier_bundle": list(_SOURCE_BUNDLE),
                "cleanup_step": "fresh case identity and fresh local output scope before rerun",
                "rerun_carrier_bundle": list(_SOURCE_BUNDLE),
                "compare_artifacts": [
                    "source contract id linkage",
                    "usage linkage fields",
                    "provenance linkage fields",
                ],
                "dirty_state_pollution": [
                    "remote page-selection residue",
                    "shared output scope reuse",
                ],
            },
            "kafka_partition_offset_v1": {
                "run_carrier_bundle": list(_SOURCE_BUNDLE),
                "cleanup_step": "fresh topic/run/checkpoint identity before rerun",
                "rerun_carrier_bundle": list(_SOURCE_BUNDLE),
                "compare_artifacts": [
                    "source contract id linkage",
                    "checkpoint/provenance linkage",
                ],
                "dirty_state_pollution": [
                    "checkpoint reuse",
                    "consumer progress reuse",
                ],
            },
            "fanout": {
                "run_carrier_bundle": list(_FANOUT_BUNDLE),
                "cleanup_step": "fresh child and aggregate output scope before rerun",
                "rerun_carrier_bundle": list(_FANOUT_BUNDLE),
                "compare_artifacts": [
                    "aggregate shared summary",
                    "child/aggregate output visibility",
                    "usage/governance linkage",
                ],
                "dirty_state_pollution": [
                    "child output scope mixing",
                    "aggregate output scope reuse",
                ],
            },
            "filesystem": {
                "run_carrier_bundle": list(_FANOUT_BUNDLE),
                "cleanup_step": "fresh filesystem control out_root before rerun",
                "rerun_carrier_bundle": list(_FANOUT_BUNDLE),
                "compare_artifacts": ["artifact path stability", "child output segregation"],
                "dirty_state_pollution": ["out_root reuse across reruns"],
            },
        },
    }


def build_p5_m5_s3_stability_signals() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S3 stability signals",
        "signals": {
            "artifact_path_stability": {
                "meaning": (
                    "reruns keep case-scoped output paths stable in shape "
                    "without reusing prior scope"
                ),
                "validated_by": [
                    "delivery family reruns",
                    "fanout reruns",
                    "cleanup repetition rules",
                ],
            },
            "usage_linkage_stability": {
                "meaning": "usage linkage fields remain stable across reruns",
                "validated_by": ["source deep family", "delivery family", "fanout family"],
            },
            "provenance_linkage_stability": {
                "meaning": "provenance linkage remains stable across reruns",
                "validated_by": ["source deep family", "delivery family"],
            },
            "governance_receipt_stability": {
                "meaning": "governance receipt path and core fields stay stable across reruns",
                "validated_by": [
                    "delivery family",
                    "queue family",
                    "fanout family",
                    "artifact checks",
                ],
            },
            "queue_state_stability": {
                "meaning": "queue state and inspect semantics do not drift across reruns",
                "validated_by": ["queue family"],
            },
            "source_contract_id_stability": {
                "meaning": (
                    "source contract ids remain stable across reruns for selected source anchors"
                ),
                "validated_by": ["source deep family"],
            },
            "fanout_aggregate_shared_summary_stability": {
                "meaning": "fanout aggregate/shared-summary behavior remains stable across reruns",
                "validated_by": ["fanout family"],
            },
            "helper_report_artifact_check_stability": {
                "meaning": "artifact helpers keep reporting the same S3 truth across reruns",
                "validated_by": ["s3 artifact tests", "helper --check-artifacts"],
            },
        },
        "false_stability_controls": [
            "fresh basetemp and cache scope per rerun",
            "fresh out_root and receipt scope per rerun",
            "fresh replay.db per rerun",
            "fresh spool/sqlite roots per rerun",
            "fresh topic/run/checkpoint identity per rerun",
            "fresh child/aggregate output scope per rerun",
        ],
        "not_claimed": [
            "distributed runtime stability",
            "cloud or Kubernetes load test stability",
            "new anchor discovery",
            "large-scale concurrency load testing",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5M5S3Check:
    name: str
    ok: bool
    detail: str
    severity: M5S3Severity = "error"


def build_p5_m5_s3_execution_plan() -> dict[str, object]:
    repeatability = build_p5_m5_s3_repeatability_carriers()
    closures = cast(dict[str, object], build_p5_m5_s2_deep_closures()["closures"])
    return {
        "schema_version": 1,
        "phase": "P5-M5-S3 repeatability cleanup bounded soak",
        "status": "stability_layer_only",
        "strategy": "repeat_representative_closures_not_new_anchor_discovery",
        "reused_s2_closures": list(closures.keys()),
        "execution_size": {
            "repeated_deep_closure_units": 4,
            "cleanup_repetition_units": 12,
            "bounded_soak_family_units": 4,
            "stability_sized_total_rough": 20,
        },
        "round_counts": {
            "mandatory_rounds_per_family": 2,
            "optional_rounds_per_family": 1,
        },
        "repeatability_carriers_artifact": "validation/p5_m5_s3_repeatability_carriers.json",
        "cleanup_repetition_rules_artifact": ("validation/p5_m5_s3_cleanup_repetition_rules.json"),
        "stability_signals_artifact": "validation/p5_m5_s3_stability_signals.json",
        "bounded_soak_families": [
            "delivery_replay_verify",
            "queue_governance",
            "source_deep",
            "fanout_composition",
        ],
        "grouped_execution_commands": [
            {
                "name": "delivery_round_1",
                "family": "delivery_replay_verify",
                "round": 1,
                "command": ("uv run --locked --no-sync pytest -n 0 " + " ".join(_DELIVERY_BUNDLE)),
            },
            {
                "name": "delivery_round_2",
                "family": "delivery_replay_verify",
                "round": 2,
                "command": ("uv run --locked --no-sync pytest -n 0 " + " ".join(_DELIVERY_BUNDLE)),
            },
            {
                "name": "queue_round_1",
                "family": "queue_governance",
                "round": 1,
                "command": "uv run --locked --no-sync pytest -n 0 " + " ".join(_QUEUE_BUNDLE),
            },
            {
                "name": "queue_round_2",
                "family": "queue_governance",
                "round": 2,
                "command": "uv run --locked --no-sync pytest -n 0 " + " ".join(_QUEUE_BUNDLE),
            },
            {
                "name": "source_round_1",
                "family": "source_deep",
                "round": 1,
                "command": "uv run --locked --no-sync pytest -n 0 " + " ".join(_SOURCE_BUNDLE),
            },
            {
                "name": "source_round_2",
                "family": "source_deep",
                "round": 2,
                "command": "uv run --locked --no-sync pytest -n 0 " + " ".join(_SOURCE_BUNDLE),
            },
            {
                "name": "fanout_round_1",
                "family": "fanout_composition",
                "round": 1,
                "command": "uv run --locked --no-sync pytest -n 0 " + " ".join(_FANOUT_BUNDLE),
            },
            {
                "name": "fanout_round_2",
                "family": "fanout_composition",
                "round": 2,
                "command": "uv run --locked --no-sync pytest -n 0 " + " ".join(_FANOUT_BUNDLE),
            },
            {
                "name": "s3_artifact_drift",
                "family": "artifact_consistency",
                "round": 1,
                "command": (
                    "uv run --locked --no-sync pytest -n 0 "
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m5_s3.py"
                ),
            },
        ],
        "stability_focus": repeatability,
        "deferred_beyond_s3": [
            "distributed or cloud load testing",
            "new anchor discovery",
            "flat all-pairs rerun matrix",
            "cold-operator proof",
            "release-consumable hardening",
        ],
        "out_of_scope": [
            "distributed runtime",
            "cloud or Kubernetes packaging",
            "billing pricing entitlement",
            "RBAC approval workflow",
            "enterprise connector implementation",
        ],
        "fanout_truth": {
            "surface_type": "destination_composition_orchestration",
            "single_sink_connector": False,
        },
    }


def _read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded_obj, dict):
        return None
    return cast(dict[str, object], loaded_obj)


def _json_artifact_check(
    *,
    name: str,
    path: Path,
    expected: dict[str, object],
) -> P5M5S3Check:
    observed = _read_json_object(path)
    return P5M5S3Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _repeatability_carrier_files_exist() -> P5M5S3Check:
    carriers = build_p5_m5_s3_repeatability_carriers()
    paths: list[str] = []
    families = cast(list[dict[str, object]], carriers["repeatability_families"])
    for family in families:
        rounds = cast(dict[str, object], family["rounds"])
        for round_obj in rounds.values():
            carrier_set = cast(dict[str, object], round_obj)["carrier_set"]
            for carrier in cast(list[str], carrier_set):
                paths.append(carrier.split("::", 1)[0])
    missing = [path for path in dict.fromkeys(paths) if not (repo_root() / path).exists()]
    return P5M5S3Check(
        name="s3_repeatability_carrier_files_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s3_artifacts() -> list[P5M5S3Check]:
    checks = [
        _json_artifact_check(
            name="m5_s3_execution_plan_matches_helper",
            path=P5_M5_S3_EXECUTION_PLAN_PATH,
            expected=build_p5_m5_s3_execution_plan(),
        ),
        _json_artifact_check(
            name="m5_s3_repeatability_carriers_match_helper",
            path=P5_M5_S3_REPEATABILITY_CARRIERS_PATH,
            expected=build_p5_m5_s3_repeatability_carriers(),
        ),
        _json_artifact_check(
            name="m5_s3_cleanup_repetition_rules_match_helper",
            path=P5_M5_S3_CLEANUP_REPETITION_RULES_PATH,
            expected=build_p5_m5_s3_cleanup_repetition_rules(),
        ),
        _json_artifact_check(
            name="m5_s3_stability_signals_match_helper",
            path=P5_M5_S3_STABILITY_SIGNALS_PATH,
            expected=build_p5_m5_s3_stability_signals(),
        ),
        _repeatability_carrier_files_exist(),
    ]

    plan = build_p5_m5_s3_execution_plan()
    size = cast(dict[str, object], plan["execution_size"])
    round_counts = cast(dict[str, object], plan["round_counts"])
    checks.append(
        P5M5S3Check(
            name="s3_size_stays_stability_sized",
            ok=size["stability_sized_total_rough"] == 20,
            detail=str(size),
        )
    )
    checks.append(
        P5M5S3Check(
            name="s3_round_counts_are_frozen",
            ok=round_counts["mandatory_rounds_per_family"] == 2
            and round_counts["optional_rounds_per_family"] == 1,
            detail=str(round_counts),
        )
    )
    checks.append(
        P5M5S3Check(
            name="fanout_stays_composition_not_sink_in_s3",
            ok=cast(dict[str, object], plan["fanout_truth"])["single_sink_connector"] is False,
            detail=str(plan["fanout_truth"]),
        )
    )
    signals = build_p5_m5_s3_stability_signals()
    checks.append(
        P5M5S3Check(
            name="s3_stability_signals_are_unified",
            ok=len(cast(dict[str, object], signals["signals"])) == 8,
            detail=str(list(cast(dict[str, object], signals["signals"]).keys())),
        )
    )

    if P5_M5_S3_STABILITY_MATRIX_REPORT_PATH.exists():
        report_text = P5_M5_S3_STABILITY_MATRIX_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S3 stability-validates repeated representative closures only",
            "cleanup then rerun is explicit",
            "bounded same-host soak remains non-load-test",
            "fanout remains composition/orchestration, not a sink connector",
            "false-stability risk is controlled through fresh scope per rerun",
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S3Check(
                name="m5_s3_report_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )
    else:
        checks.append(
            P5M5S3Check(
                name="m5_s3_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S3_STABILITY_MATRIX_REPORT_PATH),
            )
        )

    return checks


def format_p5_m5_s3_results(results: list[P5M5S3Check]) -> str:
    lines = ["P5-M5-S3 stability checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s3_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S3 stability proof:",
            "- scope: repeated representative closures only",
            "- focus: repeatability, cleanup repetition, bounded same-host soak",
            (
                "- signals: artifact paths, usage/provenance, governance receipts, "
                "queue state, source ids, fanout summaries"
            ),
            ("- false stability is controlled by fresh basetemp/out_root/receipt/checkpoint scope"),
            "- fanout remains composition/orchestration, not a sink connector",
            "- no distributed or cloud load testing, and no new anchor discovery",
        )
    )


def render_p5_m5_s3_json() -> str:
    bundle = {
        "execution_plan": build_p5_m5_s3_execution_plan(),
        "repeatability_carriers": build_p5_m5_s3_repeatability_carriers(),
        "cleanup_repetition_rules": build_p5_m5_s3_cleanup_repetition_rules(),
        "stability_signals": build_p5_m5_s3_stability_signals(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
