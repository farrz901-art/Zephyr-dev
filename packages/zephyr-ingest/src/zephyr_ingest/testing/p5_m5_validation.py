from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p5_capability_domains import (
    DESTINATION_CONNECTORS,
    IT_STREAM_SOURCE_CONNECTORS,
    UNS_STREAM_SOURCE_CONNECTORS,
)
from zephyr_ingest.testing.p5_product_cuts import DESTINATION_COMPOSITION_SURFACES
from zephyr_ingest.testing.p45 import repo_root

M5ValidationSeverity = Literal["error", "warning", "info"]

P5_M5_VALIDATION_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_validation_matrix.json"
)
P5_M5_VALIDATION_CHARTER_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_VALIDATION_CHARTER.md"
)
P5_M5_RETAINED_SURFACE_INVENTORY_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_retained_surface_inventory.json"
)
P5_M5_DEEP_ANCHOR_SELECTION_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_deep_anchor_selection.json"
)

M5_EXCLUDED_SCOPE: Final[tuple[str, ...]] = (
    "distributed runtime",
    "cloud or Kubernetes packaging",
    "billing pricing entitlement",
    "RBAC approval workflow",
    "enterprise connector implementation",
    "real deep matrix execution in S0",
    "benchmark expansion in S0",
    "release bundle implementation in S0",
)

M5_MUST_VALIDATE_CHAINS: Final[tuple[str, ...]] = (
    "source -> task -> run -> delivery -> usage_record.json",
    (
        "source failure/abnormal path -> run_meta.json/delivery_receipt.json -> "
        "usage failure surface"
    ),
    "delivery failure -> replay -> verify -> governance receipt",
    "poison/orphan -> requeue -> inspect -> verify",
    "batch summary -> batch usage record -> governance/provenance visibility",
    "source contract id -> usage record -> governance receipt",
)


def build_p5_m5_retained_surface_inventory() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S0 retained-surface inventory freeze",
        "inventory_status": "frozen_for_m5_validation_planning",
        "m4_closeout_dependency": "P5-M4 closeout gate resolved; M5 does not reopen M4 debt",
        "retained_sources": {
            "uns_stream": [
                {
                    "source_id": source_id,
                    "flow_family": "uns",
                    "domain": "base",
                    "enterprise_grade_today": False,
                    "m5_expectations": [
                        "at_least_one_success_path",
                        "at_least_one_abnormal_or_failure_path",
                        "usage_and_provenance_visibility",
                    ],
                }
                for source_id in UNS_STREAM_SOURCE_CONNECTORS
            ],
            "it_stream": [
                {
                    "source_id": source_id,
                    "flow_family": "it",
                    "domain": "base",
                    "enterprise_grade_today": False,
                    "m5_expectations": [
                        "at_least_one_success_path",
                        "at_least_one_abnormal_or_failure_path",
                        "usage_and_provenance_visibility",
                    ],
                }
                for source_id in IT_STREAM_SOURCE_CONNECTORS
            ],
        },
        "retained_destinations": {
            "single_sink_destinations": [
                {
                    "destination": destination,
                    "surface_type": "single_sink_destination_connector",
                    "domain": "base",
                    "enterprise_grade_today": False,
                    "m5_expectations": ["at_least_one_baseline_success_chain"],
                }
                for destination in DESTINATION_CONNECTORS
            ],
            "composition_orchestration_surfaces": [
                {
                    "destination": "fanout",
                    "surface_type": "destination_composition_orchestration",
                    "single_sink_connector": False,
                    "domain": "base_composition_surface",
                    "m5_expectations": [
                        "at_least_one_success_chain",
                        "at_least_one_partial_or_failure_chain",
                    ],
                }
            ],
        },
        "runtime_governance_usage_provenance_surfaces": [
            "TaskV1",
            "FlowProcessor",
            "Delivery",
            "run_meta.json",
            "delivery_receipt.json",
            "batch_report.json",
            "usage_record.json",
            "queue.sqlite3",
            "spool queue",
            "queue inspect",
            "queue requeue",
            "replay delivery",
            "verify recovery result",
            "_governance/actions/<action_id>.json",
            "_dlq/delivery",
        ],
        "capability_domain_assumptions": {
            "current_retained_sources": "base_non_enterprise",
            "current_retained_destinations": "base_non_enterprise",
            "technical_domains_not_commercial": True,
            "pro": "base plus current uns-stream optional extras; not a default M5 execution claim",
            "extra": "incubating/non-default; not part of retained M5 execution surface",
        },
        "excluded_scope": list(M5_EXCLUDED_SCOPE),
    }


def build_p5_m5_deep_anchor_selection() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S0 representative deep-anchor selection freeze",
        "selection_strategy": "representative_layered_anchors_not_equal_depth_all_pairs",
        "anchors": {
            "filesystem": {
                "anchor_type": "lightweight_baseline_destination",
                "why": (
                    "low-cost artifact path for repeated execution and baseline output stability"
                ),
                "depth": "deep_anchor",
            },
            "opensearch": {
                "anchor_type": "real_service_destination",
                "why": "service-backed sink requiring cleanup/isolation discipline",
                "depth": "deep_anchor",
            },
            "webhook": {
                "anchor_type": "delivery_failure_replay_verify",
                "why": "simple failure surface for replay and verify governance checks",
                "depth": "representative_deep_anchor",
            },
            "kafka": {
                "anchor_type": "delivery_failure_replay_verify",
                "why": (
                    "structured sink failure/replay anchor without claiming "
                    "broker-runtime ownership"
                ),
                "depth": "representative_deep_anchor",
            },
            "spool_queue": {
                "anchor_type": "poison_orphan_requeue_inspect_verify",
                "why": "local artifact queue governance anchor",
                "depth": "representative_deep_anchor",
            },
            "sqlite_queue": {
                "anchor_type": "poison_orphan_requeue_inspect_verify",
                "why": "same-host/shared-storage queue governance anchor",
                "depth": "representative_deep_anchor",
            },
            "google_drive_document_v1": {
                "anchor_type": "representative_uns_saas_source",
                "why": "bounded SaaS document acquisition with source->usage visibility",
                "depth": "representative_deep_anchor",
            },
            "confluence_document_v1": {
                "anchor_type": "representative_uns_saas_source",
                "why": "knowledge-space acquisition with bounded source/provenance pressure",
                "depth": "representative_deep_anchor",
            },
            "kafka_partition_offset_v1": {
                "anchor_type": "representative_it_streaming_source",
                "why": (
                    "bounded partition/offset source pressure without distributed consumer claims"
                ),
                "depth": "representative_deep_anchor",
            },
            "fanout": {
                "anchor_type": "destination_composition_orchestration",
                "why": (
                    "composition aggregate and partial-failure behavior must not be read as a sink"
                ),
                "depth": "composition_deep_anchor",
                "single_sink_connector": False,
            },
        },
        "non_deep_surfaces": {
            "coverage": "baseline_success_and_abnormal_paths_as_applicable",
            "not": "equal-depth all-pairs proof",
        },
        "excluded_scope": list(M5_EXCLUDED_SCOPE),
    }


def build_p5_m5_validation_matrix() -> dict[str, object]:
    inventory = build_p5_m5_retained_surface_inventory()
    deep_anchors = build_p5_m5_deep_anchor_selection()
    return {
        "schema_version": 1,
        "phase": "P5-M5-S0 validation matrix freeze",
        "matrix_status": "charter_only_not_executed",
        "strategy": "layered_matrix_not_flat_all_pairs",
        "m4_closeout_dependency": "all M4 closeout gates resolved before M5 matrix execution",
        "retained_surface_inventory": "validation/p5_m5_retained_surface_inventory.json",
        "deep_anchor_selection": "validation/p5_m5_deep_anchor_selection.json",
        "layers": {
            "layer_1_baseline_coverage": {
                "sources": {
                    "retained_source_count": len(UNS_STREAM_SOURCE_CONNECTORS)
                    + len(IT_STREAM_SOURCE_CONNECTORS),
                    "expectations": [
                        "one success path per retained source",
                        "one abnormal/failure path per retained source",
                        "usage/provenance visibility per retained source",
                    ],
                },
                "destinations": {
                    "single_sink_destination_count": len(DESTINATION_CONNECTORS),
                    "expectations": [
                        "one baseline success chain per retained single-sink destination"
                    ],
                },
                "composition": {
                    "surfaces": list(DESTINATION_COMPOSITION_SURFACES),
                    "expectations": ["fanout success chain", "fanout partial/failure chain"],
                    "not": "single sink connector proof",
                },
            },
            "layer_2_representative_deep_validation": {
                "anchors": cast(dict[str, object], deep_anchors["anchors"]),
                "must_validate_chains": list(M5_MUST_VALIDATE_CHAINS),
            },
            "layer_3_repeatability_soak_cleanup": {
                "status": "future_m5_stage_not_s0_execution",
                "anchors": {
                    "filesystem": "repeated execution and artifact stability",
                    "opensearch": "cleanup/isolation/repeated service runs",
                    "queue_governance": "receipt/history/cleanup repeated validation",
                    "fanout": "repeated-run aggregate stability",
                    "representative_saas_sources": "limited repeat plus cleanup discipline",
                },
            },
        },
        "retained_sources": inventory["retained_sources"],
        "retained_destinations": inventory["retained_destinations"],
        "explicit_non_claims": list(M5_EXCLUDED_SCOPE),
    }


@dataclass(frozen=True, slots=True)
class P5M5ValidationCheck:
    name: str
    ok: bool
    detail: str
    severity: M5ValidationSeverity = "error"


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
) -> P5M5ValidationCheck:
    observed = _read_json_object(path)
    return P5M5ValidationCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_m5_validation_artifacts() -> list[P5M5ValidationCheck]:
    checks = [
        _json_artifact_check(
            name="m5_validation_matrix_matches_helper",
            path=P5_M5_VALIDATION_MATRIX_PATH,
            expected=build_p5_m5_validation_matrix(),
        ),
        _json_artifact_check(
            name="m5_retained_surface_inventory_matches_helper",
            path=P5_M5_RETAINED_SURFACE_INVENTORY_PATH,
            expected=build_p5_m5_retained_surface_inventory(),
        ),
        _json_artifact_check(
            name="m5_deep_anchor_selection_matches_helper",
            path=P5_M5_DEEP_ANCHOR_SELECTION_PATH,
            expected=build_p5_m5_deep_anchor_selection(),
        ),
    ]

    matrix = build_p5_m5_validation_matrix()
    checks.append(
        P5M5ValidationCheck(
            name="m5_strategy_is_layered_not_flat_all_pairs",
            ok=matrix["strategy"] == "layered_matrix_not_flat_all_pairs",
            detail=cast(str, matrix["strategy"]),
        )
    )

    inventory = build_p5_m5_retained_surface_inventory()
    destinations = cast(dict[str, object], inventory["retained_destinations"])
    sink_entries = cast(list[dict[str, object]], destinations["single_sink_destinations"])
    composition_entries = cast(
        list[dict[str, object]], destinations["composition_orchestration_surfaces"]
    )
    sink_names = [cast(str, entry["destination"]) for entry in sink_entries]
    composition_names = [cast(str, entry["destination"]) for entry in composition_entries]
    checks.append(
        P5M5ValidationCheck(
            name="fanout_is_composition_not_sink_in_m5_inventory",
            ok="fanout" in composition_names and "fanout" not in sink_names,
            detail=f"sinks={sink_names!r}; composition={composition_names!r}",
        )
    )

    exclusions = cast(list[object], matrix["explicit_non_claims"])
    missing_exclusions = [scope for scope in M5_EXCLUDED_SCOPE if scope not in exclusions]
    checks.append(
        P5M5ValidationCheck(
            name="m5_excluded_scope_is_explicit",
            ok=not missing_exclusions,
            detail="missing=" + (", ".join(missing_exclusions) if missing_exclusions else "none"),
        )
    )

    deep_anchors = cast(dict[str, object], build_p5_m5_deep_anchor_selection()["anchors"])
    required_anchors = (
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
    )
    missing_anchors = [anchor for anchor in required_anchors if anchor not in deep_anchors]
    checks.append(
        P5M5ValidationCheck(
            name="m5_required_deep_anchors_selected",
            ok=not missing_anchors,
            detail="missing=" + (", ".join(missing_anchors) if missing_anchors else "none"),
        )
    )

    if P5_M5_VALIDATION_CHARTER_PATH.exists():
        charter_text = P5_M5_VALIDATION_CHARTER_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "layered, not flat all-pairs",
            "fanout is composition/orchestration, not a single sink",
            "M5-S0 does not reopen M4 debt",
            "distributed runtime",
            "billing pricing entitlement",
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in charter_text]
        checks.append(
            P5M5ValidationCheck(
                name="m5_validation_charter_required_phrases",
                ok=not missing_phrases,
                detail=("missing=" + (", ".join(missing_phrases) if missing_phrases else "none")),
            )
        )
    else:
        checks.append(
            P5M5ValidationCheck(
                name="m5_validation_charter_required_phrases",
                ok=False,
                detail=str(P5_M5_VALIDATION_CHARTER_PATH),
            )
        )

    return checks


def format_p5_m5_validation_results(results: list[P5M5ValidationCheck]) -> str:
    lines = ["P5-M5-S0 validation charter checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_validation_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S0 retained-surface validation charter:",
            "- status: charter and matrix freeze only; no deep matrix execution",
            "- strategy: layered, not flat all-pairs",
            "- retained sources: all current Base non-enterprise uns/it sources",
            (
                "- retained destinations: current Base single sinks; "
                "fanout is composition/orchestration"
            ),
            (
                "- deep anchors: filesystem, opensearch, webhook/kafka, queues, "
                "google_drive, confluence, kafka source, fanout"
            ),
            (
                "- non-claims: distributed runtime, cloud/Kubernetes, "
                "billing/entitlement/RBAC, enterprise connectors"
            ),
        )
    )


def render_p5_m5_validation_json() -> str:
    bundle = {
        "validation_matrix": build_p5_m5_validation_matrix(),
        "retained_surface_inventory": build_p5_m5_retained_surface_inventory(),
        "deep_anchor_selection": build_p5_m5_deep_anchor_selection(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
