from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

M5S1Severity = Literal["error", "warning", "info"]

P5_M5_S1_EXECUTION_PLAN_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s1_execution_plan.json"
)
P5_M5_S1_BASELINE_MATRIX_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M5_S1_BASELINE_MATRIX.md"
)
P5_M5_S1_EXACT_CARRIERS_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s1_exact_carriers.json"
)
P5_M5_S1_CLEANUP_RULES_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_m5_s1_cleanup_rules.json"
)

_UNS_SOURCE_CARRIERS: Final[tuple[tuple[str, str], ...]] = (
    ("http_document_v1", "packages/uns-stream/src/uns_stream/tests/test_uns_http_source.py"),
    ("s3_document_v1", "packages/uns-stream/src/uns_stream/tests/test_uns_s3_source.py"),
    ("git_document_v1", "packages/uns-stream/src/uns_stream/tests/test_uns_git_source.py"),
    (
        "google_drive_document_v1",
        "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py",
    ),
    (
        "confluence_document_v1",
        "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py",
    ),
)
_IT_SOURCE_CARRIERS: Final[tuple[tuple[str, str], ...]] = (
    ("http_json_cursor_v1", "packages/it-stream/src/it_stream/tests/test_http_source.py"),
    (
        "postgresql_incremental_v1",
        "packages/it-stream/src/it_stream/tests/test_postgresql_source.py",
    ),
    (
        "clickhouse_incremental_v1",
        "packages/it-stream/src/it_stream/tests/test_clickhouse_source.py",
    ),
    (
        "kafka_partition_offset_v1",
        "packages/it-stream/src/it_stream/tests/test_kafka_source.py",
    ),
    (
        "mongodb_incremental_v1",
        "packages/it-stream/src/it_stream/tests/test_mongodb_source.py",
    ),
)
_DESTINATION_CARRIERS: Final[tuple[tuple[str, str], ...]] = (
    (
        "filesystem",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py",
    ),
    (
        "webhook",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py",
    ),
    ("sqlite", "packages/zephyr-ingest/src/zephyr_ingest/tests/test_sqlite_destination.py"),
    ("s3", "packages/zephyr-ingest/src/zephyr_ingest/tests/test_s3_destination.py"),
    (
        "opensearch",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_opensearch_destination.py",
    ),
    (
        "weaviate",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_weaviate_destination.py",
    ),
    (
        "clickhouse",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_clickhouse_destination.py",
    ),
    (
        "mongodb",
        "packages/zephyr-ingest/src/zephyr_ingest/tests/test_mongodb_destination.py",
    ),
    ("loki", "packages/zephyr-ingest/src/zephyr_ingest/tests/test_loki_destination.py"),
    ("kafka", "packages/zephyr-ingest/src/zephyr_ingest/tests/test_kafka_destination.py"),
)
_FANOUT_CARRIER: Final[str] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py"
)
_LIGHT_VISIBILITY_CARRIERS: Final[tuple[str, ...]] = (
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py",
    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py",
)

_S1_EXECUTION_COMMANDS: Final[tuple[dict[str, object], ...]] = (
    {
        "name": "uns_source_baseline",
        "scope": "all retained uns success + abnormal carriers",
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/uns-stream/src/uns_stream/tests/test_uns_http_source.py "
            "packages/uns-stream/src/uns_stream/tests/test_uns_s3_source.py "
            "packages/uns-stream/src/uns_stream/tests/test_uns_git_source.py "
            "packages/uns-stream/src/uns_stream/tests/test_uns_google_drive_source.py "
            "packages/uns-stream/src/uns_stream/tests/test_uns_confluence_source.py"
        ),
    },
    {
        "name": "it_source_baseline",
        "scope": "all retained it success + abnormal carriers",
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/it-stream/src/it_stream/tests/test_http_source.py "
            "packages/it-stream/src/it_stream/tests/test_postgresql_source.py "
            "packages/it-stream/src/it_stream/tests/test_clickhouse_source.py "
            "packages/it-stream/src/it_stream/tests/test_kafka_source.py "
            "packages/it-stream/src/it_stream/tests/test_mongodb_source.py"
        ),
    },
    {
        "name": "destination_and_fanout_baseline",
        "scope": "all retained single-sink baseline success carriers plus fanout success/failure",
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_webhook.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_sqlite_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_s3_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_opensearch_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_weaviate_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_clickhouse_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_mongodb_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_loki_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_kafka_destination.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py"
        ),
    },
    {
        "name": "light_visibility_support",
        "scope": "usage/provenance/governance visibility stays light rather than deep walkthrough",
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_governance_audit.py"
        ),
    },
    {
        "name": "s1_artifact_drift",
        "scope": "S1 artifact/helper drift protection",
        "command": (
            "uv run --locked --no-sync pytest -n 0 "
            "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m5_s1.py"
        ),
    },
)


def _source_carrier_entry(
    *,
    source_id: str,
    flow_family: Literal["uns", "it"],
    carrier_path: str,
) -> dict[str, object]:
    return {
        "source_id": source_id,
        "flow_family": flow_family,
        "success_carrier": carrier_path,
        "abnormal_carrier": carrier_path,
        "carrier_granularity": "file_level_exact_carrier",
    }


def build_p5_m5_s1_exact_carriers() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S1 exact baseline carriers",
        "carrier_granularity": "file_level_exact_carrier",
        "retained_source_carriers": {
            "uns_stream": [
                _source_carrier_entry(
                    source_id=source_id,
                    flow_family="uns",
                    carrier_path=carrier_path,
                )
                for source_id, carrier_path in _UNS_SOURCE_CARRIERS
            ],
            "it_stream": [
                _source_carrier_entry(
                    source_id=source_id,
                    flow_family="it",
                    carrier_path=carrier_path,
                )
                for source_id, carrier_path in _IT_SOURCE_CARRIERS
            ],
        },
        "retained_destination_success_carriers": [
            {
                "destination": destination,
                "surface_type": "single_sink_destination_connector",
                "success_carrier": carrier_path,
            }
            for destination, carrier_path in _DESTINATION_CARRIERS
        ],
        "fanout": {
            "surface_type": "destination_composition_orchestration",
            "single_sink_connector": False,
            "success_carrier": _FANOUT_CARRIER,
            "partial_or_failure_carrier": _FANOUT_CARRIER,
        },
        "light_visibility_support": {
            "governance_visibility_level": "light",
            "support_carriers": list(_LIGHT_VISIBILITY_CARRIERS),
        },
    }


def build_p5_m5_s1_cleanup_rules() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M5-S1 cleanup and isolation rules",
        "rules": {
            "filesystem_outputs": {
                "rule": "independent out_root, basetemp, or case output dir per run",
                "applies_to": [
                    "filesystem destination carriers",
                    "usage/provenance output chains",
                    "fanout child outputs",
                ],
            },
            "opensearch_targets": {
                "rule": "independent index or unique run-specific target with explicit cleanup",
                "applies_to": ["opensearch destination carrier"],
            },
            "sqlite_queue": {
                "rule": "independent queue.sqlite3 or queue root per case",
                "applies_to": ["sqlite queue governance helpers"],
            },
            "spool_queue": {
                "rule": "independent spool root; do not reuse buckets across cases",
                "applies_to": ["spool queue governance helpers"],
            },
            "replay_db": {
                "rule": "independent replay.db per replay-supporting carrier",
                "applies_to": ["replay-related support carriers"],
            },
            "governance_receipts": {
                "rule": "independent _governance/actions/<action_id>.json scope per run",
                "applies_to": ["light governance visibility outputs"],
            },
            "fanout_outputs": {
                "rule": "child outputs and aggregate artifacts stay case-scoped together",
                "applies_to": ["fanout success and partial/failure carrier"],
            },
            "saas_and_service_live": {
                "rule": (
                    "independent case identity; no reliance on residual remote state; "
                    "use explicit cleanup or unique isolation when needed"
                ),
                "applies_to": [
                    "google drive source carrier",
                    "confluence source carrier",
                    "webhook/opensearch/weaviate/kafka service-style destinations",
                ],
            },
        },
        "not_allowed": [
            "reuse dirty residual state",
            "shared out_root across independent baseline cases",
            "fanout child output collision across cases",
            "implicit remote cleanup assumptions",
        ],
    }


def build_p5_m5_s1_execution_plan() -> dict[str, object]:
    exact_carriers = build_p5_m5_s1_exact_carriers()
    return {
        "schema_version": 1,
        "phase": "P5-M5-S1 retained-surface baseline execution",
        "status": "baseline_layer_only",
        "scope": {
            "layer": "layer_1_baseline_coverage",
            "deep_validation": "deferred_to_s2",
            "repeatability_soak_cleanup_execution": "deferred_to_s3",
            "governance_visibility": "light",
        },
        "execution_size": {
            "source_success_units": 10,
            "source_abnormal_units": 10,
            "destination_success_units": 10,
            "fanout_units": 2,
            "baseline_execution_units_total": 32,
            "light_visibility_support_units": 2,
        },
        "exact_carrier_artifact": "validation/p5_m5_s1_exact_carriers.json",
        "cleanup_rules_artifact": "validation/p5_m5_s1_cleanup_rules.json",
        "covered_chains": {
            "source_to_usage_record": {
                "carrier": (
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_usage_runtime.py"
                ),
                "coverage": "source -> task -> run -> delivery -> usage_record.json",
            },
            "source_abnormal_to_usage_failure": {
                "carrier_policy": "source-specific abnormal carriers",
                "coverage": (
                    "source abnormal/failure -> run_meta.json/delivery_receipt.json -> "
                    "usage failure surface"
                ),
            },
            "destination_success": {
                "carrier_policy": "retained destination baseline success carriers",
            },
            "fanout_success": {
                "carrier": _FANOUT_CARRIER,
                "surface_type": "composition_orchestration",
            },
            "fanout_partial_or_failure": {
                "carrier": _FANOUT_CARRIER,
                "surface_type": "composition_orchestration",
            },
            "light_governance_visibility": {
                "carriers": list(_LIGHT_VISIBILITY_CARRIERS),
                "not": "full replay/requeue/verify walkthrough per retained source",
            },
        },
        "deferred_boundaries": {
            "s2_not_run_here": [
                "representative deep source validation",
                "delivery failure -> replay -> verify deep walkthrough",
                "poison/orphan -> requeue -> inspect -> verify deep walkthrough",
            ],
            "s3_not_run_here": [
                "repeatability execution",
                "soak execution",
                "cleanup repetition proof",
            ],
        },
        "grouped_execution_commands": list(_S1_EXECUTION_COMMANDS),
        "fanout_truth": {
            "surface_type": "destination_composition_orchestration",
            "single_sink_connector": False,
        },
        "excluded_scope": [
            "distributed runtime",
            "cloud or Kubernetes packaging",
            "billing pricing entitlement",
            "RBAC approval workflow",
            "enterprise connector implementation",
        ],
        "exact_carriers": exact_carriers,
    }


@dataclass(frozen=True, slots=True)
class P5M5S1Check:
    name: str
    ok: bool
    detail: str
    severity: M5S1Severity = "error"


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
) -> P5M5S1Check:
    observed = _read_json_object(path)
    return P5M5S1Check(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def _carrier_files_exist() -> P5M5S1Check:
    exact_carriers = build_p5_m5_s1_exact_carriers()
    source_groups = cast(dict[str, object], exact_carriers["retained_source_carriers"])
    paths = [
        cast(str, entry["success_carrier"])
        for group in source_groups.values()
        for entry in cast(list[dict[str, object]], group)
    ]
    paths.extend(
        cast(str, entry["success_carrier"])
        for entry in cast(
            list[dict[str, object]], exact_carriers["retained_destination_success_carriers"]
        )
    )
    fanout = cast(dict[str, object], exact_carriers["fanout"])
    paths.extend(
        [
            cast(str, fanout["success_carrier"]),
            cast(str, fanout["partial_or_failure_carrier"]),
        ]
    )
    paths.extend(
        cast(
            list[str],
            cast(dict[str, object], exact_carriers["light_visibility_support"])["support_carriers"],
        )
    )
    missing = [path for path in dict.fromkeys(paths) if not (repo_root() / path).exists()]
    return P5M5S1Check(
        name="s1_exact_carrier_files_exist",
        ok=not missing,
        detail="missing=" + (", ".join(missing) if missing else "none"),
    )


def validate_p5_m5_s1_artifacts() -> list[P5M5S1Check]:
    checks = [
        _json_artifact_check(
            name="m5_s1_execution_plan_matches_helper",
            path=P5_M5_S1_EXECUTION_PLAN_PATH,
            expected=build_p5_m5_s1_execution_plan(),
        ),
        _json_artifact_check(
            name="m5_s1_exact_carriers_match_helper",
            path=P5_M5_S1_EXACT_CARRIERS_PATH,
            expected=build_p5_m5_s1_exact_carriers(),
        ),
        _json_artifact_check(
            name="m5_s1_cleanup_rules_match_helper",
            path=P5_M5_S1_CLEANUP_RULES_PATH,
            expected=build_p5_m5_s1_cleanup_rules(),
        ),
        _carrier_files_exist(),
    ]

    plan = build_p5_m5_s1_execution_plan()
    size = cast(dict[str, object], plan["execution_size"])
    checks.append(
        P5M5S1Check(
            name="s1_baseline_size_stays_32_units",
            ok=size["baseline_execution_units_total"] == 32,
            detail=str(size),
        )
    )
    scope = cast(dict[str, object], plan["scope"])
    checks.append(
        P5M5S1Check(
            name="s1_governance_visibility_stays_light",
            ok=scope["governance_visibility"] == "light",
            detail=cast(str, scope["governance_visibility"]),
        )
    )
    fanout_truth = cast(dict[str, object], plan["fanout_truth"])
    checks.append(
        P5M5S1Check(
            name="fanout_stays_composition_not_sink_in_s1",
            ok=fanout_truth["single_sink_connector"] is False,
            detail=str(fanout_truth),
        )
    )
    deferred = cast(dict[str, object], plan["deferred_boundaries"])
    checks.append(
        P5M5S1Check(
            name="s1_deep_and_repeatability_work_stays_deferred",
            ok=bool(cast(list[object], deferred["s2_not_run_here"]))
            and bool(cast(list[object], deferred["s3_not_run_here"])),
            detail=str(deferred),
        )
    )

    if P5_M5_S1_BASELINE_MATRIX_REPORT_PATH.exists():
        report_text = P5_M5_S1_BASELINE_MATRIX_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "S1 runs layer 1 baseline coverage only",
            "fanout is composition/orchestration, not a sink connector",
            "light governance visibility is preserved",
            "S2 representative deep validation is deferred",
            "S3 repeatability/soak/cleanup execution is deferred",
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5M5S1Check(
                name="m5_s1_report_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )
    else:
        checks.append(
            P5M5S1Check(
                name="m5_s1_report_required_phrases",
                ok=False,
                detail=str(P5_M5_S1_BASELINE_MATRIX_REPORT_PATH),
            )
        )

    return checks


def format_p5_m5_s1_results(results: list[P5M5S1Check]) -> str:
    lines = ["P5-M5-S1 baseline matrix checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_m5_s1_summary() -> str:
    return "\n".join(
        (
            "P5-M5-S1 retained-surface baseline execution:",
            "- scope: layer 1 baseline coverage only",
            "- carriers: 10 source success, 10 source abnormal, 10 destination success, 2 fanout",
            "- visibility: usage/provenance plus light governance visibility",
            "- fanout is composition/orchestration, not a sink connector",
            "- S2 deep representative walkthroughs remain deferred",
            "- S3 repeatability/soak/cleanup execution remains deferred",
        )
    )


def render_p5_m5_s1_json() -> str:
    bundle = {
        "execution_plan": build_p5_m5_s1_execution_plan(),
        "exact_carriers": build_p5_m5_s1_exact_carriers(),
        "cleanup_rules": build_p5_m5_s1_cleanup_rules(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
