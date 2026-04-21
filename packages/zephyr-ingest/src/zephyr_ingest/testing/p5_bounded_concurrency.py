from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

BoundedConcurrencyStatus = Literal["evidenced", "bounded", "unsupported"]
BoundedConcurrencySeverity = Literal["error", "warning", "info"]

P5_BOUNDED_CONCURRENCY_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_bounded_concurrency_matrix.json"
)
P5_BOUNDED_CONCURRENCY_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S8_BOUNDED_CONCURRENCY.md"
)
P5_BOUNDED_CONCURRENCY_REQUIRED_SCENARIOS: Final[tuple[str, ...]] = (
    "3_to_4_worker_same_pending_contention",
    "backlog_drain_under_multi_worker",
    "stale_lock_plus_contention_same_host",
    "poison_requeue_under_contention",
    "replay_and_queue_governance_coexistence_under_contention",
)


def _scenario(
    *,
    name: str,
    status: BoundedConcurrencyStatus,
    direct_worker_count_evidence: int,
    target_behavior: str,
    inspectability_surfaces: tuple[str, ...],
    operator_readability_facts: tuple[str, ...],
    evidence: tuple[str, ...],
) -> dict[str, object]:
    return {
        "name": name,
        "status": status,
        "direct_worker_count_evidence": direct_worker_count_evidence,
        "target_behavior": target_behavior,
        "inspectability_surfaces": list(inspectability_surfaces),
        "operator_readability_facts": list(operator_readability_facts),
        "evidence": list(evidence),
    }


def build_p5_bounded_concurrency_matrix() -> dict[str, object]:
    scenarios = (
        _scenario(
            name="3_to_4_worker_same_pending_contention",
            status="evidenced",
            direct_worker_count_evidence=4,
            target_behavior=(
                "Four local polling workers contend against one sqlite-backed pending surface "
                "using the current local lock provider; duplicate identity work is not executed "
                "while contended tasks remain inspectable and recoverable."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
                "bounded Prometheus text",
            ),
            operator_readability_facts=(
                "lock contention is visible through bounded queue/lock counters",
                "contended tasks stay in queue governance buckets rather than disappearing",
                "the claim remains same-host/shared-storage only",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m4_s8_bounded_concurrency_drills.py::test_p5_m4_s8_four_worker_same_pending_contention_is_bounded_and_inspectable",
            ),
        ),
        _scenario(
            name="backlog_drain_under_multi_worker",
            status="evidenced",
            direct_worker_count_evidence=3,
            target_behavior=(
                "Three local workers can drain after one in-flight item each while leaving "
                "the remaining backlog pending and operator-inspectable."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
                "worker runtime boundary Prometheus text",
            ),
            operator_readability_facts=(
                "drain means stop accepting new work after current in-flight work finishes",
                "pending backlog remains visible for later local workers",
                "worker runtime scope remains single_process_local_polling",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m4_s8_bounded_concurrency_drills.py::test_p5_m4_s8_backlog_drain_under_three_workers_leaves_pending_inspectable",
            ),
        ),
        _scenario(
            name="stale_lock_plus_contention_same_host",
            status="evidenced",
            direct_worker_count_evidence=2,
            target_behavior=(
                "A stale local lock can be recovered while another same-host worker contends "
                "against the refreshed active lock without implying distributed lease fencing."
            ),
            inspectability_surfaces=(
                "sqlite lock metrics",
                "queue.sqlite3",
                "bounded Prometheus text",
            ),
            operator_readability_facts=(
                "stale recovery and lock contention counters remain distinct",
                "orphaned queue governance remains inspectable",
                "stale recovery remains local lock-provider behavior",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m4_s8_bounded_concurrency_drills.py::test_p5_m4_s8_stale_lock_recovery_and_contention_remain_local_and_auditable",
            ),
        ),
        _scenario(
            name="poison_requeue_under_contention",
            status="evidenced",
            direct_worker_count_evidence=3,
            target_behavior=(
                "Contention-driven orphan transitions can reach poison under the configured "
                "bounded orphan policy, and operator requeue moves the task back to pending."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
                "requeue_local_task result",
                "bounded Prometheus text",
            ),
            operator_readability_facts=(
                "poison_orphaned stays distinct from attempts_exhausted",
                "sqlite requeue audit support remains result_only",
                "redrive semantics remain not_modeled",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m4_s8_bounded_concurrency_drills.py::test_p5_m4_s8_poison_requeue_under_contention_remains_operator_readable",
            ),
        ),
        _scenario(
            name="replay_and_queue_governance_coexistence_under_contention",
            status="evidenced",
            direct_worker_count_evidence=3,
            target_behavior=(
                "Queue-side contention/requeue governance and delivery-side replay/DLQ "
                "governance can coexist without collapsing replay and requeue provenance."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
                "replay.db",
                "_dlq/delivery/*.json",
                "_dlq/delivery_done/*.json",
                "run_meta.json",
                "bounded Prometheus text",
            ),
            operator_readability_facts=(
                "queue requeue produces run_origin=requeue and delivery_origin=primary",
                "delivery replay produces delivery_origin=replay while preserving run_origin",
                "queue governance and replay governance remain separate Zephyr-owned surfaces",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m4_s8_bounded_concurrency_drills.py::test_p5_m4_s8_replay_and_queue_governance_remain_distinct_under_contention",
            ),
        ),
    )
    return {
        "schema_version": 1,
        "phase": "P5-M4-S8 single-host bounded concurrency",
        "execution_chain": "TaskV1 -> FlowProcessor -> Delivery",
        "claim_boundary": {
            "runtime_model": "same-host/shared-storage",
            "queue_backend": "local sqlite queue",
            "lock_backends": ["local file lock", "local sqlite lock"],
            "worker_runtime_scope": "single_process_local_polling",
            "maximum_direct_worker_count_evidenced": 4,
            "not_claimed": [
                "distributed lease choreography",
                "distributed fencing",
                "broker-backed queue abstraction",
                "autoscaling or scheduler ownership",
                "cloud/Kubernetes/Helm deployment capability",
            ],
        },
        "operator_surfaces": [
            "queue.sqlite3",
            "inspect_local_sqlite_queue",
            "bounded Prometheus text",
            "requeue_local_task result",
            "replay.db",
            "_dlq/delivery/*.json",
            "_dlq/delivery_done/*.json",
            "run_meta.json",
            "batch_report.json",
            "delivery_receipt.json",
        ],
        "scenarios": [dict(scenario) for scenario in scenarios],
        "bounded_truths": [
            (
                "The direct higher-concurrency evidence is now four local workers against the "
                "same sqlite-backed pending surface."
            ),
            (
                "Drain/stop evidence remains bounded local in-flight shutdown behavior and does "
                "not claim a coordinated fleet drain."
            ),
            (
                "Replay, requeue, and resume remain separate provenance histories; S8 only adds "
                "direct contention evidence around replay and requeue coexistence."
            ),
            (
                "Queue and lock visibility remains artifact-backed and repo-side, not an external "
                "observability platform."
            ),
        ],
        "deferred_or_not_claimed": [
            (
                "No distributed runtime, scheduler, autoscaling, broker queue, "
                "or cloud packaging claim."
            ),
            "No throughput or capacity benchmark claim is made by these drills.",
            "No connector support-surface expansion is part of S8.",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5BoundedConcurrencyCheck:
    name: str
    ok: bool
    detail: str
    severity: BoundedConcurrencySeverity = "error"


def validate_p5_bounded_concurrency_artifacts() -> list[P5BoundedConcurrencyCheck]:
    checks: list[P5BoundedConcurrencyCheck] = []
    matrix_exists = P5_BOUNDED_CONCURRENCY_MATRIX_PATH.exists()
    report_exists = P5_BOUNDED_CONCURRENCY_REPORT_PATH.exists()

    checks.append(
        P5BoundedConcurrencyCheck(
            name="bounded_concurrency_matrix_exists",
            ok=matrix_exists,
            detail=str(P5_BOUNDED_CONCURRENCY_MATRIX_PATH),
        )
    )
    checks.append(
        P5BoundedConcurrencyCheck(
            name="bounded_concurrency_report_exists",
            ok=report_exists,
            detail=str(P5_BOUNDED_CONCURRENCY_REPORT_PATH),
        )
    )

    if matrix_exists:
        artifact_obj: object = json.loads(
            P5_BOUNDED_CONCURRENCY_MATRIX_PATH.read_text(encoding="utf-8")
        )
        if not isinstance(artifact_obj, dict):
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_matrix_json_object",
                    ok=False,
                    detail="artifact root is not an object",
                )
            )
            return checks
        artifact = cast(dict[str, object], artifact_obj)
        claim_boundary_obj = artifact.get("claim_boundary")
        if isinstance(claim_boundary_obj, dict):
            claim_boundary = cast(dict[str, object], claim_boundary_obj)
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_worker_count",
                    ok=claim_boundary.get("maximum_direct_worker_count_evidenced") == 4,
                    detail=(
                        "maximum_direct_worker_count_evidenced="
                        f"{claim_boundary.get('maximum_direct_worker_count_evidenced')!r}"
                    ),
                )
            )
            not_claimed = claim_boundary.get("not_claimed")
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_no_distributed_claim",
                    ok=isinstance(not_claimed, list)
                    and "distributed lease choreography" in not_claimed
                    and "broker-backed queue abstraction" in not_claimed,
                    detail="not_claimed present" if isinstance(not_claimed, list) else "missing",
                )
            )
        else:
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_claim_boundary",
                    ok=False,
                    detail="claim_boundary missing or not an object",
                )
            )

        scenarios_obj = artifact.get("scenarios")
        if isinstance(scenarios_obj, list):
            scenarios = cast(list[object], scenarios_obj)
            scenario_names: list[str] = []
            direct_worker_counts: list[int] = []
            invalid_statuses: list[str] = []
            for scenario_obj in scenarios:
                if not isinstance(scenario_obj, dict):
                    continue
                scenario = cast(dict[str, object], scenario_obj)
                name = scenario.get("name")
                if isinstance(name, str):
                    scenario_names.append(name)
                status = scenario.get("status")
                if isinstance(status, str) and status not in {
                    "evidenced",
                    "bounded",
                    "unsupported",
                }:
                    invalid_statuses.append(status)
                direct_worker_count = scenario.get("direct_worker_count_evidence")
                if isinstance(direct_worker_count, int) and not isinstance(
                    direct_worker_count,
                    bool,
                ):
                    direct_worker_counts.append(direct_worker_count)

            required_names = set(P5_BOUNDED_CONCURRENCY_REQUIRED_SCENARIOS)
            missing = sorted(required_names.difference(set(scenario_names)))
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_required_scenarios_present",
                    ok=not missing,
                    detail="missing=" + (", ".join(missing) if missing else "none"),
                )
            )
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_scenario_statuses_valid",
                    ok=not invalid_statuses,
                    detail="invalid="
                    + (", ".join(invalid_statuses) if invalid_statuses else "none"),
                )
            )
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_four_worker_direct_evidence",
                    ok=4 in direct_worker_counts,
                    detail=f"direct_worker_counts={direct_worker_counts!r}",
                )
            )
        else:
            checks.append(
                P5BoundedConcurrencyCheck(
                    name="bounded_concurrency_scenarios_list",
                    ok=False,
                    detail="scenarios field missing or not a list",
                )
            )

    if report_exists:
        report_text = P5_BOUNDED_CONCURRENCY_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "same-host/shared-storage",
            "4-worker same-pending contention",
            "backlog drain under multi-worker",
            "stale lock plus contention",
            "poison requeue under contention",
            "replay and queue governance coexistence",
            "No distributed runtime claim",
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5BoundedConcurrencyCheck(
                name="bounded_concurrency_report_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )

    return checks


def format_p5_bounded_concurrency_summary() -> str:
    return "\n".join(
        (
            "P5-M4-S8 bounded concurrency:",
            "- runtime model: same-host/shared-storage only",
            "- maximum direct worker count evidenced: 4",
            "- queue backend: local sqlite queue",
            "- worker runtime scope: single_process_local_polling",
            "- recovery-drill auth tier: recovery-drill",
            "- no distributed runtime, broker queue, scheduler, autoscaling, or cloud claim",
        )
    )


def format_p5_bounded_concurrency_results(
    results: list[P5BoundedConcurrencyCheck],
) -> str:
    lines = ["P5-M4-S8 bounded concurrency checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def render_p5_bounded_concurrency_json() -> str:
    return json.dumps(build_p5_bounded_concurrency_matrix(), ensure_ascii=False, indent=2)
