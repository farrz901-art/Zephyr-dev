from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

RuntimeBoundaryStatus = Literal["supported", "bounded", "unsupported"]
RuntimeBoundarySeverity = Literal["error", "warning", "info"]

P5_RUNTIME_BOUNDARY_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_runtime_boundary_matrix.json"
)
P5_RUNTIME_BOUNDARY_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M2_RUNTIME_BOUNDARY.md"
)
P5_RUNTIME_BOUNDARY_REQUIRED_SCENARIOS: Final[tuple[str, ...]] = (
    "single_worker_baseline",
    "two_worker_contention_shared_pending_surface",
    "stale_lock_recovery",
    "poison_after_attempts_exhausted",
    "requeue_from_poison",
    "replay_after_failed_delivery",
    "runtime_drain_stop_inflight_behavior",
    "operator_inspectability_before_after_recovery",
)


def _scenario(
    *,
    name: str,
    status: RuntimeBoundaryStatus,
    target_behavior: str,
    inspectability_surfaces: tuple[str, ...],
    governance_facts: tuple[str, ...],
    evidence: tuple[str, ...],
) -> dict[str, object]:
    return {
        "name": name,
        "status": status,
        "target_behavior": target_behavior,
        "inspectability_surfaces": list(inspectability_surfaces),
        "governance_facts": list(governance_facts),
        "evidence": list(evidence),
    }


def build_p5_runtime_boundary_matrix() -> dict[str, object]:
    scenarios = (
        _scenario(
            name="single_worker_baseline",
            status="supported",
            target_behavior=(
                "A single local worker drains pending work through the ingest-owned "
                "TaskV1 -> FlowProcessor -> Delivery chain and leaves Zephyr-owned "
                "queue/report artifacts."
            ),
            inspectability_surfaces=(
                "queue.sqlite3 or spool buckets",
                "run_meta.json",
                "delivery_receipt.json",
                "batch_report.json",
                "bounded Prometheus text",
            ),
            governance_facts=(
                "run_origin stays intake unless an explicit recovery action changes it",
                "delivery_origin stays primary for first-pass delivery",
                "queue task state is inspectable as pending/inflight/done/failed/poison",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_worker_runtime.py::test_run_worker_executes_sqlite_queue_backend_when_selected",
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_worker_runtime.py::test_runtime_supports_supported_queue_and_lock_backend_pairs",
            ),
        ),
        _scenario(
            name="two_worker_contention_shared_pending_surface",
            status="bounded",
            target_behavior=(
                "Two local workers may contend against the same sqlite-backed pending-work "
                "surface; duplicate task identity contention is recovered by requeueing the "
                "contended claim back to pending instead of double-executing it."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
                "bounded Prometheus text",
            ),
            governance_facts=(
                "lock contention is surfaced through queue requeue/orphan counters",
                "the contended task remains pending and inspectable",
                (
                    "the boundary is local single-host shared storage, "
                    "not distributed lease choreography"
                ),
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m2_runtime_boundary_drills.py::test_p5_m2_two_worker_contention_is_bounded_and_inspectable",
            ),
        ),
        _scenario(
            name="stale_lock_recovery",
            status="supported",
            target_behavior=(
                "Stale file or sqlite locks may be broken by the current local lock providers, "
                "allowing the next worker pass to continue processing the task."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "bounded Prometheus text",
                "run_meta.json",
            ),
            governance_facts=(
                "stale recovery is counted in lock metrics",
                "the task remains on the primary ingest execution path",
                "no distributed lease model is claimed",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_worker_runtime.py::test_runtime_supports_stale_lock_recovery_for_supported_lock_backends",
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py::test_p45_m7_worker_queue_lock_and_operator_surfaces_remain_inspectable",
            ),
        ),
        _scenario(
            name="poison_after_attempts_exhausted",
            status="supported",
            target_behavior=(
                "Tasks that exhaust the configured attempt budget move to poison and remain "
                "operator-visible rather than disappearing into implicit retries."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
            ),
            governance_facts=(
                "governance_problem is explicit as poison_attempts_exhausted",
                "failure_count remains visible on the poisoned task",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py::test_p45_m7_worker_queue_lock_and_operator_surfaces_remain_inspectable",
            ),
        ),
        _scenario(
            name="requeue_from_poison",
            status="supported",
            target_behavior=(
                "Operator-driven requeue moves a poisoned task back to pending through the "
                "ingest-owned governance surface."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
                "requeue_local_task result",
            ),
            governance_facts=(
                (
                    "run_origin is explicit as requeue when the recovery result "
                    "is converted to provenance"
                ),
                "current sqlite queue audit support is result_only rather than persisted history",
                "redrive semantics remain not_modeled",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py::test_p45_m7_worker_queue_lock_and_operator_surfaces_remain_inspectable",
            ),
        ),
        _scenario(
            name="replay_after_failed_delivery",
            status="supported",
            target_behavior=(
                "Failed delivery artifacts are replayed through the existing replay/DLQ path "
                "and can be moved to delivery_done after successful replay."
            ),
            inspectability_surfaces=(
                "replay.db",
                "_dlq/delivery/*.json",
                "_dlq/delivery_done/*.json",
                "run_meta.json",
            ),
            governance_facts=(
                "delivery_origin changes from primary to replay while run_origin remains explicit",
                "replay statistics remain Zephyr-owned and artifact-backed",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py::test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface",
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m2_runtime_boundary_drills.py::test_p5_m2_replay_after_failed_delivery_keeps_operator_artifacts_aligned",
            ),
        ),
        _scenario(
            name="runtime_drain_stop_inflight_behavior",
            status="bounded",
            target_behavior=(
                "A worker can be asked to drain and stop after finishing the current in-flight "
                "work item, leaving later tasks pending and inspectable."
            ),
            inspectability_surfaces=(
                "queue.sqlite3",
                "inspect_local_sqlite_queue",
                "bounded Prometheus text",
            ),
            governance_facts=(
                "drain affects new work intake, not the currently executing item",
                "pending work remains visible for later processing",
                "this is a bounded local shutdown behavior, not a fleet-wide coordinated drain",
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_worker_runtime.py::test_worker_runtime_draining_stops_new_work_and_emits_events",
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m2_runtime_boundary_drills.py::test_p5_m2_runtime_drain_leaves_pending_work_inspectable",
            ),
        ),
        _scenario(
            name="operator_inspectability_before_after_recovery",
            status="supported",
            target_behavior=(
                "Before and after queue recovery or replay actions, the current operator-facing "
                "surfaces can still answer what ran, what failed, whether it was retryable, and "
                "whether it was replayed, resumed, or requeued."
            ),
            inspectability_surfaces=(
                "inspect_local_sqlite_queue",
                "run_meta.json",
                "delivery_receipt.json",
                "batch_report.json",
                "replay.db",
                "_dlq/delivery_done/*.json",
                "bounded Prometheus text",
            ),
            governance_facts=(
                "resume remains distinct from replay and requeue",
                "shared provenance remains Zephyr-owned and explicit",
                (
                    "current operator evidence is artifact-backed, "
                    "not an external observability platform"
                ),
            ),
            evidence=(
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py::test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned",
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py::test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface",
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m2_runtime_boundary_drills.py::test_p5_m2_two_worker_contention_is_bounded_and_inspectable",
                "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_m2_runtime_boundary_drills.py::test_p5_m2_replay_after_failed_delivery_keeps_operator_artifacts_aligned",
            ),
        ),
    )
    return {
        "schema_version": 1,
        "phase": "P5-M2 runtime/queue/lock/concurrency boundary",
        "execution_chain": "TaskV1 -> FlowProcessor -> Delivery",
        "ownership": {
            "orchestrator": "zephyr-ingest",
            "queue_lock_runtime": "ingest-owned",
            "control_plane": "repo",
            "runtime_plane": "external-runtime-home",
        },
        "queue_backend_boundary": {
            "supported_current": ["spool", "sqlite"],
            "relied_on_for_boundary_drills": ["sqlite"],
            "notes": [
                "Spool remains a supported local queue backend for bounded local execution.",
                (
                    "SQLite is the current primary boundary surface for shared pending-work "
                    "inspection, poison visibility, and contention-adjacent drills."
                ),
                "No distributed broker-backed queue guarantee is claimed here.",
            ],
        },
        "lock_backend_boundary": {
            "supported_current": ["file", "sqlite"],
            "relied_on_for_boundary_drills": ["file", "sqlite"],
            "notes": [
                "Current lock behavior is bounded to local file and sqlite lock providers.",
                (
                    "Stale lock recovery is modeled locally; this milestone does not claim a "
                    "distributed lease or fencing model."
                ),
            ],
        },
        "operator_surfaces": [
            "queue.sqlite3",
            "run_meta.json",
            "delivery_receipt.json",
            "batch_report.json",
            "checkpoint.json",
            "records.jsonl",
            "replay.db",
            "_dlq/delivery_done/*.json",
            "bounded Prometheus text",
            "inspect_local_sqlite_queue",
            "requeue_local_task",
            "replay_delivery_dlq",
        ],
        "requires_recovery_drill_auth_tier": True,
        "recovery_drill_auth_tier": "recovery-drill",
        "scenarios": [dict(scenario) for scenario in scenarios],
        "supported_current_runtime_facts": [
            (
                "Single-worker local execution is supported through "
                "ingest-owned queue/lock/runtime surfaces."
            ),
            (
                "Local sqlite queue plus file/sqlite lock providers are the current explicit "
                "governance boundary for contention, stale lock, poison, "
                "requeue, and replay drills."
            ),
            (
                "Operator-visible facts are primarily artifact-backed and repo-side rather than "
                "served through an external observability platform."
            ),
            (
                "Replay, requeue, resume, and intake remain distinct provenance histories inside "
                "the current shared execution model."
            ),
        ],
        "bounded_or_deferred_facts": [
            (
                "Multi-worker behavior is proven only for a bounded "
                "same-host/shared-storage model; "
                "it is not a claim of a finished distributed runtime."
            ),
            (
                "Runtime drain/stop is a bounded local shutdown behavior around in-flight work; "
                "it is not a cluster-wide coordinated drain protocol."
            ),
            ("Redrive semantics remain not_modeled beyond explicit requeue and replay actions."),
            (
                "This milestone does not add autoscaling, scheduling, broker-backed orchestration, "
                "or cloud/Kubernetes packaging."
            ),
        ],
        "known_environment_noise": [
            (
                "Windows .tmp access-denied noise has been observed during pytest temp-root usage; "
                "it is treated as environment noise unless it changes "
                "runtime/governance correctness."
            ),
        ],
        "external_network_out_of_scope": [
            (
                "Google Drive and TUN or VPN reachability are external-network-dependent and are "
                "not part of the core queue/lock/runtime boundary claim."
            ),
        ],
    }


@dataclass(frozen=True, slots=True)
class P5RuntimeBoundaryCheck:
    name: str
    ok: bool
    detail: str
    severity: RuntimeBoundarySeverity = "error"


def validate_p5_runtime_boundary_artifacts() -> list[P5RuntimeBoundaryCheck]:
    checks: list[P5RuntimeBoundaryCheck] = []
    matrix_exists = P5_RUNTIME_BOUNDARY_MATRIX_PATH.exists()
    report_exists = P5_RUNTIME_BOUNDARY_REPORT_PATH.exists()

    checks.append(
        P5RuntimeBoundaryCheck(
            name="boundary_matrix_exists",
            ok=matrix_exists,
            detail=str(P5_RUNTIME_BOUNDARY_MATRIX_PATH),
        )
    )
    checks.append(
        P5RuntimeBoundaryCheck(
            name="boundary_report_exists",
            ok=report_exists,
            detail=str(P5_RUNTIME_BOUNDARY_REPORT_PATH),
        )
    )

    if matrix_exists:
        artifact_obj: object = json.loads(
            P5_RUNTIME_BOUNDARY_MATRIX_PATH.read_text(encoding="utf-8")
        )
        if not isinstance(artifact_obj, dict):
            checks.append(
                P5RuntimeBoundaryCheck(
                    name="boundary_matrix_json_object",
                    ok=False,
                    detail="artifact root is not an object",
                )
            )
            return checks
        artifact = cast(dict[str, object], artifact_obj)
        phase_obj = artifact.get("phase")
        checks.append(
            P5RuntimeBoundaryCheck(
                name="boundary_phase_name",
                ok=phase_obj == "P5-M2 runtime/queue/lock/concurrency boundary",
                detail=f"phase={phase_obj!r}",
            )
        )
        scenarios_obj = artifact.get("scenarios")
        if isinstance(scenarios_obj, list):
            scenarios = cast(list[object], scenarios_obj)
            scenario_names: list[str] = []
            valid_statuses = {"supported", "bounded", "unsupported"}
            invalid_statuses: list[str] = []
            for scenario_obj in scenarios:
                if not isinstance(scenario_obj, dict):
                    continue
                scenario = cast(dict[str, object], scenario_obj)
                name = scenario.get("name")
                if isinstance(name, str):
                    scenario_names.append(name)
                status = scenario.get("status")
                if isinstance(status, str) and status not in valid_statuses:
                    invalid_statuses.append(status)
            required_names = set(P5_RUNTIME_BOUNDARY_REQUIRED_SCENARIOS)
            missing = sorted(required_names.difference(set(scenario_names)))
            checks.append(
                P5RuntimeBoundaryCheck(
                    name="boundary_required_scenarios_present",
                    ok=not missing,
                    detail="missing=" + (", ".join(missing) if missing else "none"),
                )
            )
            checks.append(
                P5RuntimeBoundaryCheck(
                    name="boundary_scenario_statuses_valid",
                    ok=not invalid_statuses,
                    detail="invalid="
                    + (", ".join(invalid_statuses) if invalid_statuses else "none"),
                )
            )
        else:
            checks.append(
                P5RuntimeBoundaryCheck(
                    name="boundary_scenarios_list",
                    ok=False,
                    detail="scenarios field missing or not a list",
                )
            )

    if report_exists:
        report_text = P5_RUNTIME_BOUNDARY_REPORT_PATH.read_text(encoding="utf-8")
        required_report_phrases = (
            "single-worker baseline",
            "two-worker contention",
            "stale lock recovery",
            "poison after attempts exhausted",
            "requeue from poison",
            "replay after failed delivery",
            "runtime drain / stop",
            "Windows `.tmp` access-denied noise",
            "Google Drive/TUN/network dependency",
        )
        missing_report_phrases = [
            phrase for phrase in required_report_phrases if phrase not in report_text
        ]
        checks.append(
            P5RuntimeBoundaryCheck(
                name="boundary_report_required_phrases",
                ok=not missing_report_phrases,
                detail="missing="
                + (", ".join(missing_report_phrases) if missing_report_phrases else "none"),
            )
        )

    return checks


def format_p5_runtime_boundary_summary() -> str:
    return "\n".join(
        (
            "P5-M2 runtime boundary:",
            "- execution chain: TaskV1 -> FlowProcessor -> Delivery",
            "- queue backends: spool, sqlite",
            "- lock backends: file, sqlite",
            "- boundary drills rely on sqlite queue plus local file/sqlite locks",
            "- recovery-drill auth tier: recovery-drill",
            "- multi-worker status: bounded same-host/shared-storage only",
            "- external-network topics kept out of core runtime claim: Google Drive/TUN",
        )
    )


def format_p5_runtime_boundary_results(results: list[P5RuntimeBoundaryCheck]) -> str:
    lines = ["P5-M2 runtime boundary checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def render_p5_runtime_boundary_json() -> str:
    return json.dumps(build_p5_runtime_boundary_matrix(), ensure_ascii=False, indent=2)
