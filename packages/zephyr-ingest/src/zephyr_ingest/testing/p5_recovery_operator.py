from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.queue_backend_factory import LocalQueueBackendKind
from zephyr_ingest.queue_inspect import QueueInspectResultDict, inspect_local_queue
from zephyr_ingest.testing.p45 import repo_root

RecoveryScenarioStatus = Literal["supported", "bounded", "unsupported"]
RecoveryCheckSeverity = Literal["error", "warning", "info"]

P5_RECOVERY_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_recovery_scenario_matrix.json"
)
P5_RECOVERY_RUNBOOK_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M3_RECOVERY_OPERATOR_RUNBOOK.md"
)
P5_RECOVERY_REQUIRED_SCENARIOS: Final[tuple[str, ...]] = (
    "delivery_failure_to_replay",
    "poison_to_inspect_to_requeue",
    "stale_lock_recovery_continuation",
    "it_resume_continuation",
    "uns_reacquisition_replay_continuation",
    "retryable_vs_non_retryable_decision",
    "post_recovery_verification",
    "unsupported_deferred_recovery_topics",
)


def _scenario(
    *,
    name: str,
    status: RecoveryScenarioStatus,
    trigger_or_symptom: str,
    primary_inspect_surfaces: tuple[str, ...],
    secondary_inspect_surfaces: tuple[str, ...],
    decisive_fields: tuple[str, ...],
    operator_decision: str,
    operator_action: str,
    post_action_expected_facts: tuple[str, ...],
) -> dict[str, object]:
    return {
        "name": name,
        "status": status,
        "trigger_or_symptom": trigger_or_symptom,
        "primary_inspect_surfaces": list(primary_inspect_surfaces),
        "secondary_inspect_surfaces": list(secondary_inspect_surfaces),
        "decisive_fields": list(decisive_fields),
        "operator_decision": operator_decision,
        "operator_action": operator_action,
        "post_action_expected_facts": list(post_action_expected_facts),
    }


def build_p5_recovery_scenario_matrix() -> dict[str, object]:
    scenarios = (
        _scenario(
            name="delivery_failure_to_replay",
            status="supported",
            trigger_or_symptom=(
                "A delivery receipt or batch report records a failed delivery, and "
                "`_dlq/delivery/*.json` contains replayable delivery payload facts."
            ),
            primary_inspect_surfaces=(
                "_dlq/delivery/*.json",
                "delivery_receipt.json",
                "batch_report.json",
                "run_meta.json",
            ),
            secondary_inspect_surfaces=("replay.db", "replay_delivery_dlq result or CLI output"),
            decisive_fields=(
                "delivery_receipt.details.retryable",
                "delivery_receipt.summary.failure_retryability",
                "delivery_receipt.summary.failure_kind",
                "delivery_receipt.summary.error_code",
                "run_meta.provenance.run_origin",
                "run_meta.provenance.delivery_origin",
            ),
            operator_decision=(
                "Replay only when the failed delivery artifact is present and the failure is "
                "retryable or operator-approved for redelivery."
            ),
            operator_action="Run the replay path through `replay_delivery_dlq` or the CLI wrapper.",
            post_action_expected_facts=(
                "replay stats show attempted and succeeded counts",
                "_dlq/delivery_done/*.json contains the replayed record when move_done is enabled",
                "replayed payload has delivery_origin=replay",
            ),
        ),
        _scenario(
            name="poison_to_inspect_to_requeue",
            status="supported",
            trigger_or_symptom=(
                "Queue inspection shows poison tasks, usually with "
                "governance_problem=poison_attempts_exhausted."
            ),
            primary_inspect_surfaces=("queue.sqlite3", "inspect_local_sqlite_queue"),
            secondary_inspect_surfaces=("requeue_local_task result", "bounded Prometheus text"),
            decisive_fields=(
                "task.state",
                "task.poison_kind",
                "task.governance_problem",
                "task.failure_count",
                "task.orphan_count",
                "task.recovery_audit_support",
            ),
            operator_decision=(
                "Requeue only after confirming the task is a supported poison/inflight recovery "
                "case and the underlying issue has been corrected or deliberately retried."
            ),
            operator_action=(
                "Run `requeue_local_task` for the task id and source bucket; then inspect pending."
            ),
            post_action_expected_facts=(
                "queue pending count increases for the task",
                "QueueRecoveryResultV1 reports governance_result=moved_to_pending",
                "converted provenance has run_origin=requeue and delivery_origin=primary",
            ),
        ),
        _scenario(
            name="stale_lock_recovery_continuation",
            status="supported",
            trigger_or_symptom=(
                "A local file/sqlite lock is stale and blocks the next worker pass for a task."
            ),
            primary_inspect_surfaces=("queue.sqlite3", "bounded Prometheus text"),
            secondary_inspect_surfaces=("run_meta.json", "worker runtime log/event output"),
            decisive_fields=(
                "zephyr_ingest_lock_stale_recoveries_total",
                "queue bucket counts",
                "run_meta.provenance.run_origin",
                "run_meta.provenance.delivery_origin",
            ),
            operator_decision=(
                "Allow current local lock provider stale recovery only inside the bounded "
                "same-host/shared-storage model."
            ),
            operator_action=(
                "Run the worker/recovery drill or worker pass with stale lock recovery enabled."
            ),
            post_action_expected_facts=(
                "stale recovery metric increments",
                "task remains on the primary intake path unless another explicit action occurs",
                "no distributed lease/fencing claim is introduced",
            ),
        ),
        _scenario(
            name="it_resume_continuation",
            status="supported",
            trigger_or_symptom=(
                "An `it-stream` cursor-family flow resumes from an explicit checkpoint selection."
            ),
            primary_inspect_surfaces=("checkpoint.json", "run_meta.json", "records.jsonl"),
            secondary_inspect_surfaces=("replay.db", "delivery_receipt.json"),
            decisive_fields=(
                "run_meta.provenance.run_origin",
                "run_meta.provenance.checkpoint_identity_key",
                "checkpoint.parent_checkpoint_identity_key",
                "records.jsonl row count",
            ),
            operator_decision=(
                "Treat resume as source-side continuation, not delivery replay or queue requeue."
            ),
            operator_action=(
                "Use the current checkpoint/resume path for the explicit checkpoint id."
            ),
            post_action_expected_facts=(
                "run_origin=resume",
                "delivery_origin remains primary until delivery replay occurs",
                "checkpoint lineage points only to the immediate prior checkpoint",
            ),
        ),
        _scenario(
            name="uns_reacquisition_replay_continuation",
            status="supported",
            trigger_or_symptom=(
                "`uns-stream` reacquires one bounded document and later replays failed delivery "
                "from Zephyr-owned artifacts."
            ),
            primary_inspect_surfaces=(
                "run_meta.json",
                "delivery_receipt.json",
                "batch_report.json",
                "_dlq/delivery_done/*.json",
            ),
            secondary_inspect_surfaces=(
                "replay_delivery_dlq result",
                "replay.db when sqlite is used",
            ),
            decisive_fields=(
                "run_meta.provenance.run_origin",
                "run_meta.provenance.delivery_origin",
                "element metadata source facts",
                "delivery summary retryability/failure kind/error code",
            ),
            operator_decision=(
                "Treat reacquisition/repartition as `uns` source-local work; treat delivery "
                "replay as shared delivery recovery."
            ),
            operator_action=(
                "Inspect document provenance first, then delivery receipt/report, then replay DLQ."
            ),
            post_action_expected_facts=(
                "source facts remain document-specific and not collapsed into generic recovery",
                "delivery replay sets delivery_origin=replay",
                "batch report totals remain aligned with receipt outcome",
            ),
        ),
        _scenario(
            name="retryable_vs_non_retryable_decision",
            status="bounded",
            trigger_or_symptom=(
                "A failed delivery or queue task needs an operator recovery decision."
            ),
            primary_inspect_surfaces=(
                "delivery_receipt.json",
                "batch_report.json",
                "queue inspect output",
            ),
            secondary_inspect_surfaces=("destination-specific details", "replay/DLQ record"),
            decisive_fields=(
                "failure_retryability",
                "details.retryable",
                "failure_kind",
                "error_code",
                "governance_problem",
            ),
            operator_decision=(
                "Retry/replay retryable failures; require explicit operator judgment for "
                "non-retryable or ambiguous failures."
            ),
            operator_action=(
                "Do not hide ambiguity: inspect receipt/report first, then decide replay/requeue "
                "or leave blocked."
            ),
            post_action_expected_facts=(
                "operator action is explicit as replay or requeue when taken",
                "non-retryable cases are not silently retried by the runbook",
            ),
        ),
        _scenario(
            name="post_recovery_verification",
            status="supported",
            trigger_or_symptom=(
                "An operator completed replay or requeue and needs to verify outcome."
            ),
            primary_inspect_surfaces=(
                "_dlq/delivery_done/*.json",
                "queue inspect output",
                "replay.db",
                "run_meta.json",
            ),
            secondary_inspect_surfaces=("batch_report.json", "delivery_receipt.json"),
            decisive_fields=(
                "delivery_origin",
                "run_origin",
                "queue bucket counts",
                "ReplayStats.succeeded",
                "QueueRecoveryResultV1.governance_result",
            ),
            operator_decision=(
                "Verify the post-action state before considering the incident recovered."
            ),
            operator_action=(
                "Check done/pending/poison queue counts for requeue, or delivery_done/replay "
                "payloads for replay."
            ),
            post_action_expected_facts=(
                "replayed work is visible under delivery_done or replay sink output",
                "requeued work is visible as pending",
                "resume/replay/requeue histories remain distinguishable",
            ),
        ),
        _scenario(
            name="unsupported_deferred_recovery_topics",
            status="unsupported",
            trigger_or_symptom=(
                "An operator expects distributed redrive, cloud observability, "
                "or backend-native recovery bypassing Zephyr."
            ),
            primary_inspect_surfaces=("P5_M2_RUNTIME_BOUNDARY.md", "P5_M3 recovery runbook"),
            secondary_inspect_surfaces=("p5_runtime_boundary_matrix.json",),
            decisive_fields=("redrive_support=not_modeled", "bounded/deferred facts"),
            operator_decision=(
                "Do not treat unsupported recovery requests as current product behavior."
            ),
            operator_action=(
                "Escalate as deferred/productization work rather than performing an undocumented "
                "manual bypass."
            ),
            post_action_expected_facts=(
                "no connector-local side channel becomes the truth surface",
                "no fake universal observability claim is introduced",
            ),
        ),
    )
    return {
        "schema_version": 1,
        "phase": "P5-M3 recovery/operator/runbook productionization",
        "truth_surface": "repo-side artifact-backed operator evidence",
        "decisive_history_fields": ["run_origin", "delivery_origin"],
        "scenario_order_for_cold_operator": [
            "Inspect queue state and delivery DLQ presence first.",
            "Read run_meta provenance next, especially run_origin and delivery_origin.",
            "Use delivery receipt and batch report for retryability/failure classification.",
            "Choose replay for delivery DLQ and requeue for queue poison/inflight recovery.",
            "Verify post-action facts before closing the recovery action.",
        ],
        "scenarios": [dict(scenario) for scenario in scenarios],
        "supported_current_recovery_facts": [
            "Delivery replay through replay_delivery_dlq is supported for current replay sinks.",
            "Poison/inflight requeue through requeue_local_task is supported for local queues.",
            "it-stream resume remains source-side cursor/checkpoint continuation.",
            "uns-stream reacquisition/repartition remains source-local document work.",
            "Replay, requeue, resume, and intake remain distinct provenance histories.",
        ],
        "bounded_or_deferred_recovery_facts": [
            "Redrive semantics remain not_modeled beyond explicit requeue and replay.",
            (
                "Operator truth is artifact-backed and repo-side, "
                "not a finished observability platform."
            ),
            "Some sqlite recovery visibility is result_only rather than persisted_in_history.",
            "Backend-shaped visibility differences remain meaningful and must not be hidden.",
        ],
        "known_environment_noise": [
            (
                "Windows .tmp access-denied noise is known environment noise unless it changes "
                "runtime/governance correctness."
            ),
        ],
        "external_network_out_of_scope": [
            ("Google Drive/TUN/network reachability is outside the core recovery/operator claim."),
        ],
    }


@dataclass(frozen=True, slots=True)
class P5RecoveryCheck:
    name: str
    ok: bool
    detail: str
    severity: RecoveryCheckSeverity = "error"


def _load_json_object(path: Path) -> dict[str, object] | None:
    try:
        obj: object = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if not isinstance(obj, dict):
        return None
    return cast(dict[str, object], obj)


def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return cast(dict[str, object], value)
    return {}


def _as_str(value: object) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _recovery_hint_for_receipt(receipt: dict[str, object]) -> str:
    summary = _as_dict(receipt.get("summary"))
    details = _as_dict(receipt.get("details"))
    retryability = _as_str(summary.get("failure_retryability"))
    details_retryable = details.get("retryable")
    if retryability == "retryable" or details_retryable is True:
        return "delivery_replay_candidate"
    if retryability == "non_retryable" or details_retryable is False:
        return "operator_review_non_retryable"
    return "operator_review_unknown_retryability"


def summarize_recovery_artifacts(
    *,
    out_root: Path | None = None,
    queue_root: Path | None = None,
    queue_backend_kind: LocalQueueBackendKind = "sqlite",
) -> dict[str, object]:
    summary: dict[str, object] = {
        "truth_surface": "repo-side artifact-backed operator evidence",
        "operator_first_steps": [
            "Inspect queue state and delivery DLQ presence first.",
            "Read run_meta provenance next, especially run_origin and delivery_origin.",
            "Use delivery receipt and batch report for retryability/failure classification.",
            "Choose replay for delivery DLQ and requeue for queue poison/inflight recovery.",
            "Verify post-action facts before closing the recovery action.",
        ],
        "queue": None,
        "delivery": None,
        "provenance_histories": [],
        "suggested_next_actions": [],
        "bounded_notes": [
            "This is an artifact-backed summary, not a universal observability platform.",
            "Redrive semantics remain not_modeled beyond explicit requeue and replay.",
        ],
    }

    suggested_actions: list[str] = []
    provenance_histories: list[dict[str, str | None]] = []

    if queue_root is not None:
        queue_view: QueueInspectResultDict = inspect_local_queue(
            root=queue_root,
            backend_kind=queue_backend_kind,
        ).to_dict()
        queue_summary = queue_view["summary"]
        queue_payload: dict[str, object] = {
            "root": queue_view["root"],
            "summary": dict(queue_summary),
        }
        summary["queue"] = queue_payload
        if queue_summary["poison"] > 0:
            suggested_actions.append("inspect_poison_then_consider_requeue")
        if queue_summary["inflight"] > 0:
            suggested_actions.append("inspect_inflight_for_stale_lock_or_incomplete_work")

    if out_root is not None:
        resolved_out = out_root.expanduser().resolve()
        dlq_dir = resolved_out / "_dlq" / "delivery"
        dlq_done_dir = resolved_out / "_dlq" / "delivery_done"
        dlq_pending = sorted(dlq_dir.glob("*.json")) if dlq_dir.exists() else []
        dlq_done = sorted(dlq_done_dir.glob("*.json")) if dlq_done_dir.exists() else []
        receipt_paths = sorted(resolved_out.glob("*/delivery_receipt.json"))
        run_meta_paths = sorted(resolved_out.glob("*/run_meta.json"))
        batch_report = _load_json_object(resolved_out / "batch_report.json")
        receipt_summaries: list[dict[str, object]] = []

        for receipt_path in receipt_paths:
            receipt = _load_json_object(receipt_path)
            if receipt is None:
                continue
            receipt_summaries.append(
                {
                    "path": str(receipt_path),
                    "destination": receipt.get("destination"),
                    "ok": receipt.get("ok"),
                    "recovery_hint": _recovery_hint_for_receipt(receipt),
                    "summary": _as_dict(receipt.get("summary")),
                }
            )

        for run_meta_path in run_meta_paths:
            run_meta = _load_json_object(run_meta_path)
            if run_meta is None:
                continue
            provenance = _as_dict(run_meta.get("provenance"))
            provenance_histories.append(
                {
                    "path": str(run_meta_path),
                    "run_origin": _as_str(provenance.get("run_origin")),
                    "delivery_origin": _as_str(provenance.get("delivery_origin")),
                    "task_id": _as_str(provenance.get("task_id")),
                    "checkpoint_identity_key": _as_str(provenance.get("checkpoint_identity_key")),
                }
            )

        delivery_payload: dict[str, object] = {
            "out_root": str(resolved_out),
            "dlq_pending": len(dlq_pending),
            "dlq_done": len(dlq_done),
            "delivery_receipts": receipt_summaries,
            "batch_report_present": batch_report is not None,
        }
        if batch_report is not None:
            delivery_payload["batch_delivery"] = _as_dict(batch_report.get("delivery"))
            delivery_payload["counts_by_error_code"] = _as_dict(
                batch_report.get("counts_by_error_code")
            )
        summary["delivery"] = delivery_payload
        if dlq_pending:
            suggested_actions.append("replay_delivery_dlq")
        if dlq_done:
            suggested_actions.append("verify_replay_delivery_done")
        if any(
            item["recovery_hint"] == "operator_review_non_retryable" for item in receipt_summaries
        ):
            suggested_actions.append("operator_review_non_retryable_delivery_failure")

    summary["provenance_histories"] = provenance_histories
    summary["suggested_next_actions"] = sorted(set(suggested_actions))
    return summary


def validate_p5_recovery_artifacts() -> list[P5RecoveryCheck]:
    checks: list[P5RecoveryCheck] = []
    matrix_exists = P5_RECOVERY_MATRIX_PATH.exists()
    runbook_exists = P5_RECOVERY_RUNBOOK_PATH.exists()
    checks.append(
        P5RecoveryCheck(
            name="recovery_matrix_exists",
            ok=matrix_exists,
            detail=str(P5_RECOVERY_MATRIX_PATH),
        )
    )
    checks.append(
        P5RecoveryCheck(
            name="recovery_runbook_exists",
            ok=runbook_exists,
            detail=str(P5_RECOVERY_RUNBOOK_PATH),
        )
    )

    if matrix_exists:
        artifact = _load_json_object(P5_RECOVERY_MATRIX_PATH)
        if artifact is None:
            checks.append(
                P5RecoveryCheck(
                    name="recovery_matrix_json_object",
                    ok=False,
                    detail="artifact root is not an object",
                )
            )
        else:
            scenarios_obj = artifact.get("scenarios")
            scenario_names: list[str] = []
            invalid_statuses: list[str] = []
            valid_statuses = {"supported", "bounded", "unsupported"}
            if isinstance(scenarios_obj, list):
                scenarios = cast(list[object], scenarios_obj)
                for scenario_obj in scenarios:
                    if not isinstance(scenario_obj, dict):
                        continue
                    scenario = cast(dict[str, object], scenario_obj)
                    name = scenario.get("name")
                    status = scenario.get("status")
                    if isinstance(name, str):
                        scenario_names.append(name)
                    if isinstance(status, str) and status not in valid_statuses:
                        invalid_statuses.append(status)
            missing = sorted(set(P5_RECOVERY_REQUIRED_SCENARIOS).difference(set(scenario_names)))
            checks.append(
                P5RecoveryCheck(
                    name="recovery_required_scenarios_present",
                    ok=not missing,
                    detail="missing=" + (", ".join(missing) if missing else "none"),
                )
            )
            checks.append(
                P5RecoveryCheck(
                    name="recovery_scenario_statuses_valid",
                    ok=not invalid_statuses,
                    detail="invalid="
                    + (", ".join(invalid_statuses) if invalid_statuses else "none"),
                )
            )

    if runbook_exists:
        text = P5_RECOVERY_RUNBOOK_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "What to inspect first",
            "run_origin",
            "delivery_origin",
            "Replay",
            "Requeue",
            "Resume",
            "result_only",
            "persisted_in_history",
            "Windows `.tmp` access-denied noise",
            "Google Drive/TUN/network dependency",
        )
        missing_phrases = [phrase for phrase in required_phrases if phrase not in text]
        checks.append(
            P5RecoveryCheck(
                name="recovery_runbook_required_phrases",
                ok=not missing_phrases,
                detail="missing=" + (", ".join(missing_phrases) if missing_phrases else "none"),
            )
        )

    return checks


def format_p5_recovery_check_results(results: list[P5RecoveryCheck]) -> str:
    lines = ["P5-M3 recovery/operator checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_recovery_summary(summary: dict[str, object]) -> str:
    lines = ["P5-M3 recovery operator summary:"]
    queue = summary.get("queue")
    if isinstance(queue, dict):
        queue_payload = cast(dict[str, object], queue)
        queue_summary = _as_dict(queue_payload.get("summary"))
        lines.append(
            "- queue: "
            + ", ".join(
                f"{name}={queue_summary.get(name)}"
                for name in ("pending", "inflight", "done", "failed", "poison")
            )
        )
    delivery = summary.get("delivery")
    if isinstance(delivery, dict):
        delivery_payload = cast(dict[str, object], delivery)
        lines.append(
            "- delivery: "
            f"dlq_pending={delivery_payload.get('dlq_pending')}, "
            f"dlq_done={delivery_payload.get('dlq_done')}, "
            f"batch_report_present={delivery_payload.get('batch_report_present')}"
        )
    histories_obj = summary.get("provenance_histories")
    if isinstance(histories_obj, list):
        histories = cast(list[object], histories_obj)
        for history_obj in histories:
            if not isinstance(history_obj, dict):
                continue
            history = cast(dict[str, object], history_obj)
            lines.append(
                "- provenance: "
                f"run_origin={history.get('run_origin')}, "
                f"delivery_origin={history.get('delivery_origin')}, "
                f"task_id={history.get('task_id')}"
            )
    actions_obj = summary.get("suggested_next_actions")
    if isinstance(actions_obj, list):
        action_items = cast(list[object], actions_obj)
        actions = [item for item in action_items if isinstance(item, str)]
        lines.append("- suggested_next_actions: " + (", ".join(actions) if actions else "none"))
    return "\n".join(lines)


def render_p5_recovery_matrix_json() -> str:
    return json.dumps(build_p5_recovery_scenario_matrix(), ensure_ascii=False, indent=2)
