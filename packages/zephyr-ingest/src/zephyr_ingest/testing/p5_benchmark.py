from __future__ import annotations

import json
import os
import platform
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

BenchmarkLayer = Literal[
    "source_flow_representative",
    "destination_representative",
    "recovery_overhead",
]
BenchmarkStatus = Literal["supported", "bounded"]
BenchmarkCheckSeverity = Literal["error", "warning", "info"]

P5_BENCHMARK_REGISTRY_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_benchmark_scenario_registry.json"
)
P5_BENCHMARK_REPORT_PATH: Final[Path] = repo_root() / "validation" / "P5_M4_BENCHMARK_BASELINE.md"
P5_BENCHMARK_REQUIRED_CASES: Final[tuple[str, ...]] = (
    "uns_reacquisition_replay_representative",
    "it_resume_replay_representative",
    "filesystem_lightweight_delivery",
    "opensearch_heavier_delivery",
    "replay_after_failed_delivery_recovery_overhead",
)
P5_BENCHMARK_SLI_FIELDS: Final[tuple[str, ...]] = (
    "wall_clock_ms",
    "process_cpu_ms",
    "measurement_unit",
    "unit_count",
    "throughput_units_per_second",
    "delivery_success_count",
    "delivery_failure_count",
    "retryable_failure_count",
    "non_retryable_failure_count",
    "replay_attempt_count",
    "requeue_count",
)
P5_BENCHMARK_BOUNDED_CAVEATS: Final[tuple[str, ...]] = (
    "First-wave P5-M4 benchmark coverage is representative, not a full retained-surface matrix.",
    (
        "Source-flow representative cases, destination representative cases, and recovery "
        "overhead cases are separate layers and must not be collapsed into one headline number."
    ),
    (
        "Current results target bounded local same-host/shared-storage runtime reality, not "
        "distributed, Kubernetes, cloud, or fleet capacity claims."
    ),
    (
        "Prometheus/metrics, replay/requeue counts, and resource observations are partial "
        "surfaces unless a specific benchmark case records them."
    ),
    (
        "TUN/Google Drive/network-dependent topics are outside the core benchmark baseline "
        "unless a later SaaS-specific benchmark case explicitly includes them."
    ),
    (
        "Windows .tmp access-denied noise is known environment noise unless it changes "
        "correctness or artifact-backed result consistency."
    ),
)


@dataclass(frozen=True, slots=True)
class P5BenchmarkCase:
    case_id: str
    name: str
    layer: BenchmarkLayer
    status: BenchmarkStatus
    measurement_unit: str
    basis: str
    objective: str
    valid_comparisons: tuple[str, ...]
    invalid_comparisons: tuple[str, ...]
    measured_surfaces: tuple[str, ...]
    partial_surfaces: tuple[str, ...]
    bounded_caveats: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class P5BenchmarkCheck:
    name: str
    ok: bool
    detail: str
    severity: BenchmarkCheckSeverity = "error"


def _case_to_dict(case: P5BenchmarkCase) -> dict[str, object]:
    return {
        "case_id": case.case_id,
        "name": case.name,
        "layer": case.layer,
        "status": case.status,
        "measurement_unit": case.measurement_unit,
        "basis": case.basis,
        "objective": case.objective,
        "valid_comparisons": list(case.valid_comparisons),
        "invalid_comparisons": list(case.invalid_comparisons),
        "measured_surfaces": list(case.measured_surfaces),
        "partial_surfaces": list(case.partial_surfaces),
        "bounded_caveats": list(case.bounded_caveats),
    }


def iter_p5_benchmark_cases() -> tuple[P5BenchmarkCase, ...]:
    return (
        P5BenchmarkCase(
            case_id="uns_reacquisition_replay_representative",
            name="UNS reacquisition plus delivery replay representative",
            layer="source_flow_representative",
            status="supported",
            measurement_unit="document",
            basis="test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned",
            objective=(
                "Measure one bounded uns-stream document reacquisition/repartition path with "
                "shared delivery replay/report evidence."
            ),
            valid_comparisons=("same case repeatability across repeated local runs",),
            invalid_comparisons=(
                "direct headline comparison against the it-stream representative case",
                "filesystem-vs-opensearch destination comparison",
            ),
            measured_surfaces=(
                "wall_clock_ms",
                "document_count",
                "delivery outcome summary",
                "replay count when the case records replay artifacts",
                "run_meta provenance",
                "batch_report summary",
                "delivery_receipt outcome",
            ),
            partial_surfaces=("prometheus metrics", "resource usage snapshot"),
            bounded_caveats=(
                "One bounded document unit; not multi-document discovery or SaaS sync.",
            ),
        ),
        P5BenchmarkCase(
            case_id="it_resume_replay_representative",
            name="IT checkpoint resume plus delivery replay representative",
            layer="source_flow_representative",
            status="supported",
            measurement_unit="checkpoint_resume_task",
            basis="test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface",
            objective=(
                "Measure one bounded it-stream cursor-family resume path with shared delivery "
                "replay/report evidence."
            ),
            valid_comparisons=("same case repeatability across repeated local runs",),
            invalid_comparisons=(
                "direct headline comparison against the uns-stream representative case",
                "filesystem-vs-opensearch destination comparison",
            ),
            measured_surfaces=(
                "wall_clock_ms",
                "task_count",
                "payload_count",
                "checkpoint/resume facts",
                "delivery outcome summary",
                "run_meta provenance",
                "batch_report summary",
            ),
            partial_surfaces=("prometheus metrics", "resource usage snapshot"),
            bounded_caveats=(
                "Cursor-v1 resume only; no CDC/changefeed/consumer-group progress claim.",
            ),
        ),
        P5BenchmarkCase(
            case_id="filesystem_lightweight_delivery",
            name="Filesystem lightweight delivery representative",
            layer="destination_representative",
            status="supported",
            measurement_unit="delivery_payload",
            basis="retained filesystem destination local-real authenticity path",
            objective=(
                "Measure a lightweight durable/object-style destination path without mixing it "
                "with source-family comparisons."
            ),
            valid_comparisons=("opensearch_heavier_delivery within destination layer only",),
            invalid_comparisons=(
                "source-flow representative headline comparison",
                "recovery-overhead headline comparison",
            ),
            measured_surfaces=(
                "wall_clock_ms",
                "payload_count",
                "delivery success/failure counts",
                "delivery_receipt outcome",
                "batch_report summary",
            ),
            partial_surfaces=("resource usage snapshot",),
            bounded_caveats=("Local filesystem behavior is same-host storage only.",),
        ),
        P5BenchmarkCase(
            case_id="opensearch_heavier_delivery",
            name="OpenSearch heavier delivery representative",
            layer="destination_representative",
            status="supported",
            measurement_unit="delivery_payload",
            basis="retained OpenSearch service-live authenticity path",
            objective=(
                "Measure a heavier structured sink destination path under the current local "
                "HTTPS/basic-auth/skip-TLS-verify substrate reality."
            ),
            valid_comparisons=("filesystem_lightweight_delivery within destination layer only",),
            invalid_comparisons=(
                "source-flow representative headline comparison",
                "recovery-overhead headline comparison",
            ),
            measured_surfaces=(
                "wall_clock_ms",
                "payload_count",
                "delivery success/failure counts",
                "retryable/non-retryable breakdown",
                "delivery_receipt outcome",
                "batch_report summary",
            ),
            partial_surfaces=("resource usage snapshot", "prometheus metrics"),
            bounded_caveats=(
                (
                    "OpenSearch local runtime uses HTTPS with skip-TLS-verify for self-signed "
                    "local certs."
                ),
            ),
        ),
        P5BenchmarkCase(
            case_id="replay_after_failed_delivery_recovery_overhead",
            name="Replay after failed delivery recovery-overhead representative",
            layer="recovery_overhead",
            status="bounded",
            measurement_unit="inspect_decide_act_verify_workflow",
            basis="P5-M3 delivery_failure_to_replay recovery scenario",
            objective=(
                "Measure bounded recovery workflow overhead for inspect -> decide -> replay -> "
                "verify, not only the replay action call."
            ),
            valid_comparisons=("same recovery case repeatability across repeated local runs",),
            invalid_comparisons=(
                "destination throughput headline comparison",
                "source-flow representative headline comparison",
            ),
            measured_surfaces=(
                "wall_clock_ms",
                "dlq_pending_count",
                "dlq_done_count",
                "replay_attempt_count",
                "replay_success_count",
                "delivery_origin",
                "batch_report summary",
            ),
            partial_surfaces=("action-only replay duration", "prometheus metrics"),
            bounded_caveats=("Redrive beyond explicit requeue/replay remains not modeled.",),
        ),
    )


def build_p5_benchmark_scenario_registry() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S2 benchmark/SLI/capacity baseline harness",
        "runtime_boundary": "bounded local same-host/shared-storage",
        "headline_rule": (
            "Do not collapse source-flow, destination, and recovery-overhead layers into one "
            "headline benchmark result."
        ),
        "measurement_unit_rule": (
            "Each case declares its own primary measurement_unit. Throughput-like values are "
            "valid only when unit_count is explicit for that case."
        ),
        "first_wave_cases": [_case_to_dict(case) for case in iter_p5_benchmark_cases()],
        "sli_candidate_fields": list(P5_BENCHMARK_SLI_FIELDS),
        "bounded_caveats": list(P5_BENCHMARK_BOUNDED_CAVEATS),
        "environment_freeze": {
            "cpu": "13th Gen Intel(R) Core(TM) i7-13700H",
            "cores_logical_processors": "14 / 20",
            "memory": "about 34 GB",
            "os": "Microsoft Windows 11 Pro for Workstations 64-bit (10.0.26100)",
            "repo_drive": "E:",
            "runtime_home_drive": "E:",
            "free_space_e_drive": "about 109.98 GB",
            "tun_mode": True,
            "known_tmp_acl_noise": True,
        },
    }


def render_p5_benchmark_registry_json() -> str:
    return json.dumps(build_p5_benchmark_scenario_registry(), ensure_ascii=False, indent=2)


def get_p5_benchmark_case(case_id: str) -> P5BenchmarkCase:
    for case in iter_p5_benchmark_cases():
        if case.case_id == case_id:
            return case
    known = ", ".join(case.case_id for case in iter_p5_benchmark_cases())
    raise ValueError(f"Unknown P5-M4 benchmark case {case_id!r}; known cases: {known}")


def run_p5_benchmark_case(case_id: str) -> dict[str, object]:
    case = get_p5_benchmark_case(case_id)
    start_wall_ns = time.perf_counter_ns()
    start_cpu_ns = time.process_time_ns()
    end_cpu_ns = time.process_time_ns()
    end_wall_ns = time.perf_counter_ns()
    wall_clock_ms = (end_wall_ns - start_wall_ns) / 1_000_000
    process_cpu_ms = (end_cpu_ns - start_cpu_ns) / 1_000_000
    unit_count = 0
    throughput_units_per_second: float | None = None
    return {
        "schema_version": 1,
        "phase": "P5-M4-S2 benchmark/SLI/capacity baseline harness",
        "case": _case_to_dict(case),
        "execution_mode": "harness_dry_run",
        "measured": {
            "wall_clock_ms": wall_clock_ms,
            "process_cpu_ms": process_cpu_ms,
            "unit_count": unit_count,
            "measurement_unit": case.measurement_unit,
            "throughput_units_per_second": throughput_units_per_second,
        },
        "delivery_summary": {
            "available": False,
            "success_count": 0,
            "failure_count": 0,
            "retryable_failure_count": 0,
            "non_retryable_failure_count": 0,
        },
        "recovery_summary": {
            "available": False,
            "replay_attempt_count": 0,
            "replay_success_count": 0,
            "requeue_count": 0,
            "recovery_overhead_scope": (
                "inspect_decide_act_verify_workflow"
                if case.layer == "recovery_overhead"
                else "not_applicable"
            ),
        },
        "resource_observations": {
            "available": True,
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
            "logical_cpu_count": os.cpu_count(),
        },
        "sli_candidates": {
            "wall_clock_ms": wall_clock_ms,
            "process_cpu_ms": process_cpu_ms,
            "measurement_unit": case.measurement_unit,
            "unit_count": unit_count,
            "throughput_units_per_second": throughput_units_per_second,
            "delivery_success_count": 0,
            "delivery_failure_count": 0,
            "retryable_failure_count": 0,
            "non_retryable_failure_count": 0,
            "replay_attempt_count": 0,
            "requeue_count": 0,
        },
        "validity": {
            "status": "harness_validation_only",
            "not_full_benchmark_execution": True,
            "bounded_runtime_boundary": "local same-host/shared-storage",
            "caveats": list(P5_BENCHMARK_BOUNDED_CAVEATS),
        },
        "redaction": {
            "contains_secret_values": False,
            "rule": (
                "Benchmark output records variable categories and case facts, not secret values."
            ),
        },
    }


def format_p5_benchmark_result(result: dict[str, object]) -> str:
    case = result["case"]
    measured = result["measured"]
    validity = result["validity"]
    assert isinstance(case, dict)
    assert isinstance(measured, dict)
    assert isinstance(validity, dict)
    return "\n".join(
        (
            "P5-M4 benchmark harness result:",
            f"- case: {case['case_id']}",
            f"- layer: {case['layer']}",
            f"- execution mode: {result['execution_mode']}",
            f"- measurement unit: {measured['measurement_unit']}",
            f"- wall clock ms: {measured['wall_clock_ms']}",
            f"- unit count: {measured['unit_count']}",
            f"- validity: {validity['status']}",
            "- caveat: this is harness validation output, not a full benchmark execution wave",
        )
    )


def validate_p5_benchmark_artifacts() -> list[P5BenchmarkCheck]:
    checks: list[P5BenchmarkCheck] = []
    checks.append(
        P5BenchmarkCheck(
            name="registry_artifact_exists",
            ok=P5_BENCHMARK_REGISTRY_PATH.exists(),
            detail=str(P5_BENCHMARK_REGISTRY_PATH),
        )
    )
    checks.append(
        P5BenchmarkCheck(
            name="report_artifact_exists",
            ok=P5_BENCHMARK_REPORT_PATH.exists(),
            detail=str(P5_BENCHMARK_REPORT_PATH),
        )
    )
    if P5_BENCHMARK_REGISTRY_PATH.exists():
        raw_registry = cast(
            dict[str, object],
            json.loads(P5_BENCHMARK_REGISTRY_PATH.read_text(encoding="utf-8")),
        )
        artifact_cases_value = raw_registry.get("first_wave_cases", [])
        artifact_case_ids: set[str] = set()
        if isinstance(artifact_cases_value, list):
            artifact_cases_raw = cast(list[object], artifact_cases_value)
            for item in artifact_cases_raw:
                if isinstance(item, dict):
                    item_obj = cast(dict[object, object], item)
                    case_id = item_obj.get("case_id")
                    if isinstance(case_id, str):
                        artifact_case_ids.add(case_id)
        helper_case_ids = {case.case_id for case in iter_p5_benchmark_cases()}
        checks.append(
            P5BenchmarkCheck(
                name="registry_cases_match_helper",
                ok=artifact_case_ids == helper_case_ids,
                detail=(
                    "artifact="
                    + ", ".join(sorted(artifact_case_ids))
                    + "; helper="
                    + ", ".join(sorted(helper_case_ids))
                ),
            )
        )
    if P5_BENCHMARK_REPORT_PATH.exists():
        report_text = P5_BENCHMARK_REPORT_PATH.read_text(encoding="utf-8")
        checks.append(
            P5BenchmarkCheck(
                name="report_declares_bounded_runtime_boundary",
                ok="bounded local same-host/shared-storage" in report_text,
                detail=str(P5_BENCHMARK_REPORT_PATH),
            )
        )
        checks.append(
            P5BenchmarkCheck(
                name="report_prevents_single_headline_layer_collapse",
                ok="must not be collapsed into one headline" in report_text,
                detail=str(P5_BENCHMARK_REPORT_PATH),
            )
        )
    return checks


def format_p5_benchmark_check_results(checks: list[P5BenchmarkCheck]) -> str:
    lines = ["P5-M4 benchmark artifact checks:"]
    for check in checks:
        state = "ok" if check.ok else "failed"
        lines.append(f"- {check.name} -> {state} ({check.detail})")
    return "\n".join(lines)
