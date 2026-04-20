from __future__ import annotations

import json
import os
import platform
import re
from collections.abc import Mapping, Sequence
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
BenchmarkCarrierKind = Literal["pytest"]
BenchmarkIsolationStrategy = Literal[
    "tmp_path_scoped_artifacts",
    "unique_opensearch_index_per_run",
    "artifact_scoped_recovery_sample",
]
BenchmarkFieldMaturity = Literal["usable_now", "partial", "not_ready_for_headline"]

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
P5_BENCHMARK_FIELD_ORDER: Final[tuple[str, ...]] = (
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
    "resource_usage_snapshot",
    "prometheus_metrics_surface",
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
    (
        "The current first-wave baseline is a two-run usable baseline, not a fully statistical "
        "capacity study; targeted later r3 is only for a specific unstable case."
    ),
)
P5_BENCHMARK_BASELINE_RUN_POLICY: Final[dict[str, object]] = {
    "current_required_runs": 2,
    "current_judgment": "two_run_usable_first_wave_baseline",
    "global_r3_required": False,
    "targeted_later_r3_policy": (
        "allowed only for a specific case if future instability justifies it"
    ),
}
_RESOURCE_SLUG_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9-]+")


@dataclass(frozen=True, slots=True)
class P5BenchmarkCarrier:
    kind: BenchmarkCarrierKind
    command: tuple[str, ...]
    auth_tier: str | None
    pytest_target: str
    pytest_selector: str
    expected_artifacts: tuple[str, ...]
    isolation_strategy: BenchmarkIsolationStrategy
    env_overrides: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class P5BenchmarkFieldSpec:
    name: str
    maturity: BenchmarkFieldMaturity
    headline_ready: bool
    reason: str


@dataclass(frozen=True, slots=True)
class P5BenchmarkCase:
    case_id: str
    name: str
    layer: BenchmarkLayer
    status: BenchmarkStatus
    measurement_unit: str
    carrier: P5BenchmarkCarrier
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


def _slug(value: str, *, max_len: int = 48) -> str:
    normalized = _RESOURCE_SLUG_RE.sub("-", value.lower().replace("_", "-")).strip("-")
    if normalized == "":
        normalized = "unnamed"
    return normalized[:max_len].strip("-") or "unnamed"


def build_p5_benchmark_resource_name(
    *,
    case_id: str,
    run_id: str,
    resource_kind: str,
    prefix: str = "zephyr-p5-m4",
) -> str:
    """Build a bounded, repeatable per-run resource name for local benchmark isolation."""
    return "-".join(
        (
            _slug(prefix, max_len=32),
            _slug(case_id, max_len=42),
            _slug(run_id, max_len=36),
            _slug(resource_kind, max_len=18),
        )
    )


def _carrier_to_dict(carrier: P5BenchmarkCarrier) -> dict[str, object]:
    return {
        "kind": carrier.kind,
        "command": list(carrier.command),
        "auth_tier": carrier.auth_tier,
        "pytest_target": carrier.pytest_target,
        "pytest_selector": carrier.pytest_selector,
        "expected_artifacts": list(carrier.expected_artifacts),
        "isolation_strategy": carrier.isolation_strategy,
        "env_overrides": list(carrier.env_overrides),
    }


def _case_to_dict(case: P5BenchmarkCase) -> dict[str, object]:
    return {
        "case_id": case.case_id,
        "name": case.name,
        "layer": case.layer,
        "status": case.status,
        "measurement_unit": case.measurement_unit,
        "carrier": _carrier_to_dict(case.carrier),
        "objective": case.objective,
        "valid_comparisons": list(case.valid_comparisons),
        "invalid_comparisons": list(case.invalid_comparisons),
        "measured_surfaces": list(case.measured_surfaces),
        "partial_surfaces": list(case.partial_surfaces),
        "bounded_caveats": list(case.bounded_caveats),
        "baseline_run_policy": dict(P5_BENCHMARK_BASELINE_RUN_POLICY),
    }


def iter_p5_benchmark_field_specs() -> tuple[P5BenchmarkFieldSpec, ...]:
    return (
        P5BenchmarkFieldSpec(
            name="wall_clock_ms",
            maturity="usable_now",
            headline_ready=True,
            reason="Captured by the benchmark harness and comparable only inside a declared layer.",
        ),
        P5BenchmarkFieldSpec(
            name="process_cpu_ms",
            maturity="partial",
            headline_ready=False,
            reason="Useful local signal but not a full resource profile.",
        ),
        P5BenchmarkFieldSpec(
            name="measurement_unit",
            maturity="usable_now",
            headline_ready=True,
            reason="Required per case; prevents false cross-layer throughput comparisons.",
        ),
        P5BenchmarkFieldSpec(
            name="unit_count",
            maturity="usable_now",
            headline_ready=True,
            reason="Required before throughput-like values are headline-eligible.",
        ),
        P5BenchmarkFieldSpec(
            name="throughput_units_per_second",
            maturity="partial",
            headline_ready=False,
            reason="Derived only when unit_count and wall_clock_ms are explicit for one layer.",
        ),
        P5BenchmarkFieldSpec(
            name="delivery_success_count",
            maturity="usable_now",
            headline_ready=True,
            reason="Artifact-backed when the carrier records delivery receipt/report facts.",
        ),
        P5BenchmarkFieldSpec(
            name="delivery_failure_count",
            maturity="usable_now",
            headline_ready=True,
            reason="Artifact-backed when the carrier records delivery receipt/report facts.",
        ),
        P5BenchmarkFieldSpec(
            name="retryable_failure_count",
            maturity="partial",
            headline_ready=False,
            reason="Available only when receipt/report classification is present for the case.",
        ),
        P5BenchmarkFieldSpec(
            name="non_retryable_failure_count",
            maturity="partial",
            headline_ready=False,
            reason="Available only when receipt/report classification is present for the case.",
        ),
        P5BenchmarkFieldSpec(
            name="replay_attempt_count",
            maturity="partial",
            headline_ready=False,
            reason="Recovery/DLQ cases can record it; not universal across benchmark layers.",
        ),
        P5BenchmarkFieldSpec(
            name="requeue_count",
            maturity="not_ready_for_headline",
            headline_ready=False,
            reason="Queue requeue is not a first-wave headline benchmark surface.",
        ),
        P5BenchmarkFieldSpec(
            name="resource_usage_snapshot",
            maturity="partial",
            headline_ready=False,
            reason="Bounded stdlib process/machine snapshot, not a profiler or capacity platform.",
        ),
        P5BenchmarkFieldSpec(
            name="prometheus_metrics_surface",
            maturity="not_ready_for_headline",
            headline_ready=False,
            reason="Prometheus text exists in bounded drills but is not a full metrics platform.",
        ),
    )


def benchmark_field_maturity_map() -> dict[str, dict[str, object]]:
    return {
        spec.name: {
            "maturity": spec.maturity,
            "headline_ready": spec.headline_ready,
            "reason": spec.reason,
        }
        for spec in iter_p5_benchmark_field_specs()
    }


def _pytest_command(
    *,
    target: str,
    selector: str,
    auth_tier: str | None = None,
) -> tuple[str, ...]:
    command: tuple[str, ...] = ("uv", "run", "--locked", "--no-sync", "pytest", "-n", "0", target)
    if auth_tier is not None:
        command = (*command, "--auth-tier", auth_tier)
    return (*command, "-k", selector)


def iter_p5_benchmark_cases() -> tuple[P5BenchmarkCase, ...]:
    m7_target = "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py"
    return (
        P5BenchmarkCase(
            case_id="uns_reacquisition_replay_representative",
            name="UNS reacquisition plus delivery replay representative",
            layer="source_flow_representative",
            status="supported",
            measurement_unit="document",
            carrier=P5BenchmarkCarrier(
                kind="pytest",
                command=_pytest_command(
                    target=m7_target,
                    auth_tier="recovery-drill",
                    selector=(
                        "test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned"
                    ),
                ),
                auth_tier="recovery-drill",
                pytest_target=m7_target,
                pytest_selector=(
                    "test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned"
                ),
                expected_artifacts=(
                    "run_meta.json",
                    "delivery_receipt.json",
                    "batch_report.json",
                    "_dlq/delivery_done/*.json",
                ),
                isolation_strategy="tmp_path_scoped_artifacts",
            ),
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
            carrier=P5BenchmarkCarrier(
                kind="pytest",
                command=_pytest_command(
                    target=m7_target,
                    auth_tier="recovery-drill",
                    selector=(
                        "test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface"
                    ),
                ),
                auth_tier="recovery-drill",
                pytest_target=m7_target,
                pytest_selector=(
                    "test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface"
                ),
                expected_artifacts=("run_meta.json", "checkpoint.json", "records.jsonl"),
                isolation_strategy="tmp_path_scoped_artifacts",
            ),
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
            carrier=P5BenchmarkCarrier(
                kind="pytest",
                command=_pytest_command(
                    target=(
                        "packages/zephyr-ingest/src/zephyr_ingest/tests/"
                        "test_destination_filesystem.py"
                    ),
                    selector="test_filesystem_destination_writes_success_artifacts",
                ),
                auth_tier=None,
                pytest_target=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_filesystem.py"
                ),
                pytest_selector="test_filesystem_destination_writes_success_artifacts",
                expected_artifacts=("delivery_receipt.json", "batch_report.json"),
                isolation_strategy="tmp_path_scoped_artifacts",
            ),
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
            carrier=P5BenchmarkCarrier(
                kind="pytest",
                command=_pytest_command(
                    target=(
                        "packages/zephyr-ingest/src/zephyr_ingest/tests/"
                        "test_p45_destination_wave1_service_live.py"
                    ),
                    auth_tier="service-live",
                    selector=(
                        "test_p45_wave1_opensearch_live_success_failures_and_worker_consistency"
                    ),
                ),
                auth_tier="service-live",
                pytest_target=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/"
                    "test_p45_destination_wave1_service_live.py"
                ),
                pytest_selector=(
                    "test_p45_wave1_opensearch_live_success_failures_and_worker_consistency"
                ),
                expected_artifacts=("delivery_receipt.json", "batch_report.json"),
                isolation_strategy="unique_opensearch_index_per_run",
                env_overrides=("ZEPHYR_P45_BENCHMARK_OPENSEARCH_INDEX_PREFIX={namespace_only}",),
            ),
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
                "OpenSearch benchmark isolation is unique index per run, not manual pre-clean.",
            ),
        ),
        P5BenchmarkCase(
            case_id="replay_after_failed_delivery_recovery_overhead",
            name="Replay after failed delivery recovery-overhead representative",
            layer="recovery_overhead",
            status="bounded",
            measurement_unit="inspect_decide_act_verify_workflow",
            carrier=P5BenchmarkCarrier(
                kind="pytest",
                command=_pytest_command(
                    target=(
                        "packages/zephyr-ingest/src/zephyr_ingest/tests/"
                        "test_p5_recovery_operator.py"
                    ),
                    selector="test_p5_recovery_summary_combines_queue_delivery_and_provenance",
                ),
                auth_tier=None,
                pytest_target=(
                    "packages/zephyr-ingest/src/zephyr_ingest/tests/test_p5_recovery_operator.py"
                ),
                pytest_selector="test_p5_recovery_summary_combines_queue_delivery_and_provenance",
                expected_artifacts=(
                    "queue.sqlite3",
                    "run_meta.json",
                    "delivery_receipt.json",
                    "_dlq/delivery/*.json",
                    "_dlq/delivery_done/*.json",
                ),
                isolation_strategy="artifact_scoped_recovery_sample",
            ),
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
        "schema_version": 2,
        "phase": "P5-M4-S4 benchmark execution-surface hardening",
        "runtime_boundary": "bounded local same-host/shared-storage",
        "headline_rule": (
            "Do not collapse source-flow, destination, and recovery-overhead layers into one "
            "headline benchmark result."
        ),
        "measurement_unit_rule": (
            "Each case declares its own primary measurement_unit. Throughput-like values are "
            "valid only when unit_count is explicit for that case."
        ),
        "baseline_run_policy": dict(P5_BENCHMARK_BASELINE_RUN_POLICY),
        "first_wave_cases": [_case_to_dict(case) for case in iter_p5_benchmark_cases()],
        "field_maturity": benchmark_field_maturity_map(),
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


def validate_p5_benchmark_case_contract(case_id: str) -> dict[str, object]:
    case = get_p5_benchmark_case(case_id)
    return {
        "schema_version": 1,
        "phase": "P5-M4-S4 benchmark case contract validation",
        "case": _case_to_dict(case),
        "execution_mode": "case_contract_validation",
        "execution_surface": {
            "mode": "case_contract_validation",
            "is_benchmark_result": False,
            "real_carrier_required": True,
            "carrier_command": list(case.carrier.command),
        },
        "layer_guardrails": {
            "layer": case.layer,
            "single_headline_across_layers_allowed": False,
            "valid_comparison_scope": list(case.valid_comparisons),
            "invalid_comparisons": list(case.invalid_comparisons),
        },
        "field_maturity": benchmark_field_maturity_map(),
        "baseline_run_policy": dict(P5_BENCHMARK_BASELINE_RUN_POLICY),
        "redaction": {
            "contains_secret_values": False,
            "rule": (
                "Carrier contract output records commands and variable names, not secret values."
            ),
        },
    }


def _as_float(value: object, *, default: float | None = None) -> float | None:
    if isinstance(value, bool):
        return default
    if isinstance(value, int | float):
        return float(value)
    return default


def _as_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def record_p5_benchmark_result(
    *,
    case_id: str,
    observed: Mapping[str, object],
) -> dict[str, object]:
    case = get_p5_benchmark_case(case_id)
    wall_clock_ms = _as_float(observed.get("wall_clock_ms"))
    process_cpu_ms = _as_float(observed.get("process_cpu_ms"), default=0.0)
    unit_count = _as_int(observed.get("unit_count"))
    throughput_units_per_second: float | None = None
    if wall_clock_ms is not None and wall_clock_ms > 0 and unit_count > 0:
        throughput_units_per_second = unit_count / (wall_clock_ms / 1000)
    delivery_success_count = _as_int(observed.get("delivery_success_count"))
    delivery_failure_count = _as_int(observed.get("delivery_failure_count"))
    retryable_failure_count = _as_int(observed.get("retryable_failure_count"))
    non_retryable_failure_count = _as_int(observed.get("non_retryable_failure_count"))
    replay_attempt_count = _as_int(observed.get("replay_attempt_count"))
    requeue_count = _as_int(observed.get("requeue_count"))
    carrier_run_id = _optional_str(observed.get("carrier_run_id")) or "unknown-carrier-run"
    run_ordinal = _as_int(observed.get("run_ordinal"), default=0)
    status = "real_result_ingested" if wall_clock_ms is not None else "partial_result_ingested"
    return {
        "schema_version": 2,
        "phase": "P5-M4-S4 benchmark real-result ingestion",
        "case": _case_to_dict(case),
        "execution_mode": "real_carrier_result_ingested",
        "carrier": _carrier_to_dict(case.carrier),
        "carrier_run_id": carrier_run_id,
        "run_ordinal": run_ordinal,
        "measured": {
            "wall_clock_ms": wall_clock_ms,
            "process_cpu_ms": process_cpu_ms,
            "unit_count": unit_count,
            "measurement_unit": case.measurement_unit,
            "throughput_units_per_second": throughput_units_per_second,
        },
        "delivery_summary": {
            "available": delivery_success_count > 0 or delivery_failure_count > 0,
            "success_count": delivery_success_count,
            "failure_count": delivery_failure_count,
            "retryable_failure_count": retryable_failure_count,
            "non_retryable_failure_count": non_retryable_failure_count,
        },
        "recovery_summary": {
            "available": replay_attempt_count > 0 or requeue_count > 0,
            "replay_attempt_count": replay_attempt_count,
            "requeue_count": requeue_count,
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
            "delivery_success_count": delivery_success_count,
            "delivery_failure_count": delivery_failure_count,
            "retryable_failure_count": retryable_failure_count,
            "non_retryable_failure_count": non_retryable_failure_count,
            "replay_attempt_count": replay_attempt_count,
            "requeue_count": requeue_count,
        },
        "field_maturity": benchmark_field_maturity_map(),
        "layer_guardrails": {
            "layer": case.layer,
            "single_headline_across_layers_allowed": False,
            "valid_comparison_scope": list(case.valid_comparisons),
            "invalid_comparisons": list(case.invalid_comparisons),
        },
        "baseline_run_policy": dict(P5_BENCHMARK_BASELINE_RUN_POLICY),
        "validity": {
            "status": status,
            "not_full_retained_surface_matrix": True,
            "bounded_runtime_boundary": "local same-host/shared-storage",
            "caveats": list(P5_BENCHMARK_BOUNDED_CAVEATS),
        },
        "redaction": {
            "contains_secret_values": False,
            "rule": "Benchmark output records carrier/result facts, not secret values.",
        },
    }


def summarize_p5_benchmark_results(results: Sequence[Mapping[str, object]]) -> dict[str, object]:
    layers: set[str] = set()
    case_ids: list[str] = []
    for result in results:
        case_value = result.get("case")
        if not isinstance(case_value, Mapping):
            continue
        case_obj = cast(Mapping[str, object], case_value)
        case_id = case_obj.get("case_id")
        layer = case_obj.get("layer")
        if isinstance(case_id, str):
            case_ids.append(case_id)
        if isinstance(layer, str):
            layers.add(layer)
    mixed_layers = len(layers) > 1
    return {
        "schema_version": 1,
        "case_ids": case_ids,
        "layers": sorted(layers),
        "guardrails": {
            "mixed_layers": mixed_layers,
            "single_headline_allowed": not mixed_layers,
            "headline_reason": (
                "single-layer summary only"
                if not mixed_layers
                else "mixed benchmark layers cannot produce one headline result"
            ),
        },
    }


def format_p5_benchmark_result(result: Mapping[str, object]) -> str:
    case = result["case"]
    measured = result.get("measured", {})
    validity = result.get("validity", {})
    guardrails = result.get("layer_guardrails", {})
    assert isinstance(case, Mapping)
    assert isinstance(measured, Mapping)
    assert isinstance(validity, Mapping)
    assert isinstance(guardrails, Mapping)
    case_obj = cast(Mapping[str, object], case)
    measured_obj = cast(Mapping[str, object], measured)
    validity_obj = cast(Mapping[str, object], validity)
    guardrails_obj = cast(Mapping[str, object], guardrails)
    return "\n".join(
        (
            "P5-M4 benchmark result:",
            f"- case: {case_obj['case_id']}",
            f"- layer: {case_obj['layer']}",
            f"- execution mode: {result['execution_mode']}",
            f"- carrier: {case_obj['carrier']}",
            f"- measurement unit: {measured_obj.get('measurement_unit')}",
            f"- wall clock ms: {measured_obj.get('wall_clock_ms')}",
            f"- unit count: {measured_obj.get('unit_count')}",
            f"- validity: {validity_obj.get('status')}",
            (
                "- anti-mix-layer: single headline across layers allowed="
                f"{guardrails_obj.get('single_headline_across_layers_allowed')}"
            ),
            "- baseline policy: 2-run usable baseline; no global r3 requirement",
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
        artifact_has_carriers = True
        artifact_has_isolation = True
        if isinstance(artifact_cases_value, list):
            artifact_cases_raw = cast(list[object], artifact_cases_value)
            for item in artifact_cases_raw:
                if isinstance(item, dict):
                    item_obj = cast(dict[object, object], item)
                    case_id = item_obj.get("case_id")
                    if isinstance(case_id, str):
                        artifact_case_ids.add(case_id)
                    carrier = item_obj.get("carrier")
                    if not isinstance(carrier, dict):
                        artifact_has_carriers = False
                        artifact_has_isolation = False
                    else:
                        carrier_obj = cast(dict[object, object], carrier)
                        if not isinstance(carrier_obj.get("command"), list):
                            artifact_has_carriers = False
                        if not isinstance(carrier_obj.get("isolation_strategy"), str):
                            artifact_has_isolation = False
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
        field_maturity = raw_registry.get("field_maturity")
        checks.append(
            P5BenchmarkCheck(
                name="registry_declares_field_maturity",
                ok=isinstance(field_maturity, dict)
                and set(cast(dict[str, object], field_maturity)) == set(P5_BENCHMARK_FIELD_ORDER),
                detail="field_maturity keys",
            )
        )
        checks.append(
            P5BenchmarkCheck(
                name="registry_declares_real_carriers",
                ok=artifact_has_carriers,
                detail="carrier command contract present for all cases",
            )
        )
        checks.append(
            P5BenchmarkCheck(
                name="registry_declares_isolation_strategy",
                ok=artifact_has_isolation,
                detail="isolation strategy present for all cases",
            )
        )
        run_policy = raw_registry.get("baseline_run_policy")
        checks.append(
            P5BenchmarkCheck(
                name="registry_declares_bounded_r3_policy",
                ok=isinstance(run_policy, dict)
                and cast(dict[object, object], run_policy).get("global_r3_required") is False,
                detail="global r3 not required; targeted later r3 only",
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
        checks.append(
            P5BenchmarkCheck(
                name="report_declares_unique_opensearch_index",
                ok="unique index per run" in report_text,
                detail=str(P5_BENCHMARK_REPORT_PATH),
            )
        )
    return checks


def format_p5_benchmark_check_results(checks: Sequence[P5BenchmarkCheck]) -> str:
    lines = ["P5-M4 benchmark artifact checks:"]
    for check in checks:
        state = "ok" if check.ok else "failed"
        lines.append(f"- {check.name} -> {state} ({check.detail})")
    return "\n".join(lines)
