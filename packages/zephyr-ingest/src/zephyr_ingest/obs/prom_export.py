from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from zephyr_core import RunContext
from zephyr_core.contracts.v2.lifecycle import Lifecycle
from zephyr_ingest.obs.batch_report_v1 import BATCH_REPORT_SCHEMA_VERSION, BatchReportV1


def _escape_label_value(v: str) -> str:
    # Prometheus text format label escaping:
    # backslash, double-quote, and newlines. <!--citation:2-->
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(**pairs: str) -> str:
    if not pairs:
        return ""
    inner = ",".join(f'{k}="{_escape_label_value(v)}"' for k, v in pairs.items())
    return "{" + inner + "}"


@dataclass(frozen=True, slots=True)
class PromSample:
    name: str
    value: float
    labels: dict[str, str]


@dataclass(frozen=True, slots=True)
class PromMetricFamily:
    name: str
    help: str
    mtype: str  # "gauge"|"counter"
    samples: list[PromSample]


def _ms_to_seconds(ms: int | None) -> float | None:
    if ms is None:
        return None
    return float(ms) / 1000.0


def load_batch_report_v1(*, out_root: Path) -> BatchReportV1:
    out_root = out_root.expanduser().resolve()
    fp = (out_root / "batch_report.json").resolve()
    obj = json.loads(fp.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("batch_report.json is not an object")
    rep = cast(BatchReportV1, obj)
    if rep.get("schema_version") != BATCH_REPORT_SCHEMA_VERSION:
        raise ValueError("unsupported batch_report schema_version")
    return rep


def build_prom_families(*, report: BatchReportV1) -> list[PromMetricFamily]:
    """
    Convert a BatchReportV1 to Prometheus exposition families.

    Notes:
    - This exporter emits per-run gauges (batch job semantics).
    - Names follow Prometheus naming practices as much as practical. <!--citation:3-->
    """
    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("batch_report.metrics missing")

    # low-cardinality "info" labels
    pipeline_version = str(report["pipeline_version"])
    strategy = str(report["strategy"])
    executor = str(report["executor"])

    backend_kind = "unknown"
    cs = report.get("config_snapshot")
    if cs:
        backend = cs.get("backend")
        if backend:
            backend_kind = backend.get("kind", "unknown")

    base_labels = {
        "pipeline_version": pipeline_version,
        "strategy": strategy,
        "executor": executor,
        "backend_kind": backend_kind,
    }

    fams: list[PromMetricFamily] = []

    def add_gauge(name: str, help: str, value: float, labels: dict[str, str] | None = None) -> None:
        fams.append(
            PromMetricFamily(
                name=name,
                help=help,
                mtype="gauge",
                samples=[PromSample(name=name, value=value, labels=(labels or {}))],
            )
        )

    def add_gauge_family(name: str, help: str, samples: list[PromSample]) -> None:
        fams.append(PromMetricFamily(name=name, help=help, mtype="gauge", samples=samples))

    # info metric
    add_gauge_family(
        "zephyr_ingest_run_info",
        "Zephyr ingest run info (1 for the latest batch_report).",
        [PromSample("zephyr_ingest_run_info", 1.0, base_labels)],
    )

    # wall time + throughput
    wall_ms = metrics.get("run_wall_ms")
    if wall_ms:
        wall_s = float(wall_ms) / 1000.0
        add_gauge(
            "zephyr_ingest_run_wall_seconds",
            "Wall-clock runtime for run_documents in seconds.",
            wall_s,
            base_labels,
        )

    dpm = metrics.get("docs_per_min")
    if isinstance(dpm, (int, float)):
        add_gauge(
            "zephyr_ingest_run_docs_per_minute",
            "Derived throughput (docs/min) for the latest run.",
            float(dpm),
            base_labels,
        )

    # totals (per-run gauges)
    def _g(key: str) -> float:
        v = metrics.get(key)
        return float(v) if isinstance(v, int) else 0.0

    totals = [
        (
            "zephyr_ingest_run_docs_total",
            "Total docs processed in the latest run.",
            _g("docs_total"),
        ),
        (
            "zephyr_ingest_run_docs_success_total",
            "Docs succeeded in the latest run.",
            _g("docs_success_total"),
        ),
        (
            "zephyr_ingest_run_docs_failed_total",
            "Docs failed in the latest run.",
            _g("docs_failed_total"),
        ),
        (
            "zephyr_ingest_run_docs_skipped_total",
            "Docs skipped in the latest run.",
            _g("docs_skipped_total"),
        ),
        (
            "zephyr_ingest_run_delivery_total",
            "Deliveries attempted in the latest run.",
            _g("delivery_total"),
        ),
        (
            "zephyr_ingest_run_delivery_ok_total",
            "Deliveries succeeded in the latest run.",
            _g("delivery_ok_total"),
        ),
        (
            "zephyr_ingest_run_delivery_failed_total",
            "Deliveries failed in the latest run.",
            _g("delivery_failed_total"),
        ),
        (
            "zephyr_ingest_run_delivery_failed_retryable_total",
            "Deliveries failed (retryable) in the latest run.",
            _g("delivery_failed_retryable_total"),
        ),
        (
            "zephyr_ingest_run_delivery_failed_non_retryable_total",
            "Deliveries failed (non-retryable) in the latest run.",
            _g("delivery_failed_non_retryable_total"),
        ),
        (
            "zephyr_ingest_run_delivery_failed_unknown_total",
            "Deliveries failed (retryable unknown) in the latest run.",
            _g("delivery_failed_unknown_total"),
        ),
        (
            "zephyr_ingest_run_dlq_written_total",
            "Delivery DLQ records written in the latest run.",
            _g("dlq_written_total"),
        ),
    ]
    for name, help, val in totals:
        add_gauge(name, help, val, base_labels)

    # delivery by destination
    delivery = report.get("delivery")
    if delivery:
        by_dest = delivery.get("by_destination")
        if by_dest:
            ok_samples: list[PromSample] = []
            fail_samples: list[PromSample] = []
            for dest, counters in by_dest.items():
                ok = counters.get("ok")
                failed = counters.get("failed")
                if ok:
                    ok_samples.append(
                        PromSample(
                            "zephyr_ingest_run_delivery_ok_total",
                            float(ok),
                            {**base_labels, "destination": dest},
                        )
                    )
                if failed:
                    fail_samples.append(
                        PromSample(
                            "zephyr_ingest_run_delivery_failed_total",
                            float(failed),
                            {**base_labels, "destination": dest},
                        )
                    )

            if ok_samples:
                add_gauge_family(
                    "zephyr_ingest_run_delivery_ok_by_destination_total",
                    "Deliveries succeeded in the latest run (by destination).",
                    ok_samples,
                )
            if fail_samples:
                add_gauge_family(
                    "zephyr_ingest_run_delivery_failed_total",
                    "Deliveries failed in the latest run (by destination).",
                    fail_samples,
                )

    # doc duration + stage durations (seconds)
    durations = report.get("durations_ms")
    if durations:
        samples: list[PromSample] = []
        for stat in ("min", "max", "avg", "p95"):
            v = durations.get(stat)
            if isinstance(v, int) or v is None:
                s = _ms_to_seconds(v if isinstance(v, int) else None)
                if s is not None:
                    samples.append(
                        PromSample(
                            "zephyr_ingest_run_doc_duration_seconds",
                            s,
                            {**base_labels, "stat": stat},
                        )
                    )
        if samples:
            add_gauge_family(
                "zephyr_ingest_run_doc_duration_seconds",
                "Per-doc total duration stats in seconds (min/max/avg/p95).",
                samples,
            )

    stage = report.get("stage_durations_ms")
    if isinstance(stage, dict):
        stage_samples: list[PromSample] = []
        for stage_name in ("hash_ms", "partition_ms", "delivery_ms"):
            d = stage.get(stage_name)
            if not isinstance(d, dict):
                continue
            for stat in ("min", "max", "avg", "p95"):
                v: int | None = d.get(stat)  # type: ignore
                if isinstance(v, int) or v is None:
                    s = _ms_to_seconds(v if isinstance(v, int) else None)
                    if s is not None:
                        stage_samples.append(
                            PromSample(
                                "zephyr_ingest_run_stage_duration_seconds",
                                s,
                                {
                                    **base_labels,
                                    "stage": stage_name.replace("_ms", ""),
                                    "stat": stat,
                                },
                            )
                        )
        if stage_samples:
            add_gauge_family(
                "zephyr_ingest_run_stage_duration_seconds",
                "Stage timing stats in seconds (hash/partition/delivery; min/max/avg/p95).",
                stage_samples,
            )

    return fams


def build_worker_prom_families(
    *,
    ctx: RunContext,
    lifecycle: Lifecycle,
) -> list[PromMetricFamily]:
    phase = lifecycle.phase.value
    labels = {"pipeline_version": ctx.pipeline_version}

    return [
        PromMetricFamily(
            name="zephyr_ingest_worker_info",
            help="Zephyr ingest worker info (1 while the worker process is alive).",
            mtype="gauge",
            samples=[PromSample("zephyr_ingest_worker_info", 1.0, labels)],
        ),
        PromMetricFamily(
            name="zephyr_ingest_worker_phase",
            help="Current Zephyr ingest worker lifecycle phase (1 for the current phase).",
            mtype="gauge",
            samples=[
                PromSample(
                    "zephyr_ingest_worker_phase",
                    1.0,
                    {**labels, "phase": phase},
                )
            ],
        ),
    ]


def render_prometheus_text(*, families: list[PromMetricFamily]) -> str:
    """
    Render families to Prometheus text exposition format.
    """
    lines: list[str] = []
    for fam in families:
        lines.append(f"# HELP {fam.name} {fam.help}")
        lines.append(f"# TYPE {fam.name} {fam.mtype}")
        for s in fam.samples:
            lines.append(f"{s.name}{_labels(**s.labels)} {s.value}")
    lines.append("")
    return "\n".join(lines)
