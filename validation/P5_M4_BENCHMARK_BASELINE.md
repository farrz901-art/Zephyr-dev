# P5-M4 Benchmark / SLI / Capacity Baseline Harness

## Scope
This artifact defines the first-wave benchmark harness for the already-audited retained support surface.

It is a bounded local same-host/shared-storage benchmark package. It is not a distributed runtime benchmark, not a cloud/Kubernetes capacity claim, not a dashboard or observability-platform claim, and not a full retained-surface validation matrix.

The machine-readable scenario registry is [p5_benchmark_scenario_registry.json](p5_benchmark_scenario_registry.json).

## Runtime Boundary
Current benchmark runs are scoped to the existing external runtime-home model:

- control plane: repo-owned code, helpers, tests, reports, and benchmark tooling
- runtime plane: external runtime-home rooted at `E:\zephyr_env\.config\zephyr\p45`
- execution chain: `TaskV1 -> FlowProcessor -> Delivery`
- ownership: `zephyr-ingest` owns orchestration, queue/lock/runtime wiring, replay/DLQ policy, and governance surfaces
- runtime shape: bounded local same-host/shared-storage

The benchmark harness must not infer distributed scheduling, Kubernetes packaging, cloud autoscaling, fleet capacity, or enterprise observability from local artifact-backed evidence.

## Case To Carrier Contract
Each first-wave benchmark case now has an explicit real carrier contract in `p5_benchmark_scenario_registry.json`.

The carrier contract records:

- pytest target file
- pytest selector
- required auth tier where applicable
- expected artifact family
- isolation strategy
- benchmark-specific env override names where needed

This is the canonical mapping surface. The benchmark package must not depend on undocumented pytest-id folklore or author memory.

## OpenSearch Isolation
The OpenSearch lane no longer uses a fixed index as the benchmark end-state.

OpenSearch benchmark isolation is **unique index per run**:

- helper: `build_p5_benchmark_resource_name(case_id="opensearch_heavier_delivery", run_id=..., resource_kind="index")`
- carrier env override: `ZEPHYR_P45_BENCHMARK_OPENSEARCH_INDEX={unique_index_per_run}`
- fallback in the service-live carrier also derives a unique lower-case index from `tmp_path`

Manual pre-clean is not the productized benchmark model. This prevents stale-state contamination across repeated runs and reduces operator memory burden while staying bounded to local OpenSearch substrate semantics.

## First-Wave Cases
The first-wave registry deliberately separates benchmark layers. These cases must not be collapsed into one headline result.

| Case | Layer | Unit | Real carrier | Valid comparison |
| --- | --- | --- | --- | --- |
| `uns_reacquisition_replay_representative` | source flow representative | `document` | `test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned` | same-case repeatability only |
| `it_resume_replay_representative` | source flow representative | `checkpoint_resume_task` | `test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface` | same-case repeatability only |
| `filesystem_lightweight_delivery` | destination representative | `delivery_payload` | `test_filesystem_destination_writes_success_artifacts` | destination-layer comparison against OpenSearch only |
| `opensearch_heavier_delivery` | destination representative | `delivery_payload` | `test_p45_wave1_opensearch_live_success_failures_and_worker_consistency` | destination-layer comparison against filesystem only |
| `replay_after_failed_delivery_recovery_overhead` | recovery overhead | `inspect_decide_act_verify_workflow` | `test_p5_recovery_summary_combines_queue_delivery_and_provenance` | same recovery-case repeatability only |

The `test_p45_m7_worker_queue_lock_0` style runtime/governance drill is not a first-wave representative benchmark baseline case. It remains a later runtime/governance overhead candidate and should not dominate this harness.

## Canonical result ingestion
`tools/p5_benchmark_report.py --case ...` is no longer the benchmark surface.

Canonical result handling is now:

```powershell
uv run --locked --no-sync python tools/p5_benchmark_report.py --validate-case filesystem_lightweight_delivery --json
uv run --locked --no-sync python tools/p5_benchmark_report.py --record-result filesystem_lightweight_delivery --result-json .\observed.json --json
```

`--validate-case` validates the case contract and explicitly reports `is_benchmark_result=false`.

`--record-result` ingests a real carrier observation file and emits a first-class benchmark result with:

- case metadata
- real carrier contract
- measurement unit
- measured fields
- SLI candidate fields
- field maturity classifications
- anti-mix-layer guardrails
- bounded runtime caveats
- 2-run baseline policy

This is intentionally not a distributed benchmark framework. It is a truthful first-wave result-ingestion surface for the existing real carriers.

## Measurement Units
The benchmark unit is case-defined:

- document-oriented `uns` case: `document`
- checkpoint/resume `it` case: `checkpoint_resume_task`
- destination cases: `delivery_payload`
- recovery overhead case: `inspect_decide_act_verify_workflow`

Throughput-like SLI candidates are valid only when the harness records an explicit `unit_count` for that case.

## Field Maturity
Field maturity is unified in the machine-readable registry and emitted with ingested results.

| Field | Maturity | Headline use |
| --- | --- | --- |
| `wall_clock_ms` | usable now | allowed inside declared layer |
| `process_cpu_ms` | partial | not headline |
| `measurement_unit` | usable now | required |
| `unit_count` | usable now | required for throughput-like fields |
| `throughput_units_per_second` | partial | not headline by default |
| `delivery_success_count` | usable now | allowed when artifact-backed |
| `delivery_failure_count` | usable now | allowed when artifact-backed |
| `retryable_failure_count` | partial | not headline |
| `non_retryable_failure_count` | partial | not headline |
| `replay_attempt_count` | partial | not headline |
| `requeue_count` | not ready for headline | not headline |
| `resource_usage_snapshot` | partial | not headline |
| `prometheus_metrics_surface` | not ready for headline | not headline |

These are candidate SLI fields, not a finished SLO system. Alerting, dashboards, fleet-wide aggregation, autoscaling efficiency, and backend-native throughput guarantees remain out of scope.

## Anti-Mix-Layer Guardrails
Anti-mix-layer rules are now present in:

- scenario registry
- result schema
- CLI output
- helper summary output
- focused tests

A mixed source-flow + destination + recovery result set must not produce one headline benchmark. Summary helpers explicitly mark mixed-layer summaries as `single_headline_allowed=false`.

## Recovery Overhead Framing
The first-wave recovery-overhead case measures the bounded inspect -> decide -> act -> verify workflow. It does not reduce recovery to action-only replay call cost.

Action-only replay duration is a partial surface that may be recorded by a later harness extension, but it is not the headline recovery unit for this milestone.

## Run Count Judgment
The current first-wave baseline is a **2-run usable baseline**.

It is not a fully statistical capacity study. Global `r3` is not required now. A targeted later `r3` remains available only for a specific case if future instability justifies it.

## Machine / Environment Freeze
The current benchmark reality freeze is:

- CPU: `13th Gen Intel(R) Core(TM) i7-13700H`
- cores/logical processors: `14 / 20`
- memory: about `34 GB`
- OS: `Microsoft Windows 11 Pro for Workstations 64-bit (10.0.26100)`
- repo drive: `E:`
- runtime-home drive: `E:`
- free space on `E:`: about `109.98 GB`
- TUN mode: active
- known Windows `.tmp` ACL noise: non-blocking unless it changes correctness or artifact consistency

TUN/Google Drive/network-dependent topics are outside the core local benchmark baseline unless a later SaaS-specific benchmark case explicitly includes them.

## Partial Surfaces
The current harness can represent but must not overclaim:

- replay/requeue counts are case-dependent and partial unless recorded from artifacts
- Prometheus/metrics output is partial and artifact-backed, not a mature metrics platform
- resource observations are bounded snapshots, not a full profiler or capacity-management platform
- local OpenSearch uses HTTPS with skip-TLS-verify for self-signed local validation

## Harness Commands
Repo-side helper commands:

```powershell
uv run --locked --no-sync python tools/p5_benchmark_report.py --check-artifacts
uv run --locked --no-sync python tools/p5_benchmark_report.py --list-cases
uv run --locked --no-sync python tools/p5_benchmark_report.py --carrier-json opensearch_heavier_delivery
uv run --locked --no-sync python tools/p5_benchmark_report.py --validate-case filesystem_lightweight_delivery --json
uv run --locked --no-sync python tools/p5_benchmark_report.py --record-result filesystem_lightweight_delivery --result-json .\observed.json --json
```

## Exit Statement
P5-M4-S4 hardens the benchmark execution/result surface, carrier mapping, OpenSearch repeatability, metric maturity classification, anti-mix-layer guardrails, and `r3` policy. It still does not claim completed benchmark results for the full retained support surface.
