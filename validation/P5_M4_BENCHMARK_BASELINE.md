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

## First-Wave Cases
The first-wave registry deliberately separates benchmark layers. These cases must not be collapsed into one headline result.

| Case | Layer | Unit | Basis | Valid comparison |
| --- | --- | --- | --- | --- |
| `uns_reacquisition_replay_representative` | source flow representative | `document` | `test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned` | same-case repeatability only |
| `it_resume_replay_representative` | source flow representative | `checkpoint_resume_task` | `test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface` | same-case repeatability only |
| `filesystem_lightweight_delivery` | destination representative | `delivery_payload` | retained filesystem local-real delivery path | destination-layer comparison against OpenSearch only |
| `opensearch_heavier_delivery` | destination representative | `delivery_payload` | retained OpenSearch service-live delivery path | destination-layer comparison against filesystem only |
| `replay_after_failed_delivery_recovery_overhead` | recovery overhead | `inspect_decide_act_verify_workflow` | P5-M3 delivery failure to replay scenario | same recovery-case repeatability only |

The `test_p45_m7_worker_queue_lock_0` style runtime/governance drill is not a first-wave representative benchmark baseline case. It remains a later runtime/governance overhead candidate and should not dominate this harness.

## Measurement Units
The benchmark unit is case-defined:

- document-oriented `uns` case: `document`
- checkpoint/resume `it` case: `checkpoint_resume_task`
- destination cases: `delivery_payload`
- recovery overhead case: `inspect_decide_act_verify_workflow`

Throughput-like SLI candidates are valid only when the harness records an explicit `unit_count` for that case. A result with `unit_count=0` is harness validation output, not a throughput measurement.

## SLI Candidate Fields
The harness supports these bounded SLI candidate fields:

- `wall_clock_ms`
- `process_cpu_ms`
- `measurement_unit`
- `unit_count`
- `throughput_units_per_second`
- `delivery_success_count`
- `delivery_failure_count`
- `retryable_failure_count`
- `non_retryable_failure_count`
- `replay_attempt_count`
- `requeue_count`

These are candidate fields, not a finished SLO system. Alerting, dashboards, fleet-wide aggregation, autoscaling efficiency, and backend-native throughput guarantees remain out of scope.

## Recovery Overhead Framing
The first-wave recovery-overhead case measures the bounded inspect -> decide -> act -> verify workflow. It does not reduce recovery to action-only replay call cost.

Action-only replay duration is a partial surface that may be recorded by a later harness extension, but it is not the headline recovery unit for this milestone.

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
uv run --locked --no-sync python tools/p5_benchmark_report.py --case filesystem_lightweight_delivery --json
```

The `--case` command currently runs a named case in harness-validation mode. Full benchmark execution is deferred to the later execution stage.

## Exit Statement
P5-M4-S2 establishes a benchmark/SLI/capacity-baseline harness and scenario registry for the first representative execution wave. It does not claim completed benchmark results for the full retained support surface.
