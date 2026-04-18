**P5-M2 Runtime Boundary**
This artifact captures the current production boundary for Zephyr's ingest-owned queue, lock, worker, recovery, and operator surfaces. It does not claim a distributed runtime platform, cloud packaging, autoscaling, or broader orchestration guarantees than the current local/runtime-backed evidence supports.

**Scope**
- Execution chain remains `TaskV1 -> FlowProcessor -> Delivery`.
- `zephyr-ingest` remains owner of orchestration, queue/lock/runtime wiring, replay/DLQ policy, and governance glue.
- Control plane remains the repo.
- Runtime plane remains the external runtime-home rooted at `E:\zephyr_env\.config\zephyr\p45`.
- Current runtime/governance drill baseline already exists in `test_p45_m7_cross_cutting_drills.py` and remains intentionally gated by `--auth-tier recovery-drill`.

**Current Boundary Statement**
- Supported current runtime facts:
  - Single-worker local execution is supported through ingest-owned queue/lock/runtime surfaces.
  - `spool` and `sqlite` remain supported local queue backends.
  - `file` and `sqlite` remain supported local lock backends.
  - Current operator evidence is artifact-backed and repo-side: `queue.sqlite3`, `run_meta.json`, `delivery_receipt.json`, `batch_report.json`, `checkpoint.json`, `records.jsonl`, `replay.db`, `_dlq/delivery_done/*.json`, and bounded Prometheus text.
  - Replay, requeue, resume, and intake remain distinct provenance histories.
- Bounded current runtime facts:
  - Multi-worker behavior is only proven for a bounded same-host/shared-storage model using local sqlite queue plus local file/sqlite locks.
  - Runtime drain / stop is bounded local in-flight shutdown behavior, not a fleet-wide coordinated drain protocol.
  - Redrive semantics remain `not_modeled` beyond explicit requeue and replay.
- Intentionally unsupported or deferred:
  - No distributed lease choreography or fencing.
  - No broker-backed queue abstraction.
  - No autoscaling, scheduler, benchmark/SLI, or cloud/Kubernetes/Helm claim in this milestone.

**Scenario Matrix**
| Scenario | Status | Target Behavior | Inspectability Surfaces | Governance Facts | Evidence |
| --- | --- | --- | --- | --- | --- |
| single-worker baseline | supported | One local worker drains pending work through the shared ingest execution chain. | `queue.sqlite3` or spool buckets, `run_meta.json`, `delivery_receipt.json`, `batch_report.json`, Prometheus text | `run_origin=intake`, `delivery_origin=primary`, queue states remain visible | `test_worker_runtime.py::test_run_worker_executes_sqlite_queue_backend_when_selected`, `test_worker_runtime.py::test_runtime_supports_supported_queue_and_lock_backend_pairs` |
| two-worker contention | bounded | Two local workers contend against a shared sqlite pending-work surface; duplicate task identity contention requeues the contended claim instead of double execution. | `queue.sqlite3`, `inspect_local_sqlite_queue`, Prometheus text | orphan/requeue counters remain explicit; contended task stays pending and inspectable | `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_two_worker_contention_is_bounded_and_inspectable` |
| stale lock recovery | supported | A stale file or sqlite lock can be broken by the current local lock providers and the task can continue on the primary path. | `queue.sqlite3`, `run_meta.json`, Prometheus text | stale recovery is counted; no distributed lease claim | `test_worker_runtime.py::test_runtime_supports_stale_lock_recovery_for_supported_lock_backends`, `test_p45_m7_cross_cutting_drills.py::test_p45_m7_worker_queue_lock_and_operator_surfaces_remain_inspectable` |
| poison after attempts exhausted | supported | Attempts-exhausted work moves to poison and remains operator-visible. | `queue.sqlite3`, `inspect_local_sqlite_queue` | `governance_problem=poison_attempts_exhausted`, failure count remains visible | `test_p45_m7_cross_cutting_drills.py::test_p45_m7_worker_queue_lock_and_operator_surfaces_remain_inspectable` |
| requeue from poison | supported | Operator-driven requeue moves poison back to pending through ingest-owned governance surfaces. | `queue.sqlite3`, `inspect_local_sqlite_queue`, `requeue_local_task` result | `run_origin=requeue` stays explicit; sqlite audit support remains `result_only`; redrive semantics remain `not_modeled` | `test_p45_m7_cross_cutting_drills.py::test_p45_m7_worker_queue_lock_and_operator_surfaces_remain_inspectable` |
| replay after failed delivery | supported | Failed delivery artifacts replay through the shared replay/DLQ path and can be moved to `delivery_done` after success. | `replay.db`, `_dlq/delivery/*.json`, `_dlq/delivery_done/*.json`, `run_meta.json` | `delivery_origin` changes to `replay` while `run_origin` remains explicit | `test_p45_m7_cross_cutting_drills.py::test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface`, `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_replay_after_failed_delivery_keeps_operator_artifacts_aligned` |
| runtime drain / stop | bounded | A worker may drain after the current in-flight item, leaving later tasks pending and inspectable. | `queue.sqlite3`, `inspect_local_sqlite_queue`, Prometheus text | drain affects new intake, not current in-flight work | `test_worker_runtime.py::test_worker_runtime_draining_stops_new_work_and_emits_events`, `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_runtime_drain_leaves_pending_work_inspectable` |
| operator inspectability before and after recovery | supported | Current operator surfaces still answer what ran, what failed, whether it was retryable, and whether it was replayed, resumed, or requeued. | `inspect_local_sqlite_queue`, `run_meta.json`, `delivery_receipt.json`, `batch_report.json`, `replay.db`, `_dlq/delivery_done/*.json`, Prometheus text | replay, resume, and requeue remain distinct provenance histories | `test_p45_m7_cross_cutting_drills.py::test_p45_m7_uns_reacquisition_delivery_replay_and_batch_report_stay_aligned`, `test_p45_m7_cross_cutting_drills.py::test_p45_m7_it_resume_replay_preserves_resume_facts_through_shared_delivery_surface`, `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_two_worker_contention_is_bounded_and_inspectable`, `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_replay_after_failed_delivery_keeps_operator_artifacts_aligned` |

**Drill Package**
- Existing retained drill baseline:
  - `test_p45_m7_cross_cutting_drills.py` with `--auth-tier recovery-drill`
- New P5-M2 drill additions:
  - `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_runtime_drain_leaves_pending_work_inspectable`
  - `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_two_worker_contention_is_bounded_and_inspectable`
  - `test_p5_m2_runtime_boundary_drills.py::test_p5_m2_replay_after_failed_delivery_keeps_operator_artifacts_aligned`

**Known Environment Noise**
- Windows `.tmp` access-denied noise has been observed during pytest temp-root usage. It is treated as environment noise unless it changes runtime/governance correctness.

**External-Network-Dependent Topics Kept Out Of Core Runtime Conclusions**
- Google Drive/TUN/network dependency is real, but it is not part of the core queue/lock/runtime boundary claim.

**What This Milestone Does Not Claim**
- A finished distributed runtime platform
- Broader multi-worker guarantees than the bounded same-host/shared-storage evidence above
- Cloud/Kubernetes/Helm packaging
- Connector-breadth expansion
- Queue/lock/runtime architecture redesign
