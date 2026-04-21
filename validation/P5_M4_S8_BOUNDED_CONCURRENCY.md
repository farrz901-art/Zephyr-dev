**P5-M4-S8 Bounded Concurrency**
This artifact freezes the current higher-concurrency claim for Zephyr's ingest-owned local runtime. It strengthens the same-host/shared-storage evidence without claiming a distributed runtime, broker-backed queue, scheduler, autoscaling, cloud/Kubernetes packaging, or full fleet worker behavior.

**Scope**
- Execution chain remains `TaskV1 -> FlowProcessor -> Delivery`.
- `zephyr-ingest` remains owner of orchestration, queue/lock/runtime wiring, replay/DLQ policy, worker runtime shape, and governance/operator glue.
- Current queue basis for S8 evidence is local `queue.sqlite3`.
- Current lock basis is local file/sqlite lock behavior and sqlite claim/update semantics.
- Worker runtime remains `single_process_local_polling`.

**Current Direct Evidence**
| Scenario | Direct Worker Count | Status | Evidence |
| --- | ---: | --- | --- |
| 4-worker same-pending contention | 4 | evidenced | `test_p5_m4_s8_four_worker_same_pending_contention_is_bounded_and_inspectable` |
| backlog drain under multi-worker | 3 | evidenced | `test_p5_m4_s8_backlog_drain_under_three_workers_leaves_pending_inspectable` |
| stale lock plus contention | 2 | evidenced | `test_p5_m4_s8_stale_lock_recovery_and_contention_remain_local_and_auditable` |
| poison requeue under contention | 3 | evidenced | `test_p5_m4_s8_poison_requeue_under_contention_remains_operator_readable` |
| replay and queue governance coexistence | 3 | evidenced | `test_p5_m4_s8_replay_and_queue_governance_remain_distinct_under_contention` |

**Operator Surfaces Protected**
- `queue.sqlite3` remains the primary queue truth surface.
- `inspect_local_sqlite_queue` remains the operator-facing queue inspection surface.
- Bounded Prometheus text exposes worker runtime boundary, queue buckets, lock contention, stale recovery, and queue policy facts.
- `requeue_local_task` remains the queue-governance action surface for poison/inflight recovery.
- `replay.db`, `_dlq/delivery/*.json`, `_dlq/delivery_done/*.json`, and `run_meta.json` remain delivery replay surfaces.

**Bounded Truths**
- The maximum direct worker count now evidenced by S8 is four local workers against the same sqlite-backed pending surface.
- Drain/stop remains local in-flight shutdown behavior; it is not a coordinated fleet drain.
- Stale lock recovery remains local provider behavior; it is not distributed lease choreography or fencing.
- Queue-side requeue governance and delivery-side replay governance remain distinct provenance histories.
- Replay/requeue/resume distinctions are protected through artifact-backed facts, not an external observability platform.

**No distributed runtime claim**
- No distributed runtime or fleet-worker platform is claimed.
- No broker-backed queue abstraction is claimed.
- No autoscaling or scheduler ownership is claimed.
- No cloud/Kubernetes/Helm deployment claim is made.
- No throughput benchmark or capacity headline is made by these drills.

**Known Boundaries**
- This is same-host/shared-storage evidence only.
- SQLite queue and local lock behavior are the relied-on S8 evidence surfaces.
- Redrive remains outside the modeled surface beyond explicit requeue and replay actions.
- Windows `.tmp` ACL noise remains environment noise unless it changes runtime/governance correctness.
