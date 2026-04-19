**P5-M3 Recovery Operator Runbook**
This runbook is the current operator-facing recovery guide for Zephyr's artifact-backed recovery model. It reduces hidden knowledge, but it does not claim a finished observability platform, distributed redrive system, or cloud operations layer.

**What to inspect first**
1. Inspect queue state and delivery DLQ presence first.
2. Read `run_meta.json` provenance next, especially `run_origin` and `delivery_origin`.
3. Use `delivery_receipt.json` and `batch_report.json` for retryability and failure classification.
4. Choose Replay for delivery DLQ recovery and Requeue for queue poison/inflight recovery.
5. Verify post-action facts before closing the recovery action.

**Primary vs Secondary Evidence**
- Primary queue evidence:
  - `queue.sqlite3`
  - `inspect_local_sqlite_queue`
  - task fields: `state`, `poison_kind`, `governance_problem`, `failure_count`, `orphan_count`, `recovery_audit_support`
- Primary delivery/replay evidence:
  - `_dlq/delivery/*.json`
  - `_dlq/delivery_done/*.json`
  - `delivery_receipt.json`
  - `batch_report.json`
  - `run_meta.json`
- Primary resume evidence:
  - `checkpoint.json`
  - `records.jsonl`
  - `run_meta.json`
- Secondary evidence:
  - `replay.db`
  - bounded Prometheus text
  - CLI/helper result objects such as `ReplayStats` and `QueueRecoveryResultV1`

**Decisive Provenance Fields**
- `run_origin="intake"` means first-pass execution.
- `run_origin="resume"` means source-side continuation from an explicit checkpoint.
- `run_origin="requeue"` means queue governance moved work back to pending.
- `delivery_origin="primary"` means first-pass delivery for that run/provenance history.
- `delivery_origin="replay"` means delivery recovery from the shared replay/DLQ path.

Do not collapse Replay, Requeue, Resume, and Intake into one generic recovery story. They are distinct histories.

**Replay**
- Trigger:
  - failed delivery receipt/report
  - replayable `_dlq/delivery/*.json` artifact
- Inspect first:
  - `_dlq/delivery/*.json`
  - `delivery_receipt.json`
  - `batch_report.json`
  - `run_meta.json`
- Decision fields:
  - `failure_retryability`
  - `details.retryable`
  - `failure_kind`
  - `error_code`
  - `run_meta.provenance.run_origin`
  - `run_meta.provenance.delivery_origin`
- Action:
  - run `replay_delivery_dlq` or the CLI wrapper against the relevant `out_root`
- Verify:
  - replay result reports succeeded work
  - `_dlq/delivery_done/*.json` contains the moved record when `move_done` is enabled
  - replayed payload has `delivery_origin="replay"`

**Requeue**
- Trigger:
  - queue inspect shows `poison > 0` or supported inflight recovery
  - poison task usually reports `governance_problem="poison_attempts_exhausted"`
- Inspect first:
  - `queue.sqlite3`
  - `inspect_local_sqlite_queue`
- Decision fields:
  - `task.state`
  - `task.poison_kind`
  - `task.governance_problem`
  - `task.failure_count`
  - `task.orphan_count`
  - `task.recovery_audit_support`
- Action:
  - run `requeue_local_task` for the task id and source bucket
- Verify:
  - queue pending count increases for that task
  - `QueueRecoveryResultV1.governance_result="moved_to_pending"`
  - converted provenance has `run_origin="requeue"` and `delivery_origin="primary"`

**Resume**
- Trigger:
  - an `it-stream` cursor-family flow resumes from an explicit checkpoint selection
- Inspect first:
  - `checkpoint.json`
  - `run_meta.json`
  - `records.jsonl`
- Decision fields:
  - `run_meta.provenance.run_origin`
  - `run_meta.provenance.checkpoint_identity_key`
  - `checkpoint.parent_checkpoint_identity_key`
  - record count and cursor continuation facts
- Action:
  - use the current checkpoint/resume path for the explicit checkpoint id
- Verify:
  - `run_origin="resume"`
  - `delivery_origin` remains `primary` unless delivery replay later occurs
  - checkpoint lineage links only the immediate prior checkpoint

**Retryable vs Non-Retryable Decision**
- Retryable signals:
  - `failure_retryability="retryable"`
  - `details.retryable=true`
  - shared failure kind/error code matches a replayable condition
- Non-retryable or ambiguous signals:
  - `failure_retryability="non_retryable"`
  - `details.retryable=false`
  - missing replayable DLQ artifact
  - unsupported queue/recovery state
- Rule:
  - replay retryable delivery failures
  - requeue supported poison/inflight queue cases only after the cause is corrected or deliberately retried
  - do not silently retry non-retryable or ambiguous cases

**Audit Support Caveat**
- `persisted_in_history` means the action has persisted recovery history in that backend/path.
- `result_only` means the action result is inspectable, but the backend does not persist a richer action history.
- Backend-shaped visibility differences remain meaningful; do not smooth them over in incident notes.

**Post-Recovery Verification**
- After Replay:
  - check `_dlq/delivery_done/*.json`
  - check replay sink output such as `replay.db`
  - check payload provenance for `delivery_origin="replay"`
- After Requeue:
  - inspect pending queue
  - verify `governance_result="moved_to_pending"`
  - verify converted provenance has `run_origin="requeue"`
- After Resume:
  - inspect `run_meta.json`, `checkpoint.json`, and `records.jsonl`
  - verify `run_origin="resume"` and explicit checkpoint identity

**Known Environment Noise**
- Windows `.tmp` access-denied noise is known environment noise unless it changes runtime/governance correctness.

**External Network Topics Outside Core Recovery Claim**
- Google Drive/TUN/network dependency is real, but it is not part of the core recovery/operator claim.

**Unsupported Or Deferred**
- Finished distributed redrive platform
- Backend-native recovery that bypasses Zephyr replay/DLQ ownership
- Universal observability platform claims
- Cloud/Kubernetes/Helm recovery operations
- Connector-local side channels as recovery truth surfaces
