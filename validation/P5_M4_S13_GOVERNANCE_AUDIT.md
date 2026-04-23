# P5-M4-S13 Governance Audit

governance/manual-action truth is bounded local operator truth, not RBAC, approval workflow,
multi-operator collaboration, billing, pricing, license, subscription, or entitlement logic.

S13 adds the `governance_action_receipt_v1` receipt shape. Requeue and replay-delivery now have
persisted receipts under `_governance/actions/<action_id>.json`.

Current action truth:

- `requeue` is a real state-changing governance action. It can write a persisted action receipt.
- `replay_delivery` is a real state-changing governance action. It can write a persisted action
  receipt while preserving `delivery_origin=replay`.
- `inspect_queue` is read-only. It remains a strong result surface, and receipt support is helper
  based rather than auto-written to avoid making inspection mutate queue state.
- `verify_recovery_result` is modeled for governance truth, but verify is not first-class runtime
  action today.

Governance receipts can link task, run, provenance, usage, source contract, and evidence refs where
those facts are available. They do not claim perfect historical linkage or full platform governance.

Event taxonomy is bounded:

- `governance_action_start`
- `governance_action_result`
- `audit_linkage_written`
- `manual_verify_result` is reserved/modeled for helper or future first-class verify behavior.

Machine-readable artifacts:

- `validation/p5_governance_action_matrix.json`
- `validation/p5_manual_action_audit_manifest.json`
- `validation/p5_governance_usage_linkage.json`

Explicit non-claims:

- not RBAC
- not approval workflow
- not multi-operator platform
- not billing or entitlement ledger
- not full parity across requeue, replay, inspect, and verify persistence
