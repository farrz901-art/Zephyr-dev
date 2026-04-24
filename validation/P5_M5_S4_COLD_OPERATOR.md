# P5-M5-S4 Cold Operator Proof

S4 proves cold-operator readability, not S3 stability repetition.
S4 does not start S5 release-consumable hardening.

## What S4 Proves

S4 freezes a bounded operator package for the retained surface:

- representative scenarios across delivery, queue/governance, source-deep, and fanout families
- fixed operator questions per family
- allowed operator-visible evidence surfaces
- machine-readable scoring and pass criteria

Non-authors answer from helper/report surfaces and raw artifacts, not source code.
Helper/report CLI surfaces and raw artifact files are both allowed, but source-code reading is not.

## How Scenarios Were Chosen

The scenario set is representative rather than exhaustive. It reuses retained families already
validated in S1, S2, and S3, but only for operator judgment:

- delivery focuses on replay, verify, and receipt reading
- queue/governance focuses on inspect, requeue, provenance, and first-look summary reading
- source-deep focuses on source contract id, usage, provenance, and governance alignment
- fanout focuses on composition/orchestration reading, partial failure, and shared summary

This keeps S4 operator-proof-sized instead of widening into new retained anchors or deep rerun
work.

## How Evidence Is Consumed

Operators are expected to answer from visible retained surfaces:

- helper/report CLI outputs
- governance receipts
- `run_meta.json`
- `usage_record.json`
- `delivery_receipt.json`
- queue inspect outputs and queue backend state surfaces
- source contract reports
- fanout batch/shared-summary/child/aggregate outputs

The proof target is not blind trust in a helper. The target is that helpers plus raw artifacts are
enough to judge retained scenarios without author narration.

## What S4 Does Not Prove

S4 is not S3 stability work. It does not rerun repeatability or bounded soak families.
S4 is not release-consumable hardening. It does not build release bundles, packaging handoff, or
operator platform workflows.
fanout remains composition/orchestration, not a sink connector.

Distributed runtime, cloud/Kubernetes, billing/RBAC, and enterprise connectors remain out of scope.

## Scoring

Judgment is frozen by machine-readable scoring rules:

- pass: all mandatory questions correct and total score at least 90%
- partial: most mandatory questions correct and total score 70% to 89%
- fail: any key mandatory question is unanswerable, or total score is below 70%

Mandatory questions stay focused on identity, state, receipt, linkage, read-only-or-not, and
composition-or-not truth.
