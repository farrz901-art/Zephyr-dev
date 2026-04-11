# AGENTS.md — zephyr-ingest

`zephyr-ingest` is the orchestration and delivery layer of Zephyr.

It owns:
- orchestration / runner behavior
- sources
- delivery dispatch
- replay / DLQ handling
- observability glue
- worker/service runtime shape

It should coordinate flows, not hard-code one flow forever.

## Current architectural reality
After P3-M9, ingest is the flow-agnostic orchestration kernel.

The primary execution chain is now explicitly centered on:
- `TaskV1 -> FlowProcessor -> Delivery`

Both runner-style and worker-style execution should be shaped around that chain.
Recovery-style execution (`resume`, `replay`, `requeue`/`redrive`-derived execution) should align
to the same orchestration model where facts are shared, while keeping real semantic differences
explicit.

## What ingest should own
Keep these here:
- orchestration policy
- task intake / dispatch
- concurrency and in-flight handling
- delivery and receipt handling
- replay and DLQ policy
- config/spec projection into runtime behavior
- worker/service lifecycle wiring
- observability event emission
- queue / lock abstraction ownership and backend selection
- task-governance inspection / recovery / metrics / provenance glue

## What ingest should NOT own
Do not make ingest the home of:
- unstructured backend quirks
- airbyte-native protocol internals
- platform-wide contract definitions that belong in `zephyr-core`
- flow-local checkpoint/state contracts that are still proving themselves in `it-stream`

And do not keep long-term direct dependencies on uns-specific execution paths in the orchestrator.

## P3-M0 direction
Desired end state for this phase slice:
- runner becomes an orchestrator
- flow dispatch happens through a stable abstraction
- current uns behavior remains available through a dedicated processor implementation
- future it-stream can plug into the same orchestration path
- delivery semantics, DLQ behavior, and observability stay stable

Do not overbuild.
For P3-M0, prefer the smallest safe abstraction that removes uns-specific orchestration coupling.

## P3 End-State Boundary Decisions
Keep these explicit at P3 end:
- `zephyr-ingest` owns orchestration/runtime/governance behavior
- `it-stream` owns structured-flow-local checkpoint/state/identity/resume semantics
- `zephyr-core` owns only stable cross-package contracts

Do not promote ingest-local abstractions to `zephyr-core` only because they now have two
implementations. Queue and lock protocols are still ingest-owned until their semantics stop being
runtime-shaped.

## P4 build-on decisions
P4 should treat these ingest-local systems as stable enough to build on:
- `TaskV1 -> FlowProcessor -> Delivery` as the primary execution chain
- current task / queue / lock runtime boundaries
- current delivery receipt / replay / idempotency discipline
- current governance surfaces: inspection, recovery, metrics, provenance

P4 should not casually re-open:
- queue backend semantics
- lock provider semantics
- primary execution-chain ownership
- delivery-path ownership

Connector expansion guidance:
- new sources should plug into existing task/orchestration/provenance discipline
- new destinations should plug into the existing delivery pipeline, not bypass it
- connector count is less important than preserving the shared orchestration and delivery model

## Current supported destination surface
The current non-enterprise destination surface is explicitly supported as this bounded set:
- `s3`
- `opensearch`
- `clickhouse`
- `mongodb`
- `loki`

These destinations are supported only in the narrow subsets already proven by code and anti-drift
tests:
- `s3`: one shared delivery payload per object write, explicit bucket plus prefix subset,
  identity-key-based object naming, shared receipt classification, and shared replay through the
  current replay sink path
- `opensearch`: one shared delivery payload per document write, explicit index plus document-id
  subset, bounded request/response normalization, shared receipt classification, and shared replay
  through the current replay sink path
- `clickhouse`: one shared delivery payload per row-style write, explicit table/write-mode subset,
  bounded row normalization, shared receipt classification, and shared replay through the current
  replay sink path
- `mongodb`: one shared delivery payload per document replace/upsert, explicit database plus
  collection subset, identity-key-based document selection, shared receipt classification, and
  shared replay through the current replay sink path
- `loki`: one shared delivery payload per bounded log push, explicit stream/tenant label subset,
  bounded line-count acceptance subset, shared receipt classification, and shared replay through
  the current replay sink path

Across that full current supported surface, these semantics are currently shared and should be
treated as the stable destination boundary:
- destinations accept the shared Zephyr delivery payload as the durable input contract; they do not
  redefine payload ownership
- success/failure classification stays inside the shared delivery vocabulary:
  retryable vs non-retryable, shared `failure_kind`, and shared `error_code`
- shared summary meaning is bounded and explicit: `delivery_outcome`, `failure_retryability`,
  `failure_kind`, `error_code`, `attempt_count`, and `payload_count`
- replay/idempotency/provenance stay shared where currently supported: delivery identity remains
  stable, replay uses the shared replay path, and replayed payload provenance keeps
  `delivery_origin="replay"` with the current Zephyr-owned provenance shape
- operator-facing/reporting facts stay shared at the delivery layer: by-destination totals, shared
  failure classification, retryability, and replay/DLQ compatibility remain inspectable without
  inventing destination-owned operator surfaces

The following semantics remain intentionally destination-local and should not be normalized further
right now:
- client construction, auth/session handling, transport protocol details, and backend request
  shaping
- backend-specific detail keys such as object key/version facts, document id/index facts, row
  counts, attempted/accepted/rejected counts, stream labels, tenant identifiers, and backend error
  codes
- destination-local batching limits, write modes, acknowledgement enrichment, and request tuning
- backend-native conflict, throttling, or constraint details beyond the shared Zephyr failure
  classification boundary

The following categories or deeper capabilities remain deferred beyond the current P4 support
boundary:
- destination-owned bulk-job, manifest, export/import, or asynchronous workflow semantics
- multi-record transactional coordination or destination-owned partial-commit recovery models
- broader fan-out or destination-side routing abstractions beyond the current one-payload-at-a-time
  delivery discipline
- backend-native replay mechanisms that bypass the shared Zephyr replay/DLQ path
- additional destination families outside the currently listed five-destination breadth
- enterprise-managed destination connectors

## Change discipline
Before editing:
- identify the exact orchestration entrypoint
- identify the exact uns-specific coupling points
- identify the minimum contracts or local abstractions needed

During editing:
- keep patch size small
- preserve current CLI behavior unless the task explicitly changes it
- preserve config snapshot and batch report semantics
- preserve delivery payload and receipt semantics
- preserve event names and stable event fields

Avoid:
- queue/runtime redesign in the same patch unless explicitly requested
- destination rewrites unrelated to the orchestration abstraction
- opportunistic refactors across source/destination/spec/config all at once

## Config / spec discipline
Any runtime-facing parameter added here must stay consistent across:
- spec / config input
- runtime use
- snapshot output
- reporting / observability when applicable

Do not add knobs that are not reflected through the existing config discipline.

## Reading guidance
Start from:
- `runner.py`
- CLI entrypoints that invoke runner/orchestration
- destination base + current delivery path
- config/spec files directly consumed by runner
- replay / DLQ helpers only if the task touches failure handling

Read tests only when they directly constrain the changed behavior.

## Validation guidance
After changes, be ready for the user to run:
- `make tidy`
- `make check`
- `make test`

Shape the patch so failures, if any, are easy to localize to:
- orchestration wiring
- contract mismatch
- type checker regression
- delivery/replay compatibility
