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
