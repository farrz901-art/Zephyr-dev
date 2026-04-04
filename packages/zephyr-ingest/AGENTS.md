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
Today, parts of ingest still reflect an uns-stream-first execution path.

For P3, the highest-priority structural change is:
- convert ingest into a flow-agnostic orchestration kernel

This is P3-M0 and should come before deeper worker/queue/runtime thickening.

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

## What ingest should NOT own
Do not make ingest the home of:
- unstructured backend quirks
- airbyte-native protocol internals
- platform-wide contract definitions that belong in `zephyr-core`

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
