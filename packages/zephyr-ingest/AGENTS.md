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
The current retained non-enterprise destination surface is explicitly supported as this bounded
ten-destination set:
- baseline: `filesystem`, `webhook`, `sqlite`, `kafka`, `weaviate`
- second-round: `s3`, `opensearch`, `clickhouse`, `mongodb`, `loki`

These destinations are supported only in the narrow subsets already proven by code and anti-drift
tests:
- `filesystem`: one shared delivery payload per local artifact write, explicit output-root subset,
  bounded file-layout semantics, shared receipt classification, and shared replay compatibility
- `webhook`: one shared delivery payload per HTTP delivery attempt, explicit endpoint/header subset,
  bounded response normalization, shared receipt classification, and shared replay through the
  current replay sink path
- `sqlite`: one shared delivery payload per bounded row-style write, explicit database/table subset,
  bounded row normalization, shared receipt classification, and shared replay through the current
  replay sink path
- `kafka`: one shared delivery payload per bounded message write, explicit topic/key subset,
  bounded producer acknowledgement normalization, shared receipt classification, and shared replay
  through the current replay sink path
- `weaviate`: one shared delivery payload per bounded object write, explicit collection/object-id
  subset, bounded request/response normalization, shared receipt classification, and shared replay
  through the current replay sink path
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

Baseline vs second-round is historical lineage only. For P4.5 authenticity hardening, all ten
retained destinations are in scope and must not be held to different authenticity standards.

Across that full retained supported surface, these semantics are currently shared and should be
treated as the stable destination boundary everywhere they are currently supported:
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

Different validation form is acceptable across the retained destination world, but a weaker
authenticity standard is not. Some destinations are remote service destinations that require
live-backed hardening. Some are local/runtime-shaped destinations that require production-like
local/runtime hardening against the same shared delivery semantics.

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
- additional destination families outside the current retained ten-destination surface
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

## Current deployment / config handoff shape
Treat the completed-P4 and current-P4.5 deployment/config shape as a bounded handoff into P5, not
as a finished production deployment system.

The following config shape is already shared across the current expanded connector world:
- Zephyr-owned task/orchestration/runtime settings stay ingest-owned and flow-agnostic where they
  are already shared: runner/worker execution mode, queue/lock/runtime selection, delivery path
  selection, retry policy, and observability/reporting wiring
- shared delivery config stays bounded to Zephyr-owned destination selection plus the existing
  delivery payload/receipt/replay/DLQ discipline
- config/spec projection must continue to line up across runtime behavior, snapshot output, and
  reporting/observability surfaces

The following config remains intentionally stream-local or connector-local:
- source connector selectors, source-local read modes, checkpoint/progress inputs, auth/session
  material, provider-specific request/query settings, and document-acquisition parameters
- destination-local transport/auth settings, backend write-mode details, and backend-specific
  request tuning or acknowledgement enrichment
- flow-local execution semantics that still belong to `it-stream` or `uns-stream` rather than to a
  shared deployment/config contract

The following runtime/deployment assumptions are already implicit today and should be treated as
explicit at the current P4.5 pre-P5 boundary:
- deployment wiring is still driven by the current config/spec surfaces and local runtime
  composition, not by a separate environment-packaging layer
- connector-specific credentials/config are expected to remain outside shared Zephyr contracts
  until a later change explicitly stabilizes them
- snapshots, reporting, provenance, and delivery/replay behavior are part of the current
  deployable/configurable surface and must stay aligned with runtime inputs
- the current support boundary does not imply uniform environment variables, secret packaging,
  service manifests, or cloud/Kubernetes deployment conventions yet

Deferred to P5 or later unless a future change explicitly narrows and re-authorizes them sooner:
- standardized deployment packaging such as Docker/Kubernetes/Helm-oriented config layout
- secret-distribution conventions or environment-level config normalization across all connectors
- production-grade deployment profiles, environment overlays, or cloud-specific packaging
- broader config abstraction work that would re-open runtime, queue, lock, or connector ownership
  boundaries

## Current runtime / concurrency handoff shape
Treat the completed-P4 and current-P4.5 runtime shape as a bounded execution handoff into P5, not
as a finished runtime platform.

The following execution shape is already shared across the current expanded connector world:
- `TaskV1 -> FlowProcessor -> Delivery` remains the shared execution chain for both runner-style
  and worker-style execution
- ingest owns task intake, queue/lock/runtime wiring, delivery dispatch, replay/DLQ handling, and
  shared governance/reporting surfaces around execution
- source-side execution still converges through the same orchestration path before delivery, even
  when source-local semantics differ inside `it-stream` or `uns-stream`
- destination-side execution stays bounded to the existing shared delivery/replay path rather than
  destination-owned worker or batch-job models

The following concurrency and sequencing assumptions are already implicit today and should be
treated as explicit at the current P4.5 pre-P5 boundary:
- task execution is coordinated through the current ingest-owned queue/lock/runtime boundaries;
  those boundaries are shared, but not yet broadened into a distributed orchestration platform
- sequencing, retry, replay, and recovery facts must remain inspectable through current Zephyr-
  owned provenance/governance surfaces rather than connector-owned side channels
- source-local fetch/progress/checkpoint behavior may differ by flow, but shared orchestration
  still assumes one Zephyr-owned task execution path per unit of work
- delivery happens through the current one-payload-at-a-time shared delivery discipline unless a
  destination-specific bounded subset already says otherwise

The following execution assumptions remain intentionally stream-local or connector-local:
- `it-stream` progress/checkpoint/resume sequencing and source-local cursor handling
- `uns-stream` acquisition/fetch-to-partition-entry behavior and document-local processing details
- connector-local request/fetch batching, backoff behavior, and provider-specific read/write
  sequencing that do not change shared orchestration ownership
- backend-shaped acknowledgement or write semantics that stay inside current destination-local
  boundaries

Deferred to P5 or later unless a future change explicitly narrows and re-authorizes them sooner:
- distributed worker coordination, scheduler ownership, autoscaling, or work-pool orchestration
- broader multi-worker concurrency guarantees beyond the current ingest-owned runtime boundaries
- destination-owned execution models or source-owned runtime platforms that bypass the shared
  `TaskV1 -> FlowProcessor -> Delivery` chain
- runtime abstraction work that would re-open queue, lock, task, or flow ownership boundaries

## Current operator / metrics / provenance handoff shape
Treat the completed-P4 and current-P4.5 operator/provenance shape as a bounded handoff into P5,
not as a finished observability platform.

The following operator-facing surfaces are already shared and dependable across the current
expanded connector world:
- ingest-owned inspection, recovery, replay/DLQ, batch reporting, and delivery/governance summary
  surfaces
- shared run/delivery provenance facts that stay Zephyr-owned and inspectable through the current
  task/execution/delivery path
- shared operator-facing delivery facts such as retryability, failure classification,
  by-destination totals, replay compatibility, and bounded delivery summaries

The following reporting/summary/provenance facts are already stable enough to rely on for
production-like paths:
- task/run/delivery provenance that explains execution origin, delivery origin, task identity where
  supported, and replay/resume lineage where currently modeled
- batch-report counters and summaries that remain aligned with runtime behavior, delivery outcomes,
  retry behavior, and DLQ/replay facts
- shared delivery summary fields and bounded receipt/reporting facts already formalized in the
  current destination boundary
- source and flow provenance facts only where current shared boundaries already say they are stable
  and inspectable

The following metrics/provenance assumptions remain intentionally stream-local, connector-local, or
deliberately narrow:
- `it-stream` checkpoint/progress/resume provenance and recovery details beyond the shared
  orchestration-facing subset
- `uns-stream` acquisition/fetch/provenance details beyond the shared document-identity and
  partition-entry subset
- connector-local detail fields, backend counters, provider-specific identifiers, and source/dest
  adaptation facts that do not belong in the shared operator surface
- higher-level operational interpretation such as alert thresholds, SLO semantics, or cross-run
  rollups that are not yet part of the current Zephyr-owned contract

Deferred to P5 or later unless a future change explicitly narrows and re-authorizes them sooner:
- dashboards, alerting policy, tracing backends, or broader observability-platform integration
- stronger metric-shape or cardinality guarantees beyond the current bounded reporting surfaces
- richer cross-run operator analytics, fleet-level health views, or broader governance workflows
- operator abstractions that would collapse flow-local or connector-local provenance into a fake
  universal model

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
