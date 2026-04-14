# AGENTS.md

This repository is a strict contract-first Python monorepo for Zephyr.

Zephyr is a data logistics / preprocessing platform.
Current state:
- P2 is complete.
- P3 is complete.
- P4 is complete.
- Current stage: P4.5 authenticity hardening.
- P5 has not started.

## Branch / delivery model
- Active development happens on `master`.
- Work is merged or promoted to `main` only after a phase is complete.
- Do not assume `main` is the latest development state.
- Do not rewrite history, rename branches, or change release flow unless explicitly asked.

## Repository shape
Main packages:
- `packages/zephyr-core`: contracts, versioning, run context, error vocabulary
- `packages/uns-stream`: unstructured-document processing path
- `packages/zephyr-ingest`: orchestration, sources, delivery, replay, DLQ, observability
- `packages/zephyr-api`: service layer, currently not the main implementation focus

Other directories:
- `docs/`: design and planning context, not implementation SSOT unless a task explicitly asks for docs alignment
- `tests/`: important for anti-drift and acceptance, but do not scan broadly without task need
- `uv.lock`: generated artifact; ignore unless `pyproject.toml` changes and lock regeneration is explicitly needed

## SSOT rules
Single source of truth priority:
1. Current task instructions from the user
2. This file and package-level AGENTS.md
3. Existing code and typed contracts
4. Focused tests covering the touched behavior
5. Docs for intent / roadmap context only

Never treat stale docs as implementation truth over code + contracts.

## Global engineering invariants
- Python >= 3.12
- uv workspace is authoritative
- `make tidy`, `make check`, and `make test` are mandatory quality gates
- Pyright strict, mypy, and ruff must remain green
- Avoid `Any`, broad `type: ignore`, or weakening type contracts
- Prefer narrow typed helpers, Protocols, TypedDict/dataclass boundaries, and explicit error semantics
- Preserve backward-compatible stable shapes unless a task explicitly authorizes schema/version work

## Cost / context discipline
This repo is large enough that careless scanning wastes context and reduces output quality.

Always do this:
1. Read only the minimum files needed
2. Use targeted search first (`rg`, symbols, imports, references)
3. Open small, high-signal files before large files
4. Expand outward only when a dependency is real

Default startup read set:
- `/AGENTS.md`
- relevant package `AGENTS.md`
- `pyproject.toml`
- `Makefile`
- the exact files named in the task
- direct dependencies of those files only

By default, do NOT broad-scan:
- `docs/`
- `tests/`
- `uv.lock`

Only read them when:
- the task explicitly needs docs alignment
- a failing test or anti-drift concern requires specific tests
- dependency lock work is explicitly in scope

## Change discipline
Before editing:
- state the minimal read set
- state the intended change set
- name the invariants that must remain true

During editing:
- keep diffs small and local
- avoid opportunistic cleanup
- do not mix architectural refactors with unrelated formatting churn
- do not silently move ownership across packages

After editing:
- summarize what changed
- summarize what intentionally did NOT change
- mention likely follow-up work, but do not preemptively implement it

## Post-P3 architectural decisions
P3 is now considered complete enough that these repository decisions are explicit:

- `zephyr-ingest` is the flow-agnostic orchestration kernel
- the primary execution chain is `TaskV1 -> FlowProcessor -> Delivery`
- `uns-stream` and `it-stream` are both real flow implementations
- `uns-stream` and `it-stream` should follow the same engineering discipline, but remain independent
  implementations
- `zephyr-core` owns only stable cross-package contracts, not runtime mechanics

Treat the following as guardrails for P4:
- do not re-open queue / lock / provenance / checkpoint boundaries casually
- build new work on the proven orchestration/task/delivery path first
- prefer strengthening current shared semantics over creating a third execution path

## Flow boundary rules
`uns-stream` is for non-structured document processing.
It must not become a dumping ground for structured-data protocol or checkpoint semantics.

`it-stream` is for structured-data flow execution and local checkpoint/resume semantics.
It must not become a clone of a full upstream-native runtime/platform.

Both flows must:
- integrate through shared orchestration in `zephyr-ingest`
- expose Zephyr-owned artifacts and typed boundaries
- keep flow-local semantics local until they are clearly proven across more than one flow family

For future source connector placement:
- place a connector in `uns-stream` when the dominant source problem is document/file/content
  acquisition that immediately leads into partitioning, extraction, or document normalization, and
  the important source semantics are document-native rather than protocol/checkpoint-native
- place a connector in `it-stream` when the dominant source problem is structured record
  transport, protocol/state handling, cursor/checkpoint progression, or resume/governance semantics
  tied to the source protocol
- do not force connector families into both flows for symmetry; a source adapter belongs only where
  its primary ingestion semantics naturally live
- do not classify by vendor similarity or packaging convenience alone; classify by data shape,
  progression model, and failure/recovery semantics that Zephyr must own
- keep shared surfaces limited to stable ingestion/orchestration facts in `zephyr-ingest` and
  `zephyr-core`: task/run/provenance contracts, flow processor integration, queue/lock/governance,
  delivery handoff, and stable source config/registration shapes that do not encode flow-local
  semantics
- keep flow-local until proven otherwise: document partition/normalization rules, source-specific
  protocol handling, checkpoint/cursor identity, resume continuation policy, and any adapter
  behavior that would blur `uns` and `it` into a generic source runtime

For future source connector onboarding, review against this checklist:
- config: connector config must enter through Zephyr-owned typed/spec surfaces; do not pass
  connector-specific loose dicts through orchestration boundaries or promote source-specific config
  shapes into `zephyr-core` before they are proven stable across flows
- task identity: only stable source inputs that materially define the unit of work may contribute to
  task identity; do not key identity on volatile timestamps, transient auth/session values, or
  inspect-only toggles
- artifact ownership: outputs handed to orchestration/delivery must be Zephyr-owned artifacts and
  typed shapes; upstream-native payloads, cursors, and debug dumps may exist only as bounded
  flow-local implementation details unless explicitly stabilized
- provenance: preserve the source facts needed to explain what was read, under which connector
  config/spec, and which source-local position or document selection produced the work; do not drop
  retry/replay-relevant provenance when adapting a source
- metrics and inspection: connectors must fit existing run/observability surfaces in
  `zephyr-ingest`; expose enough counters/events/log context to inspect source selection, reads,
  retries, skips, and failure classes without inventing a parallel observability path
- tests: every connector PR must add focused tests for config validation, task identity stability,
  artifact shape ownership boundaries, provenance preservation, retryable vs non-retryable failure
  semantics, and any flow-local resume/cursor/document selection rules that could drift silently
- shared vs local: share only stable orchestration-facing facts; keep source protocol handling,
  document parsing rules, cursor/checkpoint semantics, and upstream-specific adaptation logic in the
  owning flow until more than one real connector proves a wider contract

## P4 expansion rules
P4 should expand from the now-proven foundation, not around it.

For source connector work, treat the source placement rules, onboarding checklist, and first-wave
candidate list in this file as the P4-M1 architecture lock. Future connector PRs should either
follow them or explicitly update this section and its anti-drift coverage in the same change.

## Current unsupported / experimental / deferred boundary
Treat the current expanded connector world as a bounded support surface, not an open-ended signal
that Zephyr now supports whole connector families.

Current support means only the connector surfaces and supported subsets explicitly named in:
- `packages/it-stream/AGENTS.md`
- `packages/uns-stream/AGENTS.md`
- `packages/zephyr-ingest/AGENTS.md`

Treat those package-level matrices plus this root-level unsupported/deferred section as the current
authoritative connector-world support boundary for the retained P4/P4.5 support surface.

P4.5 authenticity hardening applies across the full retained support surface, not just the late-P4
second-round focus:
- retained destinations are the baseline plus second-round set named in `packages/zephyr-ingest/AGENTS.md`
- retained sources are the full current `it-stream` and `uns-stream` sets named in their package
  `AGENTS.md` files
- `airbyte-message-json` remains a preserved `it-stream` input path with its own explicit
  authenticity-hardening treatment

During P4.5, baseline and second-round connectors must not be held to different authenticity
standards.

P4.5 runtime-location rule:
- the repository is the control plane for code, tests, helper modules, fixtures, scripts, truth
  matrix artifacts, and example templates
- real local/live authenticity runtime assets belong to the external runtime plane rooted by
  `ZEPHYR_P45_HOME`
- real env/secrets/compose files, service state, logs, and data must not default to repo-root
- repo-root template/example files are allowed as fallback/bootstrap inputs, but they are not the
  canonical real runtime location

Do not infer broader support from implementation similarity, vendor family overlap, or passing
tests around one bounded connector subset.

There is no separate broad experimental connector tier right now:
- a connector or capability is either explicitly supported in a narrow subset, or it is not part of
  current support
- narrow supported subsets must not be over-read as support for broader source-state, sync, batch,
  or delivery-platform behavior

The following capabilities are intentionally not part of current support:
- source-state platforms that require consumer-group ownership, CDC/changefeed/oplog recovery, or
  other non-cursor progress families
- source families that imply long-lived sync, webhook-owned delta state, workspace mirroring, or
  multi-document/app-owned replication semantics
- destination-owned async job, bulk-manifest, export/import, or partial-commit coordination models
- backend-native replay or recovery paths that bypass current Zephyr replay/DLQ ownership
- any connector family not explicitly listed in the current package-level support matrices

Deferred to later P4 only if they can extend the current ownership model without re-opening shared
architecture boundaries:
- carefully bounded expansion of existing source or destination families
- additional connectors that still fit current task/provenance/checkpoint/delivery/governance
  semantics
- tighter support statements or anti-drift coverage for already-proven shared boundaries

Deferred to P5 or later unless a future change explicitly narrows and re-authorizes them sooner:
- broader source-runtime/platform capabilities that would re-open checkpoint/progress/recovery
  ownership
- multi-partition, consumer-group, or long-lived lease/ack coordination semantics
- long-lived sync/mirror/stateful document-suite or content-platform ownership
- destination transaction/bulk async workflow semantics that exceed the current one-payload-at-a-
  time delivery model
- wider productization/operator guarantees that require architecture changes rather than support-
  matrix clarification

Enterprise-managed connectors and enterprise-oriented capability layers remain out of scope for the
current P4 support surface unless a later change explicitly adds them to the architecture and to
focused anti-drift coverage.

## P4 closeout / P5 handoff boundary
Treat completed P4 plus current P4.5 as a bounded pre-production handoff surface, not as proof
that Zephyr is already a finished production platform.

Current state at the P4/P4.5 boundary:
- P4 is complete
- P4.5 is the current pre-P5 authenticity-hardening phase
- P5 begins only after P4.5 exit criteria are met
- the current supported connector world is the package-level source and destination support surface
  explicitly named in `packages/it-stream/AGENTS.md`, `packages/uns-stream/AGENTS.md`, and
  `packages/zephyr-ingest/AGENTS.md`
- P4.5 hardening covers the full retained destination surface, the full retained source surface,
  and the preserved `airbyte-message-json` `it-stream` input path
- expanded source and destination breadth is now architecture-locked against shared-vs-local
  governance drift, delivery drift, and operator-surface drift through focused anti-drift coverage
- the current support boundary, unsupported/deferred boundary, deployment/config handoff shape,
  runtime/concurrency handoff shape, and operator/metrics/provenance handoff shape are now explicit
- P4 does not claim distributed runtime ownership, deployment packaging, benchmark harnesses,
  chaos/fault platforms, dashboards/alerting/tracing systems, or broader production guarantees

Initial P5 bench / scale / SLI candidates:
- first pressure the existing `TaskV1 -> FlowProcessor -> Delivery` path on one representative
  structured flow (`it`) and one representative document flow (`uns`) using already-supported
  sources and destinations rather than inventing a new benchmark-only topology
- first pressure the current shared delivery path through the already-proven destination surface,
  especially the durable/object style path and one structured sink path
- treat current bounded run/reporting facts as the initial SLI candidate surface: end-to-end run
  completion, delivery success/failure totals, retry counts, replay/DLQ entry counts, and stable
  stage-duration facts already emitted by current reporting surfaces
- do not treat autoscaling efficiency, fleet-wide utilization, backend-native throughput claims,
  or dashboard-derived SLO policy as part of current support

Initial P5 failure / recovery drill entry points:
- queue intake to worker execution boundary, especially stuck/requeued/orphaned task handling
- lock acquisition and task ownership boundary, especially the current single-runtime ownership and
  recovery semantics around re-drive rather than distributed lease choreography
- delivery failure to replay/DLQ boundary, especially retryable vs non-retryable classification and
  shared receipt/provenance preservation
- flow-local recovery boundaries that are already real today: `it-stream` checkpoint/resume
  continuation and `uns-stream` bounded document reacquisition/repartition from preserved source
  provenance
- anchor drills on existing operator-facing surfaces in `zephyr-ingest`: inspect, recovery,
  replay/DLQ, batch reporting, and shared governance summaries
- do not treat chaos systems, distributed fault injection, broker partition loss simulation, or
  multi-region failover as part of current support

P5 handoff matrix:
- P5 inherits the current package-level support matrices and supported subsets as the starting
  connector boundary; broader connector-family claims remain out of scope until explicitly added
- P5 inherits the current `zephyr-ingest` handoff shape as the starting deployment/config,
  runtime/concurrency, and operator/provenance boundary
- P5 should first measure and pressure what already exists: shared execution path behavior, bounded
  retry/replay/recovery behavior, current operator/reporting surfaces, and the most important
  supported source/destination combinations
- P5 should first add productionization depth around measurement, drillability, packaging, and
  operational confidence without re-opening source placement, delivery ownership, or flow-local
  governance boundaries by default
- anything that requires new connector families, new source-progress vocabularies, destination-
  owned async workflow semantics, distributed runtime ownership, or enterprise capability layers
  remains outside the current handoff boundary until separately authorized

Source connector expansion:
- is not the immediate priority until the current `it` governance/runtime path is mature enough
- should not bypass the existing task / provenance / checkpoint discipline
- should not force `it-stream` to absorb a full external connector platform design

First-wave source connector candidates for P4:
- `uns-stream`: local document collection source. Use this for repository/workspace/path-rooted file
  sets where the dominant problem is selecting documents and handing them into document
  partition/normalization. Attachment pattern: connector config stays as a Zephyr-owned source
  spec; task identity is derived from stable document selection facts such as source root, relative
  path, and content/version marker; outputs remain Zephyr-owned document artifacts; provenance must
  preserve source path and document selection/version facts; traversal/filtering details remain
  connector-local
- `uns-stream`: object storage / blob document collection source. Use this when documents are
  selected from bucket/container/prefix-style stores and still enter `uns` as document-native work.
  Attachment pattern: config captures stable container/prefix selection, not SDK-specific loose
  blobs; task identity is derived from stable object locator plus version/etag generation facts;
  outputs remain Zephyr-owned document artifacts; provenance must preserve object locator and
  version facts; listing pagination/auth specifics remain connector-local
- `it-stream`: cursor or pagination-driven structured API source. Use this when the dominant source
  problem is structured record transport with protocol-defined page/cursor/watermark progression and
  retry/resume semantics. Attachment pattern: config captures the stable endpoint/resource/partition
  selector and read mode; task identity is derived from stable source selector plus the intended
  read slice, never transient auth/session state; outputs remain Zephyr-owned structured
  record/state artifacts rather than raw response envelopes; provenance must preserve selector,
  cursor/window, and response slice facts needed for replay and inspection; pagination, rate-limit,
  and protocol adaptation details remain connector-local

Flow-local source connectors should live in the owning flow package:
- `packages/uns-stream/src/uns_stream/sources/`
- `packages/it-stream/src/it_stream/sources/`

Keep only genuinely shared source-entry logic in:
- `packages/zephyr-ingest/src/zephyr_ingest/sources/`

Not first-wave by default:
- database/CDC connectors until `it` source-state governance is hardened by at least one simpler
  structured source
- message-bus/streaming connectors that would re-open runtime and checkpoint architecture questions
- vendor-specific connector proliferation before the candidate classes above are proven

First second-round non-enterprise `uns-stream` source batch for P4 (P4-M19-01):
- after the now-complete second-round `it-stream` breadth, expand `uns-stream` source breadth
  through one controlled two-source batch rather than another single-example source step
- the first second-round `uns-stream` source batch is:
  `s3`-compatible object-storage document collection source and `git`-compatible repository
  document collection source
- this batch is preferred over SaaS-docs-style sources right now because both candidates stay
  document-native, pressure acquisition/provenance/partition-entry semantics in distinct ways, and
  still fit the current `uns-stream` model without introducing app-owned sync state, account-level
  workflow semantics, or remote change-token architecture
- this batch is meant to pressure the current `uns-stream` model in two realistic ways without
  changing ownership boundaries: remote object listing/version acquisition and repository tree/ref
  acquisition with document-level provenance

Shared-governance pressure map for the first second-round `uns-stream` source batch:
- `s3`-compatible object-storage document collection source. acquisition/discovery pressure: prove
  that bucket/prefix-style listing, stable object selection, and bounded fetch retries can remain
  source-local while still entering `uns` as document-native work rather than a storage-native job
  system. provenance/source-identity pressure: preserve bucket, prefix, object key, and stable
  version facts such as etag/version id or generation markers so document identity remains
  inspectable and replay-relevant. fetch-to-partition-entry pressure: fetched object bytes must
  enter the current document partition/normalization path through Zephyr-owned document artifacts,
  not through storage-client response envelopes or remote-file handles. shared vs local: shared
  task/provenance/artifact ownership and partition-entry semantics stay in current `uns-stream` and
  `zephyr-ingest` surfaces; listing pagination, object metadata/header mapping, auth/session
  handling, and backend fetch tuning stay source-local
- `git`-compatible repository document collection source. acquisition/discovery pressure: prove
  that repository/ref/path-rooted document acquisition can fit the current `uns-stream` model when
  bounded to explicit ref/tree selection rather than long-lived workspace sync or VCS-owned
  execution state. provenance/source-identity pressure: preserve repository locator, ref/commit
  identity, relative path, and stable file/blob version facts so document identity and later
  inspection do not depend on ambient checkout state. fetch-to-partition-entry pressure: repository
  blobs must enter the existing document partition path as Zephyr-owned document artifacts rather
  than live working-tree handles or VCS-native patch/diff objects. shared vs local: shared
  task/provenance/artifact ownership and partition-entry semantics stay shared; clone/fetch
  strategy, sparse/path filtering, credential handling, ignore rules, and VCS-specific tree/blob
  adaptation stay source-local

Deferred after the first second-round `uns-stream` source batch:
- SaaS-docs/document-suite sources remain deferred until the object-store and git-style batch
  proves how far shared acquisition/provenance/partition-entry semantics can stretch without
  introducing app-owned sync cursors, tenant/workspace permissions state, or workflow-native
  document semantics
- broader remote-content families that require long-lived synchronization, webhook-driven state, or
  document-app-native delta tokens remain deferred until `uns-stream` has proven a second batch of
  document-native acquisition without widening its current boundary
- enterprise-managed document sources remain out of scope unless a later change explicitly adds
  them to the architecture

Second second-round non-enterprise `uns-stream` source batch for P4 (P4-M20-01):
- after the object-store and git-style batch is completed and architecture-locked, continue
  `uns-stream` breadth through one more controlled two-source batch rather than pivoting away from
  document-native source expansion or returning to one-off source additions
- the second second-round `uns-stream` source batch is:
  `google-drive`-compatible cloud document-suite file source and `confluence`-compatible
  knowledge-space document source
- this batch is preferred over other remaining document-native candidates right now because it
  covers the next two high-value non-enterprise `uns` source families, pressures the current
  acquisition/provenance/partition-entry model through app-native document identities and bounded
  export/page-version facts, and can still be implemented as explicit snapshot-style document
  selection rather than long-lived sync, webhook, or app-owned delta-token architecture
- this batch is meant to pressure the current `uns-stream` model in two new but grounded ways
  without changing ownership boundaries: document-suite export/download acquisition under stable
  file/revision identity and knowledge-space page acquisition under stable page/version identity

Shared-governance pressure map for the second second-round `uns-stream` source batch:
- `google-drive`-compatible cloud document-suite file source. acquisition/discovery pressure:
  prove that bounded drive/folder/file selection, explicit export-or-download mode, and stable
  file discovery can remain source-local while still entering `uns` as document-native work rather
  than as a sync client or workspace mirror. provenance/source-identity pressure: preserve drive or
  workspace locator, file id, parent/folder selection facts, stable revision/version markers, and
  chosen export mime where Zephyr owns it so document identity remains inspectable and replay-
  relevant without depending on ambient OAuth session state. fetch-to-partition-entry pressure:
  downloaded or exported bytes must enter the current document partition path through Zephyr-owned
  document artifacts, not through provider-native response envelopes or app-specific file handles.
  shared vs local: shared task/provenance/artifact ownership and partition-entry semantics stay
  shared; OAuth/session handling, folder traversal, export-format mapping, permission-scoped
  discovery, and provider pagination stay source-local
- `confluence`-compatible knowledge-space document source. acquisition/discovery pressure: prove
  that explicit site/space/page-rooted acquisition and bounded child-page or attachment discovery
  can fit the current `uns-stream` model when kept snapshot-oriented rather than becoming a wiki
  sync engine or webhook-driven content mirror. provenance/source-identity pressure: preserve site
  locator, space key, page id, stable page version, relative content selection facts, and any
  bounded attachment identity needed to explain what content was read without relying on mutable UI
  paths alone. fetch-to-partition-entry pressure: page bodies, rendered exports, or attachment bytes
  must enter the existing document partition path as Zephyr-owned document artifacts rather than
  live app-session objects, page trees, or provider-specific payload dumps. shared vs local: shared
  task/provenance/artifact ownership and partition-entry semantics stay shared; page expansion,
  attachment download strategy, auth/session handling, ancestor traversal, and provider-specific
  content rendering adaptation stay source-local

Deferred after the second second-round `uns-stream` source batch:
- block-native workspace/document tools that lack stable exported revision identity across all
  document types remain deferred until the drive- and knowledge-space-style batch proves how far
  shared provenance can stretch without widening the current `uns` contract
- broader remote-content families that require long-lived sync cursors, webhook-owned change state,
  or conversation/timeline semantics such as email, chat, or ticket-native sources remain deferred
  because they still pressure `uns-stream` toward a source-state platform too early
- enterprise-managed knowledge/content systems remain out of scope unless a later change explicitly
  adds them to the architecture

First second-round non-enterprise `it-stream` source batch for P4 (P4-M16-01):
- after the now-complete second-round non-enterprise destination breadth, expand `it-stream` source
  breadth through one controlled two-source batch rather than another single-example step
- the first second-round `it-stream` source batch is:
  `postgresql`-compatible relational incremental table source and `clickhouse`-compatible warehouse
  incremental query source
- this batch is preferred over `stream` and `nosql` source categories right now because both
  candidates pressure the current `it-stream` governance model through ordered watermark/keyset
  progression that still fits the already-proven cursor/checkpoint/resume subset, while avoiding an
  immediate re-open of consumer-group/ack/rebalance or partition-token/shard-recovery architecture
- this batch is meant to pressure the current `it-stream` model in two distinct but realistic ways
  without changing its ownership boundaries: relational row-window progression under stable table
  ordering and warehouse batch-window progression under stable query slice ordering

Shared-governance pressure map for the first second-round `it-stream` source batch:
- `postgresql`-compatible relational incremental table source. progress-family pressure: prove that
  ordered relational progress can stay in the current cursor-family model using stable keyset or
  watermark progression rather than introducing CDC-log or trigger-native progress families.
  checkpoint/resume pressure: resume must continue from explicit ordered table-selection facts and
  checkpoint cursor state, not from connection-local transaction handles or server-session state.
  lineage/provenance pressure: preserve schema/table selector, ordering column set, last confirmed
  cursor or watermark, read direction, and bounded filter/snapshot facts needed to explain which
  rows were selected. task-identity pressure: derive identity from stable connection/resource
  selector, table or query-spec selector, ordering/progress mode, and intended read slice, never
  transient credentials, session parameters, or inspect-only SQL hints. shared vs local: shared
  task/checkpoint/provenance/governance semantics stay in current `it-stream` and `zephyr-ingest`
  surfaces; SQL dialect shaping, pagination query text, consistency/read-mode hints, and local
  cursor encoding stay source-local
- `clickhouse`-compatible warehouse incremental query source. progress-family pressure: prove that
  warehouse-style batch-window reads can still fit the current cursor-family model when the source
  advances through stable ordered query slices rather than destination-owned bulk-export jobs or
  async manifest workflows. checkpoint/resume pressure: resume must restart from explicit query
  slice boundaries and stable watermark/key facts, not ephemeral query ids, server-side cursors, or
  background job state. lineage/provenance pressure: preserve database/table or query-resource
  selector, watermark/window bounds, ordering facts, and any stable partition/cluster read facts
  needed for replay and inspection. task-identity pressure: derive identity from stable warehouse
  resource selector plus intended read slice and progress mode, never transient execution settings
  or opportunistic tuning flags. shared vs local: shared checkpoint/provenance/reporting/recovery
  semantics stay shared; warehouse SQL shaping, batching/window sizing, partition pruning, and
  engine-specific read tuning stay source-local

Second second-round non-enterprise `it-stream` source batch for P4 (P4-M17-01):
- after the relational/warehouse batch is completed and architecture-locked, continue `it-stream`
  breadth through one more controlled two-source batch rather than pivoting early to `uns-stream`
  planning or returning to one-off source additions
- the second second-round `it-stream` source batch is:
  `kafka`-compatible partitioned stream source and `mongodb`-compatible ordered document source
- this batch is preferred over deferring `stream` or `nosql` again because it covers the two
  remaining high-value non-enterprise `it` source categories while still allowing Zephyr to stay
  inside the current task/checkpoint/provenance model when both are bounded to explicit ordered
  progression, rather than full consumer-group ownership or change-stream token recovery
- this batch is meant to pressure the current `it-stream` model in two new but grounded ways
  without changing ownership boundaries: partition/offset progression under explicit slice
  selection and ordered document progression under stable collection/query selection

Shared-governance pressure map for the second second-round `it-stream` source batch:
- `kafka`-compatible partitioned stream source. progress-family pressure: prove that bounded
  partition/offset progression can fit the current explicit progress-family discipline without
  collapsing Zephyr into broker-owned consumer-group or rebalance state. checkpoint/resume
  pressure: resume must continue from explicit topic/partition/offset facts and intended slice
  selection, not from member ids, lease ownership, or opaque broker session state.
  lineage/provenance pressure: preserve topic, partition set, starting/ending offset facts,
  ordering mode, and bounded decode/window-selection facts needed to explain what was read.
  task-identity pressure: derive identity from stable broker/resource selector, topic or
  subscription selector, partition-selection mode, and intended read slice, never transient
  consumer-group membership, session generation, or poll timing. shared vs local: shared
  task/checkpoint/provenance/governance semantics stay in current `it-stream` and
  `zephyr-ingest` surfaces; broker client construction, rebalance handling, poll tuning,
  deserialization, and local batching/commit strategy stay source-local
- `mongodb`-compatible ordered document source. progress-family pressure: prove that ordered
  document reads can still use the current cursor-family model through stable document watermark or
  keyset progression rather than introducing oplog/change-stream token families. checkpoint/resume
  pressure: resume must continue from explicit collection/query selector facts and last confirmed
  ordering key state, not from driver cursor handles, server sessions, or snapshot transaction
  state. lineage/provenance pressure: preserve database/collection selector, ordering key,
  last confirmed document watermark, bounded filter/projection facts, and any stable read-scope
  facts needed for replay and inspection. task-identity pressure: derive identity from stable
  connection/resource selector, collection/query selector, ordering/progress mode, and intended
  read slice, never transient auth/session values or inspect-only driver options. shared vs local:
  shared task/checkpoint/provenance/governance semantics stay shared; BSON shaping, projection
  mapping, read concern, query hinting, and local cursor encoding stay source-local

Deferred after the second second-round `it-stream` source batch:
- log-based CDC, changefeed, or enterprise-managed database/warehouse connectors remain deferred
  because they still introduce new checkpoint/progress families too early
- broader streaming variants that require Zephyr-owned consumer-group balancing, cross-partition
  acknowledgement coordination, or long-lived lease management remain deferred until the
  `kafka`-style bounded source proves how far explicit partition/offset recovery can stretch
- wider `nosql` families such as key-value, wide-column, or shard-token-driven connectors remain
  deferred until the `mongodb`-style ordered document adapter proves how far shared
  cursor/watermark governance can stretch without widening the source-progress vocabulary

Destination connector expansion:
- should build on the hardened delivery / receipt / idempotency / replay semantics already in place
- should not introduce destination-specific shortcuts around the shared delivery path

For destination connector work, treat the destination boundary rules, onboarding checklist, and
first-wave candidate list in this file as the P4-M2 architecture lock. Future destination PRs
should either follow them or explicitly update this section and its anti-drift coverage in the
same change.

For future destination connector placement and review:
- treat a destination as a Zephyr destination adapter only when it accepts Zephyr-owned delivery
  payloads and returns through the shared receipt / failure / replay discipline, rather than
  redefining delivery around a backend-native client model
- keep these delivery semantics shared across destinations: delivery payload ownership, receipt /
  details shape discipline, idempotency expectations, retryable and `failure_kind` / `error_code`
  semantics, replay compatibility, and observability/provenance facts emitted from delivery
- keep destination-local only what is genuinely backend-shaped: client construction, auth/session
  handling, request batching limits, backend-specific option mapping, transport protocol quirks,
  receipt enrichment that fits the shared receipt shape, and backend-specific retry hints that do
  not replace Zephyr failure semantics
- do not bypass the shared delivery path by letting a destination define its own payload contract,
  success/failure vocabulary, ad-hoc receipt schema, replay mechanism, idempotency shortcut, or
  observability path
- do not treat backend client responses or SDK objects as the stable contract; translate them into
  Zephyr-owned payload/receipt/provenance shapes at the adapter boundary
- keep wider abstractions local until proven by more than one real destination; do not promote
  backend-specific batching, transaction, bulk-job, or acknowledgement semantics to `zephyr-core`
  or generic delivery contracts just because one destination needs them

For future destination connector onboarding, review against this checklist:
- config/spec: destination config must enter through the existing Zephyr config/spec discipline and
  remain consistent across runtime behavior, snapshots, and observability; do not pass backend
  client options through the delivery path as loose adapter-owned blobs
- delivery payload: destinations must accept the shared Zephyr delivery payload as the stable input
  contract; backend request shaping may adapt that payload locally, but may not redefine or narrow
  the shared payload contract for one destination
- receipt/details: destinations must return through the shared receipt/details discipline and
  normalize backend acknowledgements into Zephyr-owned receipt facts; do not expose raw SDK/client
  objects as the durable receipt contract
- idempotency: destinations must respect current Zephyr idempotency expectations and any existing
  dedupe/receipt semantics; backend-specific idempotency features may enrich the adapter but may not
  replace the shared delivery contract
- retry and error semantics: map backend outcomes into current Zephyr `retryable`,
  `failure_kind`, and `error_code` expectations; do not invent destination-local success/failure
  vocabularies that bypass shared delivery behavior
- replay compatibility: destination behavior must remain compatible with the shared replay/DLQ path;
  do not require a destination-only replay mechanism or drop the facts needed to safely re-drive
  delivery
- metrics/provenance/inspectability: destinations must fit existing delivery observability surfaces
  and preserve the facts needed to inspect delivery attempts, outcomes, backend acknowledgements,
  and replay/idempotency-relevant behavior without creating a parallel inspection path
- tests: every destination PR must add focused tests for config/spec wiring, payload contract
  preservation, receipt/details normalization, idempotency behavior, retryable/failure/error
  classification, replay compatibility, and destination-local behavior that could silently drift
- shared vs local: keep only backend-shaped transport, auth/session, batching, option mapping, and
  acknowledgement enrichment local; keep payload ownership, receipt discipline, replay semantics,
  idempotency expectations, and delivery observability shared

First-wave destination connector candidates for P4:
- object storage / blob archive destination. This is a strong first-wave addition because it extends
  the already-proven filesystem-style delivery pattern into remote durable storage without
  re-opening shared delivery semantics. Attachment pattern: config captures stable bucket/container,
  prefix, write mode, and object naming policy; the shared Zephyr delivery payload remains the input
  contract; receipts normalize backend acknowledgements into object locator/version facts; shared
  idempotency/retry/error/replay semantics remain in force; multipart upload, metadata headers, and
  backend auth/session handling remain destination-local
- relational row/upsert destination. This is a strong first-wave structured sink because it expands
  from the current delivery path into durable tabular storage while still fitting shared receipt,
  idempotency, and replay discipline when bounded to Zephyr-owned payload adaptation. Attachment
  pattern: config captures stable connection/resource selector, target table, write mode, and key
  policy; the shared delivery payload is adapted locally into row/upsert operations rather than
  replaced; receipts preserve normalized table/operation/row-count facts; retryable and
  `failure_kind` / `error_code` mapping stays shared; SQL dialect, parameter binding, and
  transaction/batch tuning remain destination-local
- search/index document destination. This is a strong first-wave addition because it stays close to
  the existing document/vector-style delivery family while validating a second document-oriented
  indexing class without changing replay/idempotency architecture. Attachment pattern: config
  captures stable index/resource selector, write mode, and identity policy; the shared delivery
  payload remains authoritative and is only adapted locally into index requests; receipts normalize
  backend acknowledgement ids/status into shared receipt facts; shared replay and error semantics
  stay intact; bulk API shaping, refresh/indexing options, and backend-specific request tuning
  remain destination-local

Second-round non-enterprise destination batch for P4 (P4-M13-01):
- after the current non-enterprise destination set (`filesystem`, `kafka`, `sqlite`, `weaviate`,
  `webhook`), expand through one controlled breadth batch rather than another single-example
  connector step
- the second-round batch is:
  `s3`-compatible cloud object destination, `opensearch`-compatible search/index destination, and
  `clickhouse`-compatible warehouse destination
- this batch is preferred over `nosql` and observability destinations right now because it covers
  three missing non-enterprise destination categories while still fitting the current shared
  delivery payload / receipt / replay / idempotency model with bounded local adaptation
- this batch is meant to pressure the current delivery contract in three distinct ways without
  re-opening architecture: remote object acknowledgement/version facts, search bulk partial-success
  outcomes, and warehouse row-batch write outcomes

Shared-contract pressure map for the second-round batch:
- `s3`-compatible cloud object destination. Retryability pressure: transient network/storage
  failures stay retryable, while auth/config/policy failures stay non-retryable. `failure_kind` /
  `error_code` pressure: normalize bucket/container, prefix, auth, size, and object-policy outcomes
  without leaking SDK-local exception vocabularies. details/summary naming pressure: keep stable
  object locator, write mode, and version/etag facts in shared receipt details. idempotency /
  replay pressure: replay must re-drive the same object naming policy and distinguish overwrite
  conflict vs safe re-put behavior through shared idempotency semantics rather than backend-local
  shortcuts. operator-facing reporting pressure: operators need bucket/container, prefix, object
  key, and version/etag facts through existing delivery reporting. Shared vs local: payload
  ownership, receipt shape, retryability, `failure_kind` / `error_code`, replay compatibility, and
  reporting stay shared; multipart upload, metadata/header mapping, object naming helpers, and
  auth/session handling stay destination-local
- `opensearch`-compatible search/index destination. Retryability pressure: transport and
  index-backpressure failures may be retryable, while mapping/schema/validation failures stay
  non-retryable. `failure_kind` / `error_code` pressure: bulk partial failures must collapse into
  the current shared failure vocabulary instead of leaking backend status names. details/summary
  naming pressure: receipt details must stabilize index/resource selector, write mode,
  attempted/accepted/rejected document counts, and representative acknowledgement ids/statuses.
  idempotency / replay pressure: replay must preserve document identity and write-mode semantics
  without creating a destination-local replay workflow for refresh or reindex behavior.
  operator-facing reporting pressure: operators need counts and representative failure classes for
  partial bulk outcomes without reading raw backend responses. Shared vs local: payload ownership,
  receipt/details discipline, retryable classification, failure vocabulary, replay compatibility,
  and operator surfaces stay shared; bulk request shaping, refresh options, routing keys, and
  backend response parsing stay destination-local
- `clickhouse`-compatible warehouse destination. Retryability pressure: transient transport or
  cluster-availability outcomes may be retryable, while schema/type/constraint mismatches stay
  non-retryable. `failure_kind` / `error_code` pressure: normalize insert, merge, and dedupe-related
  failures into the current shared vocabulary without creating warehouse-only success/failure
  states. details/summary naming pressure: receipt details must preserve target table, write mode,
  attempted/accepted row counts, and any stable batch/part acknowledgement facts that fit the
  shared receipt model. idempotency / replay pressure: replay must remain safe under append/upsert
  or dedupe-oriented modes and must not rely on destination-owned staged-load jobs or commit
  polling in this batch. operator-facing reporting pressure: operators need row-count, table, and
  write-mode visibility that aligns with current delivery reporting instead of a warehouse-specific
  dashboard contract. Shared vs local: payload ownership, receipt shape, failure vocabulary,
  idempotency expectations, replay path, and reporting stay shared; column mapping, insert
  batching, partitioning, and engine-specific tuning stay destination-local

Remaining second-round non-enterprise destination batch for P4 (P4-M14-01):
- finish the current non-enterprise destination breadth plan before any source-batch pivot by
  adding one `nosql` representative and one observability representative, rather than reopening the
  destination slate again
- the remaining batch is: `mongodb`-compatible document-store destination and `loki`-compatible
  observability/log-stream destination
- `mongodb` is the preferred `nosql` representative because it pressures collection/document
  identity, insert-vs-upsert semantics, and duplicate-key or validation failures without forcing a
  destination-owned async job model, partition-token model, or wide-table reconciliation contract
- `loki` is the preferred observability representative because it pressures append-only
  telemetry-style delivery, label/stream metadata, and backend rate limits while still fitting the
  current HTTP-shaped delivery, receipt, and replay path better than metrics- or trace-native
  protocols that would blur Zephyr delivery with telemetry ingestion semantics
- this remaining batch is preferred over other `nosql` or observability candidates right now:
  `dynamodb` and `cassandra` introduce partition/throughput or wide-column planning pressure that is
  too close to new delivery-governance work, while OTLP metrics/traces or vendor-native event
  systems would push Zephyr into telemetry-specific payload shaping before the shared delivery
  contract has been proven on a simpler observability sink

Shared-contract pressure map for the remaining batch:
- `mongodb`-compatible document-store destination. Retryability pressure: transient network,
  primary-election, or server-availability failures may be retryable, while auth, collection
  policy, validation, and duplicate-key outcomes stay non-retryable. `failure_kind` / `error_code`
  pressure: normalize duplicate-key, validation, document-size, and write-concern outcomes into the
  current shared vocabulary without leaking driver exception classes into the durable contract.
  details/summary naming pressure: keep stable collection, write mode, document identity,
  attempted/accepted/rejected counts, and any bounded matched/modified/upserted facts in receipt
  details, while keeping summary naming shared. idempotency / replay pressure: replay must reuse
  the same document identity policy and make insert-vs-replace-vs-upsert behavior explicit through
  shared idempotency semantics rather than destination-local retry shortcuts. operator-facing
  reporting pressure: operators need collection, write mode, and representative document-id or
  duplicate-key facts through the existing delivery reporting path. Shared vs local: payload
  ownership, receipt shape, retryability, `failure_kind` / `error_code`, replay compatibility,
  idempotency expectations, and reporting stay shared; BSON mapping, collection options, write
  concern tuning, and driver session/auth handling stay destination-local
- `loki`-compatible observability/log-stream destination. Retryability pressure: transport,
  ingester-backpressure, and rate-limit failures may be retryable, while auth, tenant, label, or
  payload-validation failures stay non-retryable. `failure_kind` / `error_code` pressure: map
  telemetry-style HTTP failures into the existing shared vocabulary without creating observability-
  only success/failure states. details/summary naming pressure: receipt details must preserve
  tenant/stream selector facts, write mode, and attempted/accepted/rejected line counts where they
  are real, while shared summary naming stays unchanged. idempotency / replay pressure: replay must
  remain compatible with append-style log delivery and deterministic stream/document identity facts
  where Zephyr owns them, without claiming a backend-native dedupe guarantee that Loki does not
  provide. operator-facing reporting pressure: operators need stream/tenant, label-set summary, and
  retry/failure visibility through Zephyr's existing delivery reporting rather than a parallel
  observability dashboard contract. Shared vs local: payload ownership, receipt/details discipline,
  retryability, failure vocabulary, replay path, and operator-facing reporting stay shared; label
  shaping, timestamp mapping, tenant headers, stream batching, and backend-specific HTTP response
  parsing stay destination-local

Deferred after the remaining batch:
- other `nosql` families such as key-value, wide-column, or cache-oriented destinations remain
  deferred until the `mongodb`-style document-store adapter proves how far shared document identity
  and write-mode semantics can stretch without widening the shared delivery contract
- telemetry-native observability families beyond log/event delivery remain deferred until the
  `loki`-style sink proves that Zephyr can target observability backends without redefining the
  shared payload around metrics, traces, or collector-specific envelopes
- enterprise-managed destination families stay out of scope for this batch unless a later change
  explicitly adds them to the repo architecture

Not first-wave by default:
- warehouse/bulk-load job destinations that require async manifest upload, staged commit, or
  destination-owned job polling before current receipt/replay semantics are proven sufficient
- destinations that require a destination-specific reconciliation or commit-log framework to become
  usable
- vendor-specific proliferation of current destination families before the candidate classes above
  are proven against the shared delivery path

Prefer:
- deeper hardening of current orchestration/governance surfaces
- connector additions that fit the existing execution and delivery contracts

Avoid:
- connector proliferation that re-opens architectural questions already settled in P3
- flow-specific orchestration forks
- promoting first-generation local runtime semantics to `zephyr-core` too early

## Validation expectations
The user executes privileged commands manually when needed.
You may prepare changes assuming the user will run:
- `make tidy`
- `make check`
- `make test`
- relevant `uv` commands

When proposing a patch:
- call out expected validation commands
- call out likely failure surfaces
- keep the patch shaped so failures are easy to localize

Repository-local validation hygiene:
- `pyproject.toml` is the SSOT for pytest runtime options, including the workspace-local basetemp.
- Use the repository-configured pytest path under `.tmp/pytest`; do not rely on the system temp directory.
- Do not create ad-hoc pytest temp directories in the repo root when the repository config already defines the stable path.
- Use the repository-configured pytest path unless the user explicitly instructs otherwise.

## Communication style for implementation tasks
For non-trivial tasks, provide:
1. minimal read set
2. minimal patch plan
3. implementation
4. concise change summary
5. validation guidance

Do not start with a full-repo summary unless explicitly asked.
Do not consume context on broad explanations when the task is narrow.
