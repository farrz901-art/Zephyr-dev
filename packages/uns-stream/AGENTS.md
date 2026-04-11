# AGENTS.md — uns-stream

`uns-stream` owns the non-structured document processing path.

Its job is to:
- encapsulate document partition / normalization behavior
- isolate backend-specific details
- expose stable typed outputs to the rest of the platform

It should be an implementation provider, not the orchestration owner.

## Package role
`uns-stream` is responsible for:
- service-level processing of document inputs
- backend selection / adaptation
- normalization into Zephyr-owned output shapes
- preserving clear retryable vs non-retryable error semantics

## Boundaries
`uns-stream` may depend on:
- `zephyr-core` contracts and error vocabulary

`uns-stream` must NOT:
- own cross-flow orchestration
- own queueing, scheduling, or worker lifecycle
- directly define platform-wide delivery semantics
- force runner design in `zephyr-ingest`

For P3, `uns-stream` should fit behind a flow-agnostic processor interface.
It must not remain the hidden center of orchestration.

## Change discipline
When editing here:
- preserve current artifact semantics unless explicitly asked to change them
- preserve stable output shapes consumed by ingest
- keep backend-specific concerns contained
- keep upstream library quirks from leaking into shared contracts

Do not:
- push unstructured-native types into `zephyr-core`
- make `zephyr-ingest` depend on uns-specific internal helpers
- smuggle orchestration logic into service helpers

## Preferred direction for P3-M0
The desired direction is:
- current behavior continues through an `UnsFlowProcessor`-style integration
- `zephyr-ingest` orchestrates
- `uns-stream` executes and normalizes

This package should provide a clean execution surface, not orchestrator policy.

## Post-P3 boundary decisions
At P3 end, keep these decisions explicit:
- `uns-stream` remains the non-structured document flow
- it should not absorb structured-data checkpoint/protocol semantics
- it should not reassert itself as the hidden default execution model for all flows

For P4:
- preserve `uns` quality and contract discipline
- expand `uns` only where the work is truly document-flow-specific
- do not use `uns-stream` as a generic home for features that belong in shared orchestration or in
  `it-stream`

## Current supported `uns-stream` source surface
The current non-enterprise `uns-stream` source surface is explicitly supported as this bounded set:
- `http_document_v1`
- `s3_document_v1`
- `git_document_v1`
- `google_drive_document_v1`
- `confluence_document_v1`

These sources are supported only in the narrow subsets already proven by code and anti-drift tests:
- `http_document_v1`: explicit document URL fetch over HTTP(S), bounded request-header subset,
  one fetched document per task, inferred filename/mime subset, and direct handoff into the current
  document partition path
- `s3_document_v1`: explicit bucket plus key selection, optional version id, bounded object fetch,
  one fetched object per task, and direct handoff into the current document partition path
- `git_document_v1`: explicit repo-root plus commit plus relative-path selection, bounded blob
  fetch, stable resolved commit/blob provenance subset, one fetched file per task, and direct
  handoff into the current document partition path
- `google_drive_document_v1`: explicit file selection, bounded export-or-download acquisition
  subset, optional drive locator, bounded export mime subset, one fetched file per task, and direct
  handoff into the current document partition path
- `confluence_document_v1`: explicit site plus page selection, optional requested page-version
  subset, bounded page-body fetch, one fetched page per task, and direct handoff into the current
  document partition path

Across that full current supported surface, these semantics are currently shared and should be
treated as the stable `uns-stream` source boundary:
- source identity is derived from stable document-selection facts; secrets, transient sessions, and
  inspect-only toggles must not affect identity
- acquisition/discovery stays bounded to explicit snapshot-style selection of one document/blob/page
  unit per task; long-lived sync ownership is not part of the current contract
- fetched content enters `uns-stream` through Zephyr-owned document artifacts and the existing
  partition-entry path, not provider-native handles or live remote objects
- provenance preserves the stable source facts needed to explain what document was fetched and why
  that unit of work is replay-relevant
- shared output ownership remains document metadata, partition results, and Zephyr-owned normalized
  elements rather than source-native response envelopes

The following semantics remain intentionally source-local and should not be normalized further
right now:
- HTTP request shaping, accept/header details, redirect/auth behavior, and content-type inference
- object-store client construction, region/endpoint/auth handling, listing/fetch tuning, and
  metadata/header adaptation
- git clone/fetch strategy, local repo access strategy, ignore/path filtering, and VCS-native blob
  handling
- Google Drive OAuth/session handling, export-format mapping, workspace/drive scoping, and provider
  request shaping
- Confluence auth/session handling, page-expansion strategy, body-format mapping, and knowledge
  space traversal details
- source-local provenance/detail fields such as URL, bucket/key/version, commit/blob/path, drive or
  file identifiers, export mime, page id, page version, and body format

The following categories or deeper capabilities remain deferred beyond the current P4 support
boundary:
- directory, folder, prefix, space-tree, or other multi-document discovery that would widen the
  current one-document-per-task acquisition subset
- long-lived sync, delta-token, webhook-driven, or app-owned mirror/workspace semantics
- attachment families, child-page recursion, or broader knowledge-space traversal beyond the
  current bounded page-body subset
- broader document-suite workflow/state semantics beyond the current explicit file export/download
  subset
- conversation, email, chat, ticket, or other timeline-native source families
- additional `uns` source families outside the currently listed five-source breadth
- enterprise-managed source connectors

## Reading guidance
Start with:
- `service.py`
- backend base definitions
- concrete backends
- retry policy / normalization helpers
- only then relevant tests if needed

Avoid reading unrelated files just to “understand everything”.

## Testing guidance
When changing behavior:
- keep tests focused on output shape and error semantics
- avoid broad golden updates unless necessary
- prefer small anti-drift additions over wide test churn
