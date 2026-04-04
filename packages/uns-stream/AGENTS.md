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
