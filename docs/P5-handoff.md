# Zephyr P5 Handoff

## Repository anchor
- Repo: https://github.com/farrz901-art/Zephyr-dev
- Working branch: `master`
- Phase-sync branch: `main`
- Repository / phase baseline: `8863aeb9acf6f40b09fea9c8d6157e95c8edf7bc`
- Current docs-alignment state: `7193d13229367524f643d89cfc3127612fc0a908`
- Earlier docs-reorg checkpoint: `dd1b4834ab2b8f7684f1f121bc614a157ec54f4d`

## Current phase state
- P3 complete
- P4 complete
- Current phase: P4.5 authenticity hardening
- P5 has not started
- P5 begins only after P4.5 exit criteria are met

## Core architecture rules
- Strict contract-first Python monorepo
- Source connectors are not forced into one universal abstraction
- `it-stream` owns `it` sources
- `uns-stream` owns `uns` sources
- Destination connectors continue to be shared in `zephyr-ingest`
- Shared governance, provenance, replay, and operator-facing surfaces remain important and already
  formalized in bounded ways

## What P4 achieved
1. Source breadth and destination breadth were completed as a bounded retained support surface.
2. Shared orchestration, delivery, replay, provenance, and operator-facing boundaries were
   formalized.
3. Unsupported and deferred boundaries were made explicit.
4. P4 handoff inputs for deployment/config, runtime/concurrency, and operator/provenance were
   prepared without claiming full productionization.

## Current P4.5 gate
P4.5 is the pre-P5 authenticity-hardening gate. It covers the full retained support surface, not
just the late-P4 second-round focus:
- 10 retained destinations
- 10 retained sources
- preserved `airbyte-message-json` as its own real `it-stream` input path

Baseline and later-added connectors must not be held to different authenticity standards during
P4.5.

## Important reality check
Zephyr is a bounded pre-production system with real source and destination breadth plus real shared
contracts. It is not yet production-proven, and P5 has not started because the current gate is
P4.5 authenticity hardening.

## Critical instruction for P5
Do not default to minimal implementation.

P5 should begin from a production-grade bounded-solution mindset:
1. real production constraints
2. real backend behavior
3. real auth/secrets/env handling
4. real timeout/retry/rate-limit/provider quirks
5. real concurrency/resource limits
6. real replay/DLQ/recovery paths
7. real operator/metrics/provenance
8. benchmark, scale, drill, and recovery validation

Still preserve:
- contract-first thinking
- clear boundaries
- no unnecessary broad redesigns

## Expected P5 working style
- Continue using Phase -> M -> detailed task -> precise prompt workflow
- Task granularity should be chosen to complete the phase safely and well
- Always distinguish:
  - currently supported
  - bounded-subset supported
  - unsupported / deferred
  - production-risk areas
