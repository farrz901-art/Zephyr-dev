# Zephyr P5 Handoff

## Repository anchor
- Repo: https://github.com/farrz901-art/Zephyr-dev
- Branch: main
- Baseline commit: 8863aeb9acf6f40b09fea9c8d6157e95c8edf7bc

## Current phase state
- P3 complete
- P4 complete
- P5 not started yet

## Core architecture rules
- Strict contract-first Python monorepo
- Source connectors are not forced into one universal abstraction
- `it-stream` owns `it` sources
- `uns-stream` owns `uns` sources
- Destination connectors continue to be shared in `zephyr-ingest`
- Shared governance, provenance, replay, operator-facing surfaces are important and already partially formalized

## What P4 achieved
1. Second-round non-enterprise destination breadth completed, validated, support-bounded, and integration-locked
2. Second-round `it-stream` source breadth completed, validated, support-bounded, and integration-locked
3. Second-round `uns-stream` source breadth completed, validated, support-bounded, and integration-locked
4. Expanded connector-world governance/operator/anti-drift validation completed
5. Source support matrix formalized
6. Destination support matrix formalized
7. Unsupported / experimental / deferred boundary formalized
8. P4 -> P5 handoff shape formalized:
   - deployment/config shape
   - runtime/concurrency assumptions
   - operator/metrics/provenance shape
   - bench/scale/SLI candidates
   - failure/recovery drill entry points
   - P5 handoff matrix

## Important reality check
Zephyr is not just a concept prototype anymore.
It is currently a bounded pre-production system.

It has real source/destination breadth and real shared contracts.
However, it is NOT yet production-proven.

## Critical instruction for P5
Do NOT default to "minimal viable implementation" anymore.

From P5 onward, decision priority changes to:
1. real production constraints
2. real backend behavior
3. real auth/secrets/env handling
4. real timeout/retry/rate-limit/provider quirks
5. real concurrency/resource limits
6. real replay/DLQ/recovery paths
7. real operator/metrics/provenance
8. benchmark / scale / drill / recovery validation

Still preserve:
- contract-first thinking
- clear boundaries
- no unnecessary broad redesigns

## Expected P5 working style
- Continue using Phase -> M -> detailed task -> precise prompt workflow
- M tasks do NOT need to be mechanically split into 4 subtasks
- Task granularity should be chosen to best complete the phase safely and well
- Always distinguish:
  - currently supported
  - bounded-subset supported
  - unsupported / deferred
  - production-risk areas
