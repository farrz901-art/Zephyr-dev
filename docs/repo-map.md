# Zephyr Repository Map

## Repo anchor
- Repo: https://github.com/farrz901-art/Zephyr-dev
- Branch: main
- Baseline: 8863aeb9acf6f40b09fea9c8d6157e95c8edf7bc

## Most important rule files
- `/AGENTS.md`
- `/packages/zephyr-ingest/AGENTS.md`
- `/packages/it-stream/AGENTS.md`
- `/packages/uns-stream/AGENTS.md`
- `/packages/zephyr-core/AGENTS.md`

These files are authoritative surfaces for:
- support boundary
- supported subsets
- deferred/unsupported boundaries
- P4->P5 handoff shape

## Core packages
### `packages/zephyr-ingest`
Shared ingest/delivery world:
- shared destination handling
- flow processor
- replay / DLQ / provenance / operator-facing surfaces

### `packages/it-stream`
Structured source world:
- task identity
- progress/checkpoint/resume
- lineage/provenance
- bounded structured-source support

### `packages/uns-stream`
Document-native source world:
- source identity
- acquisition/discovery
- fetch-to-partition-entry
- provenance
- bounded document-source support

## Key batch-protection tests
### Destination-side
- second-round destination contract tests
- second-round destination integration tests
- replay/delivery-related tests
- flow_processor tests where directly relevant

### it-stream source-side
- second-round source batch tests
- representative connector-local tests
- flow/identity/checkpoint/provenance-related tests

### uns-stream source-side
- second-round uns source batch tests
- representative connector-local tests
- flow/acquisition/provenance-related tests

## Doc usage rule
Do not treat the whole `docs/` folder as authoritative.
Many old docs may be stale.
Prefer:
1. current `AGENTS.md` files
2. new `Phase.md`
3. explicit handoff documents
4. tests and representative implementation files
