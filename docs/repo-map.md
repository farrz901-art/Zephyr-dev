# Zephyr Repository Map

## Repo anchor
- Repo: https://github.com/farrz901-art/Zephyr-dev
- Branch: main
- Documentation-reorg baseline: `dd1b4834ab2b8f7684f1f121bc614a157ec54f4d`

## Current phase
- P4 is complete
- P4.5 authenticity hardening is current
- P5 has not started

## Most important rule files
- `/AGENTS.md`
- `/packages/zephyr-ingest/AGENTS.md`
- `/packages/it-stream/AGENTS.md`
- `/packages/uns-stream/AGENTS.md`
- `/packages/zephyr-core/AGENTS.md`

These `AGENTS.md` files are the current authoritative rules and architecture surfaces.

## Historical doc rule
- `docs/archive/legacy-p0-p4/` is historical reference only
- `docs/review-needed/` is not current truth by default

## Core packages
### `packages/zephyr-ingest`
Shared ingest/delivery world:
- shared delivery semantics across the retained destination surface
- flow processor
- replay / DLQ / provenance / operator-facing surfaces

### `packages/it-stream`
Structured source world:
- task identity
- progress/checkpoint/resume
- retained `it` source support
- preserved `airbyte-message-json` input path

### `packages/uns-stream`
Document-native source world:
- source identity
- acquisition/discovery
- fetch-to-partition-entry
- retained `uns` source support

## Current support-surface framing
- retained destinations: 10 total across baseline and second-round groups
- retained sources: 10 total across `it-stream` and `uns-stream`
- preserved `airbyte-message-json` remains in current P4.5 hardening scope
