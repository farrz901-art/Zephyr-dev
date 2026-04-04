# AGENTS.md — zephyr-core

`zephyr-core` is the contract and vocabulary center of the monorepo.

It owns stable platform primitives such as:
- versioning
- run context
- typed errors / error codes
- contract models that are shared across packages

## What belongs here
Put things here only if they are genuinely cross-package and stable:
- typed contracts shared by multiple packages
- schema/version constants
- platform-neutral lifecycle or task vocabulary
- stable run metadata and error semantics

## What does NOT belong here
Do not put these in `zephyr-core` unless explicitly required:
- uns-specific execution logic
- ingest runtime orchestration logic
- queue backend implementations
- HTTP server/service framework code
- destination implementation details
- airbyte- or unstructured-native runtime objects

`zephyr-core` should describe the platform, not run the platform.

## Design bar
Anything added here must be:
- small
- typed
- dependency-light
- easy to version
- robust under pyright strict
- anti-drift testable

Prefer:
- `Protocol`
- `TypedDict`
- `dataclass(frozen=True)` where appropriate
- explicit field names and narrow unions
- compatibility shims only when migration is real and justified

Avoid:
- runtime-heavy abstractions
- convenience wrappers that hide semantics
- importing package-specific implementation types

## Backward compatibility discipline
Changes here have wide blast radius.

Before changing a public contract:
- identify all importing packages
- identify shape-sensitive tests
- preserve existing stable fields unless version work is explicitly part of the task
- prefer additive change over breaking change

If a shape must change:
- make the versioning story explicit
- add or update anti-drift tests
- document compatibility intent in the patch summary

## P3 guidance
For P3, `zephyr-core` may own only the minimum flow-agnostic vocabulary needed by orchestration, such as:
- task / flow kind vocabulary
- lifecycle states
- health result contracts
- processor-facing stable request/outcome contracts

But keep this package platform-neutral.
Do not let `zephyr-core` become a dumping ground for ingest runtime mechanics.

## Reading guidance
When working in this package, start from:
- `versioning.py`
- exported package `__init__`
- relevant `contracts/`
- error code definitions
- direct importers in `zephyr-ingest` / `uns-stream`

Do not broad-scan the entire monorepo from here unless necessary.

## Testing guidance
Prefer targeted anti-drift tests for:
- field shape
- enum / literal stability
- import/export stability
- compatibility behavior

Keep tests narrow and structural.
