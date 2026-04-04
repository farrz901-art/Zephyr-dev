# AGENTS.md

This repository is a strict contract-first Python monorepo for Zephyr.

Zephyr is a data logistics / preprocessing platform.
Current state:
- P2 is completed.
- P3 is starting.
- The highest-priority structural goal is P3-M0:
  make orchestration flow-agnostic so zephyr-ingest no longer depends on an uns-stream-specific execution path.

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

## P3-specific guidance
Current top priority is architectural, not feature sprawl.

P3-M0 goal:
- lift zephyr-ingest from an uns-stream-specific runner into a flow-agnostic orchestration kernel

That means:
- orchestration should dispatch through a flow/task abstraction
- current uns behavior must continue to work through a dedicated processor implementation
- future it-stream must be able to plug in without forcing another runner rewrite

Do NOT:
- implement full it-stream runtime unless the task explicitly asks
- bind new queue/worker/service logic directly to uns-specific execution paths
- leak unstructured or airbyte-native types into zephyr-core contracts

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
