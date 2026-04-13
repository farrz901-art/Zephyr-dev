# Zephyr Project Instructions for ChatGPT

You are assisting with the Zephyr repository.

## Ground rules
- Always anchor analysis to the real repo state and baseline commit
- Do not abstract the project into generic ETL talk
- Respect current support matrices and deferred boundaries
- Distinguish clearly between:
  - currently supported
  - bounded-subset supported
  - unsupported / deferred
  - production-risk areas

## Phase transition rule
P4 is complete.
Current stage: P4.5 authenticity hardening.
P5 has not started yet.

Current support/authenticity discussions must use the full retained support surface rather than
only the late-P4 second-round focus.

P5 must not default to minimal implementation.
P5 must prefer production-grade bounded solutions.

## P4.5 priorities
- truth-matrix and support-surface alignment
- stronger authenticity evidence across the retained destination world
- stronger authenticity evidence across the retained source world
- preserved `airbyte-message-json` protocol-path hardening
- real auth / secrets / env handling
- real timeout / retry / provider quirks
- real concurrency/resource assumptions
- real replay / DLQ / recovery
- real operator / metrics / provenance

## Style rule
Continue using:
- Phase planning
- M-level task planning
- precise prompts for implementation

But do not assume every M must be split into exactly 4 subtasks.
Task decomposition should follow what best serves the phase.

## Testing language rule
Never overclaim current tests.
Current tests are mixed:
- real contract and orchestration coverage
- some local-real integration coverage
- plus fake backend / fixture-driven bounded-subset testing

Do not call current state production-grade full-real testing.
