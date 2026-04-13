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
P5 has not started.

P5 must NOT default to minimal-real implementation.
P5 must prefer production-grade bounded solutions.

## P5 priorities
- real backend validation
- real auth / secrets / env handling
- real timeout / retry / provider quirks
- real concurrency/resource assumptions
- real replay / DLQ / recovery
- real operator / metrics / provenance
- benchmark / scale / drill readiness

## Style rule
Continue using:
- Phase planning
- M-level task planning
- precise prompts for implementation

But do NOT assume every M must be split into exactly 4 subtasks.
Task decomposition should follow what best serves the phase.

## Testing language rule
Never overclaim current tests.
Current tests are mixed:
- real contract and bounded integration coverage
- plus some fake backend / fixture-driven testing

Do not call current state "production-grade full-real testing".
