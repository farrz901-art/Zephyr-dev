# Zephyr Testing Reality

## What current tests do prove
Current tests do prove that Zephyr has real shared contracts and bounded integration coverage
across:
- real contract tests
- real orchestration / flow-boundary tests
- local-real integration tests
- fake backend, fixture-driven, or other bounded-subset tests

## What current tests do not fully prove
Current tests do not yet prove full production-grade real-environment correctness across the full
retained support surface.

## Most accurate current description
Current testing is mixed:
- real contract tests defend shape and semantics
- real orchestration / flow-boundary tests defend shared execution and governance paths
- local-real integration tests exercise production-like local/runtime-backed paths where available
- fake backend / fixture-driven / bounded-subset tests still cover parts of the retained surface

Passing `make test` means the current bounded support surface is largely coherent and anti-drift
coverage exists. It does not mean every backend or provider path has full live production-grade
validation.

## P4.5 implication
P4.5 exists to raise the full retained support surface to a higher authenticity level.

Current support/authenticity discussions must use the retained surface framing:
- the destination world is not only the late-P4 second-round destinations
- the source world is not only the late-P4 source additions
- preserved `airbyte-message-json` is also part of the current retained hardening scope

## Decision rule for future conversations
Do not describe current testing as full real production validation.
Use wording like:
- bounded pre-production validation
- real contract/orchestration coverage plus mixed local-real and fake-backed testing
- not yet production-grade full-real validation
