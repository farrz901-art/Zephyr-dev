# P6-M3 Base Acceptance Gates

## Principle

M3 cannot be sealed by CI green alone.

## Required Gates

Required acceptance gates include:

- Base shell smoke
- local file selection smoke
- local file to public core to filesystem output smoke
- receipt and evidence display smoke
- error diagnosis smoke
- no license, payment, or entitlement scan violations
- no private core import scan violations
- source SHA recorded
- public manifest hash recorded
- design asset checklist linked

## Explicit Rules

- CI-only is not sufficient
- local-only product flow must be demonstrable
- filesystem output must be part of the acceptance surface
- normalized_text and content_evidence display path must be part of the
  acceptance surface
- contamination and boundary scans remain mandatory

## Out of Scope for M3 Acceptance

The following are not M3 acceptance requirements:

- Pro entitlement flow
- Web-core commercial API
- cloud processing
- Docker substrate boot for ordinary Base users
- payment or license runtime logic
