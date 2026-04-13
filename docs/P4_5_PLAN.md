# P4.5 authenticity hardening plan

P4.5 is a distinct phase between completed P4 and not-yet-started P5.

## Scope
- retained destinations: 10
- retained sources: 10
- preserved `airbyte-message-json` as a separate retained `it-stream` input path

Baseline and second-round connectors do not use different authenticity standards during P4.5.

## Objective
Move the current retained support surface from mixed fake/local/live confidence toward stronger
real-backed or production-like authenticity evidence without widening the supported surface.

## Work bundles
- truth matrix and support-surface freeze
- destination authenticity hardening
- source authenticity hardening
- `airbyte-message-json` protocol-path hardening
- auth / secrets / env hardening
- failure / replay / recovery calibration
- runtime / queue / lock / operator drills
- exit audit

## Exit rule
P4.5 exit is required before P5 begins.
