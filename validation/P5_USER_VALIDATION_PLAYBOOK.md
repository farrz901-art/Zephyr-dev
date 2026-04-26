# P5 User Validation Playbook

This is a P5 final user validation prep package, not the execution itself.
This playbook does not claim P6 product shell is done.
Source-code reading is not required.
Platform-side visual checking is secondary corroboration.
User feedback must be classified instead of turning into vague scope expansion.

## How To Use This Playbook

Use the machine-readable user validation plan to guide the final P5 user-style pass.
For each task:

- perform the user action
- compare expected vs actual result
- inspect operator-visible helper/report surfaces
- inspect artifact-visible evidence
- record feedback using the machine-readable feedback template

## Task Families

The playbook covers five concrete task families:

- UNS user task
- IT user task
- failure/recovery user task
- fanout user task
- release/handoff user task

## Interpretation Guardrails

- Do not convert a user note into broad scope expansion without classifying it first.
- Do not treat a P6 product shell request as an M5 blocker.
- Do not treat cloud, deployment, or commercial requests as P5 closeout truth.
- Do not treat visual inspection alone as primary truth over the canonical artifacts and helpers.
