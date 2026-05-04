# Release Source SHA Policy

## Purpose

Every future Zephyr derived release must trace back to a Zephyr-dev source
revision and a derivation manifest identity.

P6-M2 establishes lineage policy only.
It does not perform a real product release, artifact signing, or repository
split.

## Required Lineage Facts

Every future Base, Pro, Web, Web-core, and Site release must record:

- the Zephyr-dev source SHA
- the public or private export manifest hash used to derive that release
- the combined derivation manifest hash for the release plan
- the final artifact digest once real release artifacts exist

## Why This Is Required

Public and private export cannot depend on informal human memory.
Every derived product shell must be traceable back to:

- a specific Zephyr-dev technical baseline
- a specific derivation manifest set
- a specific downstream release artifact set

## P6-M2 Boundary

P6-M2 only computes and records the lineage policy and dry-run hashes.

It does not:

- create release artifacts
- sign packages
- upload packages
- publish release tags for downstream repos

## Later Release Requirement

When actual downstream repos exist, every release process must carry forward:

- source SHA: required
- export manifest hash: required
- artifact digest: required

Without those facts, public/private export must be treated as incomplete.
