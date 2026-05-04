# NOTICE SBOM Preflight Policy

## Purpose

P6-M2 performs engineering preflight only for license, NOTICE, and SBOM
readiness.

This is not legal advice and not a final legal review.

## Public Release Expectation

Before any public repo release such as Zephyr-base, Zephyr-Web, or Zephyr-site:

- license status must be reviewed
- NOTICE requirements must be reviewed
- SBOM generation must be reviewed
- third-party dependency attribution must be reviewed

Public release must not rely on P6-M2 preflight alone.

## Private Release Expectation

Private releases such as Zephyr-Pro or Zephyr-Web-core also require:

- dependency attribution review
- license review
- internal release-compliance review

Private visibility does not remove attribution obligations.

## P6-M2 Boundary

P6-M2 only answers:

- whether a repository license file is present
- whether dependency manifests exist
- whether NOTICE exists
- whether SBOM exists

P6-M2 does not answer:

- whether legal review is complete
- whether attribution is sufficient
- whether a downstream product is safe to release

## Later Gate

Full legal, NOTICE, SBOM, and release-compliance finalization belongs to later
P6 release gates.
