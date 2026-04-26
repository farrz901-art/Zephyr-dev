# P5-M5-S7 Real Workflows

S7 freezes real workflow chain truth, not load testing.
S7 does not repeat S3 stability work, S4 cold-operator proof, S5 release bundle hardening, or S6 operational envelope profiling.
S7 does not start installer implementation, deployment packaging, or cloud validation.

## What S7 Proves

S7 turns four representative retained workflow families into a machine-readable workflow package:

- UNS happy workflow
- IT happy workflow
- failure / recovery workflow
- fanout workflow

Each workflow now has explicit scenario truth, evidence chains, success/failure criteria, failure
matrix interpretation, and user/operator/artifact views.

## What S7 Does Not Prove

S7 is not distributed runtime proof.
S7 is not cloud or Kubernetes workflow proof.
S7 is not installer implementation.
S7 is not load testing.
S7 does not expand retained anchors.

## Exact Carrier Policy

Exact test ids are preferred; any remaining file-level carriers are intentional and explicitly bounded.
For this S7 package, the retained workflow scenarios are frozen entirely with exact test ids, so
there are currently no intentional file-level carriers.

## How Workflow Evidence Is Consumed

Every scenario freezes the same explicit chain:

- source evidence
- task/run evidence
- delivery/output evidence
- usage evidence
- provenance evidence
- governance/recovery evidence where relevant
- operator/helper evidence
- release/handoff evidence
- missing-link statement

Workflow truth is consumable without source-code reading as a requirement.
Primary truth is the canonical artifacts plus helper/report surfaces, while platform-side checking
remains only secondary corroboration.

## Scenario Boundaries

- UNS happy: retained document acquisition through bounded output and usage/provenance linkage
- IT happy: retained cursor/checkpoint identity through bounded delivery and usage linkage
- failure/recovery: failure classification, replay/requeue, governance receipt, and coherent
  provenance
- fanout: child outputs, aggregate result, shared summary, and interpretable partial failure

fanout remains composition/orchestration, not a sink connector.

## Why S7 Is Distinct

S7 is not S3 stability repetition.
S7 is not S4 cold-operator scoring.
S7 is not S5 release bundle hardening.
S7 is not S6 operational envelope profiling.

S7 exists to prove that the retained workflow chains themselves are explicit and judgeable from
artifacts/helper surfaces rather than from author memory or raw source traversal.
