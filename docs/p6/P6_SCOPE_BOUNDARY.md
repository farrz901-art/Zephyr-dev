# P6 Scope Boundary

## Intent

P6 is the Zephyr commercial productization phase.

P6 does not reclassify Zephyr-dev as a commercial product repo.
Zephyr-dev remains the pure core mother repo even while the wider Zephyr family
branches into public and private product repos.

## What P6 Is Allowed To Do

P6 may:

- define the six-repo product architecture
- define public/private derivation strategy
- define commercial-boundary ownership
- define export manifests, release lineage, and security review gates
- build fuller productization plans than a minimal placeholder MVP
- document how Base, Pro, Web, Web-core, and Site will relate to the pure core

## What P6 Must Not Do In Zephyr-dev

P6 must not turn Zephyr-dev into:

- a payment repo
- a billing repo
- a licensing or entitlement repo
- a user-account repo
- a download-authorization repo
- a commercial API gate
- an RBAC or enterprise permission system

## P5.1 Truth Freeze

P6 does not reopen P5.1 connector truth, runtime truth, DeliveryPayloadV1
truth, or retained matrix truth by default.

P5.1 remains the sealed technical baseline. P6 builds product layers around
that baseline.

## Phase Discipline

P6 may stretch and become fuller than a tiny MVP, but every new product concern
must land in the correct repo:

- pure technical truth stays in Zephyr-dev
- commercial judgment stays in Zephyr-Web-core
- public product shells stay public
- private commercial internals stay private

## Deferred Beyond P6

The following are explicitly deferred to P7 or later unless separately
re-authorized:

- Kubernetes and Helm deployment claims
- broader cloud runtime ownership
- multi-tenant SaaS control plane claims
- enterprise RBAC and enterprise permission systems
- advanced anti-crack and anti-tamper systems
- complex financial reconciliation and back-office billing operations
