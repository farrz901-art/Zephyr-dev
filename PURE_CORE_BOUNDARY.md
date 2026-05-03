# PURE_CORE_BOUNDARY

## Purpose

Zephyr-dev is the pure core mother repo for the Zephyr product family.

Its job is to preserve technical truth, runtime evidence, connector truth, and
stable contracts that later product repos can inherit. It is not the place
where Zephyr makes commercial decisions.

## Core Rule

Zephyr-dev may say what technical fact happened.

Zephyr-Web-core may decide whether a user is entitled, whether money should be
charged, whether a download is authorized, whether an update is allowed,
whether a quota decision blocks execution, or whether a risk/fraud policy must
intervene.

In short:

- Zephyr-dev records technical truth.
- Zephyr-Web-core owns commercial judgment.

## Zephyr-dev Allowed Surface

Zephyr-dev may retain and evolve these categories:

- raw usage fact
- technical capability domain
- source, destination, and backend contracts
- DeliveryPayloadV1
- content_evidence
- runtime, build, cut, and evidence facts
- pure technical input and output contracts
- connector readiness and destination evidence facts
- non-commercial diagnostic facts
- bounded operator and governance facts that remain technical and non-RBAC
- handoff, repair-plan, and release-pack evidence that stays non-commercial

Allowed examples:

- whether a source was readable
- whether a destination accepted DeliveryPayloadV1
- what run_id, sha256, provenance, and content_evidence were emitted
- whether a retry was retryable or non-retryable
- what local/runtime substrate fact blocked a bounded validation run

## Zephyr-dev Forbidden Surface

Zephyr-dev must not become the source of truth for commercial judgment or
commercial enforcement.

The following classes are forbidden in Zephyr-dev runtime, contracts, and
connector logic:

- payment
- subscription
- license entitlement
- account entitlement
- paid_user
- checkout
- invoice
- quota decision
- billing decision
- HWID
- device fingerprint
- activation limit
- download authorization
- presigned paid URL
- update entitlement
- manifest signature policy
- package signing policy
- risk score
- fraud detection
- commercial API gate
- pricing table
- refund logic
- user account
- RBAC

Forbidden examples:

- deciding whether a user may unlock Pro features
- deciding whether a user may download or update a package
- deciding whether usage should be billed
- deciding whether a device is over activation limit
- deciding whether a user account is licensed or entitled
- deciding whether a risk score blocks product access

## Boundary by Repo

- Zephyr-dev: pure technical facts and stable shared contracts
- Zephyr-base: public free desktop product shell derived from the pure core
- Zephyr-Pro: private paid desktop product shell that consumes commercial truth
- Zephyr-Web: public web entry shell
- Zephyr-Web-core: private commercial control plane and decision authority
- Zephyr-site: public site, brand, and marketing surface

Zephyr-dev may emit technical facts that other repos consume.
Zephyr-dev must not become the authority for commercial policy.

## Public and Private Hygiene

Public repos must not leak:

- private connectors
- Pro-only strings that imply hidden private capability surfaces
- Web-core internal decision logic
- commercial API secrets
- protected entitlement or billing internals

Private repos may contain commercial logic, but they must be reviewed there,
not normalized back into Zephyr-dev.

## P6 Rule

P6 is the commercial productization phase for the broader Zephyr family.
P6 does not commercialize Zephyr-dev itself.

P6 work may grow broader and more product-like, but each new commercial
responsibility must land in the correct downstream repo rather than polluting
the pure core mother repo.
