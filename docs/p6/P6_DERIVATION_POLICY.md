# P6 Derivation Policy

## Six-Repo Model

The Zephyr product family derives from a six-repo architecture:

- Zephyr-dev: pure core mother repo
- Zephyr-base: public free desktop product
- Zephyr-Pro: private paid desktop product
- Zephyr-Web: public web frontend shell
- Zephyr-Web-core: private commercial control plane
- Zephyr-site: public site, brand, and marketing repo

## Commercial Logic Ownership

Commercial judgment belongs only in Zephyr-Web-core.

Zephyr-Web-core may own:

- entitlement decisions
- payment and billing decisions
- quota decisions
- risk and fraud decisions
- download authorization
- update authorization
- account-level commercial truth

Zephyr-Pro may:

- call entitlement services
- cache or display entitlement state
- adapt UI to authorized product state

Zephyr-Pro must not:

- become the primary source of commercial truth
- make independent billing or licensing decisions

Zephyr-base:

- remains a public free desktop product
- does not perform license checks
- does not perform paid-user decisions

Zephyr-Web:

- remains a public web entry shell
- may present UI and route user actions
- does not own commercial truth

Zephyr-site:

- owns brand, product pages, marketing, and legal-facing public web material
- must not claim unsupported P7-level capabilities

Zephyr-dev:

- contains no commercial judgment
- remains the technical source of truth for core contracts and runtime facts

## Public and Private Policy

Public repos must not contain:

- private connectors
- Pro-only secret product logic
- commercial API secrets
- Web-core internal decision code
- downstream commercial enforcement internals

Private repos may contain commercial logic, but each such change must pass a
security review gate.

Every product release must track:

- the Zephyr-dev source SHA
- an export manifest showing what crossed into the product repo
- the public/private policy used for that export

## Review Gate Policy

Changes that introduce or touch the following must receive explicit security or
commercial-boundary review in the owning private repo:

- entitlement logic
- payment or billing logic
- download or update authorization
- device binding or activation enforcement
- pricing or quota policy
- risk and fraud policy
- package signing or manifest authorization policy

## P6 Execution Strategy

- M1: establish boundaries and contamination guardrails
- M2: implement public/private export and derivation mechanics
- M3 and M4: build Base and Pro desktop product shells
- M5 and M6: build Web-core and Web bounded product surfaces
- M7 and M8: build Site, signing, download, and update controls
- M9 and M10: run end-to-end validation and final release-pack closeout
