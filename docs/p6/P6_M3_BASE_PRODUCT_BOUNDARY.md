# P6-M3 Base Product Boundary

## Product Position

Zephyr-base is the free local desktop product.

It is intentionally narrower than Zephyr-dev retained connector breadth and
narrower than future Pro or Web-core product surfaces.

## Allowed M3 Boundary

Base may do:

- local file selection
- local text input
- local public-core processing
- normalized_text display
- content_evidence display
- receipt display
- raw usage fact display
- local filesystem output
- local error diagnosis display

Usage display remains technical fact only.
Usage is not billing.

## Forbidden Base Scope

Base must not include:

- license
- entitlement
- paid_user
- Pro-only connector controls
- Web-core API dependency
- cloud processing requirement
- external connector UI
- payment
- quota authority
- commercial decision logic

## Runtime Rule

Base local-only is the default M3 product route.

Base must not require:

- P4.5 substrate boot
- Docker compose startup
- external service credentials
- private core export

## Export Rule

Base consumes only the public subset defined by
`docs/p6/public_core_export_manifest.json`.

Base must not consume:

- `private_core_export`
- Web-core internal code
- Pro private code

Every later Base derivation must record:

- Zephyr-dev source SHA
- public manifest hash

## Repo Creation Rule

M3-0 does not create the real Zephyr-base repo.

Real scaffold creation may start only after:

- repo scaffold plan
- Base boundary freeze
- public export source SHA
- public manifest hash
- public leakage scan
- commercial contamination scan
