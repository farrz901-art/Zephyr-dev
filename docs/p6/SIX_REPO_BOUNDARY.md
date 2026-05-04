# Six Repo Boundary

## Purpose

P6 does not turn Zephyr-dev into a commercial product repo.
P6 defines how the Zephyr family splits into six repos with explicit public and
private boundaries.

## The Six Repos

- Zephyr-dev: pure core mother repo
- Zephyr-base: public free desktop product repo
- Zephyr-Pro: private paid desktop product repo
- Zephyr-Web: public web frontend shell repo
- Zephyr-Web-core: private commercial control-plane repo
- Zephyr-site: public site, brand, and marketing repo

## Public vs Private

Public repos:

- Zephyr-base
- Zephyr-Web
- Zephyr-site

Private or internal repos:

- Zephyr-dev until bounded public export is intentionally produced
- Zephyr-Pro
- Zephyr-Web-core

Public repos must not absorb private commercial logic or private connector
internals.

## Commercial Authority

Zephyr-Web-core is the only repo allowed to own commercial judgment.

That includes:

- entitlement decisions
- billing and payment decisions
- quota authority
- download authorization
- update authorization
- user-account commercial truth
- risk and fraud decisions

No other repo is the commercial source of truth.

## Display-Only Repos

These repos may display status but must not become decision authorities:

- Zephyr-Pro may call entitlement or activation services and display resulting
  status, but it does not own server-side commercial truth
- Zephyr-Web may route and present UI, but it does not own commercial truth
- Zephyr-site may market and link, but it does not own commercial truth

## Why Zephyr-dev Must Stay Pure

Zephyr-dev exists to preserve:

- technical facts
- runtime evidence
- connector truth
- DeliveryPayloadV1 and content_evidence
- bounded readiness and audit tooling
- stable source, destination, and backend contracts

If Zephyr-dev is polluted with billing, licensing, entitlement, or download
authority logic, it stops being a reusable pure core.

## Base, Pro, Web, Site Constraints

- Base does not accept license or paid-user decisions
- Pro may call downstream entitlement clients, but does not own service-side
  commercial truth
- Web is an entry shell and display layer, not a commercial authority
- Site is a brand and marketing surface, not a commercial authority

## M1 Boundary

P6-M1 only establishes boundaries, manifests, scans, and review gates.
It does not create the real product features or repos yet.
