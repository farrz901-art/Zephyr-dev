# Public and Private Export Boundary

## Purpose

P6-M1 defines draft export manifests.
It does not perform real code export or code migration yet.

## Public Export

Public export means a bounded subset of Zephyr-dev technical truth that may be
consumed by public product repos such as:

- Zephyr-base
- Zephyr-Web

Public export is not the same as publishing the whole Zephyr-dev repo.

Examples of draft public export candidates:

- DeliveryPayloadV1 public contract view
- content_evidence public view
- bounded local UNS flow subset
- bounded filesystem destination subset
- receipt and usage-fact viewer surface
- public error-taxonomy view

## Private Export

Private export means a bounded subset intended only for private downstream repos
such as:

- Zephyr-Pro
- Zephyr-Web-core

Private export may include broader retained connector surfaces or internal audit
tooling, but it still must not copy commercial decision logic back into
Zephyr-dev.

## Consumer Rules

- Base and Web consume only the public subset
- Pro and Web-core may consume a private subset
- Site does not consume core runtime export directly

## Release Lineage Requirements

Every later export must preserve:

- the Zephyr-dev source SHA
- the export manifest identity
- the manifest hash recorded with the downstream release

## Current Status

In P6-M1 these manifests are draft and candidate-only.

If a path is written as `packages/zephyr-core/src/...` or `bounded public subset
only`, it is intentionally a draft placeholder. It is not a claim that code has
already been migrated or split.

## M2 Boundary

Actual dry-run export mechanics start in P6-M2.
P6-M1 only freezes the policy surface for those later mechanics.
