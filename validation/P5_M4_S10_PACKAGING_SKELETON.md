# P5-M4-S10 Packaging Skeleton

S10 turns S9 technical-domain truth into a product-cut and packaging skeleton. These are not installable product packages.

The domains remain technical domains, not entitlement, licensing, subscription, or pricing domains.

## Product-Cut Skeleton

Base is intended to contain shared core/service/orchestration surfaces, current `it-stream`, the direct non-extra `uns-stream` surface, all current Base source connectors, all current single-sink destinations, and the current destination composition surface.

Pro is intended to add the current `uns-stream` optional extras on top of Base. `uns-stream remains mixed`: direct non-extra capability is Base, optional extras are Pro, and the package is not modeled as whole-package Pro.

Extra remains a non-default incubation domain for `audio` and `model_init`. It is not installable or packaged by default.

## Sink vs Composition

`fanout is composition`: it is destination composition/orchestration over child destinations, not a single sink connector. The single sink list is limited to filesystem, webhook, sqlite, kafka, weaviate, s3, opensearch, clickhouse, mongodb, and loki.

`base.py` remains an abstract destination base, not a connector capability.

## Packaging Output Boundary

S10 outputs are validation/report artifacts only. They are not installable product packages and do not create a release artifact.

Future Base packaging work may consume the S9/S10 machine-readable manifests, but it must not assume `uns-stream` optional extras or an installer contract.

Future Pro packaging work may consume the Base skeleton plus the explicit `uns-stream` optional extra list, but it must not assume whole-package Pro or implemented enterprise connectors.

Future Extra work may consume the reserved `audio` / `model_init` truth, but Extra is not default packaged.

## Deferred

- installer or release-consumable outputs
- domain-sliced tests and fixtures
- repo split
- commercial entitlement enforcement
- deployment packaging
