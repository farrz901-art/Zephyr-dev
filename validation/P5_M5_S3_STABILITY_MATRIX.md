# P5-M5-S3 Stability Matrix

S3 stability-validates repeated representative closures only. It reuses the S2 anchor set and does not widen into new-anchor discovery, all-pairs rerun expansion, or distributed/cloud validation.

## What S3 Validates

S3 validates four repeated deep closures across repeated execution rounds, cleanup then rerun, and bounded same-host soak:

1. failure -> replay -> verify -> governance receipt
2. poison/orphan -> requeue -> inspect -> verify
3. source contract id -> usage -> governance -> provenance
4. fanout partial failure -> shared summary -> governance/usage

## How S3 Differs From S2

S2 proved each representative closure once. S3 proves that the same closures remain stable across reruns with fresh basetemp, out_root, replay.db, queue roots, receipt scopes, checkpoint identities, and child-output scopes.

## Cleanup Then Rerun

cleanup then rerun is explicit. Each selected anchor has a machine-readable cleanup step, rerun carrier bundle, compare-artifact set, and dirty-state pollution definition. S3 does not treat dirty residual state as proof of stability.

## Bounded Soak

bounded same-host soak remains non-load-test. The same 4 family bundles are rerun for mandatory round 2 and may optionally run round 3, but S3 does not become distributed runtime testing, cloud testing, or large-scale load testing.

## Stability Signals

S3 unifies artifact path stability, usage linkage stability, provenance linkage stability, governance receipt stability, queue state stability, source contract id stability, fanout aggregate/shared-summary stability, and helper/report artifact-check stability into one machine-readable truth package.

## Fanout Boundary

fanout remains composition/orchestration, not a sink connector. Its repeated-run stability is evaluated through aggregate/shared-summary behavior plus child/aggregate scope separation, not by turning it into an all-pairs sink matrix.

## False-Stability Controls

false-stability risk is controlled through fresh scope per rerun: fresh basetemp, fresh out_root, fresh governance receipt scope, fresh replay.db, fresh queue roots, fresh topic/run/checkpoint identity, and fresh child/aggregate output scopes.
