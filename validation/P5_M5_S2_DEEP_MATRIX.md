# P5-M5-S2 Deep Matrix

S2 deeply validates representative anchors only. It does not attempt flat all-pairs equal-depth proof, and it does not become S3 repeatability or soak execution.

## What S2 Deeply Validates

S2 turns four grouped deep closures into explicit machine-readable truth:

1. failure -> replay -> verify -> governance receipt
2. poison/orphan -> requeue -> inspect -> verify
3. source contract id -> usage -> governance receipt -> provenance
4. fanout partial failure -> shared summary -> governance/usage visibility

The selected anchors are representative: webhook, kafka, opensearch, spool_queue, sqlite_queue, google_drive_document_v1, confluence_document_v1, kafka_partition_offset_v1, fanout, and filesystem as a low-cost control anchor.

## Why These Anchors Are Representative

They cover the deepest retained risk surfaces without expanding to all retained source x destination permutations. Destination failure anchors pressure replay and verify. Queue anchors pressure requeue, inspect, and differing audit-support levels. Source anchors pressure source->usage->governance->provenance linkage. fanout pressures composition behavior rather than sink behavior.

## Grouped Deep Closures Are Explicit

The important change in S2 is that grouped deep closures are explicit. Shared replay, verify, governance, and usage carriers are listed directly in machine-readable artifacts rather than being left for author inference after reading multiple files.

## S2 Boundary

S1 was baseline coverage; S2 is representative deep proof. S2 still does not prove every retained pair deeply, and S3 repeatability/soak/cleanup execution is deferred. Distributed runtime, cloud or Kubernetes packaging, billing pricing entitlement, RBAC approval workflow, and enterprise connector implementation remain excluded.

## Fanout Boundary

fanout remains composition/orchestration, not a sink connector. Its deep proof is aggregate- and summary-oriented and uses filesystem only as a low-cost control anchor, not as a signal to expand into child-sink all-pairs permutations.

## Queue/Governance Boundary

spool_queue and sqlite_queue stay under one grouped deep closure, but their support levels remain explicit rather than flattened: spool keeps stronger persisted-in-history support, while sqlite remains result-only.

## Cleanup and Isolation Discipline

Deep proof remains valid only with case-scoped cleanup and isolation. webhook, kafka, opensearch, queue roots, replay db, verify receipts, backfill outputs, SaaS source identities, kafka progress artifacts, and fanout child/aggregate outputs all keep explicit machine-readable cleanup rules.
