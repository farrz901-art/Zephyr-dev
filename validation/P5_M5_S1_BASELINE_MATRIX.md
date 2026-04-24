# P5-M5-S1 Baseline Matrix

S1 runs layer 1 baseline coverage only. It executes the retained-surface baseline carriers that S0 froze, while staying clearly shallower than S2 and S3.

## What S1 Runs

S1 runs exact retained source success carriers, exact retained source abnormal carriers, exact retained destination baseline success carriers, and both fanout success plus fanout partial/failure carriers. It also runs light visibility support so usage, provenance, and governance surfaces stay observable without turning S1 into a deep governance walkthrough.

## What S1 Does Not Run

S2 representative deep validation is deferred. S3 repeatability/soak/cleanup execution is deferred. S1 does not claim distributed runtime, cloud or Kubernetes packaging, billing pricing entitlement, RBAC approval workflow, or enterprise connector implementation.

## Why S1 Stays Baseline-Sized

The execution plan freezes 32 baseline execution units: 10 retained source success units, 10 retained source abnormal units, 10 retained destination success units, and 2 fanout units. Usage/provenance and governance visibility are supported separately at light depth rather than by expanding every retained source into full recovery walkthroughs.

## Fanout Boundary

fanout is composition/orchestration, not a sink connector. S1 runs one fanout success carrier and one fanout partial/failure carrier, but it does not classify fanout as a single-sink destination.

## Governance Visibility Boundary

light governance visibility is preserved through usage/runtime and governance-audit support carriers. S1 deliberately does not require replay/verify or requeue/inspect/verify deep walkthroughs on every retained source or destination. Those walkthroughs remain later-layer work.

## Cleanup and Isolation Discipline

S1 depends on explicit cleanup and isolation rules for filesystem outputs, opensearch targets, sqlite/spool queues, replay db, governance receipts, fanout child outputs, and SaaS/service-live isolation. The rules are frozen in validation/p5_m5_s1_cleanup_rules.json so baseline execution does not depend on residual state.
