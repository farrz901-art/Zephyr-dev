# P5-M5 Validation Charter

P5-M5 starts from the P5-M4 closed retained surface. M5-S0 does not reopen M4 debt; it freezes the validation plan that later M5 stages will execute.

## Goal

On M4-closed retained surfaces, perform layered matrix-based real validation convergence.

## Non-Goals

M5-S0 does not execute the deep matrix. It does not claim distributed runtime, cloud or Kubernetes packaging, billing pricing entitlement, RBAC approval workflow, enterprise connector implementation, benchmark expansion, soak execution, cold-operator execution, or release bundle implementation.

## Layered Matrix Strategy

The M5 validation plan is layered, not flat all-pairs. Layer 1 gives every retained source at least one success path, one abnormal/failure path, and usage/provenance visibility. Layer 1 also gives every retained single-sink destination at least one baseline success chain.

Layer 2 uses representative deep anchors instead of equal-depth testing for every source x destination permutation. The selected anchors are filesystem, opensearch, webhook, kafka, spool/sqlite queue governance, google_drive_document_v1, confluence_document_v1, kafka_partition_offset_v1, and fanout.

Layer 3 is reserved for later M5 repeatability, soak, and cleanup work. S0 freezes that expectation but does not run it.

## Fanout Boundary

fanout is composition/orchestration, not a single sink. It receives its own success and partial/failure composition chains and must not be counted as a single-sink connector.

## Scope Control

The retained inventory is frozen in validation/p5_m5_retained_surface_inventory.json. The layered matrix is frozen in validation/p5_m5_validation_matrix.json. Representative anchor rationale is frozen in validation/p5_m5_deep_anchor_selection.json. These files are planning truth for M5 validation, not proof that validation has already completed.

## M4 Dependency

M4 closeout remains closed and resolved. M5 inherits runtime, recovery, benchmark, bounded-concurrency, capability-domain, product-cut, usage, source, and governance truth as closed input surfaces. S0 does not turn any M4-core item into residual risk.
