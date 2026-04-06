# it-stream

Minimal structured-data flow package for Zephyr.

## Package ownership at P3 end

`it-stream` is the home of the structured-data flow's local contracts and runtime semantics.

Keep these here for now:
- structured input normalization
- `it` checkpoint/state artifact shapes
- `it` task/checkpoint identity normalization
- checkpoint-driven resume selection and continuation rules

These are intentionally **not** promoted to `zephyr-core` yet.

Rationale:
- they are still first-generation and narrow
- they are currently shaped by one real flow
- resume/checkpoint semantics are not yet proven across multiple flow families

What is already aligned with the wider platform:
- `it` participates in the shared orchestration chain through `TaskV1 -> FlowProcessor -> Delivery`
- `it` uses shared run/provenance contracts from `zephyr-core`
- `it` recovery execution now aligns with shared provenance/execution facts without freezing
  flow-local checkpoint semantics prematurely

## P4 expansion guidance

P4 should build on the `it` flow only where the current governance/runtime path is already mature
enough.

That means:
- do not treat `it-stream` as the place to recreate a full upstream connector/runtime platform
- do not prioritize wide source connector expansion ahead of hardening the current checkpoint /
  resume / governance boundaries
- keep new `it` capabilities aligned to the shared orchestration and delivery path in
  `zephyr-ingest`

`it-stream` is expected to grow as a real second flow, but it should grow through Zephyr-owned
contracts and bounded runtime semantics, not by importing an external platform model wholesale.
