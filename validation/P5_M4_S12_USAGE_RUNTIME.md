# P5-M4-S12 Usage Runtime

S12 moves the usage model from manifest-only truth to bounded runtime-native emission.

Runtime usage truth is emitted as `usage_record.json`. It is a raw technical usage fact artifact,
not billing, pricing, quota, allowance, license, subscription, or entitlement logic.

Runtime fields now protected by contract:

- Per-task `usage_record.json` is the dedicated usage fact artifact beside `run_meta.json` and
  `delivery_receipt.json`; batch-root `usage_record.json` is the aggregate non-billing usage
  summary beside `batch_report.json`.
- `capability_domains_used` records technical-domain evidence as Base / Pro / Extra booleans plus
  evidence strings.
- `source to usage linkage` preserves the retained source contract id when `TaskV1` carries a
  source value that maps to a retained source contract.
- `uns` raw unit remains `document`; page is not universal.
- `it` raw unit remains `record_or_emitted_item` and is still bounded/not fully first-class.
- Success, failure, batch partial/empty status, resume, replay, requeue, and redrive remain
  distinguishable in runtime usage records.

Machine-readable artifacts:

- `validation/p5_usage_runtime_contract.json`
- `validation/p5_usage_runtime_output_surface.json`
- `validation/p5_source_usage_linkage.json`

Explicit non-claims:

- Usage runtime output is not billing telemetry.
- `run_meta.json` is not overstuffed with usage facts.
- Runtime usage evidence is local artifact-backed truth, not a distributed/fleet metering platform.
