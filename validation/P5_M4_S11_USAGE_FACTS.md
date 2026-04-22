# P5-M4-S11 Usage Facts

S11 defines raw usage facts, not billing. It does not introduce pricing, entitlement, license, subscription, allowance, or quota decisions.

## Raw Usage Record Truth

The current usage record shape is `raw_usage_fact_v1_skeleton`. It is machine-readable groundwork and is not runtime-emitted by `RunMetaV1` today.

The stable linkage basis is:

- `task.kind`
- `task.task_id`
- `run_meta.run_id`
- `run_meta.provenance.task_id`
- `run_meta.provenance.run_origin`
- `run_meta.provenance.delivery_origin`
- `run_meta.provenance.execution_mode`
- result facts from run metadata, delivery receipt, and batch counters

`TaskV1.kind` remains only `uns | it`. Runtime observation labels such as replay, delivery, batch, or worker task are not task kinds.

## Raw Unit Truth

`uns raw unit: document`.

`page is not universal`, and page facts must not be treated as a stable raw unit. `elements_count` and `normalized_text_len` are derivative metrics, not primary raw units.

`it` raw unit is conservatively `record_or_emitted_item`. It is useful for structured-flow usage thinking, but it is still bounded because current runtime artifacts have not promoted it into a universal first-class field.

Delivery success is not a raw processing unit.

## Capability Domains Used

The usage marker is `capability_domains_used`. It is a technical capability-domain marker, not an entitlement marker.

The marker shape is:

- `base: bool`
- `pro: bool`
- `extra: bool`
- `evidence: list[str]`

Current Base paths use `base=true`, `pro=false`, and `extra=false` unless there is explicit evidence otherwise.

Mixed `uns-stream` truth remains intact: direct non-extra document processing is Base, not Pro. Root dev `uns-stream[all-docs]` is engineering convenience and must not mark runtime usage as Pro. Extra is non-default unless an explicit Extra capability such as `audio` or `model_init` is touched.

## Success and Failure

Both success and failure usage facts are recordable as raw facts.

Success surfaces include run success, delivery success, and batch success or partial success.

Failure surfaces include run failure, delivery failure, and batch failure or partial failure. Delivery failure preserves retryability, failure kind, and error code facts.

Recovery surfaces remain distinct:

- resume preserves `run_origin=resume` and checkpoint linkage
- replay preserves `delivery_origin=replay` and delivery/DLQ linkage
- requeue preserves `run_origin=requeue` and queue-governance linkage
- redrive is not collapsed into replay/requeue unless explicitly modeled

## Non-Claims

S11 does not claim runtime `RunMetaV1` already emits `usage_record` or `capability_domains_used`.

S11 does not define retry billing, replay billing, failure chargeability, pricing, entitlement, licensing, or quota logic.
