# P5-M4-S13 Source Spec Parity

source spec parity remains partial. S13 reduces drift risk by capturing canonical source spec
direction and runtime source-linkage canonicalization, but it does not claim full `spec.registry`
parity.

Current bounded truth:

- Source contract truth is first-class in `validation/p5_source_contract_matrix.json`.
- Source usage linkage truth is first-class in `validation/p5_source_usage_linkage.json`.
- Source specs are not fully registered in the unified spec registry today.
- canonical source spec direction is now machine-readable in
  `validation/p5_source_spec_parity.json`.
- `zephyr_ingest.usage_record.normalize_source_contract_id` is the runtime canonicalization helper.
- flow_family_only fallback remains honest when a runtime task lacks a retained source contract id.

Asymmetry remains explicit:

- `uns` source ids describe document-acquisition sources.
- `it` source ids describe structured progress sources.

Explicit non-claims:

- no full source `spec.registry` parity yet
- no dishonest `uns` / `it` source symmetry
- no guarantee that future source additions automatically join usage linkage without manifest/test
  updates
