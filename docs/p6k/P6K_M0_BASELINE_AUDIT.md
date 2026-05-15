# P6K-M0 Baseline Audit

- Repository: Zephyr-dev
- Branch: master
- Baseline SHA: d899f640cc9f084bc5457ea8ab725a4d4856d052
- Current head SHA at report generation: d899f640cc9f084bc5457ea8ab725a4d4856d052
- Current unstructured version: 0.21.5
- Target unstructured version: 0.22.28
- dependency_changed: false
- uv_lock_changed: false
- runtime_behavior_changed: false
- Base changed: false
- commercial logic added: false

## S1 Scope / Roadmap / Non-goals

- P6K is a Zephyr-dev core hardening line for future Pro/Web common-kernel needs.
- P6K does not modify Zephyr-base and does not add commercial logic.
- P6K-M0 is audit-only; P6K-M1 is the first implementation slice.

## S2 Unstructured 0.22.28 Enhanced Partition Profile gap

- Current version: 0.21.5
- Target version: 0.22.28
- Wrapper exposed params now: filename, strategy, unique_element_ids, backend, run_id, pipeline_version, sha256, size_bytes
- Missing wrapper params: languages, detect_language_per_element, language_fallback, skip_infer_table_types, infer_table_structure, pdf_infer_table_structure, extract_image_block_types, extract_image_block_output_dir, extract_image_block_to_payload, data_source_metadata, metadata_filename, hi_res_model_name, model_name, starting_page_number, **kwargs
- Missing CLI flags: --profile, --languages, --detect-language-per-element, --skip-infer-table-types, --extract-image-block-types, --extract-image-block-output-dir, --extract-image-block-to-payload, --hi-res-model-name, --starting-page-number, --pdf-infer-table-structure, --infer-table-structure
- Missing metadata guards: text_as_html, image_base64, image_mime_type, parent_id, detection_class_prob, coordinates.system, layout_width, layout_height, page_number, filetype, filename, languages, data_source

## S3 PackageManifest gap

- Missing package fields: package_id, artifact_id, workflow_id, node_id, source_id, profile, strategy_identity, package_manifest.json, artifact_descriptors, package_run_meta, node_output_identity, package_aware_content_evidence
- Compatibility issue: DeliveryPayloadV1 points at artifact paths and summary evidence, but it cannot describe multiple package-scoped artifacts or stable node-level identities.

## S4 it-stream Airbyte-like gap

- Current supported concepts: ConnectorSpecV1 exists as a generic Zephyr connector field spec, not an Airbyte-like sync protocol., airbyte-message-json subset currently accepts RECORD, STATE, and LOG messages., Single-stream bounded message normalization exists in it-stream service., Checkpoint and resume semantics exist for bounded cursor/token/page/state_dict progress kinds., Existing bounded source lanes include http_json_cursor_v1, postgresql_incremental_v1, clickhouse_incremental_v1, kafka_partition_offset_v1, and mongodb_incremental_v1., Current artifacts include records.jsonl, checkpoint.json, and logs.jsonl.
- Missing concepts: DiscoverResultV1, CatalogV1, ReadProtocolV1, TRACE support, ERROR support as a first-class message type, Per-stream state contract, Per-slice checkpoint contract, Schema evolution policy, Normalization plan, Multi-stream read catalog and selection semantics

## S5 Destination package mapping gap

- Direct-compatible destinations: filesystem, kafka, loki, s3, webhook
- Destinations needing mapping changes: clickhouse, mongodb, opensearch, sqlite, weaviate

## S6 Base non-regression boundary

- Base remains public, local-only, and non-commercial.
- P6K must not force Base to inherit heavy unstructured extras or package-aware outputs by default.

## S7 Execution plan

- Next milestone: P6K-M1
- Execution plan frozen in docs/p6k/P6K_EXECUTION_PLAN.md and validation/p6k_m0_execution_plan.json.

## Blockers

- Current environment is still on unstructured 0.21.5; M1 must handle 0.22.28 upgrade work.
- Enhanced Partition Profiles are not implemented yet; M0 is audit-only by design.
- PackageManifestV1 is not implemented yet; M2 is the first package-aware identity slice.
- Airbyte-like structured sync contracts are not implemented yet; M4 is the first bounded it-stream hardening slice.

## Judgment

- Status: pass
- P6K-M0 intentionally documents the gap without implementing it.
