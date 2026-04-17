# P4.5 Closeout Audit

## Scope
This is the formal exit audit for `P4.5 authenticity hardening`.

It audits the full retained support surface:

- retained destinations: `filesystem`, `webhook`, `sqlite`, `kafka`, `weaviate`, `s3`, `opensearch`, `clickhouse`, `mongodb`, `loki`
- retained sources: `http_json_cursor_v1`, `postgresql_incremental_v1`, `clickhouse_incremental_v1`, `kafka_partition_offset_v1`, `mongodb_incremental_v1`, `http_document_v1`, `s3_document_v1`, `git_document_v1`, `google_drive_document_v1`, `confluence_document_v1`
- preserved input path: `airbyte-message-json`
- shared system surfaces: runtime/auth/env, replay/DLQ/recovery, queue/lock/runtime/operator, batch-report/governance summary, provenance/inspectability, control-plane/runtime-plane separation

This report is paired with [p45_closeout_exit_matrix.json](/E:/Github_Projects/Zephyr/validation/p45_closeout_exit_matrix.json), which is the structured exit artifact.

`validation/p45_truth_matrix.json` remains the retained-support coverage artifact. Its tier statuses remain placeholder-shaped there by design, so the actual exit judgment and residual-risk accounting live in the closeout exit matrix.

## Destinations
### Baseline
| Destination | Authenticity level | Evidence | Recovery / replay evidence | Residual risk | Blocking |
| --- | --- | --- | --- | --- | --- |
| `filesystem` | local-real/runtime-hardened | `test_p45_destination_wave1_local_real.py`, `test_p45_m7_cross_cutting_drills.py` | path failure normalization, crash-adjacent overwrite recovery, replay success, duplicate replay stability, batch/worker consistency | bounded to explicit output-root subset | no |
| `webhook` | service-live against runtime-backed HTTP fixture | `test_p45_destination_wave1_service_live.py`, `test_p45_destination_wave1_recovery_drill.py`, `test_p45_m7_cross_cutting_drills.py` | success, retryable `503`, non-retryable `400`, replay success, duplicate replay stability, batch/worker consistency | no provider-owned auth workflow in retained subset | no |
| `sqlite` | local-real/runtime-hardened | `test_p45_destination_wave1_local_real.py`, `test_p45_m7_cross_cutting_drills.py` | lock-sensitive failure, replay success, duplicate replay stability, batch/worker consistency | bounded row-style/table subset only | no |
| `kafka` | service-live | `test_p45_destination_wave2_service_live.py`, `test_p45_destination_wave2_recovery_drill.py` | retryable broker failure, replay success, truthful duplicate append behavior, batch/worker consistency | current Redpanda runtime is plaintext, so auth is not truthfully testable there | no |
| `weaviate` | service-live | `test_p45_destination_wave2_service_live.py`, `test_p45_destination_wave2_recovery_drill.py` | auth/config failure, retryable transport failure, replay success, duplicate replay truthfully constrained by object identity | duplicate semantics are constraint/id-based, not generic exact-once | no |

### Second-round
| Destination | Authenticity level | Evidence | Recovery / replay evidence | Residual risk | Blocking |
| --- | --- | --- | --- | --- | --- |
| `s3` | service-live | `test_p45_destination_wave1_service_live.py`, `test_p45_destination_wave1_recovery_drill.py` | missing-bucket/config failure, auth failure, retryable endpoint failure, replay success, duplicate replay stability | bounded to one-object-per-payload writes | no |
| `opensearch` | service-live over HTTPS/basic-auth/self-signed local runtime | `test_p45_destination_wave1_service_live.py`, `test_p45_destination_wave1_recovery_drill.py` | auth/config failure, retryable connection failure, replay success, duplicate replay stability | bounded to explicit index/document-id writes | no |
| `clickhouse` | service-live | `test_p45_destination_wave1_service_live.py`, `test_p45_destination_wave1_recovery_drill.py` | auth failure, missing-table non-retryable failure, retryable connection failure, replay success, duplicate replay truthfully deduped by retained idempotent subset | bounded row-style write subset only | no |
| `mongodb` | service-live | `test_p45_destination_wave2_service_live.py`, `test_p45_destination_wave2_recovery_drill.py` | auth failure, retryable connection failure, replay success, duplicate replay/upsert stability | bounded to replace/upsert subset | no |
| `loki` | service-live | `test_p45_destination_wave2_service_live.py`, `test_p45_destination_wave2_recovery_drill.py` | tenant/auth failure, retryable transport failure, replay success, duplicate replay audited truthfully without exact-once claim | append-style semantics remain explicit | no |

### Destination audit judgment
- All ten retained destinations reached the same authenticity bar.
- The validation form differs only where backend topology makes it truthful:
  - local-real/runtime-hardened for `filesystem` and `sqlite`
  - service-live for the service-backed destinations
- No baseline destination was held to a lower bar than a second-round destination.

## Sources
### `it-stream`
| Source | Authenticity level | Supported subset actually audited | Checkpoint / resume evidence | Residual risk | Blocking |
| --- | --- | --- | --- | --- | --- |
| `http_json_cursor_v1` | local-real/runtime-backed HTTP cursor path | one URL, one cursor param, JSON-object response subset, optional next-cursor continuation, state-only `cursor_v1` resume | `test_p45_source_wave1_service_live.py`, `test_p45_source_wave1_recovery_drill.py` | no record-level continuation, no separate auth surface in bounded fixture path | no |
| `postgresql_incremental_v1` | service-live | one ascending cursor column, explicit selected columns, bounded batch size, `cursor_v1` resume | `test_p45_source_wave1_service_live.py`, `test_p45_source_wave1_recovery_drill.py`, `test_p45_m7_cross_cutting_drills.py` | no CDC or transaction-session resume semantics | no |
| `clickhouse_incremental_v1` | service-live | one ascending cursor column, explicit selected columns, bounded batch size, `cursor_v1` resume | `test_p45_source_wave1_service_live.py`, `test_p45_source_wave1_recovery_drill.py` | no async export/job semantics | no |
| `kafka_partition_offset_v1` | service-live | one topic, one partition, ascending offset progression, optional starting offset, bounded batch size, JSON-object payload subset, `cursor_v1` resume | `test_p45_source_wave2_service_live.py`, `test_p45_source_wave2_recovery_drill.py` | current plaintext runtime does not provide truthful auth failure; no consumer-group or multi-partition semantics | no |
| `mongodb_incremental_v1` | service-live | ordered collection reads over one ascending cursor field, explicit projected fields, bounded batch size, `cursor_v1` resume | `test_p45_source_wave2_service_live.py`, `test_p45_source_wave2_recovery_drill.py` | no oplog/change-stream semantics | no |

`it` source identity stability, checkpoint identity distinctness, resumed provenance continuity, and secret exclusion from identity were proven across the retained source set.

### `uns-stream`
| Source | Authenticity level | Supported subset actually audited | Fetch / partition-path evidence | Residual risk | Blocking |
| --- | --- | --- | --- | --- | --- |
| `http_document_v1` | service-live against runtime-backed HTTP fixture plus real partition path | one explicit document URL, bounded request-header subset, inferred filename/mime subset, one fetched document per task | `test_p45_source_wave3_service_live.py`, `test_p45_m7_cross_cutting_drills.py` | no separate auth surface in bounded fixture path | no |
| `s3_document_v1` | service-live plus real partition path | explicit bucket plus key, optional version id, one object fetch per task | `test_p45_source_wave3_service_live.py` | no prefix listing or multi-object discovery semantics | no |
| `git_document_v1` | local-real plus real partition path | explicit repo-root plus commit plus relative-path selection, bounded blob fetch, one fetched file per task | `test_p45_source_wave3_local_real.py` | bounded local model has no truthful auth surface and no stable dedicated retryable remote-fetch case | no |
| `google_drive_document_v1` | saas-live plus real partition path | explicit file selection, bounded export-or-download acquisition subset, optional drive locator, bounded export mime subset, one fetched file per task | `test_p45_source_wave4_saas_live.py`, `test_p45_validation_helpers.py` | dedicated provider-controlled retryable case is not isolated as a separate stable surface; transient network noise is bounded in the live harness instead | no |
| `confluence_document_v1` | saas-live plus real partition path | explicit site plus page selection, optional requested page-version subset, bounded page-body fetch, one fetched page per task | `test_p45_source_wave4_saas_live.py`, `test_uns_confluence_source.py` | no attachment crawling or page-tree recursion semantics | no |

`uns` source identity stability, provenance correctness, secret non-leakage, and Zephyr-owned partition/output ownership were proven across the retained source set.

### Source audit judgment
- Baseline and later-added sources reached the same authenticity standard.
- The evidence form differs only because the source topologies are different:
  - local-real/runtime-backed
  - service-live
  - saas-live
- No retained source is being excused from identity/provenance/secret-discipline or recovery/fetch-path evidence because of lineage.

## Preserved `airbyte-message-json`
`airbyte-message-json` is audited separately because it is a preserved `it-stream` input path, not a remote source connector.

Evidence:

- local-real protocol-path and corpus evidence:
  - `packages/it-stream/src/it_stream/tests/test_p45_source_wave2_local_real.py`
- recovery/provenance evidence:
  - `packages/it-stream/src/it_stream/tests/test_p45_source_wave2_recovery_drill.py`
- retained input-path ownership and normalization evidence:
  - `packages/it-stream/src/it_stream/tests/test_service.py`

What is proven:

- representative retained corpus success
- malformed protocol-path failure
- unsupported protocol-path failure
- checkpoint creation where the preserved path participates in `cursor_v1`
- resumed provenance continuity
- task identity stability
- Zephyr-owned artifact ownership

Residual risks:

- this does not claim remote fetch coverage
- this does not widen into broader Airbyte platform/runtime support
- coverage remains bounded to the retained corpus/protocol subset already in repo

Blocking:

- no

## Cross-cutting shared surfaces
### Auth / secrets / env hardening
Evidence:

- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_validation_helpers.py`

Proven:

- control-plane repo vs runtime-plane external home separation
- runtime-home compose/env resolution
- env precedence and fallback discipline
- redaction of secret values in helper summaries
- required SaaS env detection
- legacy Google Drive runtime-home alias mapping into canonical control-plane names

Residual risk:

- secrets remain external runtime-home concerns by design; repo artifacts reference only variable names/categories

### Replay / DLQ / recovery / redrive calibration
Evidence:

- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_destination_wave1_recovery_drill.py`
- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_destination_wave2_recovery_drill.py`
- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py`

Proven:

- retryable vs non-retryable classification leads to the expected replay/DLQ ownership path
- representative replay success and representative replay failure remain inspectable
- replay provenance remains explicit and stable
- resume remains distinct from replay and from requeue-derived execution

### Runtime / queue / lock / operator drills
Evidence:

- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py`

Proven:

- real queue/lock/runtime/operator surfaces are pressured through current `zephyr-ingest` ownership
- stale lock recovery remains inspectable
- failed/replayed/requeued work remains inspectable through current Zephyr-owned surfaces
- operator-facing surfaces can still answer:
  - what ran
  - what failed
  - whether it was retryable
  - whether it was replayed, resumed, or requeued
  - which stable provenance facts explain that history

Residual risk:

- current queue/lock/runtime shape remains explicitly local/single-runtime oriented; this is inherited into P5 rather than hidden

### Batch-report / governance summary calibration
Evidence:

- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_destination_fanout.py`
- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py`

Proven:

- shared summary fields remain aligned with real outcomes after replay/recovery activity:
  - `delivery_outcome`
  - `failure_retryability`
  - `failure_kind`
  - `error_code`
  - `attempt_count`
  - `payload_count`
- fanout-level failures now surface truthful shared summary facts when child failure semantics agree

### Shared provenance / operator-facing inspectability
Evidence:

- `packages/it-stream/src/it_stream/tests/test_p45_source_wave1_recovery_drill.py`
- `packages/it-stream/src/it_stream/tests/test_p45_source_wave2_recovery_drill.py`
- `packages/uns-stream/src/uns_stream/tests/test_p45_source_wave3_service_live.py`
- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_m7_cross_cutting_drills.py`

Proven:

- `it` resume provenance remains explicit and inspectable
- `uns` reacquisition/repartition facts remain flow-local but inspectable through shared surfaces
- shared operator/report surfaces distinguish `primary`, `resume`, and `replay`

### Control-plane vs runtime-plane separation
Evidence:

- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p45_validation_helpers.py`

Proven:

- repo-owned code/tests/helpers stay in the control plane
- real compose/env/secrets/runtime state stay in the external runtime home
- closeout does not require moving runtime assets into repo

## Baseline vs second-round parity
Destination parity:

- approved
- baseline and second-round destinations reached the same authenticity bar
- differences in evidence type are truthful topology differences only

Source parity:

- approved
- baseline and later-added sources reached the same authenticity bar
- differences in evidence type are truthful topology differences only

Dual-standard behavior:

- none remains in the retained support surface

## Exit judgment
Judgment:

- `P4.5 exit approved`

Why:

- all retained destinations have truthful authenticity evidence at the required bar
- all retained sources have truthful authenticity evidence at the required bar
- preserved `airbyte-message-json` has separate protocol-path authenticity evidence
- shared replay/recovery/runtime/operator surfaces have system-level drill evidence
- baseline and later-added surfaces are not operating under dual authenticity standards

Non-blocking residual risks:

- several retained subsets remain intentionally bounded and do not claim broader sync, consumer-group, CDC, attachment-crawl, transaction, or async bulk-job semantics
- some auth or dedicated retryable cases remain not truthfully testable where the retained runtime topology does not provide them, but those absences match the explicit support boundary and do not create dual standards
- SaaS live validation depends on external network reachability and valid runtime-home credentials; closeout records only the bounded surfaces actually proven and does not expose secret values

Result:

- `P5 may begin from this audited baseline`
