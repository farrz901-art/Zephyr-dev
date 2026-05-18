# P6K-M2 PackageManifestV1

## Why M2 Exists

Before M2, Zephyr output identity was mostly rooted in source `sha256`. That is insufficient once the
same source can legitimately produce different outputs under different partition profiles,
strategies, backends, OCR settings, and future workflow nodes. It also leaves destinations without
a stable way to distinguish artifact kinds or reason about a package as a coherent unit.

M2 adds a production-stable `PackageManifestV1` layer so Zephyr can describe:

- what package was produced
- why it is distinct from another package for the same source
- which artifacts belong to it
- which artifacts are required vs optional
- how later phases can map packages to package-aware delivery behavior

## Contract Surface

M2 adds versioned, typed, JSON-round-trippable contracts in `zephyr-core`:

- `PackageManifestV1`
- `PackageRunMetaV1`
- `ArtifactDescriptorV1`

The contract includes:

- `schema_version`
- `package_id`
- `source_sha256`
- `source_id`, `workflow_id`, `node_id`
- `profile`
- `strategy_identity`
- `package_run_meta`
- `artifacts`
- `primary_artifact_id`
- `manifest_sha256`
- `created_at_utc`
- additive `metadata`

## Why Source SHA Alone Is Insufficient

`source_sha256` is still preserved, but it is no longer treated as the full package identity.

`package_id` is built from deterministic canonical JSON over:

- `source_sha256`
- `profile`
- `strategy_identity`
- `engine_name`
- `engine_backend`
- `workflow_id`
- `node_id`
- `source_id`

Null identity inputs are included through explicit stable null markers. This makes the hashing rule
durable before future workflow/node identifiers become first-class runtime inputs.

`artifact_id` is also deterministic and includes:

- `package_id`
- `artifact_kind`
- `role`
- normalized `relative_path`
- `producer`

Windows `\` separators are normalized to `/` before hashing.

## Artifact Descriptor Semantics

Each artifact descriptor records:

- stable `artifact_id`
- `artifact_kind`
- `role`
- absolute `path`
- normalized `relative_path`
- `media_type`
- file `sha256`
- `size_bytes`
- `required`
- `producer`
- optional descriptor metadata

Required artifacts fail manifest construction if they are missing. Optional artifacts are tolerated
and simply omitted when absent.

Current artifact semantics in M2 are:

- `run_meta.json` -> `run_meta` / `metadata`
- `elements.json` -> `elements` / `primary_structured_output`
- `normalized.txt` -> `normalized_text` / `primary_text_output`
- `records.jsonl` -> `it_records` / `structured_records`
- `checkpoint.json` -> `checkpoint` / `progress_state`
- `logs.jsonl` -> `structured_logs` / `structured_logs`
- `delivery_receipt.json` -> `delivery_receipt` / `delivery_evidence`
- `usage_record.json` -> `usage_record` / `usage_fact`

## Runtime Integration

M2 writes `package_manifest.json` into the existing artifact directory:

`<out_root>/<sha256>/package_manifest.json`

Existing artifact layout is preserved. M2 is additive.

Manifest generation now happens for real successful outputs in both:

- UNS runs
- IT runs

For successful runs, Zephyr now ensures a local artifact bundle exists before destination delivery.
This keeps package identity and package-aware output material stable across filesystem and
non-filesystem destinations.

After `delivery_receipt.json` and `usage_record.json` are written, Zephyr refreshes the manifest so
those post-delivery artifacts can also be described without moving existing files.

## DeliveryPayloadV1 Compatibility

`DeliveryPayloadV1` remains backward compatible.

M2 adds optional `artifacts.package_manifest_path` when the manifest exists. Existing consumers that
only read the prior fields continue to work.

M2 does not rewrite destination mapping. Destinations are not required to parse the manifest yet.

Minimal package-aware evidence remains intentionally small in M2:

- manifest availability
- artifact count
- primary artifact kinds

Full package-aware destination behavior is deferred to M3.

## What M2 Intentionally Does Not Do

M2 does not add:

- workflow runtime
- node execution runtime
- package-aware destination rewrite
- commercial packaging
- Base product behavior
- OCR changes
- Paddle native runtime work

`unstructured==0.22.28` remains unchanged. OCR behavior remains unchanged.

## M3 Boundary

M3 will build on M2 by using `PackageManifestV1` as the stable package/artifact identity source for:

- package-aware destination mapping
- future workflow/node artifact expansion
- deterministic output verification across more destination families

M2 only establishes the identity and manifest substrate needed for that work.

## Boundary Notes

No commercial logic is added.

Base is unchanged.

The isolated Paddle native runtime experiment remains out of scope for M2 and does not affect this
package identity foundation.
