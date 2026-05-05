# P6-M3 Base Public Export Consumption Plan

## Boundary

`Zephyr-base` consumes only the public subset defined by
`docs/p6/public_core_export_manifest.json`.

`Zephyr-base` must not consume:

- `private_core_export`
- Pro private code
- Web-core internal code
- commercial decision logic

## Initial Bridge Strategy

M3 initial integration uses:

- JSON artifact bridge
- process bridge

Python public core produces local artifacts.
Rust command bridge reads those artifacts.
Web UI displays the safe result view.

## Non-Requirement

M3 does not require:

- Python to Rust full rewrite
- P4.5 substrate boot
- cloud processing
- private connector runtime

## Lineage Rule

Every later Base derivation must record:

- Zephyr-dev source SHA
- public manifest hash
- bridge contract version
- Base scaffold manifest hash

## Ownership Rule

Python public core remains the owner of:

- normalized_text generation
- content_evidence generation
- receipt-shaped technical outputs

Rust and Tauri own:

- desktop shell orchestration
- local command bridge
- artifact loading
- error display
- local-only UX state
