# P6-M3 Base Repo Scaffold Plan

## Target Repo

- name: `Zephyr-base`
- visibility: `public`
- role: free local desktop product
- commercial logic allowed: `false`

This plan defines a future public repo scaffold only.
P6-M3-S1 does not create the real repository.

## Technical Route

The frozen Base route is:

- Tauri desktop shell
- Rust command bridge
- Web UI
- JSON artifact bridge and process bridge into Zephyr public core

M3 does not require:

- Python to Rust full rewrite
- Tauri production runtime implementation
- product signing, entitlement, or update control

## Planned Structure

```text
Zephyr-base/
  README.md
  PRODUCT_BOUNDARY.md
  LICENSE
  NOTICE.md
  docs/
    USER_GUIDE.md
    LOCAL_ONLY_PRIVACY.md
    TROUBLESHOOTING.md
    SOURCE_LINEAGE.md
  src-tauri/
    Cargo.toml
    tauri.conf.json
    src/
      main.rs
      commands.rs
      bridge.rs
      errors.rs
      lineage.rs
  ui/
    package.json
    index.html
    src/
      App.tsx
      components/
        Welcome.tsx
        FileDropZone.tsx
        ProgressPanel.tsx
        ResultSummary.tsx
        EvidenceCard.tsx
        ReceiptCard.tsx
        UsageFactCard.tsx
        ErrorDiagnosisPanel.tsx
      styles/
        tokens.css
  public-core-bridge/
    bridge_contract.json
    sample_input_request.json
    sample_output_artifact.json
    run_public_core.placeholder.py
  manifests/
    base_capability_manifest.json
    public_export_lineage.json
    design_asset_request.json
  tests/
    smoke/
      local_file_flow_smoke.md
      boundary_scan_smoke.md
  packaging/
    windows/
      installer_plan.md
    update/
      update_manifest_consumer_plan.md
```

## Shadow Scaffold Rule

The following are shadow scaffold outputs in P6-M3-S1:

- repository tree draft
- placeholder README and boundary docs
- placeholder Tauri, Rust, and UI files
- placeholder bridge contract samples
- placeholder packaging notes

These files are generated for scaffold planning only.
They are not production implementation.

## Later Realization Boundary

The following stay for later M3 steps:

- real Tauri app initialization
- real Rust command wiring
- real Web UI implementation
- real Python process bridge execution
- real installer packaging
- real repo creation and remote setup

## Real Repo Creation Rule

The real `Zephyr-base` repo must not be created from this step automatically.

Real creation requires:

- user authorization
- source SHA recorded
- public manifest hash recorded
- public leakage scan pass
- commercial contamination scan pass
- forbidden private import scan pass
- scaffold plan reviewed
