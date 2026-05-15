# P6K-M1-S0 CI Cleanup

## Why this cleanup is required

During P6-M1~M3, Zephyr-dev temporarily carried local validation helpers and tests used while building and checking the external `Zephyr-base` desktop product. Those files validated product packaging, Tauri layout, installer/package artifacts, and external proof paths.

That work was useful during local Base development, but it does not belong in Zephyr-dev pure-core CI because GitHub CI does not have:

- a sibling `Zephyr-base` repository
- local Windows paths such as `E:\Github_Projects\Zephyr-base`
- local Tauri product outputs or installer/package layouts
- local desktop runtime proof artifacts

Leaving those files in normal pytest collection makes Zephyr-dev CI depend on external product state instead of reproducible core behavior.

## What was quarantined

The following classes of files were moved to `validation/local_only/p6_m1_m3_base_product/`:

- `tools/p6_m3_*` Base-product handoff and local product validation tools
- `packages/zephyr-ingest/src/zephyr_ingest/tests/test_p6_m3_*` Base-product handoff tests

These files remain in the repository only as historical local validation references. They are intentionally outside normal pytest collection.

## What was deleted

The following files were removed as temporary one-off junk:

- `tools/.codex_p6_m3_s12_patch_base_fix.py`
- `tools/.codex_s17p_patch_zephyr_base.py`

These were local patch helpers, not stable repository assets.

## What was intentionally kept

This cleanup did not remove or weaken real Zephyr-dev core coverage. In particular, it kept:

- P6K-M0 audit tools
- repo-local P6-M1 handoff coverage
- repo-local P6-M2 handoff coverage
- normal package tests that operate only on repository-local fixtures and pure-core behavior

## Why this does not lower core quality

This change removes only non-reproducible Base-product coupling from Zephyr-dev CI collection. It does not:

- disable pytest
- skip broad test directories
- weaken runtime contracts
- change partition behavior
- change dependencies
- change `uv.lock`

The result is a cleaner CI boundary: Zephyr-dev CI tests pure core behavior, while old Base product validation remains available as local historical reference.

## Why this is necessary before P6K-M1

P6K-M1 is the first real implementation slice for `unstructured==0.22.28` Enhanced Partition Profiles. That work needs a clean Zephyr-dev CI baseline first, otherwise unrelated Base-product path assumptions can mask or pollute real kernel-hardening failures.

P6K-M1 still targets a product-grade stronger core. This cleanup is not a retreat to a weaker baseline; it is a boundary correction so the next implementation work lands on reproducible core CI instead of local product coupling.
