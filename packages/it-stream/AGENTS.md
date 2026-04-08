# AGENTS.md

This file adds `it-stream` package-local guardrails on top of the repository root `AGENTS.md`.
Use it for `it-stream` recovery, checkpoint, and source/runtime changes.

## Package role
- `it-stream` owns structured-flow parsing and local checkpoint/resume semantics for `it` inputs.
- It must keep flow-local recovery/runtime behavior in-package and must not push first-generation
  recovery mechanics into shared orchestration surfaces casually.

## Recovery governance boundary
Treat the current P4 recovery model as an explicit architecture boundary, not an implementation
accident.

Keep these concepts distinct:
- task identity: identifies the requested work
- checkpoint identity: identifies a specific checkpoint entry for that work
- lineage: `parent_checkpoint_identity_key` links only the immediate prior checkpoint
- progress family: `progress_kind` is explicit and remains separate from raw `progress` payload data
- resume selection: selected checkpoint facts stay explicit and inspectable
- resume provenance: resumed execution facts stay explicit and inspectable
- recovery failure class: `malformed`, `incompatible`, `unsupported`, and `blocked` remain distinct

Do not collapse these concepts into one another through convenience helpers, implicit inference, or
source-specific shortcuts.

## Current supported recovery model
- Compatibility/schema checks happen before resume selection and remain separate from runtime resume
  decisions.
- Resume selection is explicit and currently supports the proven cursor-based subset.
- Recognized progress families may still be unsupported for resume; recognition is not the same as
  resumability.
- Blocked recovery within a supported family is distinct from malformed and incompatible
  checkpoints.
- Real-path HTTP cursor source recovery is currently bounded by the existing execution model:
  cursor-state continuation is supported, while record-level continuation on that path remains
  blocked unless resumable record timestamps exist.

## Change discipline
When changing `it-stream` checkpoint/resume behavior:
- update this file in the same change if you alter the recovery boundary or the supported recovery
  subset
- keep tests focused on boundary semantics, not broad invalid-input matrices
- prefer strengthening existing checkpoint/provenance/recovery semantics over creating a parallel
  recovery path

If a future P4/P5 change intentionally expands recovery support, make the boundary change explicit
here and in focused anti-drift tests in the same patch.
