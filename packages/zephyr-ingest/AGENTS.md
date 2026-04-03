# zephyr-ingest — AGENTS.md

This package is the production data-plane runner + delivery layer.

Rules
- Never break artifacts invariants (run_meta always written; delivery receipt; DLQ on delivery failure).
- DeliveryPayload shape is SSOT in zephyr-core (v2). Replay must match.
- Preserve observability event names/fields; add new events only if needed, never rename.
- Config/spec/CLI must remain anti-drift: registry -> argparse -> snapshot must stay consistent.
