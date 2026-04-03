# zephyr-core — AGENTS.md

This package is the platform SSOT for contracts and minimal governance primitives.

Rules
- No runtime services, no network clients, no heavy deps. Keep dependencies minimal/empty.
- New contract modules must be versioned (v1/v2) and re-exported intentionally via __init__.py + __all__.
- Any contract shape change requires schema_version bump OR explicit backward-compatible shim.
- Add/adjust anti-drift tests for any exported contract.
