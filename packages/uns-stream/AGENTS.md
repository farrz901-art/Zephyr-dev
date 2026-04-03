# uns-stream — AGENTS.md

Adapter layer around Unstructured (local library or HTTP uns-api).

Rules
- Keep backend pluggable via PartitionBackend Protocol (local vs http).
- Do not leak unstructured element classes into zephyr-core contracts.
- Keep retryable semantics consistent (retry_policy as SSOT for classification).
- Metadata normalization must remain stable and privacy-aware.
