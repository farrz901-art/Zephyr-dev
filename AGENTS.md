# Zephyr-dev — AGENTS.md (Codex Project Operating Manual)

You are a coding agent working in a strict, contract-first Python monorepo (uv workspace).
Goal: keep changes small, pass gates, and never drift contracts/governance.

CURRENT STAGE
- P2 (ops-ready) completed; entering P3 (Production runtime / Scale-out).
- Keep commercialization / webserver / licensing out of this repo (future separate repo).

NON-NEGOTIABLE SSOT
- Contracts SSOT: packages/zephyr-core/src/zephyr_core/contracts (v1/v2). Do not drift JSON shapes.
- Observability SSOT: event names + stable fields are contracts. Do not rename existing events.
- Delivery SSOT: zephyr_core.contracts.v2.delivery_payload / delivery_receipt / batch_report / config_snapshot.

ARTIFACTS INVARIANTS (must always hold)
Per sha256 directory:
- Always write run_meta.json (success or failure).
- On success write elements.json + normalized.txt.
- Delivery attempts always write delivery_receipt.json.
- Delivery failure writes _dlq/delivery/*.json.
- Replay must emit the SAME payload shape as normal delivery.

QUALITY GATES (must pass before finalizing)
- make tidy
- make check  (pyright strict + mypy + ruff)
- make test

TYPING / STYLE (pyright strict friendly)
- Prefer Protocols for plugin points; prefer keyword-only signatures for callables used as Protocols.
- Avoid Any/Unknown leaks; use isinstance + cast patterns.
- Use StrEnum for string-valued enums.
- Do not import upstream types (unstructured/airbyte) into zephyr-core contracts.

CONTEXT / COST DISCIPLINE (minimize token usage)
- Do NOT open/read uv.lock unless you are changing pyproject.toml.
- Avoid scanning docs/ and tests/ unless the task explicitly requires it.
- Use targeted grep/rg first; open only the minimal files needed.
- Prefer small diffs; do not reformat unrelated code.

WORK METHOD
1) Write a short plan (6-12 bullets).
2) List exact files you will read/change.
3) Implement minimal patch for the task.
4) Run gates: make tidy, make check, make test.
5) If gates fail: fix until green, then summarize changes + reasoning.
