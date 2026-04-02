# 4.不可破坏的工程约束

# Engineering Invariants (Zephyr) — MUST FOLLOW

This document defines *non-negotiable* engineering rules for Zephyr.
If a change violates any rule here, treat it as a breaking change and fix the design.

Last verified stable SHA (reference point): 49ffa9bec0c785c0dfc31c82ddf6319c0027f237

---

## 0. Core invariants (project philosophy)
1) Zephyr is an upstream “data logistics hub”.
   - Ingest upstream data → normalize/enrich → deliver stable, versioned artifacts.
2) uns-stream aligns to Unstructured capability boundaries.
   - Zephyr should not reimplement Unstructured core algorithms.
   - Zephyr adds contracts + governance + pluggable backends/destinations.
3) it-stream (future) aligns to Airbyte concepts but stays loosely coupled from uns-stream.
4) Every recommendation or refactor must be verified against a *fixed commit SHA*.

---

## 1. Toolchain invariants (CI gates)
These commands MUST stay green:
- `make tidy`
- `make check`
- `make test`

`make check` MUST include:
- `ruff format --check .`
- `ruff check .`
- `pyright` (strict)
- `mypy packages`
- `python -m py_compile tools/*.py` (tools scripts must not silently rot)

GitHub Actions MUST pin critical actions by SHA (supply-chain hardening):
- `actions/checkout@<sha>`
- `astral-sh/setup-uv@<sha>`

---

## 2. Python / packaging invariants
- Python >= 3.12 across workspace.
- uv workspace is the single source of dependency locking (`uv.lock` committed).
- Each package under `packages/*` is a proper Python project with `src/` layout.
- Workspace internal deps must use `[tool.uv.sources] { workspace = true }`.

---

## 3. Typing invariants (pyright strict)
### 3.1 No “hand-wavy typing”
- Avoid `Any` at public boundaries.
- If upstream returns `Any` (e.g., `httpx.Response.json()`), it MUST be narrowed via:
  - `isinstance` checks + `cast(...)`
  - or a small Pydantic validation model (preferred for messy metadata)

### 3.2 Avoid `# type: ignore` as default
- Do NOT use broad ignores to silence strict errors.
- Preferred approaches:
  1) precise imports from the actual module (avoid `from package import X` unless it is explicitly re-exported)
  2) Protocol + keyword-only callables
  3) typed default_factory functions
  4) `isinstance` + `cast`
  5) local stubs in `typings/` only if truly needed

### 3.3 Typed default factories (Unknown inference hardening)
In strict mode, `field(default_factory=list)` can become `list[Unknown]`.
We MUST use a typed factory:

```python
def _empty_str_list() -> list[str]:
    return []

warnings: list[str] = field(default_factory=_empty_str_list)
```

Use the same pattern for dicts, tuples, etc.

### 3.4 Keyword-only Protocols (critical)

If a function is called as keyword-only (`f(*, a=..., b=...)`) then the type MUST be expressed as:

```
class Writer(Protocol):
    def __call__(self, *, a: int, b: str) -> Return: ...
```

Do NOT use `Callable[[int, str], Return]` for keyword-only callables. It will break pyright strict.

This rule applies to:

-   `dump_partition_artifacts(*, out_root=..., sha256=..., meta=..., result=...)`
-   partition functions that are keyword-only
-   Destination plugins

------

## 4. Contract invariants (versioned output schemas)

### 4.1 RunMeta contract is owned by zephyr-core

-   `RunMetaV1` is the single source of truth for run_meta.json shape.
-   `schema_version` MUST exist and MUST bump when RunMeta changes.
-   All writers (tools, ingest, destinations) must generate run_meta via `RunMetaV1.to_dict()`.

### 4.2 Delivery payload contract is owned by zephyr-ingest

-   `DeliveryPayloadV1` is the single source of truth for destination payload.
-   Destinations must not hand-roll payload dicts.

### 4.3 Stable artifact layout (filesystem)

Per document (sha256 directory):

-   `<out_root>/<sha256>/run_meta.json` (always written)
-   `<out_root>/<sha256>/elements.json` (success only)
-   `<out_root>/<sha256>/normalized.txt` (success only)
-   `<out_root>/<sha256>/delivery_receipt.json` (if destination invoked)

DLQ:

-   `<out_root>/_dlq/delivery/*.json`
-   `<out_root>/_dlq/delivery_done/*.json` (after replay success)

Batch report:

-   `<out_root>/batch_report.json`

These paths are contract-like. Avoid changing without a migration plan.

------

## 5. Governance invariants (retry / DLQ / replay)

-   Partition retry and delivery retry are separate concerns.
-   “retryable” must be explicit and must be carried into:
    -   `ZephyrError.details["retryable"]`
    -   run_meta error details (inject if missing)
-   Delivery failures must never be dropped:
    -   write DLQ record
    -   support replay

------

## 6. Logging invariants

Partition logging events are contract-like strings:

-   `partition_start`
-   `partition_done`
-   `partition_error`
-   `partition_failed`

Do not rename these keys casually; tests enforce them.

------

## 7. Backend invariants (local vs uns-api)

-   Backends must implement the same PartitionBackend-style callable:

    -   provide `name/backend/version` for EngineInfo
    -   keyword-only `partition_elements(...) -> list[ZephyrElement]`

-   ```
    uns-api
    ```



    backend must use Unstructured API endpoint:

    -   POST `/general/v0/general`
    -   header `unstructured-api-key` when key is enabled

-   Always normalize element metadata via normalize pipeline

    -   remove `file_directory`
    -   cast `"true"/"false"` to bool
    -   derive `bbox` from `coordinates.points`

------

## 8. Cross-platform invariants (Windows dev + Linux CI)

-   `.gitattributes` must enforce LF line endings.
-   pre-commit hook `mixed-line-ending` may stay disabled on Windows, but repo must remain clean.
-   Avoid relying on OS-specific binaries for CI tests.
    -   Integration tests requiring extras/system deps must be optional/skip.

------

## 9. How to review changes (self-checklist)

Before pushing:

1.  Run `make tidy && make check && make test`
2.  Check run_meta schema_version changes if contract changed
3.  Confirm keyword-only Protocol compatibility (no Callable positional mismatch)
4.  Confirm tools scripts still py_compile clean
5.  If adding new destination/backend, ensure:
    -   it returns DeliveryReceipt
    -   it uses DeliveryPayloadV1 (for destinations)


---
(Appendix) Observability invariants (Non-negotiable)

Operational observability contracts are treated as engineering invariants.
See: `docs/OBSERVABILITY_INVARIANTS.md`

Key SSOT locations:
- log events: `zephyr_ingest/obs/events.py`, `uns_stream/service.py`
- prom export: `zephyr_ingest/obs/prom_export.py`
- config snapshot sources keys: `zephyr_core/contracts/v2/config_snapshot.py`
