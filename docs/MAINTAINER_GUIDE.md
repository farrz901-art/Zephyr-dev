# 测试策略/已知隐患与债务（未完成）

# Zephyr Maintainer Guide
(Testing strategy, tech debt, roadmap)

This document is intended for future maintainers and for cross-session AI handoff.

---

## 1) Testing Strategy (how we keep CI stable under strict typing)

### 1.1 Test tiers
We maintain 3 tiers of tests:

#### Tier A — Unit tests (CI MUST run)
Goal: deterministic, no external services, no OS-specific dependencies.

Approach:
- Use Protocol-compatible mock classes (NOT bare lambdas) to satisfy pyright strict.
- Avoid real Unstructured heavy parsing in CI (PDF/OCR/docx may require system deps).
- Narrow Any/Unknown via `isinstance` + `cast`, or via small Pydantic validation models.

Examples:
- destination tests using mock destination returning `DeliveryReceipt`
- runner tests using fake partition_fn producing `PartitionResult`
- webhook tests using `httpx.MockTransport`

#### Tier B — Contract tests (CI SHOULD run)
Goal: enforce contract shapes do not drift.

Contract objects:
- `RunMetaV1.to_dict()` must contain `schema_version/outcome/metrics.attempts`
- `DeliveryPayloadV1` must remain JSON-serializable and stable keys
- artifact layout under `<out>/<sha>/...` is stable

Techniques:
- json.dumps payload and assert keys
- file existence tests for artifact writers
- caplog tests for required log event names

#### Tier C — Integration tests (Optional / local / nightly)
Goal: verify real parsing quality and performance.

Rules:
- Integration tests may require:
  - Unstructured extras (pdf/docx/pptx/xlsx/image)
  - OS binaries (tesseract/poppler/libreoffice/pandoc)
  - large fixtures (not committed to repo)
- Integration tests must be gated:
  - skip if fixture not present
  - skip if required module not importable
- Do NOT make CI depend on these.

Recommended fixture source:
- Unstructured API `sample-docs/` pulled via `git sparse-checkout` into local ignored folder.

---

## 2) How to avoid pyright strict regressions (non-negotiable patterns)

### 2.1 Keyword-only Protocols
If a callable is invoked as keyword-only, it MUST be typed as:
```python
class Writer(Protocol):
    def __call__(self, *, a: int, b: str) -> Ret: ...
```

Never use positional `Callable[[...], ...]` for keyword-only callables.

Affected areas:

-   `dump_partition_artifacts(*, out_root=..., sha256=..., ...)`
-   Destination plugins
-   Partition backends

### 2.2 Typed default_factory

Avoid:

-   `field(default_factory=list)` (can become list[Unknown]) Prefer:

```
def _empty_str_list() -> list[str]:
    return []
warnings: list[str] = field(default_factory=_empty_str_list)
```

### 2.3 Narrow Any/Unknown aggressively

-   For

    ```
    httpx.Response.json()
    ```

    :

    -   `isinstance(payload, list)` then `payload_list = cast(list[object], payload)`
    -   narrow item via `isinstance(item_obj, dict)` then `cast
