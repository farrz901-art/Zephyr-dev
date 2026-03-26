# 测试策略/已知隐患与债务

# Zephyr Maintainer Guide
(Testing strategy, tech debt, known issues, roadmap)

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

典型来源：

-   `httpx.Response.json()` -> `Any`
-   `json.loads()` -> `Any`
-   第三方库返回 dict/list 的松散结构

推荐模式：

```
from typing import Any, cast

payload: Any = resp.json()

if not isinstance(payload, list):
    raise ValueError("expected list")

items = cast(list[object], payload)
for item in items:
    if not isinstance(item, dict):
        continue
    obj = cast(dict[str, object], item)
    # 对 obj["metadata"] 等字段继续 isinstance 收口
```

当你确实需要“短期绕过”以保证测试可写（例如异常对象属性在 pyright 下难以精确表达）：

-   优先用 `getattr(e, "code")` 这种**显式、局部**的收口方式
-   不要在核心逻辑里堆 `type: ignore`

------

### 2.4 测试里的 Mock 约定（让严格类型不妨碍测试可读性）

-   后端/目的地 mock：用最小 class 实现协议（DummyOKBackend / DummyExplodeBackend / DummyZephyrErrorBackend）
-   HTTP mock：用 `httpx.MockTransport(handler)`，在 handler 里断言 request headers/body

------

## 3) Known Issues / Tech Debt（已知隐患与债务）

>   这一节不是“吐槽清单”，而是为了让后续维护者知道：哪些坑已经被看见、为什么重要、推荐如何修。


### 3.1 .zephyr.lock 的“stale lock”问题（中优先级，部分解决）
现状：

[已实现] 自动清理（TTL）：现已支持通过 --stale-lock-ttl-s (CLI) 或 RunnerConfig.stale_lock_ttl_s 配置过期时间。若检测到 st_mtime 超过阈值，Worker 会自动尝试 unlink 并抢锁。

[待改进] 语义模糊：目前无论是“处理中被锁”还是“清理过期锁失败”，统一返回 SKIPPED_EXISTING，导致外部难以区分是“已存在结果”还是“并发冲突”。

剩余债务与后续改进：

元信息增强：目前 lock 文件仅写入 run_id。建议增加 timestamp、pid 和 hostname，以支持更精准的 stale 判定（目前仅依赖文件系统的 mtime）。

明确 Outcome：引入 RunOutcome.SKIPPED_LOCKED 显式枚举，以区分“已存在”与“竞争中”。这涉及 core contract 变更，需谨慎评估。

健壮性：在极其高并发的分布式文件系统（如 NFS/SMB）上，open("x") 后的 unlink 抢锁逻辑仍存在微小理论竞态，长期看可能需要 portalocker 等跨平台锁库。

修改建议说明：
优先级下调：从“高”改为“中”，因为最头疼的“永久死锁”问题已经通过 TTL 补丁解决了。

状态标注：明确标注哪些是“已实现”，哪些是“待改进”，这对 AI 跨 Session 协作非常友好。

技术细节同步：提到了目前仅依赖 mtime，这提醒了维护者如果文件系统时间戳不准，可能会有潜在问题。

------

### 3.2 DLQ 增长与清理策略缺失（中高优先级）

现状：

-   delivery 失败会写入 `<out>/_dlq/delivery/`
-   batch_report 会记录 dlq_dir 与写入总数

风险：

-   长期运行会导致 `_dlq` 无限增长
-   replay 成功后的归档/删除策略需要明确（否则磁盘治理困难）

建议：

-   定义“replay 成功”后的归档目录（例如 `_dlq/delivery_done/`）与保留周期
-   增加 `zephyr-ingest prune-dlq`（按天/按数量/按大小）
-   对 webhook/kafka 等目的地增加失败分级（retryable vs non-retryable），避免无意义重放

------

### 3.3 integration tests 的“骨架化/注释化”（中优先级）

现状：

-   已配置 `integration` marker
-   某些集成测试用例可能处于注释骨架状态（需要维护者决策与补齐 fixtures 管理）

建议：

-   先把 Tier C 测试变成“可跑但默认 skip”：
    -   fixture 不存在 -> skip
    -   extra 不存在 -> skip
-   在 README 或 Runbook 加一段“如何准备 fixtures + 如何跑 integration”

------

### 3.4 docker-compose 的 `unstructured-api:latest` 漂移风险（中优先级）

现状：

-   `docker-compose.uns-api.yml` 使用 `downloads.unstructured.io/.../unstructured-api:latest`

风险：

-   上游镜像更新可能导致行为变化（接口细节/解析输出/依赖变化），从而使回归难定位

建议：

-   把镜像 tag pin 到具体版本（并在升级时做一次“输出差异审计”）
-   或者提供一份“推荐版本矩阵”（Zephyr pipeline_version -> unstructured-api version）

------

### 3.5 mypy `ignore_missing_imports=true` 的长期收紧路线（中优先级）

现状：

-   为了早期推进速度，mypy 允许缺失导入类型

建议：

-   逐步对关键包关闭 ignore_missing_imports（按包/按模块）
-   依赖缺 stub 的：优先补 `typings/`，避免在业务逻辑里散落 ignore

------

### 3.6 Windows 换行符 / 工具链一致性（低中优先级）

现状：

-   `.gitattributes` 强制文本 LF
-   pre-commit 的 mixed-line-ending hook 目前是注释状态（可选）

建议：

-   保持 `.gitattributes` 为主
-   如遇到频繁换行冲突，再考虑启用 mixed-line-ending hook（但注意会增加 pre-commit 噪声）

------

## 4) Maintainer Checklist（每次改动前后的自检）

### 4.1 改 contracts / artifacts / payload

-   必须先读 `docs/CONTRACTS_INDEX.md`
-   必须补 contract test（Tier B）
-   必须确认下游 destination 不会因 key/shape 漂移而破

### 4.2 改工具链 / CI

-   `make check` 与 `make test` 必须在本地过
-   CI action 依赖更新必须 pin SHA
-   Python 版本变化必须联动 `.python-version` + `uv.lock`

### 4.3 加新 destination / backend

-   先写 Protocol/contract，再写实现
-   先写 unit tests（Dummy/MockTransport），再接真实外部系统
-   外部系统集成测试放 Tier C（默认 skip）
