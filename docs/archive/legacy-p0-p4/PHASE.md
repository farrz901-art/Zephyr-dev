# Phase简介

## Phase0

确定 Python 版本基线

uv workspace 稳定

建立质量门禁入口

CI稳定

Ruff/pyright/mypy 规范

Makefile 统一入口

## Phase1
### Phase1 Done 的定义：

packages/uns-stream 已建立，并对外暴露稳定接口树，例如：
uns_stream.partition.pdf.partition_pdf
uns_stream.partition.docx.partition_docx
uns_stream.partition.auto.partition（你的“路由总入口”，但不直接调用 unstructured.partition.auto，避免 Windows/libmagic 干扰）
zephyr-core 中存在 Zephyr Contract v1（轻量但稳定）：ZephyrElement / PartitionResult / Strategy / EngineInfo / ErrorCode。
uns-stream 默认 backend 是 local unstructured（in-process），可在 PyCharm 里断点进 unstructured.* 调试。
CI（make check/test）继续绿；并且测试只覆盖 不需要额外依赖的格式（txt/html/xml/email 等），避免你在 Windows/CI 因 pdf/docx 系统依赖卡死。Unstructured 官方 quickstart 也明确：.txt/.html/.xml/.eml/.msg/.p7s 这些类型不需要额外依赖；其它类型需要安装对应 extras（如 all-docs 等）。


### Phase1细分阶段
Phase1-1：先落地 Zephyr Contract v1（zephyr-core）

Phase1-2：创建 packages/uns-stream（“中量胶水实体层”）

Phase1-3：铺出 uns_stream.partition.* 模块树（先 stub，再逐个实现）

Phase1-4：测试策略（保证 CI 绿，同时不被 pdf/docx 依赖拖死）

Phase1-5：把 uns-stream 接入你现有的 monorepo 工具链



### P1 工程总结：已经建成了什么（非常关键）
以现在 master 的核心文件为证据链：

1.已经形成了“稳定产物三件套”的平台写入点
uns_stream/_internal/artifacts.py 已经把落盘逻辑集中为 dump_partition_artifacts(...)，并且无论成功失败都会写 run_meta.json；成功时额外写 elements.json 和 normalized.txt。
这一步意味着：未来 zephyr-api/zephyr-ingest 都可以复用这一个 writer，而不是复制脚本逻辑。

2.已经有了 run_meta 的强契约（但还可以再加强）
zephyr_core/contracts/v1/run_meta.py 已经定义了 RunMetaV1、EngineMetaV1、MetricsV1、ErrorInfoV1，并且用手写 to_dict() 固化 JSON shape，避免序列化漂移。
当前注意点：
warnings 仍是 field(default_factory=lambda: [])（为了 pyright strict 的 Unknown 推断，这个可以接受，但更推荐用“具名 typed factory 函数”来取代 lambda，后续更易维护）。
目前 RunMetaV1 还没有 schema_version 字段（建议在 P2 第一周补上，方便演进）。

3.已经建立了“故障分诊中心”并且严格考虑了 pyright strict
uns_stream/_internal/retry_policy.py 的 is_retryable_exception(e) 已经把：
ZephyrError 的 code 分支（缺依赖/不支持类型不可重试）
details.retryable 显式覆盖
Timeout/Connection/socket 临时性错误可重试
rate limit/too many requests 文本启发式
集中在一个地方，非常“平台化”。



## Phase2
### P2 里程碑总览
P2-M1（必做）：zephyr-ingest v0（本地目录 → uns-stream → artifacts → 文件系统输出）
P2-M2：批处理治理（skip-existing/force、汇总 batch_report、失败分类、重试策略）
P2-M3：并发与吞吐（workers、队列、资源限制、可观测指标）
P2-M4：目的地插件化（Kafka/Weaviate/DB/Webhook 先实现 1 个）
P2-M5（可选）：第二后端（HTTP unstructured-api backend）与容器化部署
P2-M6： 扩展destinations连接器


### P2 阶段性任务
#### P2-M1：zephyr-ingest v0（强主线，优先做）
目标：一条命令把一个目录的文件批量处理成 <out>/<sha256>/{run_meta.json,elements.json,normalized.txt}

任务拆解

P2-1.1 定义最小输入对象 DocumentRef（建议放 zephyr-core）

字段：path, source, discovered_at_utc, （可选）mime_guess
验收：sources 输出的是 Iterator[DocumentRef] 而不是裸 Path（为未来 ITstream/事件总线留接口）
P2-1.2 zephyr_ingest/sources/local_file.py

支持：path（目录/单文件）、glob、递归、文件大小过滤（可选）
验收：能稳定枚举文件，且输出顺序可控（按 path 排序，便于可复现）
P2-1.3 zephyr_ingest/runner.py（核心执行器）

逻辑：对每个 DocumentRef
生成 RunContext.new(...)（run_id/pipeline_version/timestamp）
调 uns_stream.partition.auto.partition(...)（支持 strategy 参数）
构造 RunMetaV1（成功/失败都要）
调 dump_partition_artifacts(...) 写三件套
验收：坏文件不会中断批次；每个文件都有输出目录（至少 run_meta）
P2-1.4 zephyr_ingest/cli.py（建议先 CLI，不先 API）

用法建议：
uv run --package zephyr-ingest zephyr-ingest run --path ./inbox --out ./out --strategy auto --workers 1
验收：你不用写一行 Python，就能跑批处理
P2-1.5 tests

test_local_file_source_enumeration
test_runner_writes_artifacts(tmp_path)：用 txt/html/xml fixtures（不要求 pdf）
验收：CI 绿 + 本地可复现
为什么 P2 先做 ingest，而不是先做 zephyr-api：因为你现在要的是“上游搬运能力”。API 是控制面，批处理 runner 才是物流中心的“生产线”。


#### P2-M2：批处理治理（幂等、断点续跑、报表）


#### P2-M3：并发与吞吐（先本地并发，不上分布式）


#### P2-M4：目的地插件化（先定义接口，再实现 1 个）


#### P2-M5：可选（后期）：HTTP backend（unstructured-api）与容器化


#### P2‑M6 定义（我们要交付什么）


#### P2-M6 的阶段性任务：
P2‑M6‑01（Commit A）：固定投递 payload 契约

P2‑M6‑02（Commit B/C）：KafkaDestination

P2‑M6‑03（Commit D/E）：WeaviateDestination（面向 RAG 下游）

P2‑M6‑04（可选）：Delivery 统计扩展（你已经有 P2‑M4‑04）

P2-M6优化


# p3
P3-M0：编排内核流无关化

P3-M1：运行形态标准化（Worker/Service）

P3-M2：队列与任务模型

P3-M3：分布式幂等与锁

P3-M4：Backpressure 与高并发投递

P3-M5：it-stream 最小真实闭环（Airbyte Protocol path）

P3-M6：it-stream 的 State / Checkpoint 治理

P3-M7：可运营性升级（任务治理 + 运维工具 + 指标）

P3-M8：第二真实 backend 验证点（queue 或 lock 至少再落一个）

P3-M9：统一执行链收束（runner 世界 × worker 世界）

P3-M10：P3 收官与 P4 预备

P4 的真实目标确定：

connector 扩展

第二 backend 深化

企业级运营面增强

还是 API/service 化进一步深入

正式版 P4 总纲
P4：规模化铺路与平台深化阶段
总目标
在不破坏 P3 已经收束好的边界前提下，把 Zephyr 从：

双 flow 已成立

主执行链已统一

queue / lock / governance / provenance 已成形

第二 backend 已验证

推进到：

connector 扩展模式被验证

it-stream 治理更成熟

queue / lock / operator 面更稳

delivery 语义更适合大规模扩展

为 P5 真生产化落地 做完关键铺路

P4 的阶段定位
P4 不是最终生产化阶段。
P4 是：

为了 P5 真生产化而进行的超长铺路阶段。

P4-M1：source connector 扩展模式固化
目标：

明确 uns-stream 与 it-stream 各自适合扩什么 source

建立 source connector onboarding 原则

先定模式，不先拼数量

P4-M2：destination connector 扩展模式固化
目标：

建立 destination onboarding 原则

明确所有新 destination 必须复用现有 hardened delivery semantics

把 receipt / retryable / failure_kind / idempotency / metrics / provenance 规则钉死

P4-M3：首批 connector 扩展示范
目标：

首批增加少量 source / destination

同时覆盖 uns 与 it

验证 connector onboarding 模式真的可行

第二段：it-stream 治理深化
P4-M4：checkpoint lineage / compatibility
目标：

lineage

compatibility checks

version-aware behavior

P4-M5：richer progress shape
目标：

让 it 的 progress / checkpoint 语义更丰富

但不复制 Airbyte 全栈

P4-M6：resume provenance / recovery semantics 深化
目标：

让 resume / recovery 更可治理、更可审计

P4-M7：it-stream focused integration & corruption tests
目标：

corrupted checkpoint

incompatible version

partial write / partial recovery

resume provenance 边界测试

第三段：queue / lock / governance / operator 面深化
P4-M8：second backend parity 补齐
目标：

第二 queue backend / 第二 lock backend 在 inspection / recovery / metrics / provenance 上补对齐

P4-M9：lock 语义增强
目标：

stale / owner / lease 等语义更清楚

不做完整 distributed coordination platform

P4-M10：task governance 升级


P4-M11：operator-facing surfaces 深化


第四段：delivery / destination 继续生产化铺路
P4-M12：cross-destination 稳定语义收束

P4-M13：Cloud Object + warehouse destinations
目标：先补两个最值钱且和现有五类明显不同的 destination 类型。

P4-M13-01
批次定板与 contract pressure mapping
明确这两个 destination 对 shared delivery contract 的压力点：

object store 的 object/key/write outcome 语义

warehouse 的 load/upsert/batch outcome 语义

P4-M13-02
实现 Cloud Object destination

P4-M13-03
实现 warehouse destination

P4-M13-04
focused validation + integration tests

P4-M14：search + NoSQL + observability destinations
目标：把另外三类补齐。

P4-M14-01
批次定板与 contract pressure mapping

P4-M14-02
实现 search destination

P4-M14-03
实现 NoSQL destination

P4-M14-04
实现 observability destination
这里更像：

structured event sink

audit/telemetry sink

operator-facing external event/output

P4-M15：cross-destination batch validation
目标：把 P4-M13 / M14 这一批 destination 一起验证。

P4-M15-01
shared retryability / failure vocabulary batch validation

P4-M15-02
details/summary / operator-facing consistency validation

P4-M15-03
replay/idempotency/provenance validation

P4-M15-04
focused integration locking

P4-M16：destination breadth 收口
目标：确认非企业级 destination 第一轮 breadth 已经够用。

P4-M16-01
destination support matrix / supported subset formalization

P4-M16-02
unsupported gaps / provider-local semantics clarification

P4-M16-03
focused tests / architecture lock

P4-M16-04
P4 destination breadth closeout

P4-M17：it-stream source breadth 第二轮
目标：开始批量补 it source。

建议类型：

Database

Warehouse

Stream

NoSQL

P4-M17-01
批次定板与 placement/pressure mapping

P4-M17-02
实现 Database source

P4-M17-03
实现 Warehouse source

P4-M17-04
focused validation / tests

P4-M18：it-stream source breadth 第三轮
P4-M18-01
实现 Stream source

P4-M18-02
实现 NoSQL source

P4-M18-03
shared progress/checkpoint/resume validation

P4-M18-04
focused integration locking

P4-M19：uns-stream source breadth 第二轮
目标：补 uns 那边真正缺的文档型 source。

建议类型：

Object Store / Cloud document source

Git source

P4-M19-01
批次定板与 placement/pressure mapping

P4-M19-02
实现 Object Store / Cloud doc source

P4-M19-03
实现 Git source

P4-M19-04
focused validation / tests

P4-M20：uns-stream source breadth 第三轮
建议类型：

SaaS Docs

视情况补一个 document-oriented cloud source 变体

P4-M20-01
实现 SaaS Docs source

P4-M20-02
实现另一个高价值文档型 source 或统一接入层加厚

P4-M20-03
shared document acquisition / provenance / partition entry validation

P4-M20-04
focused integration locking

P4-M21：connector batch governance validation
目标：不是再加 connector，而是检查这么一批 connector 扩张后：

governance 没漂

operator surface 没坏

shared contracts 还站得住

P4-M21-01
source governance validation

P4-M21-02
destination governance validation

P4-M21-03
operator-facing surface validation

P4-M21-04
focused anti-drift locking

P4-M22：connector support matrix / supported subset formalization
目标：把 P4 的 breadth 形成正式支持面。

P4-M22-01
source support matrix

P4-M22-02
destination support matrix

P4-M22-03
unsupported / experimental / future work 边界 formalization

P4-M22-04
docs/contracts/tests 收口

P4-M23：P5 productionization preparation I
P4-M23-01
deployment/config shape preparation

P4-M23-02
runtime shape / concurrency assumptions preparation

P4-M23-03
operator/metrics/provenance preparation for production paths

P4-M23-04
focused validation

P4-M24：P5 productionization preparation II / P4 closeout
P4-M24-01
bench/scale/SLI candidate preparation

P4-M24-02
failure drill entry points preparation

P4-M24-03
P4 closeout review

P4-M24-04
P5 handoff matrix
