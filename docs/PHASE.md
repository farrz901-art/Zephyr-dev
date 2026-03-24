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
P2-2.1 幂等策略

--skip-existing：如果 <out>/<sha>/run_meta.json 存在就跳过
--force：重新跑（run_id 不同）
验收：跑两次同目录，第二次明显更快（skip 生效）
P2-2.2 batch_report.json

统计：总数、成功、失败、失败可重试数、按 kind 分布、耗时分布（P50/P95）
验收：batch_report 可直接作为 BI 输入


#### P2-M3：并发与吞吐（先本地并发，不上分布式）
参数：--workers N
实现：concurrent.futures.ProcessPoolExecutor 或 ThreadPool（视你的实际瓶颈；OCR/hi_res 倾向进程）
验收：N=1 与 N=4 输出结构一致；失败不会堵塞队列


#### P2-M4：目的地插件化（先定义接口，再实现 1 个）
destinations/base.py：Destination.write(run_meta, elements, normalized_text, blobs?)
destinations/filesystem.py：复用 artifacts writer（默认）
选一个先实现：
kafka（发布 run_meta + elements 摘要）
weaviate（写入 normalized_text + metadata，embedding 暂留空）
postgres（只存 run_meta 索引，不存大文本）


#### P2-M5：可选（后期）：HTTP backend（unstructured-api）与容器化
你现在是本地 in-process，很适合开发调试；当你要部署/扩展时再加：

HttpUnstructuredBackend：与 LocalUnstructuredBackend 实现同一接口
由 config 切换 backend（不改调用方）


#### P2‑M6 定义（我们要交付什么）
P2‑M1/M2/M3 让 Zephyr 具备了“批处理生产线（ingest）+ 并发 + 治理（retry/统计）”，P2‑M4/M5 让它具备了“交付（filesystem/webhook）+ DLQ/replay + 后端切换（local/uns-api）+ 工程门禁”。

P2‑M6 的核心是：让 Zephyr 真的成为“物流集散中心”的下游供水口——在保持现有 filesystem/webhook 的同时，新增 1–2 个“企业常用目的地”插件，并把“投递 payload 契约”固定下来，避免后面越加越乱。


#### P2-M6 的阶段性任务：
P2‑M6‑01（Commit A）：固定投递 payload 契约（强烈建议先做）
新增：packages/zephyr-ingest/src/zephyr_ingest/_internal/delivery_payload.py

DeliveryPayloadV1 dataclass（或 TypedDict）字段建议：
sha256: str
run_meta: dict（来自 RunMetaV1.to_dict()）
artifacts: {out_dir, run_meta_path, elements_path, normalized_path}
delivery: {attempt, destination, timestamp_utc}（可选）
提供 build_delivery_payload(out_root, sha256, meta) -> DeliveryPayloadV1
验收：加一个单测，保证 payload JSON 可序列化且 keys 稳定。

这样 Kafka/Webhook/Weaviate 都消费同一个 payload，不会出现“每个 destination 发一套格式”。


P2‑M6‑02（Commit B/C）：KafkaDestination（推荐先做）
设计选择（Windows 友好）
为避免 Windows 本地编译/依赖地狱，我建议：

先用 纯 Python 的客户端（或你自己写一个最小 Producer Protocol），并把 Kafka client 做成 可注入，单测不连 Kafka。
真实生产环境你再换 confluent-kafka（可做成 optional extra）。
实现拆分
B1：新增 destination

zephyr_ingest/destinations/kafka.py
KafkaProducer Protocol（只定义 produce(topic, key, value) / flush()）
KafkaDestination.__call__：
payload = build_delivery_payload(...)
key = f"{sha256}:{meta.run_id}"
value = json.dumps(payload)
produce → receipt
B2：CLI 接入（fanout）

--kafka-brokers / --kafka-topic
若传了 kafka 参数：dest = FanoutDestination(destinations=(fs, wh?, kafka))
B3：测试

fake producer 记录 produce 参数
断言 topic/key/value 形状正确；receipt.ok True/False 分支可测
P2‑M6‑03（Commit D/E）：WeaviateDestination（面向 RAG 下游）
最小可交付模式（不强依赖 embedding）
Weaviate 里先存：

sha256、run_id、pipeline_version、timestamp_utc
normalized_text（可选：长度上限）
run_meta（JSON string 或结构字段） 向量化可交给 Weaviate 自己的 module（外部配置），你这边不在 P2‑M6 强行引入 embedding 模型。
实现拆分
D1：新增 destination

zephyr_ingest/destinations/weaviate.py
采用“可注入 client/transport”以便测试（类似你 webhook 的 transport 注入）
写入成功返回 receipt.ok True；失败写 details（status/exception）
D2：CLI 接入（fanout）

--weaviate-url、--weaviate-api-key（可选）
同样加入 fanout
D3：测试

用 mock client/transport 验证 payload 被发送，保证 strict 下无 Unknown
P2‑M6‑04（可选）：Delivery 统计扩展（你已经有 P2‑M4‑04）
batch_report 增加 destination 的延迟统计（delivery_duration_ms）
将 DLQ/replay 也扩展到 Kafka/Weaviate（可先只扩 webhook）



## 目前阶段  P2-M6-02（待做）
