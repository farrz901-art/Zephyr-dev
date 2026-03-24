# 1.（项目宪法/核心理念）
# Zephyr Project Charter (v1)

## Core positioning
Zephyr is an upstream “data logistics hub” for enterprises:
- ingest upstream data (documents / semi-structured / structured)
- clean + normalize + enrich metadata
- deliver stable, versioned artifacts to downstream systems (AI/BI/business systems)

Zephyr does NOT aim to re-implement Unstructured’s core algorithms. Zephyr’s value is:
- contracts (RunMeta/Artifacts/DeliveryPayload)
- governance (retryability, DLQ, replay)
- pluggable backends (local vs uns-api)
- pluggable destinations (filesystem/webhook/kafka/weaviate/...)

## Streams
- **uns-stream**: non-structured document pipeline, aligned with Unstructured capability boundaries.
- **it-stream** (future): IT structured/semi-structured movement aligned with Airbyte concepts; loosely coupled from uns-stream.
- OT-stream is currently out of scope.

## Non-negotiable engineering constraints
- Python >= 3.12
- pyright strict enabled (no “hand-wavy” typing)
- keyword-only Protocols for callables (avoid Callable positional mismatch)
- prefer typed dataclass wrappers for argparse Namespace
- avoid blanket ignores; resolve Unknown via isinstance+cast or typed factories
- all recommendations must be validated against a fixed commit SHA

## Current stable milestone
- Latest stable SHA: 49ffa9bec0c785c0dfc31c82ddf6319c0027f237
- Completed: Phase0/Phase1, Phase2 M1..M5, Phase2 M6-01 (DeliveryPayloadV1)
- Next: Phase2 M6-02 KafkaDestination, then M6-03 WeaviateDestination
