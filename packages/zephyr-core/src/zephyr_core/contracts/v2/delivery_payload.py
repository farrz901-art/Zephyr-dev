from __future__ import annotations

from typing import Any, Literal, TypedDict

DELIVERY_PAYLOAD_SCHEMA_VERSION: Literal[1] = 1


class ArtifactsPathsV1(TypedDict):
    out_dir: str
    run_meta_path: str
    elements_path: str
    normalized_path: str


class DeliveryPayloadV1(TypedDict):
    """
    Stable delivery payload sent to destinations (webhook/kafka/weaviate...).

    Notes:
    - run_meta is a JSON-serializable dict (from RunMetaV1.to_dict()).
    - artifacts are absolute paths for convenience in replay and indexing.
    """

    schema_version: Literal[1]
    sha256: str
    run_meta: dict[str, Any]
    artifacts: ArtifactsPathsV1
