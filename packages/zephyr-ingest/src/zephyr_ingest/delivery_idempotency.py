from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DeliveryIdentityV1:
    sha256: str
    run_id: str


def normalize_delivery_idempotency_key(*, identity: DeliveryIdentityV1) -> str:
    return f"{identity.sha256}:{identity.run_id}"


def normalize_weaviate_delivery_object_id(*, sha256: str) -> str:
    # Align with weaviate.util.generate_uuid5(identifier, namespace=""):
    # uuid5(NAMESPACE_DNS, str(namespace)+str(identifier))
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, sha256))
