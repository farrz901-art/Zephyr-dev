from __future__ import annotations

from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
    normalize_weaviate_delivery_object_id,
)


def test_delivery_idempotency_normalization_is_deterministic() -> None:
    identity = DeliveryIdentityV1(sha256="abc", run_id="r1")

    first = normalize_delivery_idempotency_key(identity=identity)
    second = normalize_delivery_idempotency_key(identity=identity)

    assert first == second
    assert first == "abc:r1"


def test_equivalent_deliveries_share_same_idempotency_key() -> None:
    left = DeliveryIdentityV1(sha256="abc", run_id="r1")
    right = DeliveryIdentityV1(sha256="abc", run_id="r1")

    assert normalize_delivery_idempotency_key(identity=left) == normalize_delivery_idempotency_key(
        identity=right
    )


def test_distinct_deliveries_produce_distinct_idempotency_keys() -> None:
    left = DeliveryIdentityV1(sha256="abc", run_id="r1")
    right = DeliveryIdentityV1(sha256="abc", run_id="r2")
    third = DeliveryIdentityV1(sha256="def", run_id="r1")

    assert normalize_delivery_idempotency_key(identity=left) != normalize_delivery_idempotency_key(
        identity=right
    )
    assert normalize_delivery_idempotency_key(identity=left) != normalize_delivery_idempotency_key(
        identity=third
    )


def test_weaviate_delivery_object_id_is_deterministic() -> None:
    first = normalize_weaviate_delivery_object_id(sha256="abc")
    second = normalize_weaviate_delivery_object_id(sha256="abc")
    third = normalize_weaviate_delivery_object_id(sha256="def")

    assert first == second
    assert first != third
