from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Protocol, cast

from zephyr_ingest.destinations.weaviate import WeaviateCollectionProtocol


class WeaviateClientProtocol(Protocol):
    def close(self) -> None: ...
    @property
    def collections(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class WeaviateConnectParams:
    http_host: str
    http_port: int
    http_secure: bool
    grpc_host: str
    grpc_port: int
    grpc_secure: bool
    api_key: str | None
    skip_init_checks: bool


def connect_weaviate_and_get_collection(
    *,
    params: WeaviateConnectParams,
    collection_name: str,
) -> tuple[WeaviateClientProtocol, WeaviateCollectionProtocol]:
    """
    Connect to Weaviate via weaviate-client v4 and return (client, collection).

    - Uses weaviate.connect_to_custom(...) with http/grpc params.
    - Uses Auth.api_key(...) if api_key is provided.
    - Returns a collection implementing WeaviateCollectionProtocol (for batch.dynamic()).

    Raises:
        ImportError: if weaviate-client is not installed
        RuntimeError: if expected attributes are missing
    """
    try:
        weaviate_mod = importlib.import_module("weaviate")
        init_mod = importlib.import_module("weaviate.classes.init")
    except ModuleNotFoundError as e:
        raise ImportError(
            "weaviate-client is not installed. Install with: uv sync --extra weaviate "
            "or uv pip install 'zephyr-ingest[weaviate]'"
        ) from e

    connect_to_custom = getattr(weaviate_mod, "connect_to_custom", None)
    if connect_to_custom is None or not callable(connect_to_custom):
        raise RuntimeError(
            "weaviate.connect_to_custom not found (unexpected weaviate-client install)"
        )

    Auth = getattr(init_mod, "Auth", None)
    if Auth is None:
        raise RuntimeError(
            "weaviate.classes.init.Auth not found (unexpected weaviate-client install)"
        )

    auth_credentials: Any = None
    if params.api_key is not None:
        auth_credentials = Auth.api_key(params.api_key)

    # Official helper signature includes skip_init_checks. <!--citation:2-->
    client_any: Any = connect_to_custom(
        http_host=params.http_host,
        http_port=params.http_port,
        http_secure=params.http_secure,
        grpc_host=params.grpc_host,
        grpc_port=params.grpc_port,
        grpc_secure=params.grpc_secure,
        auth_credentials=auth_credentials,
        skip_init_checks=params.skip_init_checks,
    )

    # collections.get("MyCollection") is the commonly documented path. <!--citation:4-->
    collections_any: Any = getattr(client_any, "collections", None)
    if collections_any is None:
        # Ensure we don't leak an invalid client into the runner.
        try:
            close_fn = getattr(client_any, "close", None)
            if callable(close_fn):
                close_fn()
        finally:
            raise RuntimeError("Weaviate client has no .collections attribute")

    get_fn = getattr(collections_any, "get", None)
    use_fn = getattr(collections_any, "use", None)

    if callable(get_fn):
        collection_any: Any = get_fn(collection_name)
    elif callable(use_fn):
        collection_any = use_fn(collection_name)
    else:
        try:
            close_fn = getattr(client_any, "close", None)
            if callable(close_fn):
                close_fn()
        finally:
            raise RuntimeError("Weaviate client collections has neither .get nor .use")

    client = cast(WeaviateClientProtocol, client_any)
    collection = cast(WeaviateCollectionProtocol, collection_any)
    return client, collection
