from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from zephyr_core.contracts.v2.spec import ConnectorSpecV1, SpecFieldV1
from zephyr_ingest.source_contracts import SOURCE_SPEC_ID_BY_CONTRACT_ID
from zephyr_ingest.spec.registry import get_spec, list_spec_ids


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _repo_relative(path: Path) -> str:
    return path.relative_to(_repo_root()).as_posix()


@dataclass(frozen=True, slots=True)
class ImplementationRef:
    module: str | None
    path: str | None


@dataclass(frozen=True, slots=True)
class ConnectorBlueprint:
    id: str
    family: str
    flow_kind: str
    implementation: ImplementationRef
    required_env: tuple[str, ...]
    optional_env: tuple[str, ...]
    python_dependencies: tuple[str, ...]
    service_readiness_kind: str
    endpoint_readback_kind: str
    implementation_status: str
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConnectorRecord:
    id: str
    family: str
    flow_kind: str
    implementation_module: str | None
    implementation_path: str | None
    implementation_status: str
    registry_status: str
    spec_fields_status: str
    severity: str
    required_env: list[str]
    optional_env: list[str]
    python_dependencies: list[str]
    service_readiness_kind: str
    endpoint_readback_kind: str
    notes: list[str]


SOURCE_BLUEPRINTS: tuple[ConnectorBlueprint, ...] = (
    ConnectorBlueprint(
        id="source.uns.http_document.v1",
        family="source",
        flow_kind="uns",
        implementation=ImplementationRef(
            module="uns_stream.sources.http_source",
            path="packages/uns-stream/src/uns_stream/sources/http_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx", "unstructured"),
        service_readiness_kind="http_document_endpoint_ready",
        endpoint_readback_kind="http_body_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.uns.s3_document.v1",
        family="source",
        flow_kind="uns",
        implementation=ImplementationRef(
            module="uns_stream.sources.s3_source",
            path="packages/uns-stream/src/uns_stream/sources/s3_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("boto3", "botocore", "unstructured"),
        service_readiness_kind="s3_object_ready",
        endpoint_readback_kind="s3_object_body_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.uns.git_document.v1",
        family="source",
        flow_kind="uns",
        implementation=ImplementationRef(
            module="uns_stream.sources.git_source",
            path="packages/uns-stream/src/uns_stream/sources/git_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("unstructured",),
        service_readiness_kind="git_repo_and_commit_ready",
        endpoint_readback_kind="git_blob_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Requires git executable on PATH in addition to Python package dependencies.",
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.uns.google_drive_document.v1",
        family="source",
        flow_kind="uns",
        implementation=ImplementationRef(
            module="uns_stream.sources.google_drive_source",
            path="packages/uns-stream/src/uns_stream/sources/google_drive_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx", "unstructured"),
        service_readiness_kind="google_drive_file_ready",
        endpoint_readback_kind="download_or_export_body_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.uns.confluence_document.v1",
        family="source",
        flow_kind="uns",
        implementation=ImplementationRef(
            module="uns_stream.sources.confluence_source",
            path="packages/uns-stream/src/uns_stream/sources/confluence_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx", "unstructured"),
        service_readiness_kind="confluence_page_ready",
        endpoint_readback_kind="page_body_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.it.http_json_cursor.v1",
        family="source",
        flow_kind="it",
        implementation=ImplementationRef(
            module="it_stream.sources.http_source",
            path="packages/it-stream/src/it_stream/sources/http_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx",),
        service_readiness_kind="http_json_endpoint_ready",
        endpoint_readback_kind="json_record_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.it.postgresql_incremental.v1",
        family="source",
        flow_kind="it",
        implementation=ImplementationRef(
            module="it_stream.sources.postgresql_source",
            path="packages/it-stream/src/it_stream/sources/postgresql_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("psycopg",),
        service_readiness_kind="postgresql_table_ready",
        endpoint_readback_kind="sql_row_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.it.clickhouse_incremental.v1",
        family="source",
        flow_kind="it",
        implementation=ImplementationRef(
            module="it_stream.sources.clickhouse_source",
            path="packages/it-stream/src/it_stream/sources/clickhouse_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("clickhouse-connect",),
        service_readiness_kind="clickhouse_table_ready",
        endpoint_readback_kind="sql_row_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.it.kafka_partition_offset.v1",
        family="source",
        flow_kind="it",
        implementation=ImplementationRef(
            module="it_stream.sources.kafka_source",
            path="packages/it-stream/src/it_stream/sources/kafka_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("confluent-kafka",),
        service_readiness_kind="kafka_topic_partition_ready",
        endpoint_readback_kind="consumer_payload_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
    ConnectorBlueprint(
        id="source.it.mongodb_incremental.v1",
        family="source",
        flow_kind="it",
        implementation=ImplementationRef(
            module="it_stream.sources.mongodb_source",
            path="packages/it-stream/src/it_stream/sources/mongodb_source.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("pymongo",),
        service_readiness_kind="mongodb_collection_ready",
        endpoint_readback_kind="document_query_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Source spec exists in zephyr_ingest.spec.registry but fields are currently empty.",
        ),
    ),
)


DESTINATION_BLUEPRINTS: tuple[ConnectorBlueprint, ...] = (
    ConnectorBlueprint(
        id="filesystem",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.filesystem",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/filesystem.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=(),
        service_readiness_kind="local_filesystem_writable",
        endpoint_readback_kind="file_artifact_readback",
        implementation_status="implemented_without_spec_registry",
        notes=(
            "Filesystem destination is implemented directly and is not exposed "
            "through zephyr_ingest.spec.registry.",
        ),
    ),
    ConnectorBlueprint(
        id="destination.webhook.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.webhook",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/webhook.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx",),
        service_readiness_kind="http_post_endpoint_ready",
        endpoint_readback_kind="request_echo_or_http_readback",
        implementation_status="implemented_and_registered",
    ),
    ConnectorBlueprint(
        id="destination.kafka.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.kafka",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/kafka.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("confluent-kafka",),
        service_readiness_kind="kafka_topic_ready",
        endpoint_readback_kind="consumer_payload_readback",
        implementation_status="implemented_and_registered",
    ),
    ConnectorBlueprint(
        id="destination.weaviate.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.weaviate",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/weaviate.py",
        ),
        required_env=(),
        optional_env=("ZEPHYR_WEAVIATE_API_KEY", "WEAVIATE_API_KEY"),
        python_dependencies=("weaviate-client",),
        service_readiness_kind="weaviate_collection_ready",
        endpoint_readback_kind="collection_object_query_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Implementation accepts an injected collection protocol; the real "
            "client dependency lives outside the destination class.",
        ),
    ),
    ConnectorBlueprint(
        id="destination.s3.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.s3",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/s3.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("boto3",),
        service_readiness_kind="s3_bucket_ready",
        endpoint_readback_kind="s3_object_body_readback",
        implementation_status="implemented_and_registered",
    ),
    ConnectorBlueprint(
        id="destination.opensearch.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.opensearch",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/opensearch.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx",),
        service_readiness_kind="opensearch_index_ready",
        endpoint_readback_kind="search_document_readback",
        implementation_status="implemented_and_registered",
    ),
    ConnectorBlueprint(
        id="destination.clickhouse.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.clickhouse",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/clickhouse.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx",),
        service_readiness_kind="clickhouse_table_ready",
        endpoint_readback_kind="sql_row_readback",
        implementation_status="implemented_and_registered",
    ),
    ConnectorBlueprint(
        id="destination.mongodb.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.mongodb",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/mongodb.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("pymongo",),
        service_readiness_kind="mongodb_collection_ready",
        endpoint_readback_kind="document_query_readback",
        implementation_status="implemented_and_registered",
    ),
    ConnectorBlueprint(
        id="destination.loki.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.loki",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/loki.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=("httpx",),
        service_readiness_kind="loki_push_endpoint_ready",
        endpoint_readback_kind="log_query_readback",
        implementation_status="implemented_and_registered",
    ),
    ConnectorBlueprint(
        id="destination.sqlite.v1",
        family="destination",
        flow_kind="delivery",
        implementation=ImplementationRef(
            module="zephyr_ingest.destinations.sqlite",
            path="packages/zephyr-ingest/src/zephyr_ingest/destinations/sqlite.py",
        ),
        required_env=(),
        optional_env=(),
        python_dependencies=(),
        service_readiness_kind="sqlite_db_path_ready",
        endpoint_readback_kind="sqlite_row_readback",
        implementation_status="missing_or_unregistered",
        notes=(
            "SQLite destination implementation exists, but destination.sqlite.v1 "
            "is not present in zephyr_ingest.spec.registry or spec-driven CLI "
            "lists at this commit.",
        ),
    ),
)


BACKEND_BLUEPRINTS: tuple[ConnectorBlueprint, ...] = (
    ConnectorBlueprint(
        id="backend.uns_api.v1",
        family="backend",
        flow_kind="backend",
        implementation=ImplementationRef(
            module="uns_stream.backends.http_uns_api",
            path="packages/uns-stream/src/uns_stream/backends/http_uns_api.py",
        ),
        required_env=("ZEPHYR_UNS_API_KEY", "UNS_API_KEY", "UNSTRUCTURED_API_KEY"),
        optional_env=(),
        python_dependencies=("httpx",),
        service_readiness_kind="http_partition_api_ready",
        endpoint_readback_kind="partition_response_readback",
        implementation_status="implemented_and_registered",
        notes=(
            "Cataloged as backend only; it must not be counted as a destination.",
        ),
    ),
)


def _connector_blueprints() -> tuple[ConnectorBlueprint, ...]:
    return SOURCE_BLUEPRINTS + DESTINATION_BLUEPRINTS + BACKEND_BLUEPRINTS


def _spec_map() -> dict[str, ConnectorSpecV1]:
    spec_ids = list_spec_ids()
    mapping: dict[str, ConnectorSpecV1] = {}
    for spec_id in spec_ids:
        spec = get_spec(spec_id=spec_id)
        if spec is not None:
            mapping[spec_id] = spec
    return mapping


def _env_names(fields: list[SpecFieldV1], *, required: bool) -> list[str]:
    names: list[str] = []
    for field in fields:
        if field["required"] is required and "env_names" in field:
            for env_name in field["env_names"]:
                if env_name not in names:
                    names.append(env_name)
    return names


def _source_contract_id_for_spec_id(spec_id: str) -> str | None:
    for contract_id, current_spec_id in SOURCE_SPEC_ID_BY_CONTRACT_ID.items():
        if current_spec_id == spec_id:
            return contract_id
    return None


def _registry_status(blueprint: ConnectorBlueprint, spec: ConnectorSpecV1 | None) -> str:
    if blueprint.id == "filesystem":
        return "not_in_connector_spec_registry"
    if blueprint.id == "destination.sqlite.v1":
        return "missing_from_connector_spec_registry"
    if spec is None:
        return "missing_from_connector_spec_registry"
    return "registered_in_connector_spec_registry"


def _spec_fields_status(blueprint: ConnectorBlueprint, spec: ConnectorSpecV1 | None) -> str:
    if spec is None:
        return "missing"
    if blueprint.family == "source" and not spec["fields"]:
        return "empty"
    if not spec["fields"]:
        return "not_applicable_or_empty"
    return "present"


def _severity(
    blueprint: ConnectorBlueprint,
    spec_fields_status: str,
) -> str:
    if blueprint.id == "destination.sqlite.v1":
        return "major"
    if blueprint.family == "source" and spec_fields_status == "empty":
        return "major_for_source_ux"
    if blueprint.id == "filesystem":
        return "info"
    return "none"


def _notes(
    blueprint: ConnectorBlueprint,
    spec: ConnectorSpecV1 | None,
    spec_fields_status: str,
) -> list[str]:
    notes = list(blueprint.notes)
    if spec is not None and spec_fields_status == "present":
        notes.append(f"Spec registry field count: {len(spec['fields'])}.")
    if blueprint.id == "destination.sqlite.v1":
        notes.append(
            "Implementation is present, but the P5.1 catalog treats the "
            "connector as missing_or_unregistered because registry/spec wiring "
            "is absent."
        )
    if spec is None and blueprint.id != "destination.sqlite.v1":
        notes.append("No connector spec registry entry exists for this connector at this commit.")
    if spec_fields_status == "empty":
        contract_id = _source_contract_id_for_spec_id(blueprint.id)
        if contract_id is not None:
            notes.append(
                f"Source contract {contract_id} exists, but the matching source "
                "spec is a placeholder with zero fields."
            )
    return notes


def build_connector_catalog() -> dict[str, object]:
    spec_map = _spec_map()
    records: list[dict[str, object]] = []
    for blueprint in _connector_blueprints():
        spec = spec_map.get(blueprint.id)
        spec_fields_status = _spec_fields_status(blueprint=blueprint, spec=spec)
        required_env = list(blueprint.required_env)
        optional_env = list(blueprint.optional_env)
        if spec is not None:
            for env_name in _env_names(spec["fields"], required=True):
                if env_name not in required_env:
                    required_env.append(env_name)
            for env_name in _env_names(spec["fields"], required=False):
                if env_name not in required_env and env_name not in optional_env:
                    optional_env.append(env_name)
        record = ConnectorRecord(
            id=blueprint.id,
            family=blueprint.family,
            flow_kind=blueprint.flow_kind,
            implementation_module=blueprint.implementation.module,
            implementation_path=blueprint.implementation.path,
            implementation_status=blueprint.implementation_status,
            registry_status=_registry_status(blueprint=blueprint, spec=spec),
            spec_fields_status=spec_fields_status,
            severity=_severity(blueprint=blueprint, spec_fields_status=spec_fields_status),
            required_env=required_env,
            optional_env=optional_env,
            python_dependencies=list(blueprint.python_dependencies),
            service_readiness_kind=blueprint.service_readiness_kind,
            endpoint_readback_kind=blueprint.endpoint_readback_kind,
            notes=_notes(blueprint=blueprint, spec=spec, spec_fields_status=spec_fields_status),
        )
        records.append(asdict(record))

    return {
        "catalog_id": "zephyr.p5.1.connector_catalog.v1",
        "repo_root": _repo_root().as_posix(),
        "generated_from": _repo_relative(Path(__file__)),
        "summary": {
            "source_count": len(SOURCE_BLUEPRINTS),
            "destination_count": len(DESTINATION_BLUEPRINTS),
            "backend_count": len(BACKEND_BLUEPRINTS),
            "backend_ids_not_counted_as_destinations": ["backend.uns_api.v1"],
        },
        "connectors": records,
    }


def _markdown_cell_list(items: list[str]) -> str:
    if not items:
        return "-"
    return "<br>".join(items)


def render_markdown(catalog: dict[str, object]) -> str:
    connectors = catalog["connectors"]
    assert isinstance(connectors, list)
    lines = [
        "# Zephyr P5.1 Connector Catalog",
        "",
        "| id | family | flow_kind | implementation_status | severity | "
        "required_env | optional_env | python_dependencies | "
        "service_readiness_kind | endpoint_readback_kind | notes |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in connectors:
        assert isinstance(item, dict)
        typed_item = item
        notes_obj = typed_item.get("notes", [])
        required_env_obj = typed_item.get("required_env", [])
        optional_env_obj = typed_item.get("optional_env", [])
        dependencies_obj = typed_item.get("python_dependencies", [])
        notes = notes_obj if isinstance(notes_obj, list) else []
        required_env = required_env_obj if isinstance(required_env_obj, list) else []
        optional_env = optional_env_obj if isinstance(optional_env_obj, list) else []
        dependencies = dependencies_obj if isinstance(dependencies_obj, list) else []
        lines.append(
            "| "
            + " | ".join(
                [
                    str(typed_item["id"]),
                    str(typed_item["family"]),
                    str(typed_item["flow_kind"]),
                    str(typed_item["implementation_status"]),
                    str(typed_item["severity"]),
                    _markdown_cell_list([str(value) for value in required_env]),
                    _markdown_cell_list([str(value) for value in optional_env]),
                    _markdown_cell_list([str(value) for value in dependencies]),
                    str(typed_item["service_readiness_kind"]),
                    str(typed_item["endpoint_readback_kind"]),
                    _markdown_cell_list([str(value) for value in notes]),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _emit(*, content: str, out_path: Path | None) -> None:
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content, encoding="utf-8")
        return
    print(content, end="")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="p5_connector_catalog")
    parser.add_argument("--out", type=Path, default=None, help="Write output to a file path.")
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Render markdown instead of JSON.",
    )
    args = parser.parse_args(argv)

    catalog = build_connector_catalog()
    if args.markdown:
        content = render_markdown(catalog)
    else:
        content = json.dumps(catalog, ensure_ascii=False, indent=2) + "\n"
    _emit(content=content, out_path=args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
