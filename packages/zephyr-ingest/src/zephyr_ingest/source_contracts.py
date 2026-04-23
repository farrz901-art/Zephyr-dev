from __future__ import annotations

from typing import Final, Literal

SourceFlowFamily = Literal["uns", "it"]

UNS_SOURCE_CONTRACT_BY_ALIAS: Final[dict[str, str]] = {
    "http": "http_document_v1",
    "http_document": "http_document_v1",
    "http_document_v1": "http_document_v1",
    "url": "http_document_v1",
    "s3": "s3_document_v1",
    "s3_document": "s3_document_v1",
    "s3_document_v1": "s3_document_v1",
    "git": "git_document_v1",
    "git_document": "git_document_v1",
    "git_document_v1": "git_document_v1",
    "google_drive": "google_drive_document_v1",
    "google_drive_document": "google_drive_document_v1",
    "google_drive_document_v1": "google_drive_document_v1",
    "confluence": "confluence_document_v1",
    "confluence_document": "confluence_document_v1",
    "confluence_document_v1": "confluence_document_v1",
}

IT_SOURCE_CONTRACT_BY_ALIAS: Final[dict[str, str]] = {
    "http_json_cursor": "http_json_cursor_v1",
    "http_json_cursor_v1": "http_json_cursor_v1",
    "postgresql": "postgresql_incremental_v1",
    "postgresql_incremental": "postgresql_incremental_v1",
    "postgresql_incremental_v1": "postgresql_incremental_v1",
    "clickhouse": "clickhouse_incremental_v1",
    "clickhouse_incremental": "clickhouse_incremental_v1",
    "clickhouse_incremental_v1": "clickhouse_incremental_v1",
    "kafka": "kafka_partition_offset_v1",
    "kafka_partition_offset": "kafka_partition_offset_v1",
    "kafka_partition_offset_v1": "kafka_partition_offset_v1",
    "mongodb": "mongodb_incremental_v1",
    "mongodb_incremental": "mongodb_incremental_v1",
    "mongodb_incremental_v1": "mongodb_incremental_v1",
}

SOURCE_SPEC_ID_BY_CONTRACT_ID: Final[dict[str, str]] = {
    "http_document_v1": "source.uns.http_document.v1",
    "s3_document_v1": "source.uns.s3_document.v1",
    "git_document_v1": "source.uns.git_document.v1",
    "google_drive_document_v1": "source.uns.google_drive_document.v1",
    "confluence_document_v1": "source.uns.confluence_document.v1",
    "http_json_cursor_v1": "source.it.http_json_cursor.v1",
    "postgresql_incremental_v1": "source.it.postgresql_incremental.v1",
    "clickhouse_incremental_v1": "source.it.clickhouse_incremental.v1",
    "kafka_partition_offset_v1": "source.it.kafka_partition_offset.v1",
    "mongodb_incremental_v1": "source.it.mongodb_incremental.v1",
}


def retained_source_contract_ids() -> tuple[str, ...]:
    return tuple(SOURCE_SPEC_ID_BY_CONTRACT_ID.keys())


def normalize_source_contract_id(
    *, task_kind: SourceFlowFamily, task_document_source: str
) -> str | None:
    source = task_document_source.strip().lower()
    if task_kind == "uns":
        return UNS_SOURCE_CONTRACT_BY_ALIAS.get(source)
    return IT_SOURCE_CONTRACT_BY_ALIAS.get(source)


def source_spec_id_for_contract_id(source_contract_id: str) -> str | None:
    return SOURCE_SPEC_ID_BY_CONTRACT_ID.get(source_contract_id)
