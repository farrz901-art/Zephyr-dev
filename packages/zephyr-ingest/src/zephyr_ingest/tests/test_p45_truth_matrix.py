from __future__ import annotations

import pytest

from zephyr_ingest.testing.p45_matrix import P45_TIERS, load_p45_truth_matrix, surface_names_by_kind


@pytest.mark.auth_contract
def test_p45_truth_matrix_covers_full_retained_surface() -> None:
    matrix = load_p45_truth_matrix()

    assert matrix["phase"] == "P4.5 authenticity hardening"
    assert tuple(matrix["taxonomy"]) == P45_TIERS
    assert set(surface_names_by_kind(matrix, kind="destination")) == {
        "filesystem",
        "webhook",
        "sqlite",
        "kafka",
        "weaviate",
        "s3",
        "opensearch",
        "clickhouse",
        "mongodb",
        "loki",
    }
    assert set(surface_names_by_kind(matrix, kind="source")) == {
        "http_json_cursor_v1",
        "postgresql_incremental_v1",
        "clickhouse_incremental_v1",
        "kafka_partition_offset_v1",
        "mongodb_incremental_v1",
        "http_document_v1",
        "s3_document_v1",
        "git_document_v1",
        "google_drive_document_v1",
        "confluence_document_v1",
    }
    assert set(surface_names_by_kind(matrix, kind="input-path")) == {"airbyte-message-json"}


@pytest.mark.auth_contract
def test_p45_truth_matrix_uses_uniform_tier_placeholders() -> None:
    matrix = load_p45_truth_matrix()
    for surface in matrix["surfaces"]:
        assert set(surface["authenticity"]) == set(P45_TIERS)
        for tier in P45_TIERS:
            status = surface["authenticity"][tier]["status"]
            assert status in {"planned", "not_applicable"}
