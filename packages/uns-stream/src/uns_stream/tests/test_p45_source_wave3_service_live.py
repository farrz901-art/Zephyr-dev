from __future__ import annotations

import json
from pathlib import Path

import pytest

from uns_stream.sources import normalize_uns_input_identity_sha, process_file
from uns_stream.tests._p45_source_wave3_helpers import (
    assert_no_secret_fields,
    http_fixture_base,
    reserve_unused_port,
    s3_access_key,
    s3_bucket,
    s3_endpoint,
    s3_region,
    s3_secret_key,
    seed_minio_text_object,
    write_http_source_spec,
    write_s3_source_spec,
)
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError
from zephyr_ingest.testing.p45 import LoadedP45Env


@pytest.mark.auth_service_live
def test_p45_m55_http_document_live_success_failure_identity_and_partition_path(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    base_url = http_fixture_base(p45_env)
    left = tmp_path / "http-left.json"
    right = tmp_path / "http-right.json"
    third = tmp_path / "http-third.json"
    write_http_source_spec(
        left,
        url=f"{base_url}/documents/plain",
        accept="text/plain",
        timeout_s=5.0,
    )
    right.write_text(
        json.dumps(
            {
                "source": {
                    "timeout_s": 30.0,
                    "accept": "text/plain",
                    "url": f"{base_url}/documents/plain",
                    "kind": "http_document_v1",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_http_source_spec(
        third,
        url=f"{base_url}/documents/plain",
        accept="application/octet-stream",
        timeout_s=5.0,
    )

    first_sha = normalize_uns_input_identity_sha(filename=str(left), default_sha="http-left")
    second_sha = normalize_uns_input_identity_sha(filename=str(right), default_sha="http-right")
    different_sha = normalize_uns_input_identity_sha(filename=str(third), default_sha="http-third")

    assert first_sha == second_sha
    assert first_sha != different_sha

    result = process_file(
        filename=str(left),
        strategy=PartitionStrategy.AUTO,
        sha256=first_sha,
    )

    assert result.document.filename == "plain.txt"
    assert result.document.mime_type == "text/plain"
    assert result.document.sha256 == first_sha
    assert result.engine.name == "unstructured"
    assert result.engine.backend == "local"
    assert len(result.elements) > 0
    assert "Zephyr P4.5 fixture document." in result.normalized_text
    for element in result.elements:
        assert element.metadata["source_kind"] == "http_document_v1"
        assert element.metadata["source_url"] == f"{base_url}/documents/plain"
        assert element.metadata["fetched_filename"] == "plain.txt"
        assert element.metadata["fetched_mime_type"] == "text/plain"
        assert_no_secret_fields(element.metadata)
        assert "headers" not in element.metadata
        assert "content" not in element.metadata

    retryable_spec = tmp_path / "http-retryable.json"
    write_http_source_spec(
        retryable_spec,
        url=f"http://127.0.0.1:{reserve_unused_port()}/documents/plain",
        accept="text/plain",
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="http-retryable",
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    retryable_details = retryable_exc.value.details
    assert retryable_details is not None
    assert retryable_details["retryable"] is True
    assert retryable_details["source_kind"] == "http_document_v1"

    missing_spec = tmp_path / "http-missing.json"
    write_http_source_spec(
        missing_spec,
        url=f"{base_url}/status/404",
        accept="text/plain",
    )
    with pytest.raises(ZephyrError) as missing_exc:
        process_file(
            filename=str(missing_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="http-missing",
        )

    assert missing_exc.value.code == ErrorCode.IO_READ_FAILED
    assert missing_exc.value.details == {
        "retryable": False,
        "source_kind": "http_document_v1",
        "status_code": 404,
        "url": f"{base_url}/status/404",
    }


@pytest.mark.auth_service_live
def test_p45_m55_s3_document_live_success_failure_identity_and_partition_path(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    pytest.importorskip("boto3")

    bucket = s3_bucket(p45_env)
    key = "p45-m55/reports/report.txt"
    etag = seed_minio_text_object(
        p45_env,
        bucket=bucket,
        key=key,
        body="Zephyr P4.5 S3 fixture document.\n",
    )

    left = tmp_path / "s3-left.json"
    right = tmp_path / "s3-right.json"
    third = tmp_path / "s3-third.json"
    write_s3_source_spec(
        left,
        bucket=bucket,
        key=key,
        region=s3_region(p45_env),
        endpoint_url=s3_endpoint(p45_env),
        access_key=s3_access_key(p45_env),
        secret_key=s3_secret_key(p45_env),
    )
    right.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "s3_document_v1",
                    "bucket": bucket,
                    "key": key,
                    "region": s3_region(p45_env),
                    "endpoint_url": s3_endpoint(p45_env),
                    "access_key": "rotated-access",
                    "secret_key": "rotated-secret",
                    "session_token": "rotated-token",
                    "version_id": None,
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_s3_source_spec(
        third,
        bucket=bucket,
        key="p45-m55/reports/other.txt",
        region=s3_region(p45_env),
        endpoint_url=s3_endpoint(p45_env),
        access_key=s3_access_key(p45_env),
        secret_key=s3_secret_key(p45_env),
    )

    first_sha = normalize_uns_input_identity_sha(filename=str(left), default_sha="s3-left")
    second_sha = normalize_uns_input_identity_sha(filename=str(right), default_sha="s3-right")
    different_sha = normalize_uns_input_identity_sha(filename=str(third), default_sha="s3-third")

    assert first_sha == second_sha
    assert first_sha != different_sha

    result = process_file(
        filename=str(left),
        strategy=PartitionStrategy.AUTO,
        sha256=first_sha,
    )

    assert result.document.filename == "report.txt"
    assert result.document.mime_type == "text/plain"
    assert result.document.sha256 == first_sha
    assert result.engine.name == "unstructured"
    assert result.engine.backend == "local"
    assert len(result.elements) > 0
    assert "Zephyr P4.5 S3 fixture document." in result.normalized_text
    for element in result.elements:
        assert element.metadata["source_kind"] == "s3_document_v1"
        assert element.metadata["source_bucket"] == bucket
        assert element.metadata["source_key"] == key
        assert element.metadata["fetched_filename"] == "report.txt"
        assert element.metadata["fetched_mime_type"] == "text/plain"
        assert_no_secret_fields(element.metadata)
        if etag is not None:
            assert element.metadata["source_etag"] == etag

    bad_auth_spec = tmp_path / "s3-bad-auth.json"
    write_s3_source_spec(
        bad_auth_spec,
        bucket=bucket,
        key=key,
        region=s3_region(p45_env),
        endpoint_url=s3_endpoint(p45_env),
        access_key=s3_access_key(p45_env),
        secret_key="wrong-secret",
    )
    with pytest.raises(ZephyrError) as bad_auth_exc:
        process_file(
            filename=str(bad_auth_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="s3-bad-auth",
        )

    assert bad_auth_exc.value.code == ErrorCode.IO_READ_FAILED
    bad_auth_details = bad_auth_exc.value.details
    assert bad_auth_details is not None
    assert bad_auth_details["retryable"] is False
    assert bad_auth_details["source_kind"] == "s3_document_v1"
    assert bad_auth_details["bucket"] == bucket
    assert bad_auth_details["key"] == key
    assert "wrong-secret" not in json.dumps(bad_auth_details, ensure_ascii=False)

    retryable_spec = tmp_path / "s3-retryable.json"
    write_s3_source_spec(
        retryable_spec,
        bucket=bucket,
        key=key,
        region=s3_region(p45_env),
        endpoint_url=s3_endpoint(p45_env, port=reserve_unused_port()),
        access_key=s3_access_key(p45_env),
        secret_key=s3_secret_key(p45_env),
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="s3-retryable",
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    retryable_details = retryable_exc.value.details
    assert retryable_details is not None
    assert retryable_details["retryable"] is True
    assert retryable_details["source_kind"] == "s3_document_v1"
    assert retryable_details["bucket"] == bucket
    assert retryable_details["key"] == key

    missing_spec = tmp_path / "s3-missing.json"
    write_s3_source_spec(
        missing_spec,
        bucket=bucket,
        key="p45-m55/reports/missing.txt",
        region=s3_region(p45_env),
        endpoint_url=s3_endpoint(p45_env),
        access_key=s3_access_key(p45_env),
        secret_key=s3_secret_key(p45_env),
    )
    with pytest.raises(ZephyrError) as missing_exc:
        process_file(
            filename=str(missing_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="s3-missing",
        )

    assert missing_exc.value.code == ErrorCode.IO_READ_FAILED
    missing_details = missing_exc.value.details
    assert missing_details is not None
    assert missing_details["retryable"] is False
    assert missing_details["source_kind"] == "s3_document_v1"
    assert missing_details["bucket"] == bucket
    assert missing_details["key"] == "p45-m55/reports/missing.txt"
