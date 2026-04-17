from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from uns_stream.sources import normalize_uns_input_identity_sha, process_file
from uns_stream.tests._p45_source_wave3_helpers import assert_no_secret_fields, reserve_unused_port
from zephyr_core import ErrorCode, PartitionResult, PartitionStrategy, ZephyrError
from zephyr_ingest.testing.p45 import LoadedP45Env, missing_saas_env


def write_google_drive_source_spec(
    path: Path,
    *,
    file_id: str,
    access_token: str,
    acquisition_mode: str = "download",
    export_mime_type: str | None = None,
    drive_id: str | None = None,
    timeout_s: float = 10.0,
) -> None:
    source: dict[str, object] = {
        "kind": "google_drive_document_v1",
        "file_id": file_id,
        "access_token": access_token,
        "acquisition_mode": acquisition_mode,
        "timeout_s": timeout_s,
    }
    if export_mime_type is not None:
        source["export_mime_type"] = export_mime_type
    if drive_id is not None:
        source["drive_id"] = drive_id
    path.write_text(json.dumps({"source": source}, ensure_ascii=False, indent=2), encoding="utf-8")


def write_confluence_source_spec(
    path: Path,
    *,
    site_url: str,
    page_id: str,
    access_token: str,
    email: str | None = None,
    space_key: str | None = None,
    page_version: int | None = None,
    timeout_s: float = 10.0,
) -> None:
    source: dict[str, object] = {
        "kind": "confluence_document_v1",
        "site_url": site_url,
        "page_id": page_id,
        "access_token": access_token,
        "timeout_s": timeout_s,
    }
    if email is not None:
        source["email"] = email
    if space_key is not None:
        source["space_key"] = space_key
    if page_version is not None:
        source["page_version"] = page_version
    path.write_text(json.dumps({"source": source}, ensure_ascii=False, indent=2), encoding="utf-8")


def _optional_positive_int(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    return int(raw)


def _skip_if_missing(service: str, env: LoadedP45Env) -> None:
    missing = missing_saas_env(service, env)
    if missing:
        pytest.skip(f"missing P4.5 SaaS env for {service}: {', '.join(missing)}")


def _process_with_retryable_jitter_tolerance(
    *,
    filename: str,
    sha256: str,
    attempts: int = 3,
) -> PartitionResult:
    last_error: ZephyrError | None = None
    for attempt in range(1, attempts + 1):
        try:
            return process_file(
                filename=filename,
                strategy=PartitionStrategy.AUTO,
                sha256=sha256,
            )
        except ZephyrError as err:
            details = err.details
            if details is None or details.get("retryable") is not True:
                raise
            last_error = err
            if attempt == attempts:
                break
            time.sleep(1.0)
    assert last_error is not None
    pytest.skip(
        "google drive saas-live blocked by transient direct Google API connectivity under "
        f"trust_env=False after {attempts} attempts: {last_error.message}"
    )


def _expect_nonretryable_google_drive_failure(
    *,
    filename: str,
    sha256: str,
    attempts: int = 3,
) -> ZephyrError:
    last_retryable_error: ZephyrError | None = None
    for attempt in range(1, attempts + 1):
        try:
            process_file(
                filename=filename,
                strategy=PartitionStrategy.AUTO,
                sha256=sha256,
            )
        except ZephyrError as err:
            details = err.details
            if details is not None and details.get("retryable") is not True:
                return err
            last_retryable_error = err
            if attempt == attempts:
                break
            time.sleep(1.0)
            continue
        raise AssertionError("expected Google Drive fetch to fail")
    assert last_retryable_error is not None
    pytest.skip(
        "google drive saas-live non-retryable probe blocked by transient direct Google API "
        f"connectivity under trust_env=False after {attempts} attempts: "
        f"{last_retryable_error.message}"
    )


@pytest.mark.auth_saas_live
def test_p45_m6_google_drive_live_success_auth_and_provenance_if_configured(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    _skip_if_missing("google-drive", p45_env)

    file_id = p45_env.require("ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID")
    access_token = p45_env.require("ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN")
    drive_id = p45_env.get("ZEPHYR_P45_GOOGLE_DRIVE_DRIVE_ID")
    export_mime_type = p45_env.get("ZEPHYR_P45_GOOGLE_DRIVE_EXPORT_MIME_TYPE")
    acquisition_mode = "export" if export_mime_type is not None else "download"

    left = tmp_path / "google-drive-left.json"
    right = tmp_path / "google-drive-right.json"
    different = tmp_path / "google-drive-different.json"

    write_google_drive_source_spec(
        left,
        file_id=file_id,
        access_token=access_token,
        drive_id=drive_id,
        acquisition_mode=acquisition_mode,
        export_mime_type=export_mime_type,
        timeout_s=10.0,
    )
    write_google_drive_source_spec(
        right,
        file_id=file_id,
        access_token="rotated-token",
        drive_id=drive_id,
        acquisition_mode=acquisition_mode,
        export_mime_type=export_mime_type,
        timeout_s=30.0,
    )
    write_google_drive_source_spec(
        different,
        file_id=f"{file_id}-different",
        access_token=access_token,
        drive_id=drive_id,
        acquisition_mode=acquisition_mode,
        export_mime_type=export_mime_type,
    )

    first_sha = normalize_uns_input_identity_sha(filename=str(left), default_sha="drive-left")
    second_sha = normalize_uns_input_identity_sha(filename=str(right), default_sha="drive-right")
    different_sha = normalize_uns_input_identity_sha(
        filename=str(different),
        default_sha="drive-different",
    )
    assert first_sha == second_sha
    assert first_sha != different_sha

    result = _process_with_retryable_jitter_tolerance(
        filename=str(left),
        sha256=first_sha,
    )

    assert result.document.filename
    assert result.document.mime_type is not None
    assert result.document.sha256 == first_sha
    assert result.document.size_bytes > 0
    assert result.engine.name == "unstructured"
    assert result.engine.backend == "local"
    assert len(result.elements) > 0
    assert result.normalized_text.strip() != ""
    for element in result.elements:
        metadata_json = json.dumps(element.metadata, ensure_ascii=False)
        assert element.metadata["source_kind"] == "google_drive_document_v1"
        assert element.metadata["source_file_id"] == file_id
        assert element.metadata["source_acquisition_mode"] == acquisition_mode
        assert element.metadata["fetched_filename"] == result.document.filename
        assert element.metadata["fetched_mime_type"] == result.document.mime_type
        if drive_id is not None:
            assert element.metadata["source_drive_id"] == drive_id
        if export_mime_type is not None:
            assert element.metadata["source_export_mime_type"] == export_mime_type
        assert_no_secret_fields(element.metadata)
        assert access_token not in metadata_json

    bad_auth_spec = tmp_path / "google-drive-bad-auth.json"
    write_google_drive_source_spec(
        bad_auth_spec,
        file_id=file_id,
        access_token=f"{access_token}-invalid",
        drive_id=drive_id,
        acquisition_mode=acquisition_mode,
        export_mime_type=export_mime_type,
    )
    auth_error = _expect_nonretryable_google_drive_failure(
        filename=str(bad_auth_spec),
        sha256="drive-bad-auth",
    )

    assert auth_error.code == ErrorCode.IO_READ_FAILED
    auth_details = auth_error.details
    assert auth_details is not None
    assert auth_details["retryable"] is False
    assert auth_details["source_kind"] == "google_drive_document_v1"
    assert auth_details["file_id"] == file_id
    assert access_token not in json.dumps(auth_details, ensure_ascii=False)

    missing_spec = tmp_path / "google-drive-missing.json"
    write_google_drive_source_spec(
        missing_spec,
        file_id=f"{file_id}-missing",
        access_token=access_token,
        drive_id=drive_id,
        acquisition_mode=acquisition_mode,
        export_mime_type=export_mime_type,
    )
    missing_error = _expect_nonretryable_google_drive_failure(
        filename=str(missing_spec),
        sha256="drive-missing",
    )

    assert missing_error.code == ErrorCode.IO_READ_FAILED
    missing_details = missing_error.details
    assert missing_details is not None
    assert missing_details["retryable"] is False
    assert missing_details["source_kind"] == "google_drive_document_v1"
    assert missing_details["file_id"] == f"{file_id}-missing"
    assert access_token not in json.dumps(missing_details, ensure_ascii=False)


@pytest.mark.auth_saas_live
def test_p45_m6_confluence_live_success_failure_identity_and_partition_path(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    _skip_if_missing("confluence", p45_env)

    site_url = p45_env.require("ZEPHYR_P45_CONFLUENCE_SITE_URL")
    page_id = p45_env.require("ZEPHYR_P45_CONFLUENCE_PAGE_ID")
    access_token = p45_env.require("ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN")
    email = p45_env.get("ZEPHYR_P45_CONFLUENCE_EMAIL")
    space_key = p45_env.get("ZEPHYR_P45_CONFLUENCE_SPACE_KEY")
    page_version = _optional_positive_int(p45_env.get("ZEPHYR_P45_CONFLUENCE_PAGE_VERSION"))

    left = tmp_path / "confluence-left.json"
    right = tmp_path / "confluence-right.json"
    different = tmp_path / "confluence-different.json"

    write_confluence_source_spec(
        left,
        site_url=site_url,
        page_id=page_id,
        access_token=access_token,
        email=email,
        space_key=space_key,
        page_version=page_version,
        timeout_s=10.0,
    )
    write_confluence_source_spec(
        right,
        site_url=f"{site_url}/",
        page_id=page_id,
        access_token="rotated-token",
        email="rotated@example.test" if email is not None else None,
        space_key=space_key,
        page_version=page_version,
        timeout_s=30.0,
    )
    write_confluence_source_spec(
        different,
        site_url=site_url,
        page_id=f"{page_id}-different",
        access_token=access_token,
        email=email,
        space_key=space_key,
        page_version=page_version,
    )

    first_sha = normalize_uns_input_identity_sha(
        filename=str(left),
        default_sha="confluence-left",
    )
    second_sha = normalize_uns_input_identity_sha(
        filename=str(right),
        default_sha="confluence-right",
    )
    different_sha = normalize_uns_input_identity_sha(
        filename=str(different),
        default_sha="confluence-different",
    )
    assert first_sha == second_sha
    assert first_sha != different_sha

    result = process_file(
        filename=str(left),
        strategy=PartitionStrategy.AUTO,
        sha256=first_sha,
    )

    assert result.document.filename.endswith(".html")
    assert result.document.mime_type == "text/html"
    assert result.document.sha256 == first_sha
    assert result.document.size_bytes > 0
    assert result.engine.name == "unstructured"
    assert result.engine.backend == "local"
    assert len(result.elements) > 0
    assert result.normalized_text.strip() != ""
    resolved_version = None
    for element in result.elements:
        metadata_json = json.dumps(element.metadata, ensure_ascii=False)
        assert element.metadata["source_kind"] == "confluence_document_v1"
        assert element.metadata["source_site_url"] == site_url.rstrip("/")
        assert element.metadata["source_page_id"] == page_id
        assert element.metadata["source_body_format"] == "storage"
        assert isinstance(element.metadata["source_page_title"], str)
        assert element.metadata["fetched_filename"] == result.document.filename
        assert element.metadata["fetched_mime_type"] == "text/html"
        if space_key is not None:
            assert element.metadata["source_space_key"] == space_key
        if page_version is not None:
            assert element.metadata["source_requested_page_version"] == page_version
        if "source_page_version" in element.metadata:
            resolved_version = int(element.metadata["source_page_version"])
        assert_no_secret_fields(element.metadata)
        assert access_token not in metadata_json

    bad_auth_spec = tmp_path / "confluence-bad-auth.json"
    write_confluence_source_spec(
        bad_auth_spec,
        site_url=site_url,
        page_id=page_id,
        access_token=f"{access_token}-invalid",
        email=email,
        space_key=space_key,
        page_version=page_version,
    )
    with pytest.raises(ZephyrError) as auth_exc:
        process_file(
            filename=str(bad_auth_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="confluence-bad-auth",
        )

    assert auth_exc.value.code == ErrorCode.IO_READ_FAILED
    auth_details = auth_exc.value.details
    assert auth_details is not None
    assert auth_details["retryable"] is False
    assert auth_details["source_kind"] == "confluence_document_v1"
    assert auth_details["site_url"] == site_url.rstrip("/")
    assert auth_details["page_id"] == page_id
    assert access_token not in json.dumps(auth_details, ensure_ascii=False)

    retryable_spec = tmp_path / "confluence-retryable.json"
    write_confluence_source_spec(
        retryable_spec,
        site_url=f"http://127.0.0.1:{reserve_unused_port()}",
        page_id=page_id,
        access_token=access_token,
        email=email,
        space_key=space_key,
        page_version=page_version,
        timeout_s=2.0,
    )
    with pytest.raises(ZephyrError) as retryable_exc:
        process_file(
            filename=str(retryable_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="confluence-retryable",
        )

    assert retryable_exc.value.code == ErrorCode.IO_READ_FAILED
    retryable_details = retryable_exc.value.details
    assert retryable_details is not None
    assert retryable_details["retryable"] is True
    assert retryable_details["source_kind"] == "confluence_document_v1"

    bad_version_spec = tmp_path / "confluence-bad-version.json"
    write_confluence_source_spec(
        bad_version_spec,
        site_url=site_url,
        page_id=page_id,
        access_token=access_token,
        email=email,
        space_key=space_key,
        page_version=(resolved_version or page_version or 1) + 9999,
    )
    with pytest.raises(ZephyrError) as bad_version_exc:
        process_file(
            filename=str(bad_version_spec),
            strategy=PartitionStrategy.AUTO,
            sha256="confluence-bad-version",
        )

    assert bad_version_exc.value.code == ErrorCode.IO_READ_FAILED
    bad_version_details = bad_version_exc.value.details
    assert bad_version_details is not None
    assert bad_version_details["retryable"] is False
    assert bad_version_details["source_kind"] == "confluence_document_v1"
    assert bad_version_details["site_url"] == site_url.rstrip("/")
    assert bad_version_details["page_id"] == page_id
