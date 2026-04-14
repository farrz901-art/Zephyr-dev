from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from zephyr_ingest.testing.p45 import (
    P45_ENV_FILE_NAMES,
    format_redacted_env_summary,
    get_service_definitions,
    load_p45_env,
    missing_saas_env,
)


@pytest.mark.auth_orchestration
def test_load_p45_env_honors_default_file_and_process_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temp_path = Path.cwd() / ".codex_tmp_p45_env_test"
    shutil.rmtree(temp_path, ignore_errors=True)
    temp_path.mkdir(parents=True, exist_ok=True)
    try:
        (temp_path / P45_ENV_FILE_NAMES[0]).write_text(
            "ZEPHYR_P45_POSTGRES_HOST=file-host\nZEPHYR_P45_POSTGRES_PASSWORD=file-secret\n",
            encoding="utf-8",
        )
        (temp_path / P45_ENV_FILE_NAMES[1]).write_text(
            "ZEPHYR_P45_POSTGRES_HOST=overlay-host\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("ZEPHYR_P45_POSTGRES_PORT", "25432")

        env = load_p45_env(repo_root_path=temp_path)

        assert env.get("ZEPHYR_P45_POSTGRES_HOST") == "overlay-host"
        assert env.sources["ZEPHYR_P45_POSTGRES_HOST"] == ".env.p45"
        assert env.get("ZEPHYR_P45_POSTGRES_PORT") == "25432"
        assert env.sources["ZEPHYR_P45_POSTGRES_PORT"] == "env"
    finally:
        shutil.rmtree(temp_path, ignore_errors=True)


@pytest.mark.auth_orchestration
def test_redacted_env_summary_masks_secrets_and_leaves_public_values_visible() -> None:
    env = load_p45_env(
        environ={
            "ZEPHYR_P45_POSTGRES_HOST": "db.internal",
            "ZEPHYR_P45_POSTGRES_PASSWORD": "super-secret",
        }
    )

    summary = format_redacted_env_summary(
        env,
        names=("ZEPHYR_P45_POSTGRES_HOST", "ZEPHYR_P45_POSTGRES_PASSWORD"),
    )

    assert "db.internal" in summary
    assert "super-secret" not in summary
    assert "***" in summary


@pytest.mark.auth_orchestration
def test_service_registry_covers_required_shared_substrate() -> None:
    all_services = {service.name for service in get_service_definitions()}
    assert all_services == {
        "postgresql",
        "kafka-redpanda",
        "minio-s3",
        "opensearch",
        "clickhouse",
        "mongodb",
        "loki",
        "weaviate",
        "http-fixture",
        "webhook-echo",
    }
    assert {service.name for service in get_service_definitions(tier="local-real")} == {
        "http-fixture",
        "webhook-echo",
    }
    assert {service.name for service in get_service_definitions(tier="service-live")} == (
        all_services - {"http-fixture", "webhook-echo"}
    )


@pytest.mark.auth_orchestration
def test_missing_saas_env_reports_required_google_drive_and_confluence_inputs() -> None:
    env = load_p45_env(environ={})
    assert missing_saas_env("google-drive", env) == (
        "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID",
        "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN",
    )
    assert missing_saas_env("confluence", env) == (
        "ZEPHYR_P45_CONFLUENCE_SITE_URL",
        "ZEPHYR_P45_CONFLUENCE_PAGE_ID",
        "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN",
    )
