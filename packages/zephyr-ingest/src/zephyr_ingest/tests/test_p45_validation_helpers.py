from __future__ import annotations

import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from tools.p45_substrate_healthcheck import main as healthcheck_main

from zephyr_ingest.testing.p45 import (
    P45_COMPOSE_FILE_NAME,
    P45_HOME_ENV_NAME,
    default_p45_home,
    format_redacted_env_summary,
    format_runtime_resolution,
    get_service_definitions,
    load_p45_env,
    missing_saas_env,
    resolve_p45_home,
    resolve_p45_runtime_paths,
)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


@contextmanager
def _sandbox(name: str) -> Iterator[Path]:
    root = Path.cwd() / ".codex_tmp_p45_m11_tests" / f"{name}_{uuid4().hex}"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


@pytest.mark.auth_contract
def test_resolve_p45_home_uses_env_var() -> None:
    with _sandbox("resolve_home_env") as sandbox:
        repo_root = sandbox / "repo"
        repo_root.mkdir()
        runtime_home = sandbox / "runtime-home"

        resolved = resolve_p45_home(
            repo_root_path=repo_root,
            environ={P45_HOME_ENV_NAME: str(runtime_home)},
        )

        assert resolved == runtime_home


@pytest.mark.auth_contract
def test_resolve_p45_home_defaults_to_external_user_config_when_unset() -> None:
    with _sandbox("resolve_home_default") as sandbox:
        repo_root = sandbox / "repo"
        repo_root.mkdir()
        user_home = sandbox / "user-home"

        resolved = resolve_p45_home(
            repo_root_path=repo_root,
            environ={},
            user_home_path=user_home,
        )

        assert resolved == default_p45_home(user_home_path=user_home)
        assert resolved == user_home / ".config" / "zephyr" / "p45"


@pytest.mark.auth_contract
def test_resolve_p45_compose_path_prefers_runtime_home_entrypoint() -> None:
    with _sandbox("compose_runtime") as sandbox:
        repo_root = sandbox / "repo"
        runtime_home = sandbox / "runtime-home"
        repo_root.mkdir()
        runtime_home.mkdir()
        _write_text(repo_root / P45_COMPOSE_FILE_NAME, "services: {}\n")
        _write_text(runtime_home / "docker-compose.yml", "services: {}\n")

        runtime_paths = resolve_p45_runtime_paths(
            repo_root_path=repo_root,
            environ={P45_HOME_ENV_NAME: str(runtime_home)},
        )

        assert runtime_paths.compose_path == runtime_home / "docker-compose.yml"
        assert runtime_paths.compose_source == "runtime-home:docker-compose.yml"


@pytest.mark.auth_contract
def test_resolve_p45_compose_path_falls_back_to_repo_example() -> None:
    with _sandbox("compose_fallback") as sandbox:
        repo_root = sandbox / "repo"
        runtime_home = sandbox / "runtime-home"
        repo_root.mkdir()
        runtime_home.mkdir()
        _write_text(repo_root / P45_COMPOSE_FILE_NAME, "services: {}\n")

        runtime_paths = resolve_p45_runtime_paths(
            repo_root_path=repo_root,
            environ={P45_HOME_ENV_NAME: str(runtime_home)},
        )

        assert runtime_paths.compose_path == repo_root / P45_COMPOSE_FILE_NAME
        assert runtime_paths.compose_source == f"repo-fallback:{P45_COMPOSE_FILE_NAME}"


@pytest.mark.auth_contract
def test_load_p45_env_uses_runtime_home_before_repo_fallback_then_process_env() -> None:
    with _sandbox("env_precedence") as sandbox:
        repo_root = sandbox / "repo"
        runtime_home = sandbox / "runtime-home"
        repo_root.mkdir()
        runtime_home.mkdir()
        _write_text(
            runtime_home / "env" / ".env",
            "\n".join(
                (
                    "ZEPHYR_P45_POSTGRES_HOST=runtime-postgres",
                    "ZEPHYR_P45_S3_ACCESS_KEY=runtime-access",
                )
            )
            + "\n",
        )
        _write_text(
            repo_root / ".env.p45",
            "\n".join(
                (
                    "ZEPHYR_P45_POSTGRES_HOST=repo-postgres",
                    "ZEPHYR_P45_KAFKA_HOST=repo-kafka",
                )
            )
            + "\n",
        )

        env = load_p45_env(
            repo_root_path=repo_root,
            environ={
                P45_HOME_ENV_NAME: str(runtime_home),
                "ZEPHYR_P45_POSTGRES_HOST": "process-postgres",
                "ZEPHYR_P45_WEBHOOK_ECHO_HOST": "process-webhook",
            },
        )

        assert env.values["ZEPHYR_P45_POSTGRES_HOST"] == "process-postgres"
        assert env.values["ZEPHYR_P45_S3_ACCESS_KEY"] == "runtime-access"
        assert env.values["ZEPHYR_P45_KAFKA_HOST"] == "repo-kafka"
        assert env.values["ZEPHYR_P45_POSTGRES_PORT"] == "15432"
        assert env.sources["ZEPHYR_P45_POSTGRES_HOST"] == "env"
        assert env.sources["ZEPHYR_P45_S3_ACCESS_KEY"].startswith("runtime-home:")
        assert env.sources["ZEPHYR_P45_KAFKA_HOST"].startswith("repo-fallback:")
        assert env.sources["ZEPHYR_P45_POSTGRES_PORT"] == "default"
        assert env.runtime_paths.home == runtime_home
        assert env.runtime_paths.env_dir == runtime_home / "env"


@pytest.mark.auth_contract
def test_redacted_env_summary_keeps_public_values_visible_and_masks_secrets() -> None:
    with _sandbox("redaction") as sandbox:
        repo_root = sandbox / "repo"
        repo_root.mkdir()

        env = load_p45_env(
            repo_root_path=repo_root,
            environ={
                "ZEPHYR_P45_S3_ACCESS_KEY": "top-secret-access",
                "ZEPHYR_P45_POSTGRES_HOST": "public-postgres",
            },
            user_home_path=sandbox / "user-home",
        )

        summary = format_redacted_env_summary(
            env,
            names=("ZEPHYR_P45_S3_ACCESS_KEY", "ZEPHYR_P45_POSTGRES_HOST"),
        )

        assert "public-postgres" in summary
        assert "top-secret-access" not in summary
        assert "ZEPHYR_P45_S3_ACCESS_KEY='***'" in summary


@pytest.mark.auth_contract
def test_format_runtime_resolution_reports_control_and_runtime_planes() -> None:
    with _sandbox("format_resolution") as sandbox:
        repo_root = sandbox / "repo"
        runtime_home = sandbox / "runtime-home"
        repo_root.mkdir()
        runtime_home.mkdir()
        _write_text(runtime_home / "docker-compose.yml", "services: {}\n")

        runtime_paths = resolve_p45_runtime_paths(
            repo_root_path=repo_root,
            environ={P45_HOME_ENV_NAME: str(runtime_home)},
        )

        rendered = format_runtime_resolution(runtime_paths)

        assert "control plane repo" in rendered
        assert str(repo_root) in rendered
        assert "runtime home" in rendered
        assert str(runtime_home) in rendered
        assert "compose file" in rendered


@pytest.mark.auth_contract
def test_service_registry_covers_required_shared_substrate() -> None:
    services = {service.name: service for service in get_service_definitions()}

    assert set(services) == {
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
    assert services["postgresql"].tier == "service-live"
    assert services["http-fixture"].tier == "local-real"
    assert services["webhook-echo"].tier == "local-real"


@pytest.mark.auth_contract
def test_missing_saas_env_reports_required_google_drive_and_confluence_inputs() -> None:
    with _sandbox("saas_missing") as sandbox:
        repo_root = sandbox / "repo"
        repo_root.mkdir()
        env = load_p45_env(repo_root_path=repo_root, environ={})

        assert missing_saas_env("google-drive", env) == (
            "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID",
            "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN",
        )
        assert missing_saas_env("confluence", env) == (
            "ZEPHYR_P45_CONFLUENCE_SITE_URL",
            "ZEPHYR_P45_CONFLUENCE_PAGE_ID",
            "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN",
        )


@pytest.mark.auth_contract
def test_healthcheck_print_compose_path_uses_runtime_home(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with _sandbox("healthcheck_paths") as sandbox:
        runtime_home = sandbox / "runtime-home"
        compose_path = runtime_home / "docker-compose.yml"
        _write_text(compose_path, "services: {}\n")
        monkeypatch.setenv(P45_HOME_ENV_NAME, str(runtime_home))

        exit_code = healthcheck_main(["--print-compose-path"])

        captured = capsys.readouterr()
        assert exit_code == 0
        assert captured.out.strip() == str(compose_path)
