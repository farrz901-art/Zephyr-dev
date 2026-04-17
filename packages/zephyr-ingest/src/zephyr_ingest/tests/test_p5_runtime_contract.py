from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from tools.p5_runtime_preflight import main as p5_preflight_main

from zephyr_ingest.testing.p5_runtime_contract import (
    P5_CANONICAL_RUNTIME_FILES,
    P5_REQUIRED_RUNTIME_DIRS,
    P5_RUNTIME_CONTRACT_PATH,
    P5_RUNTIME_REPORT_PATH,
    build_p5_runtime_contract,
    format_p5_preflight_results,
    iter_contract_env_names,
    validate_p5_runtime_contract,
)
from zephyr_ingest.testing.p45 import (
    P45_HOME_ENV_NAME,
    format_redacted_env_summary,
    load_p45_env,
)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


@contextmanager
def _sandbox(name: str) -> Iterator[Path]:
    root = Path.cwd() / ".codex_tmp_p5_m1_tests" / f"{name}_{uuid4().hex}"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _build_runtime_home(root: Path) -> Path:
    runtime_home = root / "runtime-home"
    for dir_name in P5_REQUIRED_RUNTIME_DIRS:
        (runtime_home / dir_name).mkdir(parents=True, exist_ok=True)
    for relative_path in P5_CANONICAL_RUNTIME_FILES:
        content = "X=1\n" if relative_path.endswith(".env") else "{}\n"
        _write_text(runtime_home / relative_path, content)
    return runtime_home


@pytest.mark.auth_contract
def test_p5_runtime_contract_artifact_matches_helper_shape() -> None:
    artifact = json.loads(P5_RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    built = build_p5_runtime_contract()
    built_groups = cast(list[dict[str, object]], built["env_groups"])

    assert artifact["phase"] == built["phase"]
    assert artifact["runtime_home_env_var"] == "ZEPHYR_P45_HOME"
    assert artifact["runtime_plane"]["required_dirs"] == list(P5_REQUIRED_RUNTIME_DIRS)
    assert artifact["runtime_plane"]["canonical_files"] == list(P5_CANONICAL_RUNTIME_FILES)
    assert {group["name"] for group in artifact["env_groups"]} == {
        group["name"] for group in built_groups
    }
    assert P5_RUNTIME_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_validate_p5_runtime_contract_passes_for_external_runtime_home_layout() -> None:
    with _sandbox("contract_pass") as sandbox:
        repo_root = sandbox / "repo"
        repo_root.mkdir()
        runtime_home = _build_runtime_home(sandbox)
        _write_text(
            runtime_home / "env" / ".env.p45.local",
            "\n".join(
                (
                    "ZEPHYR_P45_ENV=local",
                    "ZEPHYR_P45_STACK_NAME=zephyr-p45",
                    "ZEPHYR_P45_POSTGRES_HOST=127.0.0.1",
                    "ZEPHYR_P45_POSTGRES_PORT=15432",
                    "ZEPHYR_P45_POSTGRES_DB=zephyr",
                    "ZEPHYR_P45_POSTGRES_USER=zephyr",
                    "ZEPHYR_P45_POSTGRES_PASSWORD=postgres-secret",
                    "ZEPHYR_P45_KAFKA_HOST=127.0.0.1",
                    "ZEPHYR_P45_KAFKA_PORT=19092",
                    "ZEPHYR_P45_REDPANDA_ADMIN_HOST=127.0.0.1",
                    "ZEPHYR_P45_REDPANDA_ADMIN_PORT=19644",
                    "ZEPHYR_P45_KAFKA_BROKERS=127.0.0.1:19092",
                    "ZEPHYR_P45_KAFKA_TOPIC=zephyr",
                    "ZEPHYR_P45_S3_HOST=127.0.0.1",
                    "ZEPHYR_P45_S3_PORT=19000",
                    "ZEPHYR_P45_S3_ENDPOINT=http://127.0.0.1:19000",
                    "ZEPHYR_P45_S3_REGION=us-east-1",
                    "ZEPHYR_P45_S3_ACCESS_KEY=access",
                    "ZEPHYR_P45_S3_SECRET_KEY=secret",
                    "ZEPHYR_P45_S3_BUCKET=zephyr",
                    "ZEPHYR_P45_OPENSEARCH_PORT=19200",
                    "ZEPHYR_P45_OPENSEARCH_URL=https://127.0.0.1:19200",
                    "ZEPHYR_P45_OPENSEARCH_INDEX=zephyr",
                    "ZEPHYR_P45_OPENSEARCH_USERNAME=admin",
                    "ZEPHYR_P45_OPENSEARCH_PASSWORD=change-me",
                    "ZEPHYR_P45_OPENSEARCH_SKIP_TLS_VERIFY=true",
                    "ZEPHYR_P45_CLICKHOUSE_PORT=18123",
                    "ZEPHYR_P45_CLICKHOUSE_URL=http://127.0.0.1:18123",
                    "ZEPHYR_P45_CLICKHOUSE_DATABASE=zephyr",
                    "ZEPHYR_P45_CLICKHOUSE_TABLE=records",
                    "ZEPHYR_P45_CLICKHOUSE_USERNAME=zephyr",
                    "ZEPHYR_P45_CLICKHOUSE_PASSWORD=change-me",
                    "ZEPHYR_P45_MONGODB_PORT=27018",
                    "ZEPHYR_P45_MONGODB_URI=mongodb://zephyr:change-me@127.0.0.1:27018",
                    "ZEPHYR_P45_MONGODB_DATABASE=zephyr",
                    "ZEPHYR_P45_MONGODB_COLLECTION=records",
                    "ZEPHYR_P45_LOKI_PORT=13100",
                    "ZEPHYR_P45_LOKI_URL=http://127.0.0.1:13100",
                    "ZEPHYR_P45_LOKI_STREAM=zephyr",
                    "ZEPHYR_P45_LOKI_TENANT_ID=tenant-a",
                    "ZEPHYR_P45_WEAVIATE_PORT=18080",
                    "ZEPHYR_P45_WEAVIATE_GRPC_PORT=15051",
                    "ZEPHYR_P45_WEAVIATE_HTTP_HOST=127.0.0.1",
                    "ZEPHYR_P45_WEAVIATE_HTTP_PORT=18080",
                    "ZEPHYR_P45_WEAVIATE_GRPC_HOST=127.0.0.1",
                    "ZEPHYR_P45_WEAVIATE_COLLECTION=Zephyr",
                    "ZEPHYR_P45_WEAVIATE_API_KEY=weaviate-key",
                    "ZEPHYR_P45_HTTP_FIXTURE_HOST=127.0.0.1",
                    "ZEPHYR_P45_HTTP_FIXTURE_PORT=18081",
                    "ZEPHYR_P45_HTTP_FIXTURE_BASE=http://127.0.0.1:18081",
                    "ZEPHYR_P45_WEBHOOK_ECHO_HOST=127.0.0.1",
                    "ZEPHYR_P45_WEBHOOK_ECHO_PORT=18082",
                    "ZEPHYR_P45_WEBHOOK_ECHO_URL=http://127.0.0.1:18082/ingest",
                    "ZEPHYR_P45_GDRIVE_FILE_ID_EXPORT=file-1",
                    "ZEPHYR_P45_GDRIVE_ACCESS_TOKEN=token-1",
                    "ZEPHYR_P45_CONFLUENCE_SITE_URL=https://confluence.example",
                    "ZEPHYR_P45_CONFLUENCE_PAGE_ID=42",
                    "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN=token-2",
                )
            )
            + "\n",
        )

        env = load_p45_env(
            repo_root_path=repo_root,
            environ={P45_HOME_ENV_NAME: str(runtime_home)},
        )
        checks = validate_p5_runtime_contract(env)

        assert all(check.ok for check in checks), format_p5_preflight_results(checks)


@pytest.mark.auth_contract
def test_validate_p5_runtime_contract_flags_repo_fallback_and_proxy_presence() -> None:
    with _sandbox("contract_fail") as sandbox:
        repo_root = sandbox / "repo"
        runtime_home = sandbox / "repo" / "runtime-home"
        repo_root.mkdir(parents=True, exist_ok=True)
        _build_runtime_home(sandbox / "repo")
        env = load_p45_env(
            repo_root_path=repo_root,
            environ={
                P45_HOME_ENV_NAME: str(runtime_home),
                "HTTP_PROXY": "http://proxy.example:8080",
            },
        )

        checks = validate_p5_runtime_contract(
            env, environ={"HTTP_PROXY": "http://proxy.example:8080"}
        )
        check_map = {check.name: check for check in checks}

        assert check_map["runtime_home_external_to_repo"].ok is False
        assert check_map["proxy_env_absent"].ok is False


@pytest.mark.auth_contract
def test_redacted_env_summary_masks_secret_carrier_values() -> None:
    with _sandbox("carrier_redaction") as sandbox:
        repo_root = sandbox / "repo"
        repo_root.mkdir()
        env = load_p45_env(
            repo_root_path=repo_root,
            environ={
                "ZEPHYR_P45_CLICKHOUSE_URL": "http://user:pass@127.0.0.1:18123",
                "ZEPHYR_P45_WEBHOOK_ECHO_URL": "http://127.0.0.1:18082/ingest",
            },
        )

        summary = format_redacted_env_summary(
            env,
            names=("ZEPHYR_P45_CLICKHOUSE_URL", "ZEPHYR_P45_WEBHOOK_ECHO_URL"),
        )

        assert "user:pass" not in summary
        assert "ZEPHYR_P45_CLICKHOUSE_URL='***'" in summary
        assert "http://127.0.0.1:18082/ingest" in summary


@pytest.mark.auth_contract
def test_p5_runtime_preflight_cli_prints_contract_path_and_runtime_home(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with _sandbox("preflight_cli") as sandbox:
        runtime_home = _build_runtime_home(sandbox)
        monkeypatch.setenv(P45_HOME_ENV_NAME, str(runtime_home))

        exit_code = p5_preflight_main(["--print-contract-path"])
        contract_output = capsys.readouterr()
        assert exit_code == 0
        assert contract_output.out.strip().endswith("validation\\p5_runtime_contract.json")

        exit_code = p5_preflight_main(["--print-runtime-home"])
        runtime_output = capsys.readouterr()
        assert exit_code == 0
        assert runtime_output.out.strip() == str(runtime_home)


@pytest.mark.auth_contract
def test_iter_contract_env_names_includes_secret_carriers_and_saas_groups() -> None:
    names = set(iter_contract_env_names())

    assert "ZEPHYR_P45_MONGODB_URI" in names
    assert "ZEPHYR_P45_OPENSEARCH_URL" in names
    assert "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN" in names
    assert "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN" in names
