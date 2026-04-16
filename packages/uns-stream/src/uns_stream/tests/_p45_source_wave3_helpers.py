from __future__ import annotations

import importlib
import json
import socket
import subprocess
from pathlib import Path
from typing import Protocol, cast

from zephyr_ingest.testing.p45 import LoadedP45Env


def write_http_source_spec(
    path: Path,
    *,
    url: str,
    accept: str | None = None,
    timeout_s: float = 10.0,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "http_document_v1",
            "url": url,
            "timeout_s": timeout_s,
        }
    }
    if accept is not None:
        source = cast(dict[str, object], payload["source"])
        source["accept"] = accept
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_s3_source_spec(
    path: Path,
    *,
    bucket: str,
    key: str,
    region: str,
    access_key: str,
    secret_key: str,
    endpoint_url: str | None,
    session_token: str | None = None,
    version_id: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "s3_document_v1",
            "bucket": bucket,
            "key": key,
            "region": region,
            "access_key": access_key,
            "secret_key": secret_key,
            "endpoint_url": endpoint_url,
            "session_token": session_token,
            "version_id": version_id,
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_git_source_spec(
    path: Path,
    *,
    repo_root: str,
    commit: str,
    relative_path: str,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "git_document_v1",
            "repo_root": repo_root,
            "commit": commit,
            "relative_path": relative_path,
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def reserve_unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http_fixture_base(env: LoadedP45Env) -> str:
    return env.require("ZEPHYR_P45_HTTP_FIXTURE_BASE").rstrip("/")


def s3_endpoint(env: LoadedP45Env, *, port: int | None = None) -> str:
    raw = env.require("ZEPHYR_P45_S3_ENDPOINT").rstrip("/")
    if port is None:
        return raw
    prefix, rest = raw.split("://", 1)
    host = rest.split("/", 1)[0].split(":", 1)[0]
    return f"{prefix}://{host}:{port}"


def s3_bucket(env: LoadedP45Env) -> str:
    return env.require("ZEPHYR_P45_S3_BUCKET")


def s3_region(env: LoadedP45Env) -> str:
    return env.require("ZEPHYR_P45_S3_REGION")


def s3_access_key(env: LoadedP45Env) -> str:
    return env.require("ZEPHYR_P45_S3_ACCESS_KEY")


def s3_secret_key(env: LoadedP45Env) -> str:
    return env.require("ZEPHYR_P45_S3_SECRET_KEY")


class _Boto3S3ClientProtocol(Protocol):
    def list_buckets(self) -> dict[str, object]: ...

    def create_bucket(self, *, Bucket: str) -> object: ...

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> dict[str, object]: ...


def _build_boto3_s3_client(
    env: LoadedP45Env,
    *,
    endpoint_url: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    session_token: str | None = None,
) -> _Boto3S3ClientProtocol:
    boto3 = importlib.import_module("boto3")
    session_module = getattr(boto3, "session")
    session_cls = getattr(session_module, "Session")
    botocore_config_module = importlib.import_module("botocore.config")
    config_cls = getattr(botocore_config_module, "Config")
    session = session_cls(
        aws_access_key_id=s3_access_key(env) if access_key is None else access_key,
        aws_secret_access_key=s3_secret_key(env) if secret_key is None else secret_key,
        aws_session_token=session_token,
        region_name=s3_region(env),
    )
    client = session.client(
        "s3",
        endpoint_url=s3_endpoint(env) if endpoint_url is None else endpoint_url,
        region_name=s3_region(env),
        config=config_cls(connect_timeout=5, read_timeout=5, proxies={}),
    )
    return cast(_Boto3S3ClientProtocol, client)


def seed_minio_text_object(
    env: LoadedP45Env,
    *,
    bucket: str,
    key: str,
    body: str,
    content_type: str = "text/plain",
) -> str | None:
    client = _build_boto3_s3_client(env)
    listed = client.list_buckets()
    bucket_items = cast(list[object], listed.get("Buckets", []))
    bucket_names = {
        cast(dict[str, object], item).get("Name") for item in bucket_items if isinstance(item, dict)
    }
    if bucket not in bucket_names:
        client.create_bucket(Bucket=bucket)
    response = client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType=content_type,
    )
    etag = response.get("ETag")
    return etag if isinstance(etag, str) else None


def init_git_text_repo(
    repo_root: Path,
    *,
    relative_path: str,
    content: str,
) -> str:
    file_path = repo_root / Path(relative_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content, encoding="utf-8")
    _run_git(repo_root=repo_root, args=["init"])
    _run_git(repo_root=repo_root, args=["config", "user.email", "codex@example.test"])
    _run_git(repo_root=repo_root, args=["config", "user.name", "Codex"])
    _run_git(repo_root=repo_root, args=["add", relative_path.replace("\\", "/")])
    _run_git(repo_root=repo_root, args=["commit", "-m", "add report"])
    return _run_git(repo_root=repo_root, args=["rev-parse", "HEAD"]).strip()


def _run_git(*, repo_root: Path, args: list[str]) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git command failed")
    return result.stdout.strip()


def assert_no_secret_fields(metadata: dict[str, object]) -> None:
    for key, value in metadata.items():
        lowered = key.lower()
        assert "secret" not in lowered
        assert "access_key" not in lowered
        assert "session_token" not in lowered
        assert "authorization" not in lowered
        assert "password" not in lowered
        if isinstance(value, str):
            assert "minioadmin" not in value
