from __future__ import annotations

import importlib
import json
import socket
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

from zephyr_ingest.testing.p45 import LoadedP45Env


def as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def read_json_dict(path: Path) -> dict[str, object]:
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    return as_dict(raw)


def reserve_unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def write_http_source_spec(
    path: Path,
    *,
    stream: str,
    url: str,
    cursor_param: str = "cursor",
    query: dict[str, str] | None = None,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "http_json_cursor_v1",
            "stream": stream,
            "url": url,
            "cursor_param": cursor_param,
            "query": {} if query is None else query,
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_postgresql_source_spec(
    path: Path,
    *,
    stream: str,
    connection_name: str,
    dsn: str,
    schema: str,
    table: str,
    columns: Sequence[str],
    cursor_column: str,
    cursor_start: str,
    batch_size: int = 2,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "postgresql_incremental_v1",
            "stream": stream,
            "connection_name": connection_name,
            "dsn": dsn,
            "schema": schema,
            "table": table,
            "columns": list(columns),
            "cursor_column": cursor_column,
            "cursor_start": cursor_start,
            "batch_size": batch_size,
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_clickhouse_source_spec(
    path: Path,
    *,
    stream: str,
    connection_name: str,
    url: str,
    database: str,
    table: str,
    columns: Sequence[str],
    cursor_column: str,
    cursor_start: str,
    username: str,
    password: str,
    batch_size: int = 2,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "clickhouse_incremental_v1",
            "stream": stream,
            "connection_name": connection_name,
            "url": url,
            "database": database,
            "table": table,
            "columns": list(columns),
            "cursor_column": cursor_column,
            "cursor_start": cursor_start,
            "batch_size": batch_size,
            "username": username,
            "password": password,
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def http_fixture_base(env: LoadedP45Env) -> str:
    return env.require("ZEPHYR_P45_HTTP_FIXTURE_BASE").rstrip("/")


def postgresql_dsn(
    env: LoadedP45Env,
    *,
    password: str | None = None,
    port: int | None = None,
    database: str | None = None,
) -> str:
    user = env.require("ZEPHYR_P45_POSTGRES_USER")
    host = env.require("ZEPHYR_P45_POSTGRES_HOST")
    resolved_port = port or int(env.require("ZEPHYR_P45_POSTGRES_PORT"))
    resolved_database = database or env.require("ZEPHYR_P45_POSTGRES_DB")
    resolved_password = password or env.require("ZEPHYR_P45_POSTGRES_PASSWORD")
    return (
        f"postgresql://{quote(user, safe='')}:{quote(resolved_password, safe='')}"
        f"@{host}:{resolved_port}/{quote(resolved_database, safe='')}"
    )


def clickhouse_url(env: LoadedP45Env, *, port: int | None = None) -> str:
    raw_url = env.require("ZEPHYR_P45_CLICKHOUSE_URL")
    split = urlsplit(raw_url)
    resolved_port = split.port if port is None else port
    netloc = split.hostname if resolved_port is None else f"{split.hostname}:{resolved_port}"
    assert netloc is not None
    return urlunsplit((split.scheme, netloc, split.path, split.query, split.fragment))


def clickhouse_http_command(
    env: LoadedP45Env,
    *,
    query: str,
    url: str | None = None,
) -> httpx.Response:
    response = httpx.post(
        (clickhouse_url(env) if url is None else url).rstrip("/") + "/",
        params={
            "database": env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
            "query": query,
        },
        auth=(
            env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
            env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
        ),
        timeout=10.0,
        trust_env=False,
    )
    return response


class _PsycopgCursorProtocol(Protocol):
    def execute(self, query: str, params: Sequence[object] | None = None) -> object: ...

    def close(self) -> object: ...


class _PsycopgConnectionProtocol(Protocol):
    def cursor(self) -> _PsycopgCursorProtocol: ...

    def commit(self) -> object: ...

    def close(self) -> object: ...


def connect_psycopg(dsn: str) -> _PsycopgConnectionProtocol:
    psycopg_module = importlib.import_module("psycopg")
    connect = getattr(psycopg_module, "connect", None)
    assert callable(connect)
    connection = connect(dsn)
    return cast(_PsycopgConnectionProtocol, connection)


def reset_postgresql_cursor_table(
    env: LoadedP45Env,
    *,
    table: str,
    rows: Sequence[tuple[str, str, str]],
) -> None:
    assert table.isidentifier()
    connection = connect_psycopg(postgresql_dsn(env))
    cursor = connection.cursor()
    try:
        cursor.execute(f'DROP TABLE IF EXISTS public."{table}"')
        cursor.execute(
            f'CREATE TABLE public."{table}" ('
            "customer_id TEXT NOT NULL, "
            "doc_cursor TEXT PRIMARY KEY, "
            "name TEXT NOT NULL)"
        )
        for customer_id, doc_cursor, name in rows:
            cursor.execute(
                f'INSERT INTO public."{table}" (customer_id, doc_cursor, name) VALUES (%s, %s, %s)',
                (customer_id, doc_cursor, name),
            )
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def reset_clickhouse_cursor_table(
    env: LoadedP45Env,
    *,
    table: str,
    rows: Sequence[tuple[str, str, str]],
) -> None:
    assert table.isidentifier()
    clickhouse_http_command(env, query=f"DROP TABLE IF EXISTS `{table}`").raise_for_status()
    clickhouse_http_command(
        env,
        query=(
            f"CREATE TABLE `{table}` ("
            "customer_id String, "
            "doc_cursor String, "
            "segment String"
            ") ENGINE = MergeTree "
            "ORDER BY doc_cursor"
        ),
    ).raise_for_status()
    values = ", ".join(
        (f"('{customer_id}','{doc_cursor}','{segment}')")
        for customer_id, doc_cursor, segment in rows
    )
    clickhouse_http_command(
        env,
        query=f"INSERT INTO `{table}` (customer_id, doc_cursor, segment) VALUES {values}",
    ).raise_for_status()
