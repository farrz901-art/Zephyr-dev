from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from zephyr_ingest import cli
from zephyr_ingest.config.snapshot_v1 import ConfigSnapshotV1


def _write_config(path: Path, toml_text: str) -> None:
    path.write_text(toml_text, encoding="utf-8")


def test_config_file_enables_webhook_and_uns_api_env_overrides_file_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[run]
backend = "uns-api"
uns_api_url = "https://api.test.com/v0"
uns_api_timeout_s = 12.5
uns_api_key = "file-key"

[retry]
enabled = false
max_attempts = 1
base_backoff_ms = 0
max_backoff_ms = 0

[destinations.webhook]
url = "http://test.com/hook"
timeout_s = 7.0
max_inflight = 4
rate_limit = 2.5
""".strip(),
    )

    # ENV should override FILE secret when CLI did not explicitly pass --uns-api-key
    monkeypatch.setenv("ZEPHYR_UNS_API_KEY", "env-key")

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True

        # Backend is constructed as HttpUnsApiBackend with api_key applied
        assert cfg.backend is not None
        assert getattr(cfg.backend, "backend") == "uns_api"
        assert getattr(cfg.backend, "url") == "https://api.test.com/v0"
        assert getattr(cfg.backend, "timeout_s") == 12.5
        assert getattr(cfg.backend, "api_key") == "env-key"

        # Destination should be fanout (filesystem + webhook)
        dest = kwargs.get("destination")
        assert dest is not None
        assert getattr(dest, "name") == "fanout"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert snap["schema_version"] == 1
        assert snap["backend"]["kind"] == "uns-api"
        # redacted
        assert snap["backend"]["api_key"] == "***"
        assert "webhook" in snap["destinations"]
        webhook = cast(dict[str, object], snap["destinations"]["webhook"])
        assert webhook["url"] == "http://test.com/hook"
        assert webhook["timeout_s"] == 7.0
        assert webhook["max_inflight"] == 4
        assert webhook["rate_limit"] == 2.5

        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.kind") == "file"
        assert sources.get("backend.api_key") == "env"
        assert sources.get("destinations.webhook.url") == "file"
        assert sources.get("destinations.webhook.timeout_s") == "file"
        assert sources.get("destinations.webhook.max_inflight") == "file"
        assert sources.get("destinations.webhook.rate_limit") == "file"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--glob",
            "*.txt",
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
        ]
    )
    assert rc == 0
    assert called["ok"] is True


def test_cli_flag_overrides_file_field_without_requiring_cli_enable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[destinations.webhook]
url = "http://test.com/hook"
timeout_s = 7.0
""".strip(),
    )

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        assert snap["schema_version"] == 1
        assert "webhook" in snap["destinations"]
        # CLI explicitly overrides timeout_s, while url comes from file
        assert snap["destinations"]["webhook"]["timeout_s"] == 9.0

        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.kind") == "default"
        assert sources.get("destinations.webhook.url") == "file"
        assert sources.get("destinations.webhook.timeout_s") == "cli"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
            "--webhook-timeout-s",
            "9.0",
        ]
    )
    assert rc == 0
    assert called["ok"] is True


def test_cli_secret_overrides_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[run]
backend = "uns-api"
uns_api_url = "https://api.test.com/v0"
uns_api_timeout_s = 12.5
uns_api_key = "file-key"
""".strip(),
    )

    monkeypatch.setenv("ZEPHYR_UNS_API_KEY", "env-key")

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        called["ok"] = True
        assert cfg.backend is not None
        # CLI explicit --uns-api-key must win over ENV and FILE
        assert getattr(cfg.backend, "api_key") == "cli-key"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        sources = snap.get("sources")
        assert sources is not None
        assert isinstance(sources, dict)
        assert sources.get("backend.api_key") == "cli"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
            "--backend",
            "uns-api",
            "--uns-api-url",
            "https://api.test.com/v0",
            "--uns-api-timeout-s",
            "12.5",
            "--uns-api-key",
            "cli-key",
        ]
    )
    assert rc == 0
    assert called["ok"] is True


def test_config_file_enables_second_round_destinations_and_preserves_snapshot_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[destinations.s3]
bucket = "archive"
region = "us-east-1"
access_key = "file-ak"
secret_key = "file-sk"
prefix = "delivery"

[destinations.opensearch]
url = "https://search.example.test"
index = "zephyr-docs"
timeout_s = 4.5

[destinations.clickhouse]
url = "https://clickhouse.example.test"
table = "delivery_rows"
timeout_s = 6.0
""".strip(),
    )

    def _fake_s3_writer_or_exit(**kwargs: object) -> object:
        del kwargs
        return object()

    monkeypatch.setattr(cli, "_make_s3_writer_or_exit", _fake_s3_writer_or_exit)

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        del docs, cfg, ctx
        called["ok"] = True

        dest = kwargs.get("destination")
        assert dest is not None
        assert getattr(dest, "name") == "fanout"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        s3_snapshot = snap["destinations"].get("s3")
        opensearch_snapshot = snap["destinations"].get("opensearch")
        clickhouse_snapshot = snap["destinations"].get("clickhouse")
        assert s3_snapshot is not None
        assert opensearch_snapshot is not None
        assert clickhouse_snapshot is not None
        assert s3_snapshot["bucket"] == "archive"
        assert s3_snapshot["access_key"] == "***"
        assert opensearch_snapshot["timeout_s"] == 4.5
        assert clickhouse_snapshot["timeout_s"] == 6.0

        sources = snap.get("sources")
        assert sources is not None
        assert sources["destinations.s3.bucket"] == "file"
        assert sources["destinations.opensearch.url"] == "file"
        assert sources["destinations.clickhouse.url"] == "file"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
        ]
    )

    assert rc == 0
    assert called["ok"] is True


def test_config_file_enables_mongodb_destination_and_preserves_snapshot_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[destinations.mongodb]
uri = "mongodb://db.example.test"
database = "zephyr"
collection = "delivery_records"
timeout_s = 4.5
""".strip(),
    )

    class _FakeMongoClient:
        def close(self) -> None:
            return None

    def _fake_mongodb_collection_or_exit(**kwargs: object) -> tuple[_FakeMongoClient, object]:
        del kwargs
        return _FakeMongoClient(), object()

    monkeypatch.setattr(cli, "_make_mongodb_collection_or_exit", _fake_mongodb_collection_or_exit)

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        del docs, cfg, ctx
        called["ok"] = True

        dest = kwargs.get("destination")
        assert dest is not None
        assert getattr(dest, "name") == "fanout"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        mongodb_snapshot = snap["destinations"].get("mongodb")
        assert mongodb_snapshot is not None
        assert mongodb_snapshot["uri"] == "mongodb://db.example.test"
        assert mongodb_snapshot["database"] == "zephyr"
        assert mongodb_snapshot["collection"] == "delivery_records"
        assert mongodb_snapshot["timeout_s"] == 4.5

        sources = snap.get("sources")
        assert sources is not None
        assert sources["destinations.mongodb.uri"] == "file"
        assert sources["destinations.mongodb.database"] == "file"
        assert sources["destinations.mongodb.collection"] == "file"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
        ]
    )

    assert rc == 0
    assert called["ok"] is True


def test_config_file_enables_loki_destination_and_preserves_snapshot_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    _write_config(
        cfg_path,
        """
[destinations.loki]
url = "https://logs.example.test"
stream = "delivery"
tenant_id = "tenant-a"
timeout_s = 3.5
""".strip(),
    )

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        del docs, cfg, ctx
        called["ok"] = True

        dest = kwargs.get("destination")
        assert dest is not None
        assert getattr(dest, "name") == "fanout"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        loki_snapshot = snap["destinations"].get("loki")
        assert loki_snapshot is not None
        assert loki_snapshot["url"] == "https://logs.example.test"
        assert loki_snapshot["stream"] == "delivery"
        assert loki_snapshot.get("tenant_id") == "tenant-a"
        assert loki_snapshot["timeout_s"] == 3.5

        sources = snap.get("sources")
        assert sources is not None
        assert sources["destinations.loki.url"] == "file"
        assert sources["destinations.loki.stream"] == "file"
        assert sources["destinations.loki.tenant_id"] == "file"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
        ]
    )

    assert rc == 0
    assert called["ok"] is True


def test_config_file_enables_sqlite_destination_and_preserves_snapshot_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.txt").write_text("hello", encoding="utf-8")

    cfg_path = tmp_path / "cfg.toml"
    sqlite_path = tmp_path / "delivery.sqlite3"
    _write_config(
        cfg_path,
        f"""
[destinations.sqlite]
file_path = "{sqlite_path.as_posix()}"
table_name = "delivery_rows"
timeout_s = 4.5
mode = "replace_upsert"
""".strip(),
    )

    called = {"ok": False}

    def fake_run_documents(*, docs: Any, cfg: Any, ctx: Any, **kwargs: Any) -> Any:
        del docs, cfg, ctx
        called["ok"] = True

        dest = kwargs.get("destination")
        assert dest is not None
        assert getattr(dest, "name") == "fanout"

        snap = cast(ConfigSnapshotV1, kwargs.get("config_snapshot"))
        sqlite_snapshot = snap["destinations"].get("sqlite")
        assert sqlite_snapshot is not None
        assert Path(cast(str, sqlite_snapshot["file_path"])) == sqlite_path
        assert sqlite_snapshot["table_name"] == "delivery_rows"
        assert sqlite_snapshot["timeout_s"] == 4.5
        assert sqlite_snapshot["mode"] == "replace_upsert"

        sources = snap.get("sources")
        assert sources is not None
        assert sources["destinations.sqlite.file_path"] == "file"
        assert sources["destinations.sqlite.table_name"] == "file"
        assert sources["destinations.sqlite.timeout_s"] == "file"
        assert sources["destinations.sqlite.mode"] == "file"

    monkeypatch.setattr(cli, "run_documents", fake_run_documents)

    rc = cli.main(
        [
            "run",
            "--path",
            str(inbox),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(cfg_path),
        ]
    )

    assert rc == 0
    assert called["ok"] is True
