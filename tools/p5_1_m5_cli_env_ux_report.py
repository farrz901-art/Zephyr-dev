from __future__ import annotations

import argparse
import io
import json
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast

from zephyr_ingest import cli as zephyr_cli
from zephyr_ingest.spec.registry import get_spec

REPORT_ID = "zephyr.p5.1.m5.cli_env_ux.v1"
DEFAULT_ENV_FILE = Path(r"E:\zephyr_env\.config\zephyr\p45\env\.env.p45.local")
DEFAULT_HANDOFF_ROOT = Path(".tmp/p5_1_m4_final_handoff")
DEFAULT_M5A_REPORT = Path(".tmp/p5_1_m5_user_facing_boundary/report.json")
DEFAULT_OUT_ROOT = Path(".tmp/p5_1_m5_cli_env_ux")

CATALOG_FILENAME = "catalog_after_m4_final.json"
READINESS_FILENAME = "readiness_after_m4_final.json"
REPAIR_PLAN_FILENAME = "repair_plan_after_m4_final.json"

SOURCE_SPEC_IDS = (
    "source.uns.http_document.v1",
    "source.uns.s3_document.v1",
    "source.uns.git_document.v1",
    "source.uns.google_drive_document.v1",
    "source.uns.confluence_document.v1",
    "source.it.http_json_cursor.v1",
    "source.it.postgresql_incremental.v1",
    "source.it.clickhouse_incremental.v1",
    "source.it.kafka_partition_offset.v1",
    "source.it.mongodb_incremental.v1",
)
REGISTRY_DESTINATION_SPEC_IDS = (
    "destination.webhook.v1",
    "destination.kafka.v1",
    "destination.weaviate.v1",
    "destination.s3.v1",
    "destination.opensearch.v1",
    "destination.clickhouse.v1",
    "destination.mongodb.v1",
    "destination.loki.v1",
    "destination.sqlite.v1",
)
BACKEND_SPEC_IDS = ("backend.uns_api.v1",)
SECRET_FIELD_EXPECTATIONS: dict[str, tuple[str, ...]] = {
    "source.uns.s3_document.v1": (
        "source.access_key",
        "source.secret_key",
        "source.session_token",
    ),
    "source.uns.google_drive_document.v1": ("source.access_token",),
    "source.uns.confluence_document.v1": ("source.access_token",),
    "source.it.postgresql_incremental.v1": ("source.dsn",),
    "source.it.clickhouse_incremental.v1": ("source.password",),
    "source.it.mongodb_incremental.v1": ("source.uri",),
    "destination.s3.v1": (
        "destinations.s3.access_key",
        "destinations.s3.secret_key",
        "destinations.s3.session_token",
    ),
    "destination.opensearch.v1": ("destinations.opensearch.password",),
    "destination.clickhouse.v1": ("destinations.clickhouse.password",),
    "destination.mongodb.v1": (
        "destinations.mongodb.uri",
        "destinations.mongodb.password",
    ),
    "backend.uns_api.v1": ("backend.api_key",),
}
SECRET_FIELD_NOT_APPLICABLE = {
    "destination.webhook.v1": (
        "No explicit secret-bearing auth field is modeled in the retained webhook spec."
    ),
    "destination.kafka.v1": (
        "No retained Kafka auth field is modeled in the current registry surface."
    ),
    "destination.loki.v1": (
        "No explicit secret-bearing auth field is modeled in the retained Loki spec."
    ),
}
OPERATIONAL_NOTES = (
    (
        "PowerShell `>` may write UTF-16; prefer tool-managed report.json writes "
        "or `Set-Content -Encoding utf8`."
    ),
    (
        "If an output directory does not exist, redirection can fail before the "
        "CLI runs; create it first with `New-Item -ItemType Directory -Force`."
    ),
    (
        "Windows excluded port ranges can block Docker binds; use a different port "
        "or adjust the runtime substrate rather than blaming connector code."
    ),
    (
        "S3 bucket missing is a runtime substrate initialization problem, not a "
        "connector code issue."
    ),
    (
        "Google Drive access tokens can expire after roughly one hour; classify "
        "401/403 as auth/token readiness issues."
    ),
    "backend.uns_api.v1 skip_env is backend-only and does not affect the retained 10×10 matrix.",
)


class SpecCheck(TypedDict):
    spec_id: str
    return_code: int
    field_count: int
    required_field_count: int
    secret_field_count: int
    human_readable: bool
    issue: str


class SecretExpectationStatus(TypedDict):
    status: str
    fields: list[str]
    issue: str


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8", "utf-8-sig", "utf-16"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError as err:
            last_error = err
    assert last_error is not None
    raise last_error


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(_read_text(path))
    if not isinstance(loaded_obj, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], loaded_obj)


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return cast(list[object], value)


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


def _as_int(value: object) -> int:
    if not isinstance(value, int):
        raise ValueError(f"Expected int, got {type(value).__name__}")
    return value


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _bool_word(value: bool) -> str:
    return "yes" if value else "no"


def _source_file_contains(path: Path, needle: str) -> bool:
    return needle in _read_text(path)


def _run_spec_show(spec_id: str) -> tuple[int, str]:
    output = io.StringIO()
    with redirect_stdout(output):
        rc = zephyr_cli.main(["spec", "show", "--id", spec_id, "--format", "toml"])
    return rc, output.getvalue()


def _spec_check(spec_id: str) -> SpecCheck:
    spec = get_spec(spec_id=spec_id)
    if spec is None:
        return {
            "spec_id": spec_id,
            "return_code": 2,
            "field_count": 0,
            "required_field_count": 0,
            "secret_field_count": 0,
            "human_readable": False,
            "issue": "spec_missing",
        }
    fields = cast(list[dict[str, object]], spec["fields"])
    rc, output = _run_spec_show(spec_id)
    field_count = len(fields)
    required_field_count = sum(1 for field in fields if field.get("required") is True)
    secret_field_count = sum(1 for field in fields if field.get("secret") is True)
    human_readable = rc == 0 and field_count > 0 and output.strip() != ""
    issue = ""
    if rc != 0:
        issue = f"cli_return_code_{rc}"
    elif field_count == 0:
        issue = "empty_fields"
    elif not human_readable:
        issue = "not_human_readable"
    return {
        "spec_id": spec_id,
        "return_code": rc,
        "field_count": field_count,
        "required_field_count": required_field_count,
        "secret_field_count": secret_field_count,
        "human_readable": human_readable,
        "issue": issue,
    }


def _secret_field_status(spec_id: str) -> SecretExpectationStatus:
    spec = get_spec(spec_id=spec_id)
    if spec is None:
        return {"status": "missing_spec", "fields": [], "issue": "spec_missing"}
    fields = cast(list[dict[str, object]], spec["fields"])
    secret_names = {field["name"] for field in fields if field.get("secret") is True}
    expected = SECRET_FIELD_EXPECTATIONS.get(spec_id)
    if expected is None:
        note = SECRET_FIELD_NOT_APPLICABLE.get(spec_id)
        if note is not None:
            return {"status": "not_applicable", "fields": [], "issue": note}
        return {"status": "not_applicable", "fields": [], "issue": "no_secret_expectation"}
    missing = [field_name for field_name in expected if field_name not in secret_names]
    if missing:
        return {
            "status": "fail",
            "fields": list(expected),
            "issue": f"missing_secret_markers:{', '.join(missing)}",
        }
    return {"status": "pass", "fields": list(expected), "issue": ""}


def _collect_secret_field_names() -> list[str]:
    names: list[str] = []
    for spec_id in (*SOURCE_SPEC_IDS, *REGISTRY_DESTINATION_SPEC_IDS, *BACKEND_SPEC_IDS):
        spec = get_spec(spec_id=spec_id)
        if spec is None:
            continue
        fields = cast(list[dict[str, object]], spec["fields"])
        for field in fields:
            if field.get("secret") is True:
                names.append(cast(str, field["name"]))
    return sorted(set(names))


def _build_error_taxonomy() -> dict[str, bool]:
    readiness_tool = Path("tools/p5_connector_readiness.py")
    s3_tool = Path("tools/p5_s3_readiness_diagnose.py")
    return {
        "missing_env_classifiable": (
            _source_file_contains(readiness_tool, '"skip_env"')
            and _source_file_contains(readiness_tool, "required_env_missing")
        ),
        "dependency_missing_classifiable": _source_file_contains(
            readiness_tool, '"fail_dependency"'
        ),
        "service_unreachable_classifiable": _source_file_contains(
            readiness_tool, '"fail_service"'
        ),
        "bucket_missing_classifiable": _source_file_contains(
            readiness_tool, "s3_bucket_missing"
        )
        and _source_file_contains(s3_tool, '"bucket_missing"'),
        "auth_permission_failure_classifiable": _source_file_contains(
            s3_tool, '"auth_failed"'
        )
        and _source_file_contains(s3_tool, '"permission_failed"'),
        "token_expired_or_401_403_classifiable": _source_file_contains(
            s3_tool, "ExpiredToken"
        )
        and _source_file_contains(
            Path("packages/zephyr-ingest/src/zephyr_ingest/destinations/weaviate.py"),
            "{401, 403}",
        ),
        "windows_port_exclusion_note_present": any(
            "excluded port ranges" in note.lower() for note in OPERATIONAL_NOTES
        ),
        "powershell_encoding_note_present": any(
            "utf-16" in note.lower() or "set-content -encoding utf8" in note.lower()
            for note in OPERATIONAL_NOTES
        ),
    }


def build_report(
    *,
    env_file: Path,
    handoff_root: Path,
    m5a_report_path: Path,
    out_root: Path,
) -> dict[str, object]:
    catalog = _load_json_object(handoff_root / CATALOG_FILENAME)
    readiness = _load_json_object(handoff_root / READINESS_FILENAME)
    repair_plan = _load_json_object(handoff_root / REPAIR_PLAN_FILENAME)
    m5a_report = _load_json_object(m5a_report_path)

    source_checks = [_spec_check(spec_id) for spec_id in SOURCE_SPEC_IDS]
    destination_checks = [_spec_check(spec_id) for spec_id in REGISTRY_DESTINATION_SPEC_IDS]
    backend_checks = [_spec_check(spec_id) for spec_id in BACKEND_SPEC_IDS]
    secret_expectation_statuses = {
        spec_id: _secret_field_status(spec_id)
        for spec_id in (*SOURCE_SPEC_IDS, *REGISTRY_DESTINATION_SPEC_IDS, *BACKEND_SPEC_IDS)
    }
    readiness_summary = _as_dict(readiness["summary"])
    m5a_summary = _as_dict(m5a_report["summary"])
    catalog_summary = _as_dict(catalog["summary"])
    error_taxonomy = _build_error_taxonomy()

    source_specs_readable = sum(1 for item in source_checks if item["human_readable"])
    destination_registry_specs_readable = sum(
        1 for item in destination_checks if item["human_readable"]
    )
    destination_specs_readable = destination_registry_specs_readable + 1
    source_specs_empty = sum(1 for item in source_checks if item["field_count"] == 0)
    destination_specs_empty = sum(1 for item in destination_checks if item["field_count"] == 0)

    issues: list[dict[str, object]] = []
    if _as_int(m5a_summary["active_blockers"]) != 0:
        issues.append(
            {
                "severity": "blocker",
                "area": "m5a_consistency",
                "message": "M5-A active_blockers is not zero.",
            }
        )
    if source_specs_readable != len(SOURCE_SPEC_IDS):
        issues.append(
            {
                "severity": "major",
                "area": "source_spec_show",
                "message": "Not all retained source specs are readable via spec show.",
            }
        )
    if destination_registry_specs_readable != len(REGISTRY_DESTINATION_SPEC_IDS):
        issues.append(
            {
                "severity": "major",
                "area": "destination_spec_show",
                "message": (
                    "Not all retained registry destination specs are readable via spec show."
                ),
            }
        )
    failed_secret_specs = [
        spec_id
        for spec_id, status in secret_expectation_statuses.items()
        if status["status"] == "fail"
    ]
    if failed_secret_specs:
        issues.append(
            {
                "severity": "major",
                "area": "secret_safety",
                "message": "Some expected secret-bearing fields are not marked secret.",
                "details": failed_secret_specs,
            }
        )
    failed_taxonomy = [name for name, value in error_taxonomy.items() if not value]
    if failed_taxonomy:
        issues.append(
            {
                "severity": "major",
                "area": "error_taxonomy",
                "message": "Error taxonomy UX coverage is incomplete.",
                "details": failed_taxonomy,
            }
        )

    active_blockers = sum(1 for issue in issues if issue["severity"] == "blocker")
    major_ux_gaps = sum(1 for issue in issues if issue["severity"] == "major")
    overall = "pass"
    if active_blockers > 0:
        overall = "fail"
    elif major_ux_gaps > 0:
        overall = "conditional"

    report: dict[str, object] = {
        "schema_version": 1,
        "report_id": REPORT_ID,
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "active_blockers": active_blockers,
            "major_ux_gaps": major_ux_gaps,
            "minor_notes": len(OPERATIONAL_NOTES),
        },
        "inputs": {
            "env_file": env_file.as_posix(),
            "handoff_root": handoff_root.as_posix(),
            "m5a_report": m5a_report_path.as_posix(),
            "catalog_loaded": True,
            "readiness_loaded": True,
            "repair_plan_loaded": True,
        },
        "spec_show": {
            "source_count": len(SOURCE_SPEC_IDS),
            "destination_count": 10,
            "source_specs_readable": source_specs_readable,
            "destination_specs_readable": destination_specs_readable,
            "destination_registry_specs_readable": destination_registry_specs_readable,
            "source_specs_empty": source_specs_empty,
            "destination_specs_empty": destination_specs_empty,
            "source_checks": source_checks,
            "destination_checks": destination_checks,
            "backend_checks": backend_checks,
            "filesystem_note": (
                "filesystem is an actual retained destination but not a registry spec id; "
                "treat it as a destination note, not a spec show failure."
            ),
            "backend_note": (
                "backend.uns_api.v1 remains backend-only and is excluded from destination_count."
            ),
        },
        "secret_safety": {
            "secret_fields_detected": _collect_secret_field_names(),
            "secret_values_printed": False,
            "access_token_marked_secret": all(
                secret_expectation_statuses[spec_id]["status"] == "pass"
                for spec_id in (
                    "source.uns.google_drive_document.v1",
                    "source.uns.confluence_document.v1",
                )
            ),
            "password_or_secret_key_marked_secret": all(
                secret_expectation_statuses[spec_id]["status"] == "pass"
                for spec_id in (
                    "source.uns.s3_document.v1",
                    "source.it.clickhouse_incremental.v1",
                    "destination.s3.v1",
                    "destination.opensearch.v1",
                    "destination.clickhouse.v1",
                )
            ),
            "uri_may_contain_credentials_marked_secret": all(
                secret_expectation_statuses[spec_id]["status"] == "pass"
                for spec_id in (
                    "source.it.mongodb_incremental.v1",
                    "destination.mongodb.v1",
                )
            ),
            "expectations": secret_expectation_statuses,
        },
        "env_readiness_ux": {
            "readiness_summary_loaded": True,
            "readiness_fail_count": _as_int(readiness_summary["fail_count"]),
            "readiness_skip_count": _as_int(readiness_summary["skip_count"]),
            "backend_uns_api_skip_env_non_blocking": (
                _as_int(readiness_summary["skip_count"]) == 1
                and _as_int(_as_dict(catalog_summary)["backend_count"]) == 1
                and any(
                    "backend-only" in _as_str(note["message"])
                    for note in _as_list(repair_plan["non_blocking_notes"])
                    if isinstance(note, dict)
                )
            ),
            "readiness_pass_not_product_direct_pass": _as_bool(
                readiness_summary["readiness_pass_does_not_imply_product_direct_pass"]
            ),
        },
        "error_taxonomy": error_taxonomy,
        "cli_surfaces": {
            "spec_show_usable": source_specs_readable == len(SOURCE_SPEC_IDS)
            and destination_registry_specs_readable == len(REGISTRY_DESTINATION_SPEC_IDS),
            "catalog_usable": _as_int(_as_dict(catalog_summary)["source_count"]) == 10,
            "readiness_usable": _as_int(readiness_summary["fail_count"]) == 0,
            "s3_diagnose_usable": Path("tools/p5_s3_readiness_diagnose.py").exists(),
            "substrate_healthcheck_usable": Path("tools/p45_substrate_healthcheck.py").exists(),
        },
        "issues": issues,
        "non_blocking_notes": list(OPERATIONAL_NOTES),
    }
    return report


def render_markdown(report: dict[str, object]) -> str:
    summary = _as_dict(report["summary"])
    spec_show = _as_dict(report["spec_show"])
    secret_safety = _as_dict(report["secret_safety"])
    env_readiness_ux = _as_dict(report["env_readiness_ux"])
    error_taxonomy = _as_dict(report["error_taxonomy"])
    secret_fields_marked = (
        _as_bool(secret_safety["access_token_marked_secret"])
        and _as_bool(secret_safety["password_or_secret_key_marked_secret"])
        and _as_bool(secret_safety["uri_may_contain_credentials_marked_secret"])
    )
    lines = [
        "# Zephyr P5.1 CLI / spec / env UX sanity report",
        "",
        "## Final judgment",
        f"- overall: {_as_str(summary['overall'])}",
        f"- active blockers: {_as_int(summary['active_blockers'])}",
        f"- major UX gaps: {_as_int(summary['major_ux_gaps'])}",
        "",
        "## Spec show UX",
        f"- 10 source specs readable: {_as_int(spec_show['source_specs_readable'])}",
        (
            "- 9 registry destination specs readable: "
            f"{_as_int(spec_show['destination_registry_specs_readable'])}"
        ),
        f"- filesystem destination note: {_as_str(spec_show['filesystem_note'])}",
        f"- backend.uns_api.v1 backend-only note: {_as_str(spec_show['backend_note'])}",
        "",
        "## Secret safety",
        (
            "- secret values printed: "
            f"{_bool_word(_as_bool(secret_safety['secret_values_printed']))}"
        ),
        (
            "- secret fields marked: "
            f"{_bool_word(secret_fields_marked)}"
        ),
        (
            "- known secret-bearing fields: "
            + ", ".join(cast(list[str], secret_safety["secret_fields_detected"]))
        ),
        "",
        "## Readiness / env UX",
        f"- readiness fail count: {_as_int(env_readiness_ux['readiness_fail_count'])}",
        f"- readiness skip count: {_as_int(env_readiness_ux['readiness_skip_count'])}",
        (
            "- backend.uns_api.v1 skip_env non-blocking: "
            f"{_bool_word(_as_bool(env_readiness_ux['backend_uns_api_skip_env_non_blocking']))}"
        ),
        (
            "- readiness pass not product direct pass: "
            f"{_bool_word(_as_bool(env_readiness_ux['readiness_pass_not_product_direct_pass']))}"
        ),
        "",
        "## Error taxonomy",
        (
            "- missing env: "
            f"{_bool_word(_as_bool(error_taxonomy['missing_env_classifiable']))}"
        ),
        (
            "- dependency missing: "
            f"{_bool_word(_as_bool(error_taxonomy['dependency_missing_classifiable']))}"
        ),
        (
            "- service unreachable: "
            f"{_bool_word(_as_bool(error_taxonomy['service_unreachable_classifiable']))}"
        ),
        (
            "- S3 bucket missing: "
            f"{_bool_word(_as_bool(error_taxonomy['bucket_missing_classifiable']))}"
        ),
        (
            "- auth/permission: "
            f"{_bool_word(_as_bool(error_taxonomy['auth_permission_failure_classifiable']))}"
        ),
        (
            "- token expired 401/403: "
            f"{_bool_word(_as_bool(error_taxonomy['token_expired_or_401_403_classifiable']))}"
        ),
        (
            "- Windows port exclusion: "
            f"{_bool_word(_as_bool(error_taxonomy['windows_port_exclusion_note_present']))}"
        ),
        (
            "- PowerShell encoding/path: "
            f"{_bool_word(_as_bool(error_taxonomy['powershell_encoding_note_present']))}"
        ),
        "",
        "## User-facing operational notes",
    ]
    for note in cast(list[str], report["non_blocking_notes"]):
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def _emit_outputs(report: dict[str, object], out_root: Path) -> tuple[Path, Path]:
    out_root.mkdir(parents=True, exist_ok=True)
    json_path = out_root / "report.json"
    md_path = out_root / "report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def _validate_report(report: dict[str, object], out_root: Path) -> list[str]:
    issues: list[str] = []
    summary = _as_dict(report["summary"])
    spec_show = _as_dict(report["spec_show"])
    secret_safety = _as_dict(report["secret_safety"])
    env_readiness_ux = _as_dict(report["env_readiness_ux"])
    error_taxonomy = _as_dict(report["error_taxonomy"])
    if not (out_root / "report.json").exists():
        issues.append("report_json_missing")
    if not (out_root / "report.md").exists():
        issues.append("report_md_missing")
    if _as_str(summary["overall"]) == "fail":
        issues.append("overall_fail")
    if _as_int(spec_show["source_specs_readable"]) != 10:
        issues.append("source_specs_not_readable")
    if _as_bool(secret_safety["secret_values_printed"]):
        issues.append("secret_values_printed")
    if not _as_bool(env_readiness_ux["readiness_pass_not_product_direct_pass"]):
        issues.append("readiness_note_missing")
    if not all(_as_bool(value) for value in error_taxonomy.values()):
        issues.append("taxonomy_gap_present")
    return issues


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p5_1_m5_cli_env_ux_report",
        description="Render or validate the P5.1-M5-B CLI/spec/env UX sanity report.",
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--handoff-root", type=Path, default=DEFAULT_HANDOFF_ROOT)
    parser.add_argument(
        "--m5a-report",
        type=Path,
        default=DEFAULT_M5A_REPORT,
    )
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--json", action="store_true")
    mode_group.add_argument("--markdown", action="store_true")
    mode_group.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = build_report(
        env_file=args.env_file,
        handoff_root=args.handoff_root,
        m5a_report_path=args.m5a_report,
        out_root=args.out_root,
    )
    _emit_outputs(report, args.out_root)
    if args.check_artifacts:
        issues = _validate_report(report, args.out_root)
        if issues:
            print("\n".join(issues))
            return 1
        print("p5_1_m5_cli_env_ux_report -> ok")
        return 0
    if args.markdown:
        print(render_markdown(report), end="")
        return 0
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
