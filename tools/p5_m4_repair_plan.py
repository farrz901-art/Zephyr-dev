from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

Category = Literal[
    "fail_dependency",
    "fail_service",
    "fail_missing_implementation",
    "skip_env",
    "source_spec_fields_empty",
    "delivery_payload_metadata_visibility_gap",
    "remote_only_content_evidence_gap",
    "representative_route_blocked",
    "user_seeded_marker_gap",
]
EvidenceSource = Literal[
    "readiness",
    "destination_evidence",
    "representative_nm",
    "catalog",
    "gap_summary",
]
Priority = Literal["P0", "P1", "P2", "P3"]
Severity = Literal["blocker", "major", "minor", "note"]


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json_text(path: Path) -> str:
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
    raw_obj: object = json.loads(_read_json_text(path))
    if not isinstance(raw_obj, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], raw_obj)


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


def _bool_str(value: bool) -> str:
    return "yes" if value else "no"


def _markdown_lines(items: list[str]) -> str:
    if not items:
        return "-"
    return "<br>".join(items)


def _spec_required_fields(source_id: str) -> list[str]:
    mapping: dict[str, list[str]] = {
        "source.uns.http_document.v1": ["source.kind", "source.url"],
        "source.uns.s3_document.v1": [
            "source.kind",
            "source.bucket",
            "source.key",
            "source.region",
            "source.access_key",
            "source.secret_key",
            "source.endpoint_url",
        ],
        "source.uns.git_document.v1": [
            "source.kind",
            "source.repo_root",
            "source.commit",
            "source.relative_path",
        ],
        "source.uns.google_drive_document.v1": [
            "source.kind",
            "source.file_id",
            "source.access_token",
            "source.acquisition_mode",
        ],
        "source.uns.confluence_document.v1": [
            "source.kind",
            "source.site_url",
            "source.page_id",
            "source.access_token",
        ],
        "source.it.http_json_cursor.v1": [
            "source.kind",
            "source.stream",
            "source.url",
            "source.cursor_param",
            "source.query",
        ],
        "source.it.postgresql_incremental.v1": [
            "source.kind",
            "source.stream",
            "source.connection_name",
            "source.dsn",
            "source.schema",
            "source.table",
            "source.columns",
            "source.cursor_column",
            "source.cursor_start",
        ],
        "source.it.clickhouse_incremental.v1": [
            "source.kind",
            "source.stream",
            "source.connection_name",
            "source.url",
            "source.database",
            "source.table",
            "source.columns",
            "source.cursor_column",
            "source.cursor_start",
            "source.username",
            "source.password",
        ],
        "source.it.kafka_partition_offset.v1": [
            "source.kind",
            "source.stream",
            "source.connection_name",
            "source.brokers",
            "source.topic",
            "source.partition",
            "source.batch_size",
        ],
        "source.it.mongodb_incremental.v1": [
            "source.kind",
            "source.stream",
            "source.connection_name",
            "source.uri",
            "source.database",
            "source.collection",
            "source.fields",
            "source.cursor_field",
            "source.batch_size",
        ],
    }
    return mapping[source_id]


def _readiness_map(readiness: dict[str, object]) -> dict[str, dict[str, object]]:
    records = _as_list(readiness["connectors"])
    return {_as_str(_as_dict(item)["id"]): _as_dict(item) for item in records}


def _catalog_map(catalog: dict[str, object]) -> dict[str, dict[str, object]]:
    records = _as_list(catalog["connectors"])
    return {_as_str(_as_dict(item)["id"]): _as_dict(item) for item in records}


def _summary_from_gap_summary(gap_summary: dict[str, object]) -> dict[str, int]:
    if "readiness_fail_dependency" in gap_summary:
        return {
            "readiness_fail_dependency": _as_int(gap_summary["readiness_fail_dependency"]),
            "readiness_fail_service": _as_int(gap_summary["readiness_fail_service"]),
            "readiness_missing_implementation": _as_int(
                gap_summary["readiness_missing_implementation"]
            ),
            "readiness_skip_env": _as_int(gap_summary["readiness_skip_env"]),
            "remote_only_content_evidence_gap": _as_int(
                gap_summary["remote_only_content_evidence_gap"]
            ),
            "artifact_reference_only_v1": _as_int(
                gap_summary["artifact_reference_only_v1"]
            ),
            "destination_evidence_failed": _as_int(
                gap_summary["destination_evidence_failed"]
            ),
            "nm_blocked": _as_int(gap_summary["nm_blocked"]),
            "nm_failed_executed": _as_int(gap_summary["nm_failed_executed"]),
            "nm_passed": _as_int(gap_summary["nm_passed"]),
            "source_spec_fields_empty": _as_int(
                gap_summary.get("source_spec_fields_empty", 0)
            ),
            "backend_counted_as_destination": _as_int(
                gap_summary.get("backend_counted_as_destination", 0)
            ),
        }

    readiness = _as_dict(gap_summary["readiness"])
    destination_evidence = _as_dict(gap_summary["destination_evidence"])
    representative_nm = _as_dict(gap_summary["representative_nm"])
    return {
        "readiness_fail_dependency": len(_as_list(readiness["fail_dependency"])),
        "readiness_fail_service": len(_as_list(readiness["fail_service"])),
        "readiness_missing_implementation": len(
            _as_list(readiness["fail_missing_implementation"])
        ),
        "readiness_skip_env": len(_as_list(readiness["skip_env"])),
        "remote_only_content_evidence_gap": len(
            _as_list(destination_evidence["remote_only_content_evidence_gap"])
        ),
        "artifact_reference_only_v1": len(
            _as_list(destination_evidence["artifact_reference_only_v1"])
        ),
        "destination_evidence_failed": len(_as_list(destination_evidence["failed"])),
        "nm_blocked": len(_as_list(representative_nm["blocked"])),
        "nm_failed_executed": len(_as_list(representative_nm["failed_executed"])),
        "nm_passed": len(_as_list(representative_nm["passed"])),
        "source_spec_fields_empty": 0,
        "backend_counted_as_destination": 0,
    }


def _list_readiness_by_reason(
    readiness: dict[str, object],
    reason: str,
) -> list[dict[str, object]]:
    records = [_as_dict(item) for item in _as_list(readiness["connectors"])]
    return [record for record in records if record.get("reason") == reason]


def _list_destination_evidence_results(report: dict[str, object]) -> list[dict[str, object]]:
    return [_as_dict(item) for item in _as_list(report["results"])]


def _list_representative_routes(report: dict[str, object]) -> list[dict[str, object]]:
    return [_as_dict(item) for item in _as_list(report["routes"])]


def _routes_for_connector(
    routes: list[dict[str, object]],
    connector_id: str,
) -> list[str]:
    results: list[str] = []
    for route in routes:
        source_id = _as_str(route["source_id"])
        destination_id = _as_str(route["destination_id"])
        if source_id != connector_id and destination_id != connector_id:
            continue
        results.append(f"{source_id} -> {destination_id} [{_as_str(route['mode'])}]")
    return results


def _severity_for_reason(reason: str) -> Severity:
    mapping: dict[str, Severity] = {
        "fail_dependency": "major",
        "fail_service": "major",
        "fail_missing_implementation": "blocker",
        "skip_env": "minor",
    }
    return mapping.get(reason, "note")


def _priority_for_reason(reason: str) -> Priority:
    mapping: dict[str, Priority] = {
        "fail_missing_implementation": "P0",
        "fail_dependency": "P1",
        "fail_service": "P1",
        "skip_env": "P3",
    }
    return mapping.get(reason, "P3")


def _severity_rank(severity: Severity) -> int:
    return {
        "blocker": 0,
        "major": 1,
        "minor": 2,
        "note": 3,
    }[severity]


def _priority_rank(priority: Priority) -> int:
    return {
        "P0": 0,
        "P1": 1,
        "P2": 2,
        "P3": 3,
    }[priority]


def _all_repair_items(repair_groups: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for group_items in repair_groups.values():
        items.extend(group_items)
    return items


def _is_backend_connector(
    connector_id: str,
    catalog_map: dict[str, dict[str, object]],
) -> bool:
    record = catalog_map.get(connector_id)
    return record is not None and record.get("family") == "backend"


def _validation_command_for_connector(connector_id: str) -> str:
    return (
        "uv run --locked --no-sync python tools/p5_connector_readiness.py "
        "--env-file E:\\zephyr_env\\.config\\zephyr\\p45\\env\\.env.p45.local --json"
    )


def _representative_smoke_command() -> str:
    return (
        "uv run --locked --no-sync python tools/p5_nm_representative_smoke.py "
        "--env-file E:\\zephyr_env\\.config\\zephyr\\p45\\env\\.env.p45.local "
        "--out-root .tmp/p5_1_nm_representative --mode both --json"
    )


def _destination_evidence_command(*, flow: str, dest: str | None = None) -> str:
    command = (
        "uv run --locked --no-sync python tools/p5_destination_evidence_audit.py "
        "--env-file E:\\zephyr_env\\.config\\zephyr\\p45\\env\\.env.p45.local "
        "--out-root .tmp/p5_1_destination_evidence --mode both "
        f"--flow {flow}"
    )
    if dest is not None:
        command += f" --dest {dest}"
    return f"{command} --json"


def _item(
    *,
    item_id: str,
    category: Category,
    affected_connectors: list[str],
    affected_routes: list[str],
    evidence_source: list[EvidenceSource],
    current_status: str,
    user_impact: str,
    root_cause_hypothesis: str,
    recommended_fix: str,
    validation_after_fix: list[str],
    priority: Priority,
    severity: Severity,
    safe_to_fix_now: bool,
    requires_runtime_env_change: bool,
    requires_code_change: bool,
    requires_dependency_change: bool,
    requires_connector_contract_change: bool,
    notes: list[str],
) -> dict[str, object]:
    return {
        "id": item_id,
        "category": category,
        "affected_connectors": affected_connectors,
        "affected_routes": affected_routes,
        "evidence_source": evidence_source,
        "current_status": current_status,
        "user_impact": user_impact,
        "root_cause_hypothesis": root_cause_hypothesis,
        "recommended_fix": recommended_fix,
        "validation_after_fix": validation_after_fix,
        "priority": priority,
        "severity": severity,
        "safe_to_fix_now": safe_to_fix_now,
        "requires_runtime_env_change": requires_runtime_env_change,
        "requires_code_change": requires_code_change,
        "requires_dependency_change": requires_dependency_change,
        "requires_connector_contract_change": requires_connector_contract_change,
        "notes": notes,
    }


def _build_m4_a(
    *,
    readiness: dict[str, object],
    representative_routes: list[dict[str, object]],
    readiness_map: dict[str, dict[str, object]],
    catalog_map: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for record in _list_readiness_by_reason(readiness, "fail_dependency"):
        connector_id = _as_str(record["id"])
        dependency_missing = [_as_str(value) for value in _as_list(record["dependency_missing"])]
        items.append(
            _item(
                item_id=f"M4-A-{connector_id.replace('.', '-')}",
                category="fail_dependency",
                affected_connectors=[connector_id],
                affected_routes=_routes_for_connector(representative_routes, connector_id),
                evidence_source=["readiness", "representative_nm", "gap_summary"],
                current_status=f"readiness fail_dependency: {', '.join(dependency_missing)}",
                user_impact=(
                    "Representative routes stay blocked before any real "
                    "source-to-destination execution."
                ),
                root_cause_hypothesis=(
                    "Workspace dependency surface is incomplete for this "
                    "retained connector."
                ),
                recommended_fix=(
                    f"Add/install durable Python dependency for {connector_id} "
                    "and lock the workspace "
                    "surface rather than relying on ad-hoc local installs."
                ),
                validation_after_fix=[
                    _validation_command_for_connector(connector_id),
                    _representative_smoke_command(),
                ],
                priority=_priority_for_reason("fail_dependency"),
                severity=_severity_for_reason("fail_dependency"),
                safe_to_fix_now=True,
                requires_runtime_env_change=False,
                requires_code_change=False,
                requires_dependency_change=True,
                requires_connector_contract_change=False,
                notes=[
                    "Do not classify this as a business-flow failure.",
                    "Known current case: Mongo source/destination both fail on missing pymongo.",
                ],
            )
        )

    for record in _list_readiness_by_reason(readiness, "fail_service"):
        connector_id = _as_str(record["id"])
        detail = _as_str(record["detail"])
        items.append(
            _item(
                item_id=f"M4-A-{connector_id.replace('.', '-')}",
                category="fail_service",
                affected_connectors=[connector_id],
                affected_routes=_routes_for_connector(representative_routes, connector_id),
                evidence_source=["readiness", "representative_nm", "gap_summary"],
                current_status=f"readiness fail_service: {detail}",
                user_impact=(
                    "Connector is retained in matrix but cannot yet "
                    "complete representative user-visible execution."
                ),
                root_cause_hypothesis=(
                    "Runtime substrate or external service state is not "
                    "ready, even though the connector implementation exists."
                ),
                recommended_fix=(
                    "Repair runtime/service readiness for this connector, "
                    "then re-run readiness and representative route checks."
                ),
                validation_after_fix=[
                    _validation_command_for_connector(connector_id),
                    _representative_smoke_command(),
                ],
                priority=_priority_for_reason("fail_service"),
                severity=_severity_for_reason("fail_service"),
                safe_to_fix_now=True,
                requires_runtime_env_change=True,
                requires_code_change=False,
                requires_dependency_change=False,
                requires_connector_contract_change=False,
                notes=[
                    "Do not classify service readiness failure as connector "
                    "implementation failure.",
                    "Known current cases include S3 bucket missing, Google "
                    "Drive/Confluence source live probe failures, and "
                    "PostgreSQL connectivity failure.",
                ],
            )
        )

    for record in _list_readiness_by_reason(readiness, "skip_env"):
        connector_id = _as_str(record["id"])
        if _is_backend_connector(connector_id, catalog_map):
            continue
        missing_env = [_as_str(value) for value in _as_list(record["required_env_missing"])]
        items.append(
            _item(
                item_id=f"M4-A-{connector_id.replace('.', '-')}",
                category="skip_env",
                affected_connectors=[connector_id],
                affected_routes=_routes_for_connector(representative_routes, connector_id),
                evidence_source=["readiness", "gap_summary"],
                current_status=f"readiness skip_env: {', '.join(missing_env)}",
                user_impact=(
                    "Not a current product failure, but the bounded backend "
                    "surface is not ready for live use in this env."
                ),
                root_cause_hypothesis=(
                    "Required backend env is intentionally absent in the "
                    "authoritative env file."
                ),
                recommended_fix=(
                    "Only add the missing env if backend.uns_api.v1 is "
                    "intentionally retained for this phase; otherwise keep "
                    "it explicitly skipped."
                ),
                validation_after_fix=[_validation_command_for_connector(connector_id)],
                priority=_priority_for_reason("skip_env"),
                severity=_severity_for_reason("skip_env"),
                safe_to_fix_now=True,
                requires_runtime_env_change=True,
                requires_code_change=False,
                requires_dependency_change=False,
                requires_connector_contract_change=False,
                notes=["Skip must remain distinct from pass."],
            )
        )
    return items


def _build_m4_b(
    *,
    representative_routes: list[dict[str, object]],
    readiness_map: dict[str, dict[str, object]],
    catalog_map: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    connector_id = "destination.sqlite.v1"
    readiness_record = readiness_map.get(connector_id)
    catalog_record = catalog_map.get(connector_id)
    sqlite_routes = _routes_for_connector(representative_routes, connector_id)
    sqlite_blocked = any(
        route.get("readiness_status") == "blocked"
        and _as_str(route["destination_id"]) == connector_id
        for route in representative_routes
    )
    readiness_missing = (
        readiness_record is not None
        and readiness_record.get("reason") == "fail_missing_implementation"
    )
    catalog_missing = (
        catalog_record is None
        or catalog_record.get("implementation_status") == "missing_or_unregistered"
    )
    if not readiness_missing and not catalog_missing and not sqlite_blocked:
        return []

    current_status_parts: list[str] = []
    if catalog_record is not None:
        current_status_parts.append(
            "catalog implementation_status="
            + _as_str(catalog_record.get("implementation_status", "unknown"))
        )
    if readiness_record is not None:
        current_status_parts.append(
            "readiness "
            + _as_str(readiness_record.get("status", "unknown"))
            + "/"
            + _as_str(readiness_record.get("reason", "unknown"))
        )
    if sqlite_blocked:
        current_status_parts.append("representative route blocked")
    return [
        _item(
            item_id="M4-B-destination-sqlite-missing",
            category="fail_missing_implementation",
            affected_connectors=[connector_id],
            affected_routes=sqlite_routes,
            evidence_source=["catalog", "readiness", "representative_nm", "gap_summary"],
            current_status="; ".join(current_status_parts),
            user_impact=(
                "One retained destination in the advertised 10-destination "
                "matrix cannot participate in representative product validation."
            ),
            root_cause_hypothesis=(
                "SQLite destination implementation exists in code, but "
                "registry/config/reporting integration is incomplete for "
                "P5.1 matrix use."
            ),
            recommended_fix=(
                "Option 1: finish sqlite destination registry/config/"
                "readiness/evidence/representative wiring. "
                "Option 2: explicitly downgrade sqlite from retained "
                "P5.1 destination matrix. "
                "If the product matrix must stay at 10 destinations, Option 1 is preferred."
            ),
            validation_after_fix=[
                _validation_command_for_connector("destination.sqlite.v1"),
                _destination_evidence_command(flow="both", dest="sqlite"),
                _representative_smoke_command(),
            ],
            priority="P0",
            severity="blocker",
            safe_to_fix_now=True,
            requires_runtime_env_change=False,
            requires_code_change=True,
            requires_dependency_change=False,
            requires_connector_contract_change=True,
            notes=[
                "If product matrix stays at 10 destinations, registry/spec "
                "retention should be completed rather than silently ignored."
            ],
        )
    ]


def _build_m4_c(catalog: dict[str, object]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for record in [_as_dict(item) for item in _as_list(catalog["connectors"])]:
        connector_id = _as_str(record["id"])
        family = _as_str(record["family"])
        if family != "source":
            continue
        if _as_str(record["spec_fields_status"]) != "empty":
            continue
        items.append(
            _item(
                item_id=f"M4-C-{connector_id.replace('.', '-')}",
                category="source_spec_fields_empty",
                affected_connectors=[connector_id],
                affected_routes=[],
                evidence_source=["catalog", "gap_summary"],
                current_status="source spec exists but fields=[]",
                user_impact=(
                    "Source UX is weak: config init/spec show cannot "
                    "communicate required inputs cleanly."
                ),
                root_cause_hypothesis=(
                    "Spec registry placeholders were retained without full "
                    "field modeling for current supported source UX."
                ),
                recommended_fix=(
                    "Add required source spec fields and make spec show/init "
                    "surfaces reflect them explicitly."
                ),
                validation_after_fix=[
                    (
                        "uv run --locked --no-sync python -m zephyr_ingest.cli spec show "
                        f"--id {connector_id} --format toml"
                    )
                ],
                priority="P1",
                severity="major",
                safe_to_fix_now=True,
                requires_runtime_env_change=False,
                requires_code_change=True,
                requires_dependency_change=False,
                requires_connector_contract_change=True,
                notes=[
                    f"Required fields draft: {', '.join(_spec_required_fields(connector_id))}",
                    "This is not a runtime blocker, but it is a major source UX gap.",
                ],
            )
        )
    return items


def _build_m4_d(
    *,
    destination_evidence: dict[str, object],
    representative: dict[str, object],
    summary: dict[str, int],
) -> list[dict[str, object]]:
    results = _list_destination_evidence_results(destination_evidence)
    representative_routes = _list_representative_routes(representative)
    destination_issues = [
        result
        for result in results
        if _as_str(result.get("issue", "")) != "" or result.get("severity") not in {None, "none"}
    ]
    representative_issues = [
        route
        for route in representative_routes
        if _as_str(route.get("issue", "")) != "" or route.get("severity") not in {None, "none"}
    ]
    has_active_visibility_gap = (
        summary["destination_evidence_failed"] > 0
        or summary["remote_only_content_evidence_gap"] > 0
        or summary["artifact_reference_only_v1"] > 0
        or summary["nm_failed_executed"] > 0
        or bool(destination_issues)
        or bool(representative_issues)
    )
    if not has_active_visibility_gap:
        return []
    remote_gap_explanation = (
        "Current remote_only_content_evidence_gap=0 because all executed "
        "direct-mode checks passed with content evidence, while blocked "
        "routes never reached direct payload readback. This does not yet "
        "prove that delivery payload core metadata visibility is frozen."
    )
    return [
        _item(
            item_id="M4-D-delivery-payload-core-metadata-check",
            category="delivery_payload_metadata_visibility_gap",
            affected_connectors=["all_retained_destinations"],
            affected_routes=[
                "all_executed_destination_evidence_routes",
                "all_executed_representative_routes",
            ],
            evidence_source=["destination_evidence", "representative_nm", "gap_summary"],
            current_status=(
                "content evidence is checked, but full DeliveryPayloadV1 "
                "core metadata visibility is not frozen as a first-class assertion"
            ),
            user_impact=(
                "User can confirm content evidence but still cannot reliably "
                "confirm run_meta / engine / metrics / provenance visibility "
                "at each destination endpoint."
            ),
            root_cause_hypothesis=(
                "P5.1 audit tools optimized for content-evidence recovery "
                "before freezing complete payload-metadata visibility checks."
            ),
            recommended_fix=(
                "Extend p5_destination_evidence_audit.py and "
                "p5_nm_representative_smoke.py with "
                "delivery_payload_core_metadata_check covering schema_version, "
                "sha256, run_meta.run_id, run_meta.engine.name, "
                "run_meta.metrics, run_meta.provenance, "
                "content_evidence, and delivery identity / endpoint locator."
            ),
            validation_after_fix=[
                _destination_evidence_command(flow="both"),
                _representative_smoke_command(),
            ],
            priority="P1",
            severity="major",
            safe_to_fix_now=True,
            requires_runtime_env_change=False,
            requires_code_change=True,
            requires_dependency_change=False,
            requires_connector_contract_change=False,
            notes=[remote_gap_explanation],
        ),
    ]


def _blocked_route_category(issue: str) -> Category:
    if "fail_dependency" in issue:
        return "representative_route_blocked"
    if "fail_service" in issue:
        return "representative_route_blocked"
    if "fail_missing_implementation" in issue:
        return "representative_route_blocked"
    if "skip_env" in issue:
        return "representative_route_blocked"
    return "representative_route_blocked"


def _blocked_route_unblock(issue: str, source_id: str, destination_id: str) -> str:
    if "fail_dependency" in issue:
        return (
            "Repair missing Python dependency surface first, then rerun "
            "readiness and this route."
        )
    if "fail_missing_implementation" in issue:
        return "Resolve sqlite retained-matrix decision or finish registry/config wiring."
    if source_id in {"source.uns.google_drive_document.v1", "source.uns.confluence_document.v1"}:
        return (
            "Unblock real external source readiness first. After live fetch "
            "proof passes, separately decide whether a user-seeded marker "
            "proof is possible or whether only real fetch proof is retained."
        )
    if "fail_service" in issue:
        return "Repair runtime service/bucket/connectivity readiness, then rerun route."
    if "skip_env" in issue:
        return "Populate required env only if this surface is intentionally retained."
    return "Unblock the underlying readiness issue, then rerun route."


def _blocked_route_notes(source_id: str) -> list[str]:
    if source_id in {"source.uns.google_drive_document.v1", "source.uns.confluence_document.v1"}:
        return [
            "Real fetch proof and user-seeded marker proof are different. "
            "A live fetch can prove source->Zephyr->destination, while "
            "user-seeded marker proof would require controllable upstream "
            "content."
        ]
    return []


def _build_m4_e(representative: dict[str, object]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for route in _list_representative_routes(representative):
        if route.get("readiness_status") != "blocked":
            continue
        source_id = _as_str(route["source_id"])
        destination_id = _as_str(route["destination_id"])
        mode = _as_str(route["mode"])
        issue = _as_str(route["issue"])
        items.append(
            _item(
                item_id=(
                    f"M4-E-{source_id.replace('.', '-')}-"
                    f"{destination_id.replace('.', '-')}-{mode}"
                ),
                category=_blocked_route_category(issue),
                affected_connectors=[source_id, destination_id],
                affected_routes=[f"{source_id} -> {destination_id} [{mode}]"],
                evidence_source=["representative_nm", "readiness", "gap_summary"],
                current_status=f"blocked representative route: {issue}",
                user_impact="Representative user-style route cannot yet execute end-to-end.",
                root_cause_hypothesis=(
                    "Route is blocked by upstream readiness state rather than "
                    "by a post-execution payload failure."
                ),
                recommended_fix=_blocked_route_unblock(issue, source_id, destination_id),
                validation_after_fix=[_representative_smoke_command()],
                priority="P1",
                severity="major",
                safe_to_fix_now=True,
                requires_runtime_env_change="fail_service" in issue or "skip_env" in issue,
                requires_code_change="fail_missing_implementation" in issue,
                requires_dependency_change="fail_dependency" in issue,
                requires_connector_contract_change=False,
                notes=_blocked_route_notes(source_id),
            )
        )
    return items


def _non_blocking_notes(
    *,
    readiness: dict[str, object],
    catalog_map: dict[str, dict[str, object]],
    destination_evidence: dict[str, object],
    readiness_map: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    notes: list[dict[str, object]] = []
    for record in _list_readiness_by_reason(readiness, "skip_env"):
        connector_id = _as_str(record["id"])
        if not _is_backend_connector(connector_id, catalog_map):
            continue
        missing_env = [_as_str(value) for value in _as_list(record["required_env_missing"])]
        notes.append(
            {
                "id": f"note-{connector_id.replace('.', '-')}",
                "severity": "note",
                "scope": "backend_non_matrix",
                "message": (
                    f"{connector_id} remains skip_env "
                    f"({', '.join(missing_env)}) and is intentionally treated as a backend-only, "
                    "non-matrix, non-blocking surface."
                ),
            }
        )

    clickhouse_source = readiness_map.get("source.it.clickhouse_incremental.v1")
    clickhouse_destination = readiness_map.get("destination.clickhouse.v1")
    if (
        clickhouse_source is not None
        and clickhouse_destination is not None
        and clickhouse_source.get("status") == "pass"
        and clickhouse_destination.get("status") == "pass"
    ):
        notes.append(
            {
                "id": "note-clickhouse-dependency-surface-history",
                "severity": "note",
                "scope": "historical_dependency_lock",
                "message": (
                    "ClickHouse source/destination currently pass. Keep the "
                    "dependency surface locked, but do not treat prior fragility "
                    "as an active M4 blocker."
                ),
            }
        )

    state_log_statuses = {
        _as_str(result.get("structured_state_log_status", "not_checked"))
        for result in _list_destination_evidence_results(destination_evidence)
    }
    if "artifact_paths_only" in state_log_statuses:
        notes.append(
            {
                "id": "note-it-structured-state-log-future-hardening",
                "severity": "note",
                "scope": "future_hardening",
                "message": (
                    "IT StructuredState/StructuredLog visibility still includes "
                    "`artifact_paths_only` in some readbacks. Treat this as future "
                    "hardening, not an active P5.1-M4 blocker unless evidence starts failing."
                ),
            }
        )
    return notes


def _prioritized_actions(
    repair_groups: dict[str, list[dict[str, object]]],
) -> list[dict[str, object]]:
    active_items = _all_repair_items(repair_groups)
    active_items.sort(
        key=lambda item: (
            _priority_rank(cast(Priority, _as_str(item["priority"]))),
            _severity_rank(cast(Severity, _as_str(item["severity"]))),
            _as_str(item["id"]),
        )
    )
    actions: list[dict[str, object]] = []
    for index, item in enumerate(active_items, start=1):
        connectors = [_as_str(value) for value in _as_list(item["affected_connectors"])]
        title = _as_str(item["recommended_fix"])
        if connectors:
            title = f"{', '.join(connectors)}: {title}"
        actions.append(
            {
                "id": f"P5.1-M4-priority-{index}",
                "title": title,
                "why": _as_str(item["current_status"]),
                "priority": _as_str(item["priority"]),
                "severity": _as_str(item["severity"]),
            }
        )
    return actions


def _recommended_order(repair_groups: dict[str, list[dict[str, object]]]) -> list[str]:
    order_map = {
        "M4_B_missing_implementation": "1. Resolve active retained-matrix implementation gaps",
        "M4_A_dependency_and_readiness": "2. Resolve active dependency/runtime readiness blockers",
        "M4_C_source_spec_fields": "3. Close active source spec UX gaps",
        "M4_D_delivery_payload_visibility": "4. Close active DeliveryPayloadV1 visibility gaps",
        "M4_E_representative_nm_unblock": (
            "5. Rerun blocked representative routes after upstream fixes"
        ),
    }
    ordered = [
        order_map[group_name]
        for group_name, items in repair_groups.items()
        if items
    ]
    if ordered:
        return ordered
    return [
        "1. P5.1-M4 active blockers = 0",
        "2. Retained 10×10 source/destination matrix currently passes",
        "3. backend.uns_api.v1 remains a skipped backend-only note and does not block the matrix",
    ]


def build_repair_plan(
    *,
    catalog_path: Path,
    readiness_path: Path,
    destination_evidence_path: Path,
    representative_path: Path,
    gap_summary_path: Path,
) -> dict[str, object]:
    catalog = _load_json_object(catalog_path)
    readiness = _load_json_object(readiness_path)
    destination_evidence = _load_json_object(destination_evidence_path)
    representative = _load_json_object(representative_path)
    gap_summary = _load_json_object(gap_summary_path)

    summary = _summary_from_gap_summary(gap_summary)

    catalog_map = _catalog_map(catalog)
    readiness_map = _readiness_map(readiness)
    representative_routes = _list_representative_routes(representative)
    repair_groups = {
        "M4_A_dependency_and_readiness": _build_m4_a(
            readiness=readiness,
            representative_routes=representative_routes,
            readiness_map=readiness_map,
            catalog_map=catalog_map,
        ),
        "M4_B_missing_implementation": _build_m4_b(
            representative_routes=representative_routes,
            readiness_map=readiness_map,
            catalog_map=catalog_map,
        ),
        "M4_C_source_spec_fields": _build_m4_c(catalog),
        "M4_D_delivery_payload_visibility": _build_m4_d(
            destination_evidence=destination_evidence,
            representative=representative,
            summary=summary,
        ),
        "M4_E_representative_nm_unblock": _build_m4_e(representative),
    }
    active_items = _all_repair_items(repair_groups)
    non_blocking_notes = _non_blocking_notes(
        readiness=readiness,
        catalog_map=catalog_map,
        destination_evidence=destination_evidence,
        readiness_map=readiness_map,
    )
    return {
        "schema_version": 1,
        "generated_at_utc": _generated_at_utc(),
        "inputs": {
            "catalog": catalog_path.as_posix(),
            "readiness": readiness_path.as_posix(),
            "destination_evidence": destination_evidence_path.as_posix(),
            "representative": representative_path.as_posix(),
            "gap_summary": gap_summary_path.as_posix(),
            "encoding_handling": ["utf-8", "utf-8-sig", "utf-16-bom-aware"],
        },
        "summary": summary,
        "repair_groups": repair_groups,
        "active_blocker_count": sum(
            1 for item in active_items if _as_str(item["severity"]) == "blocker"
        ),
        "active_major_count": sum(
            1 for item in active_items if _as_str(item["severity"]) == "major"
        ),
        "active_minor_or_note_count": sum(
            1
            for item in active_items
            if _as_str(item["severity"]) in {"minor", "note"}
        ),
        "non_blocking_notes": non_blocking_notes,
        "prioritized_actions": _prioritized_actions(repair_groups),
        "m4_recommended_order": _recommended_order(repair_groups),
        "final_judgment": {
            "p5_1_m4_active_blockers": len(active_items),
            "retained_matrix_status": "pass" if not active_items else "active_repairs_remaining",
            "backend_uns_api_v1": "skipped_backend_non_blocking",
        },
    }


def _render_m4_a(items: list[dict[str, object]]) -> list[str]:
    lines = [
        "## M4-A Dependency and readiness fixes",
        "",
    ]
    if not items:
        lines.extend(["No active dependency/readiness repair items.", ""])
        return lines
    lines.extend(
        [
            "| Priority | Severity | Connector | Problem | User impact | Fix | Validation |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in items:
        lines.append(
            "| "
            + " | ".join(
                [
                    _as_str(item["priority"]),
                    _as_str(item["severity"]),
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["affected_connectors"])]
                    ),
                    _as_str(item["current_status"]),
                    _as_str(item["user_impact"]),
                    _as_str(item["recommended_fix"]),
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["validation_after_fix"])]
                    ),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def _render_m4_b(items: list[dict[str, object]]) -> list[str]:
    lines = [
        "## M4-B Missing implementation",
        "",
    ]
    if not items:
        lines.extend(["No active missing-implementation repair items.", ""])
        return lines
    lines.extend(
        [
            (
                "| Priority | Connector | Current status | Options | "
                "Recommended decision | Validation |"
            ),
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in items:
        lines.append(
            "| "
            + " | ".join(
                [
                    _as_str(item["priority"]),
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["affected_connectors"])]
                    ),
                    _as_str(item["current_status"]),
                    "Option 1: implement and retain<br>Option 2: downgrade from retained matrix",
                    "Option 1 if product matrix must remain 10 destinations",
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["validation_after_fix"])]
                    ),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def _render_m4_c(items: list[dict[str, object]]) -> list[str]:
    lines = [
        "## M4-C Source spec fields UX",
        "",
    ]
    if not items:
        lines.extend(["No active source spec field UX repair items.", ""])
        return lines
    lines.extend(
        [
            "| Source | Current fields status | Required fields to add | UX impact | Validation |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for item in items:
        notes = [_as_str(value) for value in _as_list(item["notes"])]
        required_fields = next(
            (
                note.removeprefix("Required fields draft: ")
                for note in notes
                if note.startswith("Required fields draft: ")
            ),
            "",
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["affected_connectors"])]
                    ),
                    _as_str(item["current_status"]),
                    required_fields,
                    _as_str(item["user_impact"]),
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["validation_after_fix"])]
                    ),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def _render_m4_d(items: list[dict[str, object]]) -> list[str]:
    lines = [
        "## M4-D DeliveryPayloadV1 visibility and content evidence",
        "",
    ]
    if not items:
        lines.extend(["No active DeliveryPayloadV1 visibility repair items.", ""])
        return lines
    lines.extend(
        [
            "| Area | Current gap | Required assertion | Affected tools | Validation |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for item in items:
        lines.append(
            "| "
            + " | ".join(
                [
                    _as_str(item["id"]),
                    _as_str(item["current_status"]),
                    _as_str(item["recommended_fix"]),
                    "tools/p5_destination_evidence_audit.py<br>tools/p5_nm_representative_smoke.py",
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["validation_after_fix"])]
                    ),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def _render_m4_e(items: list[dict[str, object]]) -> list[str]:
    lines = [
        "## M4-E Representative N×M unblock",
        "",
    ]
    if not items:
        lines.extend(["No active representative-route unblock items.", ""])
        return lines
    lines.extend(
        [
            "| Route | Mode | Block reason | Category | Unblock action | Re-test command |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in items:
        route = _as_str(_as_list(item["affected_routes"])[0])
        mode = route.rsplit("[", 1)[1].removesuffix("]")
        lines.append(
            "| "
            + " | ".join(
                [
                    route,
                    mode,
                    _as_str(item["current_status"]),
                    _as_str(item["category"]),
                    _as_str(item["recommended_fix"]),
                    _markdown_lines(
                        [_as_str(value) for value in _as_list(item["validation_after_fix"])]
                    ),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def render_markdown(plan: dict[str, object]) -> str:
    summary = _as_dict(plan["summary"])
    repair_groups = _as_dict(plan["repair_groups"])
    prioritized_actions = [_as_dict(item) for item in _as_list(plan["prioritized_actions"])]
    non_blocking_notes = [_as_dict(item) for item in _as_list(plan["non_blocking_notes"])]
    final_judgment = _as_dict(plan["final_judgment"])
    lines = [
        "# P5.1 M4 Repair Plan",
        "",
        "## Executive summary",
        "",
        "- current matrix status: retained sources=10, retained destinations=10, "
        "backend.uns_api.v1 excluded from destination count",
        (
            "- readiness blockers: "
            f"fail_dependency={summary['readiness_fail_dependency']}, "
            f"fail_service={summary['readiness_fail_service']}, "
            f"skip_env={summary['readiness_skip_env']}"
        ),
        f"- missing implementation: {summary['readiness_missing_implementation']}",
        f"- service blockers: {summary['readiness_fail_service']}",
        f"- dependency blockers: {summary['readiness_fail_dependency']}",
        (
            "- destination evidence gaps: "
            f"destination_evidence_failed={summary['destination_evidence_failed']}, "
            "remote_only_content_evidence_gap="
            f"{summary['remote_only_content_evidence_gap']}, "
            f"artifact_reference_only_v1={summary['artifact_reference_only_v1']}"
        ),
        (
            "- representative N×M status: "
            f"blocked={summary['nm_blocked']}, "
            f"failed_executed={summary['nm_failed_executed']}, "
            f"passed={summary['nm_passed']}"
        ),
        f"- active blocker count: {plan['active_blocker_count']}",
        f"- active major count: {plan['active_major_count']}",
        f"- active minor/note count: {plan['active_minor_or_note_count']}",
        "- recommended M4 order:",
    ]
    for value in _as_list(plan["m4_recommended_order"]):
        lines.append(f"  - {_as_str(value)}")
    if prioritized_actions:
        lines.append("- active prioritized actions:")
        for action in prioritized_actions:
            lines.append(
                "  - "
                + _as_str(action["priority"])
                + " "
                + _as_str(action["title"])
            )
    else:
        lines.append("- active prioritized actions: none")
    if non_blocking_notes:
        lines.append("- non-blocking notes:")
        for note in non_blocking_notes:
            lines.append(f"  - {_as_str(note['message'])}")
    lines.append("")
    lines.extend(
        _render_m4_a(
            [_as_dict(item) for item in _as_list(repair_groups["M4_A_dependency_and_readiness"])]
        )
    )
    lines.extend(
        _render_m4_b(
            [_as_dict(item) for item in _as_list(repair_groups["M4_B_missing_implementation"])]
        )
    )
    lines.extend(
        _render_m4_c(
            [_as_dict(item) for item in _as_list(repair_groups["M4_C_source_spec_fields"])]
        )
    )
    lines.extend(
        _render_m4_d(
            [_as_dict(item) for item in _as_list(repair_groups["M4_D_delivery_payload_visibility"])]
        )
    )
    lines.extend(
        _render_m4_e(
            [_as_dict(item) for item in _as_list(repair_groups["M4_E_representative_nm_unblock"])]
        )
    )
    lines.extend(
        [
            "## Final recommendation",
            "",
            (
                "- P5.1-M4 active blockers: "
                f"{final_judgment['p5_1_m4_active_blockers']}"
            ),
            (
                "- retained 10 source × 10 destination matrix: "
                f"{final_judgment['retained_matrix_status']}"
            ),
            (
                "- backend.uns_api.v1 treatment: "
                f"{final_judgment['backend_uns_api_v1']}"
            ),
            (
                "- 先修哪个: "
                "当前没有 active M4 repair blockers。"
                if not prioritized_actions
                else "按 active prioritized actions 顺序处理当前剩余修补项。"
            ),
            (
                "- 哪些是代码改动: 仅当前仍在 active repair_groups 里的项。"
            ),
            (
                "- 哪些是 env/runtime 改动: 仅当前仍在 active dependency/readiness "
                "repair items 里的项。"
            ),
            (
                "- 哪些是 dependency 改动: 仅当前仍在 active fail_dependency "
                "repair items 里的项。"
            ),
            (
                "- 哪些是产品矩阵决策: 仅当前仍在 active missing-implementation "
                "repair items 里的项。"
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _emit_outputs(plan: dict[str, object], *, out_root: Path) -> tuple[Path, Path]:
    out_root.mkdir(parents=True, exist_ok=True)
    json_path = out_root / "repair_plan.json"
    md_path = out_root / "repair_plan.md"
    json_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(plan), encoding="utf-8")
    return json_path, md_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="p5_m4_repair_plan")
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--readiness", type=Path, required=True)
    parser.add_argument("--destination-evidence", type=Path, required=True)
    parser.add_argument("--representative", type=Path, required=True)
    parser.add_argument("--gap-summary", type=Path, required=True)
    parser.add_argument("--out-root", type=Path, required=True)
    render_group = parser.add_mutually_exclusive_group()
    render_group.add_argument("--json", action="store_true")
    render_group.add_argument("--markdown", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    plan = build_repair_plan(
        catalog_path=args.catalog,
        readiness_path=args.readiness,
        destination_evidence_path=args.destination_evidence,
        representative_path=args.representative,
        gap_summary_path=args.gap_summary,
    )
    _emit_outputs(plan, out_root=args.out_root)
    if args.markdown:
        print(render_markdown(plan), end="")
        return 0
    print(json.dumps(plan, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
