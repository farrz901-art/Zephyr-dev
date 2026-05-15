from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast


def _discover_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (
            ((candidate / "pyproject.toml").exists() or (candidate / ".git").exists())
            and (candidate / "docs/p6").exists()
            and (candidate / "packages/zephyr-ingest").exists()
        ):
            return candidate
    raise RuntimeError("Could not locate repository root from tool path")


DEFAULT_ROOT = _discover_repo_root(Path(__file__).resolve().parent)
DEFAULT_BASE_ROOT = Path("E:/Github_Projects/Zephyr-base")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s7_base_ui_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "43ebf16ed3f459e4c7ea670d75f52a02b3c5d276"


class SummaryDict(TypedDict):
    overall: str
    m3_s6_status_after_seal_update: str
    m3_s7_status: str
    active_blockers: int
    major_gaps: int


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    return raw.decode("utf-8")


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(_read_text(path))
    if not isinstance(loaded_obj, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], loaded_obj)


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return value


def _git_output(repo: Path, *args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _repo_status(base_root: Path) -> dict[str, object]:
    status_path = base_root / ".tmp/s7_repo_status.json"
    status_file = _load_json_object(status_path) if status_path.exists() else None
    current_head = _git_output(base_root, "rev-parse", "HEAD")
    origin_head = _git_output(base_root, "rev-parse", "origin/main")
    if current_head is None:
        if status_file is not None:
            return {
                "previous_head_sha": status_file.get("previous_head_sha", PREVIOUS_HEAD_SHA),
                "current_head_sha": status_file.get("current_head_sha", "unknown"),
                "pushed": status_file.get("pushed", False),
            }
        return {
            "previous_head_sha": PREVIOUS_HEAD_SHA,
            "current_head_sha": "unknown",
            "pushed": False,
        }
    return {
        "previous_head_sha": PREVIOUS_HEAD_SHA,
        "current_head_sha": current_head,
        "pushed": origin_head == current_head and origin_head is not None,
    }


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _required_paths(root: Path, base_root: Path) -> dict[str, Path]:
    return {
        "s6_handoff": root / ".tmp/p6_m3_s6_tauri_bridge_handoff/report.json",
        "ui_doc": base_root / "docs/UI_ARTIFACT_CONSUMPTION.md",
        "workflow": base_root / ".github/workflows/boundary.yml",
        "boundary_report": base_root / ".tmp/base_boundary_check.json",
        "ui_shell_report": base_root / ".tmp/ui_shell_check.json",
        "tauri_check": base_root / ".tmp/tauri_command_bridge_check.json",
        "bundle_manifest": (
            base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json"
        ),
        "contracts": base_root / "ui/src/contracts/baseRunResult.ts",
        "bridge_client": base_root / "ui/src/services/baseBridgeClient.ts",
        "mock_client": base_root / "ui/src/services/mockArtifactClient.ts",
        "sample_success": base_root / "ui/src/fixtures/sampleRunResult.ts",
        "sample_error": base_root / "ui/src/fixtures/sampleErrorResult.ts",
        "app": base_root / "ui/src/App.tsx",
        "result_summary": base_root / "ui/src/components/ResultSummary.tsx",
        "normalized_preview": base_root / "ui/src/components/NormalizedTextPreview.tsx",
        "evidence_card": base_root / "ui/src/components/EvidenceCard.tsx",
        "receipt_card": base_root / "ui/src/components/ReceiptCard.tsx",
        "usage_fact_card": base_root / "ui/src/components/UsageFactCard.tsx",
        "error_panel": base_root / "ui/src/components/ErrorDiagnosisPanel.tsx",
        "lineage_card": base_root / "ui/src/components/LineageStatusCard.tsx",
        "output_plan": base_root / "ui/src/components/OutputFolderPlan.tsx",
    }


def build_report(*, root: Path, base_root: Path) -> dict[str, object]:
    required = _required_paths(root, base_root)
    missing = [
        path.relative_to(base_root).as_posix()
        if path.is_relative_to(base_root)
        else path.relative_to(root).as_posix()
        for path in required.values()
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("; ".join(missing))

    s6_handoff = _load_json_object(required["s6_handoff"])
    boundary_report = _load_json_object(required["boundary_report"])
    ui_shell_report = _load_json_object(required["ui_shell_report"])
    tauri_check = _load_json_object(required["tauri_check"])
    bundle_manifest = _load_json_object(required["bundle_manifest"])
    workflow_text = _read_text(required["workflow"])
    contract_text = _read_text(required["contracts"])
    bridge_client_text = _read_text(required["bridge_client"])
    mock_client_text = _read_text(required["mock_client"])
    sample_success_text = _read_text(required["sample_success"])
    sample_error_text = _read_text(required["sample_error"])
    app_text = _read_text(required["app"])
    usage_fact_card_text = _read_text(required["usage_fact_card"])
    repo_status = _repo_status(base_root)

    s6_summary = _as_dict(s6_handoff["summary"])
    s6_seal_update = _as_dict(s6_handoff["s6_seal_update"])
    boundary_summary = _as_dict(boundary_report["summary"])
    ui_shell_summary = _as_dict(ui_shell_report["summary"])
    tauri_summary = _as_dict(tauri_check["summary"])

    issues: list[str] = []
    if s6_summary["m3_s6_status"] != "sealed":
        issues.append("S6 seal update is not sealed.")
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Zephyr-base boundary check did not pass.")
    if int(boundary_summary["blocked_count"]) != 0:
        issues.append("Zephyr-base boundary check still has blockers.")
    if _as_bool(ui_shell_summary["pass"]) is False:
        issues.append("UI shell static check did not pass.")
    if int(ui_shell_summary["commercial_terms_blocked"]) != 0:
        issues.append("UI shell still contains blocked commercial terms.")
    if int(ui_shell_summary["network_calls_blocked"]) != 0:
        issues.append("UI shell still contains blocked network calls.")
    if _as_bool(ui_shell_summary["supported_formats_limited_to_base_first_slice"]) is False:
        issues.append("UI shell claims unsupported formats.")
    if _as_bool(ui_shell_summary["billing_semantics_false_present"]) is False:
        issues.append("UI shell sample artifacts do not preserve billing_semantics=false.")
    if _as_bool(tauri_summary["static_bridge_check_pass"]) is False:
        issues.append("Tauri command bridge static check did not pass.")

    component_paths = {
        "result_summary_component": required["result_summary"],
        "normalized_text_preview_component": required["normalized_preview"],
        "evidence_card_component": required["evidence_card"],
        "receipt_card_component": required["receipt_card"],
        "usage_fact_card_component": required["usage_fact_card"],
        "error_diagnosis_component": required["error_panel"],
        "lineage_status_component": required["lineage_card"],
        "output_folder_plan_component": required["output_plan"],
    }
    ui_components = {name: path.exists() for name, path in component_paths.items()}

    tauri_invoke_ready = "__TAURI__" in bridge_client_text and (
        "invoke_ready_not_e2e_verified" in bridge_client_text
    )
    tauri_invoke_e2e_verified = False
    billing_semantics_false = (
        "billing_semantics: false" in sample_success_text
        and "billing_semantics: false" in sample_error_text
        and "billing_semantics" in usage_fact_card_text
        and "billing" in usage_fact_card_text.lower()
    )
    supported_formats_limited = all(
        marker in app_text for marker in [".txt", ".text", ".log", ".md", ".markdown"]
    ) and all(marker not in app_text for marker in [".pdf", ".docx", ".png", ".jpg"])

    active_blockers = len(issues)
    pushed = _as_bool(repo_status["pushed"])
    if active_blockers > 0:
        overall = "fail"
        status = "open"
    elif pushed is False:
        overall = "conditional"
        status = "conditional"
    else:
        overall = "pass"
        status = "sealed"

    non_blocking_notes = [
        "S7 moves Base from backend-only command wiring to a visible UI shell first slice.",
        "Tauri invoke is wrapper-ready but not end-to-end verified in this slice.",
        "The bundled adapter still uses the current Python environment.",
        "S7 is not a final product UI, installer, or release build.",
    ]

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s7.base_ui_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s6_status_after_seal_update": s6_summary["m3_s6_status"],
            "m3_s7_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "s6_seal_update": {
            "cargo_available": s6_seal_update["cargo_available"],
            "cargo_check_pass": s6_seal_update["cargo_check_pass"],
            "static_bridge_check_pass": s6_seal_update["static_bridge_check_pass"],
            "commercial_terms_blocked": s6_seal_update["commercial_terms_blocked"],
            "local_validation_source": s6_seal_update["local_validation_source"],
            "non_blocking_warnings": s6_seal_update["non_blocking_warnings"],
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": pushed,
        },
        "ui": {
            "ui_shell_exists": True,
            "artifact_contracts_exist": "export interface BaseRunResultV1" in contract_text,
            "bridge_client_exists": "runLocalFile" in bridge_client_text,
            "mock_artifact_client_exists": "sampleSuccessResult" in mock_client_text,
            "sample_success_result_exists": "status: \"success\"" in sample_success_text,
            "sample_error_result_exists": "status: \"failed\"" in sample_error_text,
            **ui_components,
            "billing_semantics_displayed_false": billing_semantics_false,
            "supported_formats_limited_to_base_first_slice": supported_formats_limited,
        },
        "runtime": {
            "tauri_invoke_ready": tauri_invoke_ready,
            "tauri_invoke_e2e_verified": tauri_invoke_e2e_verified,
            "uses_bundled_adapter": True,
            "uses_current_python_environment": True,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "installer_runtime_complete": False,
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "ui_shell_check_pass": ui_shell_summary["pass"],
            "commercial_terms_blocked": ui_shell_summary["commercial_terms_blocked"],
            "network_calls_blocked": ui_shell_summary["network_calls_blocked"],
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "scope": {
            "production_ui_complete": False,
            "installer_built": False,
            "release_created": False,
            "cloud_core_used": False,
            "pro_or_web_core_used": False,
        },
        "ci": {
            "workflow_updated": True,
            "runs_ui_shell_check": "check_ui_shell.py --json" in workflow_text,
            "runs_tauri_build": False,
            "requires_node_install": False,
            "requires_secrets": False,
            "bundle_requires_network": bundle_manifest["requires_network"],
        },
        "issues": issues,
        "non_blocking_notes": non_blocking_notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    s6_seal_update = _as_dict(report["s6_seal_update"])
    ui = _as_dict(report["ui"])
    runtime = _as_dict(report["runtime"])
    boundary = _as_dict(report["boundary"])
    notes = _as_list(report["non_blocking_notes"])
    lines = [
        "# P6-M3-S7 Base UI artifact consumption shell handoff",
        "",
        "## S6 seal update",
        f"- cargo_available: {s6_seal_update['cargo_available']}",
        f"- cargo_check_pass: {s6_seal_update['cargo_check_pass']}",
        f"- static_bridge_check_pass: {s6_seal_update['static_bridge_check_pass']}",
        f"- commercial_terms_blocked: {s6_seal_update['commercial_terms_blocked']}",
        f"- local_validation_source: {s6_seal_update['local_validation_source']}",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s6_status_after_seal_update: {summary['m3_s6_status_after_seal_update']}",
        f"- m3_s7_status: {summary['m3_s7_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## What S7 completed",
        "- turned `ui/` from placeholder-only files into a visible Base UI shell first slice",
        "- added TypeScript contracts aligned to `base_run_result_v1` artifact shape",
        "- added a mock artifact client plus an invoke-ready bridge client wrapper",
        (
            "- added cards for result summary, normalized text, evidence, "
            "receipt, usage fact, error, lineage, and output plan"
        ),
        "",
        "## UI artifact contracts",
        f"- artifact_contracts_exist: {ui['artifact_contracts_exist']}",
        f"- sample_success_result_exists: {ui['sample_success_result_exists']}",
        f"- sample_error_result_exists: {ui['sample_error_result_exists']}",
        "",
        "## UI components",
        f"- result_summary_component: {ui['result_summary_component']}",
        f"- normalized_text_preview_component: {ui['normalized_text_preview_component']}",
        f"- evidence_card_component: {ui['evidence_card_component']}",
        f"- receipt_card_component: {ui['receipt_card_component']}",
        f"- usage_fact_card_component: {ui['usage_fact_card_component']}",
        f"- error_diagnosis_component: {ui['error_diagnosis_component']}",
        f"- lineage_status_component: {ui['lineage_status_component']}",
        f"- output_folder_plan_component: {ui['output_folder_plan_component']}",
        "",
        "## Bridge client / mock client",
        f"- bridge_client_exists: {ui['bridge_client_exists']}",
        f"- mock_artifact_client_exists: {ui['mock_artifact_client_exists']}",
        f"- tauri_invoke_ready: {runtime['tauri_invoke_ready']}",
        f"- tauri_invoke_e2e_verified: {runtime['tauri_invoke_e2e_verified']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- ui_shell_check_pass: {boundary['ui_shell_check_pass']}",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- network_calls_blocked: {boundary['network_calls_blocked']}",
        "",
        "## Current limitations",
        "- S7 still does not prove full Tauri window end-to-end invocation.",
        "- S7 still depends on the current Python environment behind the bundled adapter.",
        "- S7 is not the final production UI and does not build an installer.",
        "",
        "## What S7 did not do",
        "- did not implement a full production desktop UI",
        "- did not build an installer or release",
        "- did not add cloud, Web-core, Pro, or commercial logic",
        "",
        "## Next step",
        (
            "- P6-M3-S8: wire the visible UI shell to verified Tauri invoke "
            "flows and local result lifecycle."
        ),
        "",
        "## Notes",
    ]
    for note in notes:
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def emit_outputs(*, report: dict[str, object], out_root: Path, markdown: bool) -> None:
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / (OUTPUT_MD if markdown else OUTPUT_JSON)
    rendered = (
        render_markdown(report)
        if markdown
        else json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    out_path.write_text(rendered, encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the P6-M3-S7 Base UI handoff.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--base-root", type=Path, default=DEFAULT_BASE_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true")
    mode.add_argument("--markdown", action="store_true")
    mode.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    root = args.root.resolve()
    base_root = args.base_root.resolve()
    out_root = _resolve_from_root(root, args.out_root)
    if args.check_artifacts:
        required = [out_root / OUTPUT_JSON, out_root / OUTPUT_MD]
        missing = [path.name for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError("; ".join(missing))
        return 0
    report = build_report(root=root, base_root=base_root)
    emit_outputs(report=report, out_root=out_root, markdown=args.markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
