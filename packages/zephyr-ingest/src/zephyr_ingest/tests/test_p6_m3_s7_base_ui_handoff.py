from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s7_base_ui_handoff as handoff_tool


def _repo_root() -> Path:
    current = Path(__file__).resolve()
    for candidate in current.parents:
        if (
            ((candidate / "pyproject.toml").exists() or (candidate / ".git").exists())
            and (candidate / "docs/p6").exists()
            and (candidate / "packages/zephyr-ingest").exists()
        ):
            return candidate
    raise RuntimeError("Could not locate repository root from test file path")


def _as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _workspace_case_dir(case_name: str) -> Path:
    root = _repo_root() / ".tmp" / "p6_m3_s7_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_fake_root(repo_root: Path) -> None:
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    _write_json(
        repo_root / ".tmp/p6_m3_s6_tauri_bridge_handoff/report.json",
        {
            "summary": {"overall": "pass", "m3_s6_status": "sealed"},
            "s6_seal_update": {
                "cargo_available": True,
                "cargo_check_pass": True,
                "static_bridge_check_pass": True,
                "commercial_terms_blocked": 0,
                "local_validation_source": "user local VS BuildTools x64 environment",
                "non_blocking_warnings": [
                    "BridgeErrorKind::Input currently unused",
                    "BridgeError::input currently unused",
                ],
            },
        },
    )


def _emit_fake_base_root(base_root: Path) -> None:
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / ".github/workflows").mkdir(parents=True, exist_ok=True)
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / "runtime/public-core-bundle/manifest").mkdir(parents=True, exist_ok=True)
    (base_root / "ui/src/contracts").mkdir(parents=True, exist_ok=True)
    (base_root / "ui/src/services").mkdir(parents=True, exist_ok=True)
    (base_root / "ui/src/fixtures").mkdir(parents=True, exist_ok=True)
    (base_root / "ui/src/components").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/UI_ARTIFACT_CONSUMPTION.md").write_text("ui doc\n", encoding="utf-8")
    (base_root / ".github/workflows/boundary.yml").write_text(
        "python scripts/check_ui_shell.py --json\n",
        encoding="utf-8",
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True, "blocked_count": 0}},
    )
    _write_json(
        base_root / ".tmp/ui_shell_check.json",
        {
            "summary": {
                "pass": True,
                "commercial_terms_blocked": 0,
                "network_calls_blocked": 0,
                "supported_formats_limited_to_base_first_slice": True,
                "billing_semantics_false_present": True,
            }
        },
    )
    _write_json(
        base_root / ".tmp/tauri_command_bridge_check.json",
        {
            "summary": {
                "pass": True,
                "cargo_available": True,
                "cargo_check_pass": True,
                "static_bridge_check_pass": True,
                "commercial_terms_blocked": 0,
            }
        },
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json",
        {"requires_network": False},
    )
    (base_root / "ui/src/contracts/baseRunResult.ts").write_text(
        "export interface BaseRunResultV1 {}\n",
        encoding="utf-8",
    )
    (base_root / "ui/src/services/baseBridgeClient.ts").write_text(
        (
            "const mode = 'invoke_ready_not_e2e_verified'; "
            "const tauri = '__TAURI__'; export function runLocalFile() {}\n"
        ),
        encoding="utf-8",
    )
    (base_root / "ui/src/services/mockArtifactClient.ts").write_text(
        "export const sampleSuccessResult = { billing_semantics: false };\n",
        encoding="utf-8",
    )
    (base_root / "ui/src/fixtures/sampleRunResult.ts").write_text(
        'export const sampleSuccess = { status: "success", billing_semantics: false };\n',
        encoding="utf-8",
    )
    (base_root / "ui/src/fixtures/sampleErrorResult.ts").write_text(
        'export const sampleError = { status: "failed", billing_semantics: false };\n',
        encoding="utf-8",
    )
    (base_root / "ui/src/App.tsx").write_text(
        ".txt .text .log .md .markdown billing_semantics\n",
        encoding="utf-8",
    )
    for name in [
        "ResultSummary.tsx",
        "NormalizedTextPreview.tsx",
        "EvidenceCard.tsx",
        "ReceiptCard.tsx",
        "UsageFactCard.tsx",
        "ErrorDiagnosisPanel.tsx",
        "LineageStatusCard.tsx",
        "OutputFolderPlan.tsx",
    ]:
        (base_root / "ui/src/components" / name).write_text("// component\n", encoding="utf-8")


@pytest.mark.auth_contract
def test_s7_handoff_reports_ui_shell_contracts(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    _emit_fake_root(repo_root)
    _emit_fake_base_root(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    ui = _as_dict(report["ui"])
    runtime = _as_dict(report["runtime"])
    assert summary["m3_s6_status_after_seal_update"] == "sealed"
    assert ui["artifact_contracts_exist"] is True
    assert ui["supported_formats_limited_to_base_first_slice"] is True
    assert runtime["tauri_invoke_e2e_verified"] is False


@pytest.mark.auth_contract
def test_s7_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    out_root = _workspace_case_dir(f"{tmp_path.name}_out")
    _emit_fake_root(repo_root)
    _emit_fake_base_root(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=False)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=True)
    json_text = (out_root / "report.json").read_text(encoding="utf-8")
    markdown_text = (out_root / "report.md").read_text(encoding="utf-8")
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text
