from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p45 import repo_root

CapabilityDomainSeverity = Literal["error", "warning", "info"]

P5_CAPABILITY_DOMAIN_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_capability_domain_matrix.json"
)
P5_CAPABILITY_DOMAIN_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S9_CAPABILITY_DOMAINS.md"
)
P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_dependency_boundary_manifest.json"
)
P5_BUILD_CUT_MANIFEST_PATH: Final[Path] = repo_root() / "validation" / "p5_build_cut_manifest.json"
P5_OUTPUT_BOUNDARY_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S9_OUTPUT_BOUNDARY.md"
)

IT_STREAM_SOURCE_CONNECTORS: Final[tuple[str, ...]] = (
    "http_json_cursor_v1",
    "postgresql_incremental_v1",
    "clickhouse_incremental_v1",
    "kafka_partition_offset_v1",
    "mongodb_incremental_v1",
)
UNS_STREAM_SOURCE_CONNECTORS: Final[tuple[str, ...]] = (
    "http_document_v1",
    "s3_document_v1",
    "git_document_v1",
    "google_drive_document_v1",
    "confluence_document_v1",
)
DESTINATION_CONNECTORS: Final[tuple[str, ...]] = (
    "filesystem",
    "webhook",
    "sqlite",
    "kafka",
    "weaviate",
    "s3",
    "opensearch",
    "clickhouse",
    "mongodb",
    "loki",
)
UNS_STREAM_PRO_EXTRAS: Final[tuple[str, ...]] = (
    "all-docs",
    "pdf",
    "doc",
    "docx",
    "ppt",
    "pptx",
    "xlsx",
    "csv",
    "tsv",
    "md",
    "rtf",
    "rst",
    "epub",
    "odt",
    "org",
    "image",
)
EXTRA_RESERVED_CAPABILITIES: Final[tuple[str, ...]] = ("audio", "model_init")


def _connector_entry(*, name: str, package: str, connector_type: str) -> dict[str, object]:
    return {
        "name": name,
        "package": package,
        "connector_type": connector_type,
        "domain": "base",
        "enterprise_grade_today": False,
        "domain_reason": "current retained non-enterprise connector surface",
    }


def build_p5_capability_domain_matrix() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S9 capability-domain solidification",
        "domain_model": {
            "kind": "technical_capability_domain",
            "not": ["pricing tier", "subscription tier", "license tier", "entitlement domain"],
            "base": {
                "definition": (
                    "Current shared core capability surface: all retained connectors, it-stream, "
                    "and uns-stream non-extra unstructured processing."
                ),
                "commercial_semantics": "none",
            },
            "pro": {
                "definition": (
                    "Base plus current uns-stream optional document extras; future mature "
                    "enterprise connectors may be promoted here in P6 or later."
                ),
                "commercial_semantics": "none",
            },
            "extra": {
                "definition": (
                    "Incubating or forward-looking technical capabilities, including explicit "
                    "audio/model-init reserves and future unsupported document or enterprise "
                    "connector surfaces."
                ),
                "commercial_semantics": "none",
                "promotion_rule": (
                    "Mature Extra capabilities must be explicitly promoted into Pro by updating "
                    "this matrix, dependency/build manifests, and focused drift tests."
                ),
            },
        },
        "connectors": {
            "it_stream_sources": [
                _connector_entry(name=name, package="it-stream", connector_type="source")
                for name in IT_STREAM_SOURCE_CONNECTORS
            ],
            "uns_stream_sources": [
                _connector_entry(name=name, package="uns-stream", connector_type="source")
                for name in UNS_STREAM_SOURCE_CONNECTORS
            ],
            "destinations": [
                _connector_entry(
                    name=name,
                    package="zephyr-ingest",
                    connector_type="destination",
                )
                for name in DESTINATION_CONNECTORS
            ],
        },
        "uns_stream_capability_surface": {
            "package": "uns-stream",
            "package_domain_model": "mixed_base_plus_pro_optional_extras",
            "not_modeled_as": "whole_package_pro",
            "base_surface": {
                "dependency_basis": ["unstructured"],
                "capability": "non-extra unstructured document processing surface",
                "domain": "base",
            },
            "pro_optional_extras": [
                {"extra": extra, "domain": "pro", "basis": "optional unstructured extra"}
                for extra in UNS_STREAM_PRO_EXTRAS
            ],
            "extra_reserved": [
                {
                    "capability": capability,
                    "domain": "extra",
                    "status": "incubating_reserved_not_current_build_cut",
                }
                for capability in EXTRA_RESERVED_CAPABILITIES
            ],
        },
        "package_domain_ownership": [
            {
                "package": "zephyr-core",
                "domain_role": "cross_domain_shared_contracts",
                "domain": "shared",
            },
            {
                "package": "zephyr-ingest",
                "domain_role": "cross_domain_shared_orchestration_delivery_runtime",
                "domain": "shared",
            },
            {
                "package": "zephyr-api",
                "domain_role": "cross_domain_shared_service_layer",
                "domain": "shared",
            },
            {
                "package": "it-stream",
                "domain_role": "base_structured_flow",
                "domain": "base",
            },
            {
                "package": "uns-stream",
                "domain_role": "mixed_base_direct_surface_plus_pro_optional_extras",
                "domain": "mixed",
            },
        ],
        "extra_to_pro_promotion": {
            "current_extra_capabilities": list(EXTRA_RESERVED_CAPABILITIES),
            "requires_explicit_promotion": True,
            "required_update_surfaces": [
                "validation/p5_capability_domain_matrix.json",
                "validation/p5_dependency_boundary_manifest.json",
                "validation/p5_build_cut_manifest.json",
                "focused capability-domain drift tests",
            ],
        },
        "not_claimed": [
            "commercial pricing tier",
            "license enforcement",
            "subscription entitlement",
            "fully buildable Base cut",
            "fully buildable Pro cut",
            "isolated Extra build cut",
            "enterprise connector implementation",
        ],
    }


def build_p5_dependency_boundary_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S9 dependency-boundary groundwork",
        "domain_kind": "technical_capability_domain_not_entitlement",
        "workspace_reality": {
            "workspace_model": "single_body_workspace",
            "root_dev_environment": "mixed_engineering_surface",
            "root_dev_includes": [
                "uns-stream[all-docs]",
                "zephyr-core",
                "zephyr-ingest",
                "zephyr-api",
            ],
            "root_dev_all_docs_meaning": (
                "development convenience evidence of missing cut isolation, not evidence that "
                "the entire uns-stream package is Pro"
            ),
        },
        "package_boundaries": [
            {
                "package": "uns-stream",
                "direct_dependencies_domain": "base_surface",
                "direct_dependency_evidence": ["unstructured"],
                "optional_extra_domain": "pro",
                "optional_extras": list(UNS_STREAM_PRO_EXTRAS),
                "extra_reserved_not_packaged": list(EXTRA_RESERVED_CAPABILITIES),
                "mixed_package_truth": True,
            },
            {
                "package": "it-stream",
                "direct_dependencies_domain": "base",
                "connector_domain": "base",
            },
            {
                "package": "zephyr-ingest",
                "direct_dependencies_domain": "shared",
                "connector_domain": "base_for_current_destinations",
            },
            {
                "package": "zephyr-core",
                "direct_dependencies_domain": "shared",
                "connector_domain": "not_a_connector_package",
            },
            {
                "package": "zephyr-api",
                "direct_dependencies_domain": "shared",
                "connector_domain": "not_a_connector_package",
            },
        ],
        "not_yet_domain_sliced": [
            "fixture/resource directories",
            "pytest selection surface",
            "runtime validation outputs",
            "recovery and benchmark validation artifacts",
        ],
    }


def build_p5_build_cut_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S9 build-cut groundwork",
        "domain_kind": "technical_capability_domain_not_entitlement",
        "build_cuts": {
            "base": {
                "directly_buildable_today": False,
                "contains": [
                    "it-stream current capabilities",
                    "uns-stream non-extra direct unstructured capability surface",
                    "all current non-enterprise source connectors",
                    "all current non-enterprise destination connectors",
                    "shared zephyr-core/zephyr-ingest/zephyr-api surfaces",
                ],
                "blocking_reality": (
                    "root workspace and tests are not yet domain-sliced; root dev currently "
                    "pulls uns-stream[all-docs]"
                ),
            },
            "pro": {
                "directly_buildable_today": False,
                "contains": ["base", "current uns-stream optional document extras"],
                "blocking_reality": (
                    "optional extras are represented, but no product cut artifact or installer "
                    "contract exists yet"
                ),
            },
            "extra": {
                "directly_buildable_today": False,
                "contains": list(EXTRA_RESERVED_CAPABILITIES),
                "blocking_reality": "Extra is currently an incubation domain, not a build cut",
            },
        },
        "future_cut_groundwork": [
            "domain matrix defines current assignments",
            "dependency manifest defines direct vs optional extra reality",
            "build-cut manifest defines what is not directly cuttable yet",
            "drift tests protect current assignments and promotion rule",
        ],
        "not_claimed": [
            "installer/release-consumable Base artifact",
            "installer/release-consumable Pro artifact",
            "isolated Extra artifact",
            "repo split",
            "license or entitlement enforcement",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5CapabilityDomainCheck:
    name: str
    ok: bool
    detail: str
    severity: CapabilityDomainSeverity = "error"


def _read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded_obj, dict):
        return None
    return cast(dict[str, object], loaded_obj)


def _read_toml_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    with path.open("rb") as handle:
        loaded_obj = tomllib.load(handle)
    return cast(dict[str, object], loaded_obj)


def _json_artifact_check(
    *,
    name: str,
    path: Path,
    expected: dict[str, object],
) -> P5CapabilityDomainCheck:
    observed = _read_json_object(path)
    return P5CapabilityDomainCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_capability_domain_artifacts() -> list[P5CapabilityDomainCheck]:
    checks = [
        _json_artifact_check(
            name="capability_domain_matrix_matches_helper",
            path=P5_CAPABILITY_DOMAIN_MATRIX_PATH,
            expected=build_p5_capability_domain_matrix(),
        ),
        _json_artifact_check(
            name="dependency_boundary_manifest_matches_helper",
            path=P5_DEPENDENCY_BOUNDARY_MANIFEST_PATH,
            expected=build_p5_dependency_boundary_manifest(),
        ),
        _json_artifact_check(
            name="build_cut_manifest_matches_helper",
            path=P5_BUILD_CUT_MANIFEST_PATH,
            expected=build_p5_build_cut_manifest(),
        ),
    ]

    matrix = build_p5_capability_domain_matrix()
    connectors = cast(dict[str, object], matrix["connectors"])
    connector_groups = (
        cast(list[dict[str, object]], connectors["it_stream_sources"]),
        cast(list[dict[str, object]], connectors["uns_stream_sources"]),
        cast(list[dict[str, object]], connectors["destinations"]),
    )
    connector_domain_failures = [
        str(connector["name"])
        for group in connector_groups
        for connector in group
        if connector["domain"] != "base" or connector["enterprise_grade_today"] is not False
    ]
    checks.append(
        P5CapabilityDomainCheck(
            name="current_connectors_base_non_enterprise",
            ok=not connector_domain_failures,
            detail=(
                "all current connectors are Base/non-enterprise"
                if not connector_domain_failures
                else "unexpected=" + ", ".join(connector_domain_failures)
            ),
        )
    )

    uns_surface = cast(dict[str, object], matrix["uns_stream_capability_surface"])
    checks.append(
        P5CapabilityDomainCheck(
            name="uns_stream_mixed_not_whole_package_pro",
            ok=uns_surface.get("package_domain_model") == "mixed_base_plus_pro_optional_extras"
            and uns_surface.get("not_modeled_as") == "whole_package_pro",
            detail=str(uns_surface.get("package_domain_model")),
        )
    )

    promotion = cast(dict[str, object], matrix["extra_to_pro_promotion"])
    checks.append(
        P5CapabilityDomainCheck(
            name="extra_to_pro_promotion_is_explicit",
            ok=promotion.get("requires_explicit_promotion") is True,
            detail=f"requires_explicit_promotion={promotion.get('requires_explicit_promotion')!r}",
        )
    )

    root_pyproject = _read_toml_object(repo_root() / "pyproject.toml")
    uns_pyproject = _read_toml_object(repo_root() / "packages" / "uns-stream" / "pyproject.toml")
    checks.extend(_validate_pyproject_domain_reality(root_pyproject, uns_pyproject))

    report_checks = (
        (
            "capability_domain_report_required_phrases",
            P5_CAPABILITY_DOMAIN_REPORT_PATH,
            (
                "technical capability domains",
                "not pricing",
                "uns-stream is mixed",
                "Extra to Pro promotion",
            ),
        ),
        (
            "output_boundary_report_required_phrases",
            P5_OUTPUT_BOUNDARY_REPORT_PATH,
            (
                "Base cut directly buildable today: no",
                "Pro cut directly buildable today: no",
                "Extra cut directly buildable today: no",
                "not entitlement",
            ),
        ),
    )
    for name, path, phrases in report_checks:
        if not path.exists():
            checks.append(P5CapabilityDomainCheck(name=name, ok=False, detail=str(path)))
            continue
        text = path.read_text(encoding="utf-8")
        missing = [phrase for phrase in phrases if phrase not in text]
        checks.append(
            P5CapabilityDomainCheck(
                name=name,
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )

    return checks


def _validate_pyproject_domain_reality(
    root_pyproject: dict[str, object] | None,
    uns_pyproject: dict[str, object] | None,
) -> list[P5CapabilityDomainCheck]:
    checks: list[P5CapabilityDomainCheck] = []
    root_dev_has_all_docs = False
    if root_pyproject is not None:
        groups_obj = root_pyproject.get("dependency-groups")
        if isinstance(groups_obj, dict):
            groups = cast(dict[str, object], groups_obj)
            dev_obj = groups.get("dev")
            if isinstance(dev_obj, list):
                dev_items = cast(list[object], dev_obj)
                root_dev_has_all_docs = any(item == "uns-stream[all-docs]" for item in dev_items)
    checks.append(
        P5CapabilityDomainCheck(
            name="root_dev_declares_mixed_all_docs_reality",
            ok=root_dev_has_all_docs,
            detail="root dev includes uns-stream[all-docs]"
            if root_dev_has_all_docs
            else "missing uns-stream[all-docs]",
        )
    )

    direct_unstructured = False
    optional_extras_present: set[str] = set()
    if uns_pyproject is not None:
        project_obj = uns_pyproject.get("project")
        if isinstance(project_obj, dict):
            project = cast(dict[str, object], project_obj)
            dependencies_obj = project.get("dependencies")
            if isinstance(dependencies_obj, list):
                dependencies = cast(list[object], dependencies_obj)
                direct_unstructured = any(
                    isinstance(dep, str) and dep.startswith("unstructured") for dep in dependencies
                )
            optional_obj = project.get("optional-dependencies")
            if isinstance(optional_obj, dict):
                optional = cast(dict[str, object], optional_obj)
                optional_extras_present = set(optional)
    missing_pro_extras = sorted(set(UNS_STREAM_PRO_EXTRAS).difference(optional_extras_present))
    unexpected_extra_reserved = sorted(
        set(EXTRA_RESERVED_CAPABILITIES).intersection(optional_extras_present)
    )
    checks.append(
        P5CapabilityDomainCheck(
            name="uns_stream_direct_unstructured_base_surface",
            ok=direct_unstructured,
            detail="direct unstructured dependency present"
            if direct_unstructured
            else "direct unstructured dependency missing",
        )
    )
    checks.append(
        P5CapabilityDomainCheck(
            name="uns_stream_pro_optional_extras_present",
            ok=not missing_pro_extras,
            detail="missing=" + (", ".join(missing_pro_extras) if missing_pro_extras else "none"),
        )
    )
    checks.append(
        P5CapabilityDomainCheck(
            name="extra_reserved_not_silently_packaged_as_pro",
            ok=not unexpected_extra_reserved,
            detail="unexpected="
            + (", ".join(unexpected_extra_reserved) if unexpected_extra_reserved else "none"),
        )
    )
    return checks


def format_p5_capability_domain_results(
    results: list[P5CapabilityDomainCheck],
) -> str:
    lines = ["P5-M4-S9 capability-domain checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_capability_domain_summary() -> str:
    return "\n".join(
        (
            "P5-M4-S9 capability domains:",
            "- domain kind: technical capability domain, not pricing/licensing/entitlement",
            "- current connectors: Base and non-enterprise",
            "- uns-stream: mixed Base direct surface plus Pro optional extras",
            "- Extra: incubating reserves audio/model_init with explicit promotion into Pro",
            "- build cuts: groundwork only; Base/Pro/Extra are not directly buildable today",
        )
    )


def render_p5_capability_domain_json() -> str:
    bundle = {
        "capability_domain_matrix": build_p5_capability_domain_matrix(),
        "dependency_boundary_manifest": build_p5_dependency_boundary_manifest(),
        "build_cut_manifest": build_p5_build_cut_manifest(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
