from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p5_capability_domains import (
    DESTINATION_CONNECTORS,
    EXTRA_RESERVED_CAPABILITIES,
    IT_STREAM_SOURCE_CONNECTORS,
    UNS_STREAM_PRO_EXTRAS,
    UNS_STREAM_SOURCE_CONNECTORS,
)
from zephyr_ingest.testing.p45 import repo_root

ProductCutSeverity = Literal["error", "warning", "info"]

P5_PRODUCT_CUT_MANIFEST_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_product_cut_manifest.json"
)
P5_DEPENDENCY_SLICE_MANIFEST_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_dependency_slice_manifest.json"
)
P5_DESTINATION_SURFACE_CLASSIFICATION_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_destination_surface_classification.json"
)
P5_PACKAGING_OUTPUT_MANIFEST_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_packaging_output_manifest.json"
)
P5_PACKAGING_SKELETON_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S10_PACKAGING_SKELETON.md"
)

DESTINATION_COMPOSITION_SURFACES: Final[tuple[str, ...]] = ("fanout",)


def build_p5_product_cut_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S10 build-cut packaging skeleton",
        "domain_kind": "technical_capability_domain_not_entitlement",
        "skeleton_status": "groundwork_only_not_installable",
        "cuts": {
            "base": {
                "directly_buildable_today": False,
                "intended_contains": {
                    "shared_core": ["zephyr-core"],
                    "shared_orchestration": ["zephyr-ingest"],
                    "shared_service": ["zephyr-api"],
                    "flow_packages": [
                        "it-stream",
                        "uns-stream direct non-extra unstructured surface",
                    ],
                    "source_connectors": {
                        "it_stream": list(IT_STREAM_SOURCE_CONNECTORS),
                        "uns_stream": list(UNS_STREAM_SOURCE_CONNECTORS),
                    },
                    "single_sink_destinations": list(DESTINATION_CONNECTORS),
                    "destination_composition_surfaces": list(DESTINATION_COMPOSITION_SURFACES),
                },
                "not_directly_cuttable_because": [
                    "workspace is still single-body",
                    "root dev includes uns-stream[all-docs]",
                    "tests, fixtures, and validation outputs are not domain-sliced",
                ],
            },
            "pro": {
                "directly_buildable_today": False,
                "inherits": "base",
                "incremental_additions": {
                    "uns_stream_optional_extras": list(UNS_STREAM_PRO_EXTRAS),
                    "future_mature_enterprise_connectors": "deferred_not_in_s10",
                },
                "not_modeled_as": "whole_uns_stream_package",
            },
            "extra": {
                "directly_buildable_today": False,
                "default_packaged": False,
                "incubating_reserved_capabilities": list(EXTRA_RESERVED_CAPABILITIES),
                "promotion_rule": "explicit_promotion_to_pro_required",
            },
        },
        "not_claimed": [
            "installable Base artifact",
            "installable Pro artifact",
            "installable Extra artifact",
            "license or entitlement enforcement",
            "repo split",
            "deployment packaging",
        ],
    }


def build_p5_dependency_slice_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S10 dependency-slice skeleton",
        "domain_kind": "technical_capability_domain_not_entitlement",
        "root_dev_convenience": {
            "includes": [
                "uns-stream[all-docs]",
                "zephyr-core",
                "zephyr-ingest",
                "zephyr-api",
            ],
            "meaning": (
                "engineering convenience only; not product-cut truth and not evidence that "
                "uns-stream is whole-package Pro"
            ),
        },
        "slices": {
            "shared": {
                "packages": ["zephyr-core", "zephyr-ingest", "zephyr-api"],
                "cut_role": "shared across Base and Pro skeletons",
            },
            "base": {
                "product_cut_ready": False,
                "package_intent": [
                    "it-stream current Base structured flow",
                    "uns-stream direct non-extra unstructured surface",
                ],
                "uns_stream_dependency_mode": {
                    "direct_dependency_basis": "unstructured",
                    "exclude_optional_extras_by_default": True,
                },
                "connectors": {
                    "sources": list(IT_STREAM_SOURCE_CONNECTORS + UNS_STREAM_SOURCE_CONNECTORS),
                    "single_sink_destinations": list(DESTINATION_CONNECTORS),
                    "composition_surfaces": list(DESTINATION_COMPOSITION_SURFACES),
                },
            },
            "pro_incremental": {
                "product_cut_ready": False,
                "requires_base": True,
                "uns_stream_optional_extras": list(UNS_STREAM_PRO_EXTRAS),
                "not_modeled_as": "whole_uns_stream_package",
            },
            "extra": {
                "product_cut_ready": False,
                "default_included": False,
                "reserved_capabilities": list(EXTRA_RESERVED_CAPABILITIES),
                "dependency_intent": "incubating_non_default_no_committed_package_slice",
            },
        },
        "not_yet_sliced": [
            "fixtures/resources",
            "pytest selection",
            "runtime validation outputs",
            "release/installer outputs",
        ],
    }


def build_p5_destination_surface_classification() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S10 destination surface classification",
        "single_sink_destinations": [
            {
                "name": name,
                "surface_type": "single_sink_destination_connector",
                "domain": "base",
                "composition_surface": False,
            }
            for name in DESTINATION_CONNECTORS
        ],
        "composition_surfaces": [
            {
                "name": "fanout",
                "surface_type": "destination_composition_orchestration",
                "domain": "base_composition_surface",
                "single_sink_connector": False,
                "reason": "orchestrates child destinations rather than writing one sink itself",
            }
        ],
        "abstract_surfaces": [
            {
                "name": "base.py",
                "surface_type": "abstract_destination_base_not_connector",
                "single_sink_connector": False,
            }
        ],
        "rules": [
            "fanout must not be classified as a single sink connector",
            "single sink connector lists must not include abstract destination base classes",
            "composition surfaces may be Base capability surfaces without becoming sink connectors",
        ],
    }


def build_p5_packaging_output_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S10 packaging-output skeleton",
        "domain_kind": "technical_capability_domain_not_entitlement",
        "output_status": "validation_artifacts_only_not_product_packages",
        "future_outputs": {
            "base": {
                "installable_today": False,
                "may_consume": [
                    "validation/p5_capability_domain_matrix.json",
                    "validation/p5_product_cut_manifest.json",
                    "validation/p5_dependency_slice_manifest.json",
                    "validation/p5_destination_surface_classification.json",
                ],
                "must_not_assume": ["uns-stream optional extras", "installer contract exists"],
            },
            "pro": {
                "installable_today": False,
                "may_consume": [
                    "Base skeleton",
                    "uns-stream optional extras from dependency slice manifest",
                ],
                "must_not_assume": [
                    "whole uns-stream package is Pro",
                    "enterprise connectors are implemented",
                ],
            },
            "extra": {
                "installable_today": False,
                "default_packaged": False,
                "may_consume": ["audio/model_init reserved truth"],
                "must_not_assume": ["Extra is ready for default packaging"],
            },
        },
        "deferred": [
            "installer/release-consumable outputs",
            "domain-sliced tests and fixtures",
            "repo split",
            "commercial entitlement enforcement",
            "deployment packaging",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5ProductCutCheck:
    name: str
    ok: bool
    detail: str
    severity: ProductCutSeverity = "error"


def _read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded_obj, dict):
        return None
    return cast(dict[str, object], loaded_obj)


def _json_artifact_check(
    *,
    name: str,
    path: Path,
    expected: dict[str, object],
) -> P5ProductCutCheck:
    observed = _read_json_object(path)
    return P5ProductCutCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_product_cut_artifacts() -> list[P5ProductCutCheck]:
    checks = [
        _json_artifact_check(
            name="product_cut_manifest_matches_helper",
            path=P5_PRODUCT_CUT_MANIFEST_PATH,
            expected=build_p5_product_cut_manifest(),
        ),
        _json_artifact_check(
            name="dependency_slice_manifest_matches_helper",
            path=P5_DEPENDENCY_SLICE_MANIFEST_PATH,
            expected=build_p5_dependency_slice_manifest(),
        ),
        _json_artifact_check(
            name="destination_surface_classification_matches_helper",
            path=P5_DESTINATION_SURFACE_CLASSIFICATION_PATH,
            expected=build_p5_destination_surface_classification(),
        ),
        _json_artifact_check(
            name="packaging_output_manifest_matches_helper",
            path=P5_PACKAGING_OUTPUT_MANIFEST_PATH,
            expected=build_p5_packaging_output_manifest(),
        ),
    ]

    classification = build_p5_destination_surface_classification()
    sink_entries = cast(list[dict[str, object]], classification["single_sink_destinations"])
    composition_entries = cast(list[dict[str, object]], classification["composition_surfaces"])
    sink_names = [cast(str, entry["name"]) for entry in sink_entries]
    composition_names = [cast(str, entry["name"]) for entry in composition_entries]
    checks.append(
        P5ProductCutCheck(
            name="fanout_is_composition_not_sink",
            ok="fanout" in composition_names and "fanout" not in sink_names,
            detail=f"sinks={sink_names!r}; composition={composition_names!r}",
        )
    )

    product_cut = build_p5_product_cut_manifest()
    cuts = cast(dict[str, object], product_cut["cuts"])
    pro = cast(dict[str, object], cuts["pro"])
    extra = cast(dict[str, object], cuts["extra"])
    checks.append(
        P5ProductCutCheck(
            name="uns_stream_stays_mixed_in_product_cut",
            ok=pro.get("not_modeled_as") == "whole_uns_stream_package",
            detail=str(pro.get("not_modeled_as")),
        )
    )
    checks.append(
        P5ProductCutCheck(
            name="extra_stays_non_default_in_product_cut",
            ok=extra.get("default_packaged") is False,
            detail=f"default_packaged={extra.get('default_packaged')!r}",
        )
    )

    packaging = build_p5_packaging_output_manifest()
    future_outputs = cast(dict[str, object], packaging["future_outputs"])
    not_installable_failures = [
        name
        for name, output_obj in future_outputs.items()
        if isinstance(output_obj, dict)
        and cast(dict[str, object], output_obj).get("installable_today") is not False
    ]
    checks.append(
        P5ProductCutCheck(
            name="packaging_outputs_are_skeleton_only",
            ok=not not_installable_failures,
            detail="unexpected_installable="
            + (", ".join(not_installable_failures) if not_installable_failures else "none"),
        )
    )

    if P5_PACKAGING_SKELETON_REPORT_PATH.exists():
        report_text = P5_PACKAGING_SKELETON_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "not installable product packages",
            "fanout is composition",
            "uns-stream remains mixed",
            "technical domains, not entitlement",
        )
        missing = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5ProductCutCheck(
                name="packaging_skeleton_report_required_phrases",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
    else:
        checks.append(
            P5ProductCutCheck(
                name="packaging_skeleton_report_required_phrases",
                ok=False,
                detail=str(P5_PACKAGING_SKELETON_REPORT_PATH),
            )
        )

    return checks


def format_p5_product_cut_results(results: list[P5ProductCutCheck]) -> str:
    lines = ["P5-M4-S10 product-cut skeleton checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_product_cut_summary() -> str:
    return "\n".join(
        (
            "P5-M4-S10 product-cut skeleton:",
            "- status: skeleton only; not installable product packages",
            "- Base: shared/core plus current Base connectors and uns non-extra surface",
            "- Pro: Base plus uns-stream optional extras; uns-stream remains mixed",
            "- Extra: non-default incubation reserves audio/model_init",
            "- fanout is composition/orchestration, not a single sink connector",
            "- technical domains, not entitlement, licensing, or pricing",
        )
    )


def render_p5_product_cut_json() -> str:
    bundle = {
        "product_cut_manifest": build_p5_product_cut_manifest(),
        "dependency_slice_manifest": build_p5_dependency_slice_manifest(),
        "destination_surface_classification": build_p5_destination_surface_classification(),
        "packaging_output_manifest": build_p5_packaging_output_manifest(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
