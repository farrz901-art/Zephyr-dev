from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Final, Literal, TypedDict, cast

P45Tier = Literal[
    "contract",
    "orchestration",
    "local-real",
    "service-live",
    "saas-live",
    "recovery-drill",
]
P45SurfaceKind = Literal["destination", "source", "input-path"]

P45_TIERS: Final[tuple[P45Tier, ...]] = (
    "contract",
    "orchestration",
    "local-real",
    "service-live",
    "saas-live",
    "recovery-drill",
)


class P45TierStatus(TypedDict):
    status: str


P45Authenticity = TypedDict(
    "P45Authenticity",
    {
        "contract": P45TierStatus,
        "orchestration": P45TierStatus,
        "local-real": P45TierStatus,
        "service-live": P45TierStatus,
        "saas-live": P45TierStatus,
        "recovery-drill": P45TierStatus,
    },
)


class P45TruthState(TypedDict):
    phase_sync_branch: str
    repository_phase_baseline: str
    docs_alignment_truth_state: str
    p4_complete: bool
    current_stage: str
    p5_started: bool
    p45_covers_full_retained_support_surface: bool
    baseline_and_second_round_use_dual_authenticity_standards: bool


class P45Surface(TypedDict):
    name: str
    kind: P45SurfaceKind
    owner: str
    retained: bool
    lineage: str
    authenticity: P45Authenticity


class P45TruthMatrix(TypedDict):
    schema_version: int
    phase: str
    truth_state: P45TruthState
    taxonomy: list[str]
    surfaces: list[P45Surface]


REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[5]
P45_TRUTH_MATRIX_PATH: Final[Path] = REPO_ROOT / "validation" / "p45_truth_matrix.json"


def load_p45_truth_matrix(path: Path | None = None) -> P45TruthMatrix:
    matrix_path = P45_TRUTH_MATRIX_PATH if path is None else path
    with matrix_path.open("r", encoding="utf-8") as handle:
        return cast(P45TruthMatrix, json.load(handle))


def iter_retained_surfaces(matrix: P45TruthMatrix) -> Iterator[P45Surface]:
    for surface in matrix["surfaces"]:
        if surface["retained"]:
            yield surface


def surface_names_by_kind(
    matrix: P45TruthMatrix, *, kind: P45SurfaceKind | None = None
) -> tuple[str, ...]:
    names: list[str] = []
    for surface in iter_retained_surfaces(matrix):
        if kind is None or surface["kind"] == kind:
            names.append(surface["name"])
    return tuple(names)
