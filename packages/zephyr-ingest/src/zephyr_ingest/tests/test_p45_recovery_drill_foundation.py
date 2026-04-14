from __future__ import annotations

import pytest

from zephyr_ingest.testing.p45_matrix import P45_TIERS, load_p45_truth_matrix


@pytest.mark.auth_recovery_drill
def test_p45_recovery_drill_foundation_keeps_full_retained_surface_in_scope() -> None:
    matrix = load_p45_truth_matrix()
    retained = [surface for surface in matrix["surfaces"] if surface["retained"]]

    assert retained
    for surface in retained:
        assert set(surface["authenticity"]) == set(P45_TIERS)
        assert surface["authenticity"]["recovery-drill"]["status"] == "planned"
