from __future__ import annotations

import pytest

from zephyr_ingest.testing.p45 import LoadedP45Env, check_services, format_probe_results


@pytest.mark.auth_local_real
def test_p45_local_real_substrate_healthcheck(p45_env: LoadedP45Env) -> None:
    results = check_services(p45_env, tier="local-real")
    assert results
    assert all(result.ok for result in results), format_probe_results(results)
