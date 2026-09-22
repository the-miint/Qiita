"""Fixtures for the analytic tests, and only for them.

`qiita_common.analytic` is SQL text, and text is only worth what it does on a
connection — so `test_behaviour_miint.py` and `test_circular_gate_contract.py` execute
it against real duckdb-miint. That needs the extension present before anything LOADs it.

Scoped to this directory rather than to `tests/`: every other qiita-common test is pure
Python, and staging the extension for them would make the whole tier reach the mirror on
a cold cache.
"""

from qiita_common.duckdb_miint import setup_miint_test_env

# Point miint installs at the team mirror + a per-component private extension dir
# before any test connects (shared with the control-plane and orchestrator conftests).
setup_miint_test_env("common")

import pytest  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _stage_miint_extension():
    """Stage miint once into the per-component extension dir, so the LOAD-only
    connects in this suite find it — the unit-tier mirror of what the deploy stages
    and what the control-plane, orchestrator, and integration conftests already do.

    Plain INSTALL, not the deploy's FORCE, so the stable temp dir keeps caching: the
    first run downloads from the mirror and later ones are instant.
    """
    import duckdb  # noqa: PLC0415

    from qiita_common.duckdb_miint import (  # noqa: PLC0415
        miint_connect_config,
        miint_install_sql,
        miint_load_sql,
    )

    with duckdb.connect(":memory:", config=miint_connect_config()) as conn:
        conn.execute(miint_install_sql())
        conn.execute(miint_load_sql())
