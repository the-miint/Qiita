"""`coverage_idx` identity, against a real Postgres.

The point of a params-hash identity is that the SAME measurement always resolves to the
SAME idx (so a re-run replaces its rows instead of double-counting), and a DIFFERENT
measurement always resolves to a DIFFERENT one (so a threshold change re-mints instead of
silently landing new numbers under an idx whose stored params describe the old filter).

Both halves are asserted here, because only asserting the first would let a broken hash —
one that ignores a knob — pass.
"""

from __future__ import annotations

import pytest

from qiita_control_plane.repositories.coverage_definition import (
    build_coverage_params,
    fetch_coverage_definition_by_idx,
    lookup_coverage_idx_by_params,
    mint_coverage_definition,
)

pytestmark = pytest.mark.db


def _params(**overrides):
    base = dict(
        reference_idx=11,
        aligner="minimap2",
        preset="map-hifi",
        min_identity=0.95,
        min_aligned_fraction=0.90,
        depth_mode="include_deletions",
        mask_idx=5,
    )
    base.update(overrides)
    return build_coverage_params(**base)


async def test_the_same_config_resolves_to_the_same_idx(postgres_pool):
    """Idempotence. A resume, or a second ticket with the same config, must converge on one
    coverage_idx — that is what lets a re-run REPLACE its rows rather than double-count."""
    principal = await postgres_pool.fetchval("SELECT MIN(idx) FROM qiita.principal")

    first = await mint_coverage_definition(postgres_pool, _params(), principal)
    second = await mint_coverage_definition(postgres_pool, _params(), principal)
    assert first["coverage_idx"] == second["coverage_idx"]

    # And a pure lookup finds it without minting.
    assert await lookup_coverage_idx_by_params(postgres_pool, _params()) == first["coverage_idx"]

    stored = await fetch_coverage_definition_by_idx(postgres_pool, first["coverage_idx"])
    assert stored["params"] is not None, "the idx must be self-describing"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        # Every one of these changes the NUMBER in the feature table, so every one of them
        # must produce a different coverage_idx. A knob missing from the hash would show up
        # here as a collision — the new measurement silently reusing the old idx.
        ("reference_idx", 12),
        ("aligner", "bowtie2"),
        ("preset", "sr"),
        ("min_identity", 0.99),
        ("min_aligned_fraction", 0.50),
        ("depth_mode", "exclude_deletions"),
        ("mask_idx", 6),
    ],
)
async def test_changing_any_knob_re_mints(postgres_pool, field, value):
    principal = await postgres_pool.fetchval("SELECT MIN(idx) FROM qiita.principal")

    base = await mint_coverage_definition(postgres_pool, _params(), principal)
    changed = await mint_coverage_definition(postgres_pool, _params(**{field: value}), principal)

    assert changed["coverage_idx"] != base["coverage_idx"], (
        f"changing {field} did not re-mint — the new measurement would land under a "
        "coverage_idx whose stored params describe the OLD one, and nothing would notice"
    )
