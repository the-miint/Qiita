"""The invariant that makes a published `params_hash` verifiable: a stored config's
canonical hash still matches the digest stored beside it.

`alignment_definition.params_hash` is computed in Python from the config dict, once,
before storage. Everything that reads it back — the pool alignment listing, and the
CLI's refusal to build on a mismatch — re-hashes the config as Postgres returns it.
For that comparison to mean anything, the jsonb round trip has to be lossless as far
as `canonical_params_hash` can see, and for JSON numbers it is not always: Postgres
stores them as `numeric` and renders them in plain decimal.

These tests pin both sides — the shapes the real planner produces round-trip, and the
two known-hostile shapes are refused at the mint rather than stored and discovered
later. Written as tests rather than a comment because the boundary is a property of
`numeric`'s output format, which is not something to re-derive from memory.
"""

import json
import uuid

import pytest
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane.repositories.alignment_definition import (
    ParamsDoNotSurviveStorageError,
    mint_alignment_definition,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


@pytest.fixture
async def principal_idx(postgres_pool):
    idx = await seed_user_principal(
        postgres_pool, prefix="params-storage", suffix=str(uuid.uuid4())[:8]
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE created_by_idx = $1", idx
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", idx)


async def test_the_planners_own_config_shape_round_trips(postgres_pool, principal_idx):
    """The shape `align_planner` actually builds: ints, a string, and a list of ints.
    Nothing here goes through `numeric`'s reformatting in a way Python re-reads
    differently, which is why the digest published today is verifiable."""
    params = {
        "reference_idx": 9,
        "aligner": "minimap2",
        "mask_idx": 2,
        "shard_ids": [0, 1, 2],
        "tag": str(uuid.uuid4()),
    }
    async with postgres_pool.acquire() as conn:
        row = await mint_alignment_definition(conn, params=params, principal_idx=principal_idx)
    stored_params = await postgres_pool.fetchval(
        "SELECT params::text FROM qiita.alignment_definition WHERE alignment_idx = $1",
        row["alignment_idx"],
    )
    assert canonical_params_hash(json.loads(stored_params)) == bytes(row["params_hash"])


@pytest.mark.parametrize(
    ("label", "value"),
    [
        # jsonb renders this as 1500000000000000000000000000000 — plain decimal with no
        # fractional part — which Python then re-reads as an int, not a float.
        ("large magnitude float", 1.5e30),
        # numeric has no negative zero.
        ("negative zero", -0.0),
    ],
)
async def test_a_config_that_storage_would_reshape_is_refused(
    postgres_pool, principal_idx, label, value
):
    """At the mint, not at the read. Stored, this config would produce an
    `alignment_idx` whose published digest never again matches its own params — so
    every build over it would be refused, with nothing to point at."""
    params = {"reference_idx": 9, "aligner": "minimap2", label: value}
    async with postgres_pool.acquire() as conn:
        with pytest.raises(ParamsDoNotSurviveStorageError):
            await mint_alignment_definition(conn, params=params, principal_idx=principal_idx)


async def test_a_small_magnitude_float_is_allowed_because_it_does_round_trip(
    postgres_pool, principal_idx
):
    """The near-miss, kept as a test so the refusal above is not read as "no floats".
    jsonb expands 1.23e-05 to 0.0000123, and Python re-reads that as the same float, so
    the digest still matches and there is nothing to refuse.
    """
    params = {"reference_idx": 9, "threshold": 1.23e-05, "tag": str(uuid.uuid4())}
    async with postgres_pool.acquire() as conn:
        row = await mint_alignment_definition(conn, params=params, principal_idx=principal_idx)
    assert row["alignment_idx"] is not None
