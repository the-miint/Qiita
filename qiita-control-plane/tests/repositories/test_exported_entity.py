"""The idempotent mint for `qiita.exported_entity`."""

import asyncio

import pytest

from qiita_control_plane.repositories.exported_entity import mint_exported_entities
from qiita_control_plane.testing.db_seeds import (
    cleanup_exported_entity_probe,
    seed_exported_entity_probe,
)

pytestmark = pytest.mark.db


@pytest.fixture
async def probe(postgres_pool):
    principal_idx, study_idxs, biosample_idx = await seed_exported_entity_probe(
        postgres_pool, prefix="expent-repo", study_count=2
    )
    yield {
        "principal_idx": principal_idx,
        "study_idxs": study_idxs,
        "biosample_idx": biosample_idx,
    }
    await cleanup_exported_entity_probe(
        postgres_pool,
        principal_idx=principal_idx,
        study_idxs=study_idxs,
        biosample_idx=biosample_idx,
    )


def _expect(entries, actual):
    """The expected records for `entries`, a list of (study_idx, biosample_idx).

    The handle is minted from the table's identity sequence and so is not
    predictable, so it is looked up from `actual` by the entity it names. The
    ordering claim is carried by the two idx columns, which are known in advance,
    and `entries` is in the order the mint promises — so a row out of place, a row
    missing, or a row too many all still fail the comparison.
    """
    handles = {(row["study_idx"], row["biosample_idx"]): row["export_entity_id"] for row in actual}
    return [
        {
            "study_idx": study,
            "biosample_idx": biosample,
            "export_entity_id": handles.get((study, biosample)),
        }
        for study, biosample in entries
    ]


async def test_mint_exported_entities_names_every_entity_requested(postgres_pool, probe):
    """Tests the case where one call names studies and biosamples together.

    The mint runs an INSERT per kind but reads both back with a single query, and
    that query is where the two arms rejoin — a mistake in it drops a whole kind
    silently, which minting one kind at a time would never reveal.
    """
    first_study, second_study = probe["study_idxs"]
    rows = await mint_exported_entities(
        postgres_pool,
        study_idx=[first_study, second_study],
        biosample_idx=[probe["biosample_idx"]],
        created_by_idx=probe["principal_idx"],
    )

    actual = [dict(row) for row in rows]
    expected = _expect(
        [
            (first_study, None),
            (second_study, None),
            (None, probe["biosample_idx"]),
        ],
        actual,
    )
    assert actual == expected


async def test_mint_exported_entities_is_idempotent(postgres_pool, probe):
    """Tests the case where the same entities are minted a second time.

    A handle is published, so the second answer must equal the first and no second
    row may be written for an entity that already has one.
    """
    first_study, second_study = probe["study_idxs"]
    call = {
        "study_idx": [first_study, second_study],
        "biosample_idx": [probe["biosample_idx"]],
        "created_by_idx": probe["principal_idx"],
    }
    first_rows = await mint_exported_entities(postgres_pool, **call)
    second_rows = await mint_exported_entities(postgres_pool, **call)

    assert [dict(row) for row in second_rows] == [dict(row) for row in first_rows]
    written = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.exported_entity"
        " WHERE study_idx = ANY($1::bigint[]) OR biosample_idx = $2",
        probe["study_idxs"],
        probe["biosample_idx"],
    )
    assert written == 3


async def test_mint_exported_entities_one_kind_alone(postgres_pool, probe):
    """Tests the case where a request names one kind and leaves the other empty."""
    first_study, second_study = probe["study_idxs"]
    rows = await mint_exported_entities(
        postgres_pool,
        study_idx=[first_study, second_study],
        biosample_idx=[],
        created_by_idx=probe["principal_idx"],
    )

    actual = [dict(row) for row in rows]
    expected = _expect([(first_study, None), (second_study, None)], actual)
    assert actual == expected


async def test_mint_exported_entities_mixes_existing_and_fresh(postgres_pool, probe):
    """Tests the case where a request names one entity that already holds a handle
    and one that does not.

    The established handle has to survive verbatim: recovering it is the same
    operation as minting the new one, not a separate path.
    """
    first_study, second_study = probe["study_idxs"]
    established = await mint_exported_entities(
        postgres_pool,
        study_idx=[first_study],
        biosample_idx=[],
        created_by_idx=probe["principal_idx"],
    )
    established_handle = established[0]["export_entity_id"]

    rows = await mint_exported_entities(
        postgres_pool,
        study_idx=[first_study, second_study],
        biosample_idx=[probe["biosample_idx"]],
        created_by_idx=probe["principal_idx"],
    )

    actual = [dict(row) for row in rows]
    expected = _expect(
        [
            (first_study, None),
            (second_study, None),
            (None, probe["biosample_idx"]),
        ],
        actual,
    )
    assert actual == expected
    assert actual[0]["export_entity_id"] == established_handle


async def test_mint_exported_entities_concurrent_callers_agree(postgres_pool, probe):
    """Tests the case where two callers mint the same fresh entity at once.

    Neither caller fails and both receive the same handle.
    """
    first_study = probe["study_idxs"][0]
    call = {
        "study_idx": [first_study],
        "biosample_idx": [],
        "created_by_idx": probe["principal_idx"],
    }
    left, right = await asyncio.gather(
        mint_exported_entities(postgres_pool, **call),
        mint_exported_entities(postgres_pool, **call),
    )

    assert [dict(row) for row in left] == [dict(row) for row in right]
    written = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.exported_entity WHERE study_idx = $1", first_study
    )
    assert written == 1
