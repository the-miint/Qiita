"""The read of `qiita.exported_entity`."""

import pytest

from qiita_control_plane.repositories.exported_entity import (
    MissingExportedEntityError,
    fetch_exported_entities,
)
from qiita_control_plane.testing.db_seeds import (
    cleanup_exported_entity_probe,
    seed_exported_entity_probe,
)

pytestmark = pytest.mark.db

# An idx no entity holds: every identity column starts at 1.
_ABSENT_IDX = -1


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


async def _expect(postgres_pool, entries):
    """The expected records for `entries`, a list of (study_idx, biosample_idx).

    The handle is minted from the table's identity sequence and so is not
    predictable, so each one is looked up per entity, independently of the query
    under test. `entries` is in the order the fetch promises — so a row out of
    place, a row missing, or a row too many all still fail the comparison.
    """
    expected = []
    for study, biosample in entries:
        handle = await postgres_pool.fetchval(
            "SELECT export_entity_id FROM qiita.exported_entity"
            " WHERE study_idx IS NOT DISTINCT FROM $1"
            "   AND biosample_idx IS NOT DISTINCT FROM $2",
            study,
            biosample,
        )
        expected.append(
            {"study_idx": study, "biosample_idx": biosample, "export_entity_id": handle}
        )
    return expected


async def test_fetch_exported_entities_names_every_entity_requested(postgres_pool, probe):
    """Tests the case where one call names studies and biosamples together.

    Both kinds are read with a single query, and that query is where the two
    rejoin — a mistake in it drops a whole kind silently, which fetching one kind
    at a time would never reveal.
    """
    first_study, second_study = probe["study_idxs"]
    rows = await fetch_exported_entities(
        postgres_pool,
        study_idx=[first_study, second_study],
        biosample_idx=[probe["biosample_idx"]],
    )

    actual = [dict(row) for row in rows]
    expected = await _expect(
        postgres_pool,
        [
            (first_study, None),
            (second_study, None),
            (None, probe["biosample_idx"]),
        ],
    )
    assert actual == expected


async def test_fetch_exported_entities_one_kind_alone(postgres_pool, probe):
    """Tests the case where a request names one kind and leaves the other empty."""
    first_study, second_study = probe["study_idxs"]
    rows = await fetch_exported_entities(
        postgres_pool,
        study_idx=[first_study, second_study],
        biosample_idx=[],
    )

    actual = [dict(row) for row in rows]
    expected = await _expect(postgres_pool, [(first_study, None), (second_study, None)])
    assert actual == expected


async def test_fetch_exported_entities_raise_missing(postgres_pool, probe):
    """Tests the case where a request names an entity that has no handle.

    The fetch raises rather than returning a short list, and names only the
    missing entity and its kind, not the ones that resolved beside it.
    """
    with pytest.raises(MissingExportedEntityError) as caught:
        await fetch_exported_entities(
            postgres_pool,
            study_idx=probe["study_idxs"],
            biosample_idx=[probe["biosample_idx"], _ABSENT_IDX],
        )

    actual = {"missing": caught.value.missing, "kind": caught.value.kind}
    expected = {"missing": [_ABSENT_IDX], "kind": "biosample"}
    assert actual == expected
