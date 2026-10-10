"""The read of `qiita.exported_entity`."""

import pytest

from qiita_control_plane.repositories.exported_entity import (
    MissingExportedEntityError,
    export_entity_id_select,
    fetch_exported_entities,
    require_export_entity_id,
)
from qiita_control_plane.testing.db_seeds import (
    fetch_export_entity_id,
    seed_exported_entity_probe,
)
from qiita_control_plane.testing.db_teardown import cleanup_exported_entity_probe

pytestmark = pytest.mark.db

# An idx no entity holds: identity values are positive.
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

    The export_entity_id comes from the table's identity sequence and so is not
    predictable, so each one is looked up per entity, independently of the query
    under test. `entries` is in the order the fetch promises — so a row out of
    place, a row missing, or a row too many all still fail the comparison.
    """
    expected = []
    for study, biosample in entries:
        if study is not None:
            handle = await fetch_export_entity_id(postgres_pool, kind="study", entity_idx=study)
        else:
            handle = await fetch_export_entity_id(
                postgres_pool, kind="biosample", entity_idx=biosample
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


@pytest.mark.parametrize("kind", ["study", "biosample"])
async def test_fetch_exported_entities_raise_missing(postgres_pool, probe, kind):
    """Tests the case where a request names an entity of `kind` that has no
    export_entity_id, beside entities of both kinds that resolve.

    The fetch raises rather than returning a short list, and names only the
    missing entity and its kind: the other kind's rows, NULL in this kind's
    column, neither satisfy nor hide the gap.
    """
    study_idx = list(probe["study_idxs"])
    biosample_idx = [probe["biosample_idx"]]
    # Add the absent idx to the list of the kind under test.
    if kind == "study":
        study_idx = [*study_idx, _ABSENT_IDX]
    else:
        biosample_idx = [*biosample_idx, _ABSENT_IDX]
    with pytest.raises(MissingExportedEntityError) as caught:
        await fetch_exported_entities(
            postgres_pool, study_idx=study_idx, biosample_idx=biosample_idx
        )

    actual = {"missing": caught.value.missing, "kind": caught.value.kind}
    expected = {"missing": [_ABSENT_IDX], "kind": kind}
    assert actual == expected


def test_require_export_entity_id():
    """Tests the case where an entity row carries its export_entity_id."""
    row = {"idx": 7, "export_entity_id": "QS3"}
    assert require_export_entity_id(row, kind="study") == "QS3"


def test_require_export_entity_id_raise_missing():
    """Tests the case where an entity row's export_entity_id is NULL.

    The error names the entity by its idx and kind, rather than letting the
    NULL reach a response.
    """
    row = {"idx": 7, "export_entity_id": None}
    with pytest.raises(MissingExportedEntityError) as caught:
        require_export_entity_id(row, kind="biosample")

    actual = {"missing": caught.value.missing, "kind": caught.value.kind}
    expected = {"missing": [7], "kind": "biosample"}
    assert actual == expected


def test_export_entity_id_select_raise_bad_alias():
    """Tests the case where the table alias is not a plain identifier.

    The alias is interpolated into SQL text, so anything else is refused before it
    reaches a query.
    """
    with pytest.raises(ValueError, match="plain SQL identifier"):
        export_entity_id_select("study", alias="s; DROP TABLE qiita.study")
