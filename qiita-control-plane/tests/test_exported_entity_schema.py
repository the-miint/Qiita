"""Schema-level invariants for `qiita.exported_entity`.

These cover the guarantees that live in the database itself — the ones a caller
cannot reach and a future migration could quietly drop.
"""

import asyncpg
import pytest

from qiita_control_plane.testing.db_seeds import (
    cleanup_exported_entity_probe,
    retire_biosample,
    seed_exported_entity_probe,
)

pytestmark = pytest.mark.db

# Entity column and handle prefix per kind — the closed set the table admits, and
# the only source a kind is varied from. The kind is also the entity's table name.
_ENTITY_KINDS = {
    "study": ("study_idx", "QS"),
    "biosample": ("biosample_idx", "QB"),
}


@pytest.fixture
async def probe(postgres_pool):
    principal_idx, study_idxs, biosample_idx = await seed_exported_entity_probe(
        postgres_pool, prefix="expent-schema", study_count=1
    )
    yield {
        "principal_idx": principal_idx,
        "study_idx": study_idxs[0],
        "biosample_idx": biosample_idx,
    }
    await cleanup_exported_entity_probe(
        postgres_pool,
        principal_idx=principal_idx,
        study_idxs=study_idxs,
        biosample_idx=biosample_idx,
    )


async def _mint(postgres_pool, probe, kind) -> asyncpg.Record:
    """Insert the handle row naming `probe`'s entity of `kind`."""
    column, _prefix = _ENTITY_KINDS[kind]
    return await postgres_pool.fetchrow(
        "INSERT INTO qiita.exported_entity"
        f"       ({column}, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx, export_entity_id",
        probe[column],
        probe["principal_idx"],
    )


@pytest.mark.parametrize("kind", list(_ENTITY_KINDS))
async def test_export_entity_id_is_prefixed_and_tracks_idx(postgres_pool, probe, kind):
    """Tests the case where an entity is handed its first handle."""
    _column, prefix = _ENTITY_KINDS[kind]
    row = await _mint(postgres_pool, probe, kind)
    assert row["export_entity_id"] == f"{prefix}{row['idx']}"


async def test_export_entity_id_cannot_be_supplied(postgres_pool, probe):
    """Tests the case where a caller tries to author a public handle itself.

    GENERATED ALWAYS, so the database refuses the INSERT outright rather than
    accepting the value and overwriting it.
    """
    with pytest.raises(asyncpg.PostgresError, match="generated column"):
        await postgres_pool.execute(
            "INSERT INTO qiita.exported_entity"
            "       (export_entity_id, study_idx, created_by_idx)"
            " VALUES ('QSforged', $1, $2)",
            probe["study_idx"],
            probe["principal_idx"],
        )


async def test_export_entity_id_cannot_be_edited_after_publication(postgres_pool, probe):
    """Tests the case where an already-published handle is edited in place."""
    await _mint(postgres_pool, probe, "study")
    with pytest.raises(asyncpg.PostgresError, match="generated column"):
        await postgres_pool.execute(
            "UPDATE qiita.exported_entity SET export_entity_id = 'QSedited' WHERE study_idx = $1",
            probe["study_idx"],
        )


async def test_a_row_names_exactly_one_entity(postgres_pool, probe):
    """Tests the case where one row names two entities at once."""
    with pytest.raises(asyncpg.CheckViolationError, match="one_entity"):
        await postgres_pool.execute(
            "INSERT INTO qiita.exported_entity"
            "       (study_idx, biosample_idx, created_by_idx)"
            " VALUES ($1, $2, $3)",
            probe["study_idx"],
            probe["biosample_idx"],
            probe["principal_idx"],
        )


async def test_a_row_naming_no_entity_is_refused(postgres_pool, probe):
    """Tests the case where a row names no entity at all.

    Refused by the one-entity CHECK and by the handle's NOT NULL together: with no
    column set the prefix CASE has no branch and composes NULL.
    """
    with pytest.raises(asyncpg.IntegrityConstraintViolationError):
        await postgres_pool.execute(
            "INSERT INTO qiita.exported_entity (created_by_idx) VALUES ($1)",
            probe["principal_idx"],
        )


@pytest.mark.parametrize("kind", list(_ENTITY_KINDS))
async def test_one_handle_per_entity(postgres_pool, probe, kind):
    """Tests the case where an entity that already holds a handle is minted again.

    A handle can never be superseded, so what was published keeps resolving.
    """
    await _mint(postgres_pool, probe, kind)
    with pytest.raises(asyncpg.UniqueViolationError):
        await _mint(postgres_pool, probe, kind)


@pytest.mark.parametrize("kind", list(_ENTITY_KINDS))
async def test_an_entity_with_a_published_handle_cannot_be_hard_deleted(postgres_pool, probe, kind):
    """Tests the case where the entity under a published handle is deleted.

    RESTRICT, not CASCADE: the handle must not vanish because someone removed the
    row it names.
    """
    column, _prefix = _ENTITY_KINDS[kind]
    await _mint(postgres_pool, probe, kind)
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await postgres_pool.execute(f"DELETE FROM qiita.{kind} WHERE idx = $1", probe[column])


async def test_a_retired_entity_keeps_its_handle(postgres_pool, probe):
    """Tests the case where an entity is retired after its handle is published.

    The handle is unaffected; the entity's own row is where its retirement is read
    from.
    """
    minted = await _mint(postgres_pool, probe, "biosample")
    await retire_biosample(
        postgres_pool,
        biosample_idx=probe["biosample_idx"],
        retired_by_idx=probe["principal_idx"],
    )

    resolved = await postgres_pool.fetchrow(
        "SELECT ee.export_entity_id, bs.retired"
        "  FROM qiita.exported_entity ee"
        "  JOIN qiita.biosample bs ON bs.idx = ee.biosample_idx"
        " WHERE ee.idx = $1",
        minted["idx"],
    )
    expected = {"export_entity_id": minted["export_entity_id"], "retired": True}
    assert dict(resolved) == expected
