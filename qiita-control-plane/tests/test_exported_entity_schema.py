"""Schema-level invariants for `qiita.exported_entity`.

These cover the guarantees that live in the database itself — the ones a caller
cannot reach and a future migration could quietly drop.
"""

import asyncpg
import pytest

from qiita_control_plane.testing.db_seeds import (
    cleanup_exported_entity_probe,
    retire_biosample,
    seed_biosample,
    seed_exported_entity_probe,
    seed_service_principal,
    seed_study,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

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


@pytest.fixture
async def service_principal_idx(postgres_pool):
    principal_idx = await seed_service_principal(
        postgres_pool, prefix="expent-schema", suffix="service"
    )
    yield principal_idx
    await postgres_pool.execute(
        "DELETE FROM qiita.service_account WHERE principal_idx = $1", principal_idx
    )
    await delete_principal(postgres_pool, principal_idx)


async def _handles(postgres_pool, column, entity_idx) -> list[asyncpg.Record]:
    """Every handle row naming the entity `entity_idx` under `column`."""
    rows = await postgres_pool.fetch(
        f"SELECT idx, export_entity_id, created_by_idx FROM qiita.exported_entity"
        f" WHERE {column} = $1",
        entity_idx,
    )
    return rows


async def _handle(postgres_pool, probe, kind) -> asyncpg.Record:
    """The handle row naming `probe`'s entity of `kind`."""
    column, _prefix = _ENTITY_KINDS[kind]
    rows = await _handles(postgres_pool, column, probe[column])
    return rows[0]


@pytest.mark.parametrize("kind", list(_ENTITY_KINDS))
async def test_tg_mint_exported_entity_mints_on_insert(postgres_pool, probe, kind):
    """Tests the case where an entity is inserted and is handed exactly one handle.

    The handle is prefixed by kind, tracks the handle row's own idx, and is
    attributed to whoever created the entity.
    """
    column, prefix = _ENTITY_KINDS[kind]
    rows = await _handles(postgres_pool, column, probe[column])

    actual = [dict(row) for row in rows]
    # The handle row's idx comes from an identity sequence, so it is read back.
    handle_idx = rows[0]["idx"] if rows else None
    expected = [
        {
            "idx": handle_idx,
            "export_entity_id": f"{prefix}{handle_idx}",
            "created_by_idx": probe["principal_idx"],
        }
    ]
    assert actual == expected


async def test_tg_mint_exported_entity_accepts_service_account_creator(
    postgres_pool, probe, service_principal_idx
):
    """Tests the case where a service account creates the entities.

    The handle carries the entity's creator, and a service account is a legitimate
    one, so the mint must not reject it.
    """
    study_idx = await seed_study(
        postgres_pool,
        owner_idx=probe["principal_idx"],
        title="expent-schema service",
        created_by_idx=service_principal_idx,
    )
    biosample_idx = await seed_biosample(
        postgres_pool, owner_idx=probe["principal_idx"], created_by_idx=service_principal_idx
    )
    try:
        rows = await postgres_pool.fetch(
            "SELECT study_idx, biosample_idx, created_by_idx FROM qiita.exported_entity"
            " WHERE study_idx = $1 OR biosample_idx = $2"
            " ORDER BY study_idx IS NULL",
            study_idx,
            biosample_idx,
        )
        actual = [dict(row) for row in rows]
        expected = [
            {
                "study_idx": study_idx,
                "biosample_idx": None,
                "created_by_idx": service_principal_idx,
            },
            {
                "study_idx": None,
                "biosample_idx": biosample_idx,
                "created_by_idx": service_principal_idx,
            },
        ]
        assert actual == expected
    finally:
        await teardown_entity_graph(
            postgres_pool,
            study_idxs=[study_idx],
            biosample_idxs=[biosample_idx],
            prep_sample_idxs=[],
        )


async def test_every_entity_has_a_handle(postgres_pool, probe):
    """Tests the case where the whole database is checked for an entity without a
    handle.

    Covers the backfill and the mint on insert together: every committed study and
    biosample holds a handle, whichever of the two gave it one.
    """
    rows = await postgres_pool.fetch(
        "SELECT 'study' AS kind, s.idx FROM qiita.study s"
        "  LEFT JOIN qiita.exported_entity ee ON ee.study_idx = s.idx"
        " WHERE ee.idx IS NULL"
        " UNION ALL"
        " SELECT 'biosample' AS kind, b.idx FROM qiita.biosample b"
        "  LEFT JOIN qiita.exported_entity ee ON ee.biosample_idx = b.idx"
        " WHERE ee.idx IS NULL"
    )
    assert [dict(row) for row in rows] == []


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
    """Tests the case where an entity that already holds a handle is given another.

    The entity received its handle when it was inserted, so a second one collides.
    A handle can never be superseded, so what was published keeps resolving.
    """
    column, _prefix = _ENTITY_KINDS[kind]
    with pytest.raises(asyncpg.UniqueViolationError):
        await postgres_pool.execute(
            f"INSERT INTO qiita.exported_entity ({column}, created_by_idx) VALUES ($1, $2)",
            probe[column],
            probe["principal_idx"],
        )


@pytest.mark.parametrize("kind", list(_ENTITY_KINDS))
async def test_an_entity_with_a_published_handle_cannot_be_hard_deleted(postgres_pool, probe, kind):
    """Tests the case where the entity under a published handle is deleted.

    RESTRICT, not CASCADE: the handle must not vanish because someone removed the
    row it names.
    """
    column, _prefix = _ENTITY_KINDS[kind]
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await postgres_pool.execute(f"DELETE FROM qiita.{kind} WHERE idx = $1", probe[column])


async def test_a_retired_entity_keeps_its_handle(postgres_pool, probe):
    """Tests the case where an entity is retired after its handle is published.

    The handle is unaffected; the entity's own row is where its retirement is read
    from.
    """
    minted = await _handle(postgres_pool, probe, "biosample")
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
