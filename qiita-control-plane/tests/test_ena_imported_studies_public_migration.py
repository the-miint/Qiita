"""DB test for the migration that makes every ENA-imported study public.

The migration (db/migrations/20261120000000_ena_imported_studies_public.sql) sets
`default_tier = 'public'` on each study that an ENA import created
(`qiita.ena_import_batch_item.study_created`). It refuses to apply, naming the
studies, while one of them holds a sample that did not come from ENA: a
biosample with no `ena_sample_accession`, or a sequenced_sample with no
`ena_run_accession`.

The up- and down-SQL are read from the file, so the test tracks the real
migration rather than a copy. Each test runs them in a transaction that it rolls
back, after clearing `study_created` on every other batch's items inside that
transaction, so studies that other tests leave behind cannot trip the guard.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.models import Tier

from qiita_control_plane.repositories.ena_import_batch import (
    insert_ena_import_batch,
    insert_ena_import_batch_item,
)
from qiita_control_plane.testing.db_seeds import (
    retire_biosample_to_study_link,
    retire_prep_sample_to_study_link,
    seed_biosample,
    seed_biosample_to_study_link,
    seed_prep_sample_to_study_link,
    seed_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_study,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)
from qiita_control_plane.testing.migrations import MIGRATIONS_DIR, migrate_down, migrate_up
from qiita_control_plane.testing.unique_names import unique_ena_accession

pytestmark = pytest.mark.db

_MIGRATION = MIGRATIONS_DIR / "20261120000000_ena_imported_studies_public.sql"


@pytest_asyncio.fixture
async def mig(postgres_pool):
    """A study an import created and a study an import only matched, both at
    the `member` default, each recorded on an item of one batch."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="enapub", suffix=suffix)
    created_idx = await seed_study(
        postgres_pool, owner_idx=principal_idx, title=f"enapub created {suffix}"
    )
    matched_idx = await seed_study(
        postgres_pool, owner_idx=principal_idx, title=f"enapub matched {suffix}"
    )
    async with postgres_pool.acquire() as conn, conn.transaction():
        batch_idx = await insert_ena_import_batch(conn, submitted_by_principal_idx=principal_idx)
        for study_idx, study_created in ((created_idx, True), (matched_idx, False)):
            item_idx = await insert_ena_import_batch_item(
                conn, batch_idx=batch_idx, ena_study_accession=unique_ena_accession("PRJNA")
            )
            await conn.execute(
                "UPDATE qiita.ena_import_batch_item SET study_idx = $2, study_created = $3"
                " WHERE idx = $1",
                item_idx,
                study_idx,
                study_created,
            )
    ctx = {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "batch_idx": batch_idx,
        "created_idx": created_idx,
        "matched_idx": matched_idx,
        "biosample_idxs": [],
        "prep_sample_idxs": [],
        "sequenced_pools": [],
        "sequencing_runs": [],
    }
    yield ctx

    # The sweep takes the batch's items with their studies; the batch row is left.
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[created_idx, matched_idx],
        biosample_idxs=ctx["biosample_idxs"],
        prep_sample_idxs=ctx["prep_sample_idxs"],
    )
    await delete_idxs(postgres_pool, "ena_import_batch", [batch_idx])
    await delete_idxs(postgres_pool, "sequenced_pool", ctx["sequenced_pools"])
    await delete_idxs(postgres_pool, "sequencing_run", ctx["sequencing_runs"])
    await delete_principal(postgres_pool, [principal_idx])


async def _seed_biosample_in(mig, *, study_idx: int, with_ena_accession: bool) -> int:
    """A biosample linked to `study_idx`. With an ENA accession it carries the
    `ena_sample_accession` that an import writes, and no `biosample_accession`,
    as an import leaves it."""
    pool, owner = mig["pool"], mig["principal_idx"]
    biosample_idx = await seed_biosample(pool, owner_idx=owner, created_by_idx=owner)
    mig["biosample_idxs"].append(biosample_idx)
    if with_ena_accession:
        await pool.execute(
            "UPDATE qiita.biosample SET ena_sample_accession = $2 WHERE idx = $1",
            biosample_idx,
            unique_ena_accession("ERS"),
        )
    await seed_biosample_to_study_link(
        pool, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner
    )
    return biosample_idx


async def _seed_sequenced_sample_in(
    mig, *, study_idx: int, biosample_idx: int, with_ena_accession: bool
) -> int:
    """A sequenced_sample in `study_idx`, on `biosample_idx`, returning its
    prep_sample_idx. With an ENA accession it carries the run accession that an
    import writes."""
    pool, owner = mig["pool"], mig["principal_idx"]
    prep_sample_idx = await seed_sequenced_prep_sample(
        pool, biosample_idx=biosample_idx, owner_idx=owner
    )
    mig["prep_sample_idxs"].append(prep_sample_idx)
    await seed_prep_sample_to_study_link(
        pool, prep_sample_idx=prep_sample_idx, study_idx=study_idx, created_by_idx=owner
    )
    run_idx, pool_idx, sequenced_sample_idx = await seed_sequenced_sample_subtype(
        pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=owner,
        sequenced_pool_item_id=f"enapub-{secrets.token_hex(4)}",
    )
    mig["sequencing_runs"].append(run_idx)
    mig["sequenced_pools"].append(pool_idx)
    if with_ena_accession:
        await pool.execute(
            "UPDATE qiita.sequenced_sample SET ena_run_accession = $2 WHERE idx = $1",
            sequenced_sample_idx,
            unique_ena_accession("ERR"),
        )
    return prep_sample_idx


async def _in_isolation(conn: asyncpg.Connection, mig) -> None:
    """Inside the caller's transaction, leave this test's batch as the only one
    recording a study it created."""
    await conn.execute(
        "UPDATE qiita.ena_import_batch_item SET study_created = false WHERE batch_idx <> $1",
        mig["batch_idx"],
    )


async def _tiers(conn: asyncpg.Connection, mig) -> tuple[Tier, Tier]:
    rows = await conn.fetch(
        "SELECT idx, default_tier FROM qiita.study WHERE idx = ANY($1::bigint[])",
        [mig["created_idx"], mig["matched_idx"]],
    )
    tiers = {row["idx"]: Tier(row["default_tier"]) for row in rows}
    return tiers[mig["created_idx"]], tiers[mig["matched_idx"]]


async def _up_then_tiers(mig) -> tuple[Tier, Tier]:
    """Apply the up step in a transaction that is rolled back, and return the
    two studies' tiers as it left them."""
    async with mig["pool"].acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await _in_isolation(conn, mig)
            await conn.execute(migrate_up(_MIGRATION.read_text()))
            return await _tiers(conn, mig)
        finally:
            await tr.rollback()


async def test_up_makes_an_import_created_study_public_and_leaves_a_matched_one(mig):
    assert await _up_then_tiers(mig) == (Tier.PUBLIC, Tier.MEMBER)


async def test_up_accepts_samples_that_came_from_ena(mig):
    biosample_idx = await _seed_biosample_in(
        mig, study_idx=mig["created_idx"], with_ena_accession=True
    )
    await _seed_sequenced_sample_in(
        mig, study_idx=mig["created_idx"], biosample_idx=biosample_idx, with_ena_accession=True
    )
    assert await _up_then_tiers(mig) == (Tier.PUBLIC, Tier.MEMBER)


@pytest.mark.parametrize("retired", ["biosample", "sequenced_sample"])
async def test_up_ignores_a_sample_retired_from_the_study(mig, retired):
    """A biosample or prep_sample whose link to the import-created study is
    retired is no longer in it, so it does not block the study."""
    study_idx, by_idx = mig["created_idx"], mig["principal_idx"]
    biosample_idx = await _seed_biosample_in(
        mig, study_idx=study_idx, with_ena_accession=retired != "biosample"
    )
    if retired == "biosample":
        await retire_biosample_to_study_link(
            mig["pool"], biosample_idx=biosample_idx, study_idx=study_idx, retired_by_idx=by_idx
        )
    else:
        prep_sample_idx = await _seed_sequenced_sample_in(
            mig, study_idx=study_idx, biosample_idx=biosample_idx, with_ena_accession=False
        )
        await retire_prep_sample_to_study_link(
            mig["pool"],
            prep_sample_idx=prep_sample_idx,
            study_idx=study_idx,
            retired_by_idx=by_idx,
        )
    assert await _up_then_tiers(mig) == (Tier.PUBLIC, Tier.MEMBER)


async def test_up_ignores_a_study_that_the_import_only_matched(mig):
    """A sample without an ENA accession in a study that an import only matched
    does not block, since that study is not made public."""
    biosample_idx = await _seed_biosample_in(
        mig, study_idx=mig["matched_idx"], with_ena_accession=False
    )
    await _seed_sequenced_sample_in(
        mig, study_idx=mig["matched_idx"], biosample_idx=biosample_idx, with_ena_accession=False
    )
    assert await _up_then_tiers(mig) == (Tier.PUBLIC, Tier.MEMBER)


@pytest.mark.parametrize("lacks_accession", ["biosample", "sequenced_sample"])
async def test_up_refuses_while_an_import_created_study_holds_data_not_from_ena(
    mig, lacks_accession
):
    """A biosample with no `ena_sample_accession`, or a sequenced_sample with no
    `ena_run_accession`, did not come from ENA: making the study public would
    publish it, so the migration refuses and names the study."""
    biosample_idx = await _seed_biosample_in(
        mig, study_idx=mig["created_idx"], with_ena_accession=lacks_accession != "biosample"
    )
    await _seed_sequenced_sample_in(
        mig,
        study_idx=mig["created_idx"],
        biosample_idx=biosample_idx,
        with_ena_accession=lacks_accession != "sequenced_sample",
    )
    async with mig["pool"].acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await _in_isolation(conn, mig)
            with pytest.raises(asyncpg.RaiseError) as excinfo:
                await conn.execute(migrate_up(_MIGRATION.read_text()))
            assert str(mig["created_idx"]) in str(excinfo.value)
        finally:
            await tr.rollback()
    async with mig["pool"].acquire() as conn:
        assert await _tiers(conn, mig) == (Tier.MEMBER, Tier.MEMBER)


async def test_down_returns_import_created_studies_to_member(mig):
    sql = _MIGRATION.read_text()
    async with mig["pool"].acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await _in_isolation(conn, mig)
            await conn.execute(migrate_up(sql))
            await conn.execute(migrate_down(sql))
            assert await _tiers(conn, mig) == (Tier.MEMBER, Tier.MEMBER)
        finally:
            await tr.rollback()
