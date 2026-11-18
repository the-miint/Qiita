"""DB test for the migration that makes every ENA-imported study public.

The migration (db/migrations/20261118000000_ena_imported_studies_public.sql) sets
`default_tier = 'public'` on each study that an ENA import created
(`qiita.ena_import_batch_item.study_created`). It refuses to apply, naming the
studies, while one of them holds data that did not come from ENA: a biosample
with no `ena_sample_accession`, or a sequenced_sample with no
`ena_run_accession`.

The up- and down-SQL are read from the file, so the test tracks the real
migration rather than a copy. Each test runs them in a transaction that it rolls
back, after clearing `study_created` on every other batch's items inside that
transaction, so studies that other tests leave behind cannot trip the guard.
"""

import secrets
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.models import Tier

from qiita_control_plane.repositories.ena_import_batch import (
    insert_ena_import_batch,
    insert_ena_import_batch_item,
)
from qiita_control_plane.testing.db_seeds import (
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
from qiita_control_plane.testing.unique_names import unique_ena_accession

pytestmark = pytest.mark.db

_MIGRATION = "20261118000000_ena_imported_studies_public.sql"


def _migration_sql(section: str) -> str:
    """The migration's `up` or `down` SQL, read from the file."""
    text = (Path(__file__).resolve().parents[1] / "db" / "migrations" / _MIGRATION).read_text()
    up, down = text.split("-- migrate:up", 1)[1].split("-- migrate:down", 1)
    return {"up": up, "down": down}[section].strip()


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

    await delete_idxs(postgres_pool, "ena_import_batch", [batch_idx])
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[created_idx, matched_idx],
        biosample_idxs=ctx["biosample_idxs"],
        prep_sample_idxs=ctx["prep_sample_idxs"],
    )
    await delete_idxs(postgres_pool, "sequenced_pool", ctx["sequenced_pools"])
    await delete_idxs(postgres_pool, "sequencing_run", ctx["sequencing_runs"])
    await delete_principal(postgres_pool, [principal_idx])


async def _seed_biosample_in(mig, *, from_ena: bool) -> int:
    """A biosample linked to the import-created study. One that came from ENA
    carries the `ena_sample_accession` an import writes, and no
    `biosample_accession`, as an import leaves it."""
    pool, owner = mig["pool"], mig["principal_idx"]
    biosample_idx = await seed_biosample(pool, owner_idx=owner, created_by_idx=owner)
    mig["biosample_idxs"].append(biosample_idx)
    if from_ena:
        await pool.execute(
            "UPDATE qiita.biosample SET ena_sample_accession = $2 WHERE idx = $1",
            biosample_idx,
            f"ERS{secrets.randbelow(10**9)}",
        )
    await seed_biosample_to_study_link(
        pool, biosample_idx=biosample_idx, study_idx=mig["created_idx"], created_by_idx=owner
    )
    return biosample_idx


async def _seed_sequenced_sample_in(mig, *, biosample_idx: int, from_ena: bool) -> None:
    """A sequenced_sample in the import-created study, on `biosample_idx`. One
    that came from ENA carries the run accession an import writes."""
    pool, owner = mig["pool"], mig["principal_idx"]
    prep_sample_idx = await seed_sequenced_prep_sample(
        pool, biosample_idx=biosample_idx, owner_idx=owner
    )
    mig["prep_sample_idxs"].append(prep_sample_idx)
    await seed_prep_sample_to_study_link(
        pool, prep_sample_idx=prep_sample_idx, study_idx=mig["created_idx"], created_by_idx=owner
    )
    run_idx, pool_idx, sequenced_sample_idx = await seed_sequenced_sample_subtype(
        pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=owner,
        sequenced_pool_item_id=f"enapub-{secrets.token_hex(4)}",
    )
    mig["sequencing_runs"].append(run_idx)
    mig["sequenced_pools"].append(pool_idx)
    if from_ena:
        await pool.execute(
            "UPDATE qiita.sequenced_sample SET ena_run_accession = $2 WHERE idx = $1",
            sequenced_sample_idx,
            f"ERR{secrets.randbelow(10**9)}",
        )


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


async def test_up_makes_an_import_created_study_public_and_leaves_a_matched_one(mig):
    async with mig["pool"].acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await _in_isolation(conn, mig)
            await conn.execute(_migration_sql("up"))
            assert await _tiers(conn, mig) == (Tier.PUBLIC, Tier.MEMBER)
        finally:
            await tr.rollback()


async def test_up_accepts_samples_that_came_from_ena(mig):
    biosample_idx = await _seed_biosample_in(mig, from_ena=True)
    await _seed_sequenced_sample_in(mig, biosample_idx=biosample_idx, from_ena=True)
    async with mig["pool"].acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await _in_isolation(conn, mig)
            await conn.execute(_migration_sql("up"))
            assert await _tiers(conn, mig) == (Tier.PUBLIC, Tier.MEMBER)
        finally:
            await tr.rollback()


@pytest.mark.parametrize("not_from_ena", ["biosample", "sequenced_sample"])
async def test_up_refuses_while_an_import_created_study_holds_data_not_from_ena(mig, not_from_ena):
    """A biosample with no `ena_sample_accession`, or a sequenced_sample with no
    `ena_run_accession`, did not come from ENA: making the study public would
    publish it, so the migration refuses and names the study."""
    biosample_idx = await _seed_biosample_in(mig, from_ena=not_from_ena != "biosample")
    await _seed_sequenced_sample_in(
        mig, biosample_idx=biosample_idx, from_ena=not_from_ena != "sequenced_sample"
    )
    async with mig["pool"].acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await _in_isolation(conn, mig)
            with pytest.raises(asyncpg.RaiseError) as excinfo:
                await conn.execute(_migration_sql("up"))
            assert str(mig["created_idx"]) in str(excinfo.value)
        finally:
            await tr.rollback()
    async with mig["pool"].acquire() as conn:
        assert await _tiers(conn, mig) == (Tier.MEMBER, Tier.MEMBER)


async def test_down_returns_import_created_studies_to_member(mig):
    async with mig["pool"].acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await _in_isolation(conn, mig)
            await conn.execute(_migration_sql("up"))
            await conn.execute(_migration_sql("down"))
            assert await _tiers(conn, mig) == (Tier.MEMBER, Tier.MEMBER)
        finally:
            await tr.rollback()
