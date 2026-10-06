"""A flagged sequenced_sample (`ena_status IS NOT NULL`) must not appear in
any roster a mask, alignment, download, or export step reads from. One test
per exclusion site, each seeding a flagged run beside a healthy one and
asserting only the healthy one is returned.

Each test seeds and asserts inside one rolled-back transaction.
"""

import secrets

import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

from qiita_control_plane.repositories._sample_helpers import insert_entity_to_study
from qiita_control_plane.repositories._sample_scope import sample_scope_sql
from qiita_control_plane.repositories.alignment_definition import list_pool_prep_sample_idxs
from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample import insert_prep_sample
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.repositories.sequenced_sample import (
    fetch_pool_members,
    fetch_sequenced_pool_ena_run_roster,
    fetch_sequenced_pool_samples,
    fetch_sequenced_sample_idxs_for_run,
    fetch_sequenced_sample_idxs_for_study,
    fetch_sequenced_samples_for_run,
    insert_sequenced_sample,
)
from qiita_control_plane.repositories.sequencing_run import (
    insert_sequenced_pool,
    insert_sequencing_run,
)

pytestmark = pytest.mark.db


def _suffix(label: str) -> str:
    return f"{label}-{secrets.token_hex(4)}"


async def _create_user(conn) -> int:
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        _suffix("user"),
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{_suffix('u')}@example.com",
    )
    return pidx


async def _create_study(conn, owner) -> int:
    return await conn.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner,
        _suffix("study"),
    )


async def _seed_pool(conn, owner) -> tuple[int, int]:
    """Return (sequencing_run_idx, sequenced_pool_idx)."""
    run_idx, _ = await insert_sequencing_run(
        conn, instrument_run_id=_suffix("RUN"), platform="illumina", created_by_idx=owner
    )
    pool_idx, _ = await insert_sequenced_pool(
        conn, sequencing_run_idx=run_idx, created_by_idx=owner
    )
    return run_idx, pool_idx


async def _seed_sample(
    conn,
    owner,
    *,
    sequenced_pool_idx: int,
    ena_status: str | None = None,
    study_idx: int | None = None,
) -> tuple[int, int]:
    """Seed one biosample -> prep_sample -> sequenced_sample chain in
    `sequenced_pool_idx`, optionally flagged and/or study-linked. Return
    (prep_sample_idx, sequenced_sample_idx)."""
    biosample_idx = await insert_biosample(conn, owner_idx=owner, created_by_idx=owner)
    protocol_idx = await conn.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = 'short_read_metagenomics'"
    )
    ps_idx = await insert_prep_sample(
        conn,
        biosample_idx=biosample_idx,
        owner_idx=owner,
        prep_protocol_idx=protocol_idx,
        processing_kind="sequenced",
        created_by_idx=owner,
    )
    ss_idx = await insert_sequenced_sample(
        conn,
        prep_sample_idx=ps_idx,
        sequenced_pool_idx=sequenced_pool_idx,
        sequenced_pool_item_id=_suffix("ITEM"),
        created_by_idx=owner,
        ena_run_accession=_suffix("SRR"),
    )
    if ena_status is not None:
        await conn.execute(
            "UPDATE qiita.sequenced_sample SET ena_status = $2, ena_availability_checked_at = now()"
            " WHERE idx = $1",
            ss_idx,
            ena_status,
        )
    if study_idx is not None:
        await insert_entity_to_study(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=biosample_idx,
            study_idx=study_idx,
            created_by_idx=owner,
        )
        await conn.execute(
            "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
            " VALUES ($1, $2, $3)",
            ps_idx,
            study_idx,
            owner,
        )
    return ps_idx, ss_idx


async def test_fetch_sequenced_sample_idxs_for_run_excludes_flagged(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            run_idx = await conn.fetchval(
                "SELECT sequencing_run_idx FROM qiita.sequenced_pool WHERE idx = $1", pool_idx
            )
            _, healthy_ss = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx, ena_status="suppressed")

            result = await fetch_sequenced_sample_idxs_for_run(
                conn, sequencing_run_idx=run_idx, limit=10
            )
            assert result == [healthy_ss]
        finally:
            await tr.rollback()


async def test_fetch_sequenced_pool_samples_excludes_flagged(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            _, healthy_ss = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx, ena_status="suppressed")

            rows = await fetch_sequenced_pool_samples(conn, sequenced_pool_idx=pool_idx, limit=10)
            assert [r["sequenced_sample_idx"] for r in rows] == [healthy_ss]
        finally:
            await tr.rollback()


async def test_fetch_sequenced_pool_ena_run_roster_excludes_flagged(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            healthy_ps, _ = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx, ena_status="suppressed")

            rows = await fetch_sequenced_pool_ena_run_roster(conn, sequenced_pool_idx=pool_idx)
            assert [r["prep_sample_idx"] for r in rows] == [healthy_ps]
        finally:
            await tr.rollback()


async def test_fetch_sequenced_samples_for_run_excludes_flagged(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            run_idx = await conn.fetchval(
                "SELECT sequencing_run_idx FROM qiita.sequenced_pool WHERE idx = $1", pool_idx
            )
            _, healthy_ss = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx, ena_status="withdrawn")

            rows = await fetch_sequenced_samples_for_run(conn, sequencing_run_idx=run_idx, limit=10)
            assert [r["sequenced_sample_idx"] for r in rows] == [healthy_ss]
        finally:
            await tr.rollback()


async def test_fetch_sequenced_sample_idxs_for_study_excludes_flagged(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _create_study(conn, owner)
            _, pool_idx = await _seed_pool(conn, owner)
            _, healthy_ss = await _seed_sample(
                conn, owner, sequenced_pool_idx=pool_idx, study_idx=study_idx
            )
            await _seed_sample(
                conn,
                owner,
                sequenced_pool_idx=pool_idx,
                ena_status="suppressed",
                study_idx=study_idx,
            )

            result = await fetch_sequenced_sample_idxs_for_study(
                conn, study_idx=study_idx, limit=10
            )
            assert result == [healthy_ss]
        finally:
            await tr.rollback()


async def test_list_pool_prep_sample_idxs_excludes_flagged(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            healthy_ps, _ = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx, ena_status="suppressed")

            result = await list_pool_prep_sample_idxs(conn, pool_idx)
            assert result == [healthy_ps]
        finally:
            await tr.rollback()


async def test_sample_scope_sql_excludes_flagged(postgres_pool):
    """`sample_scope_sql`'s unconditional predicate, applied over a bare
    prep_sample roster CTE the way every gate-roster read does."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            healthy_ps, _ = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            flagged_ps, _ = await _seed_sample(
                conn, owner, sequenced_pool_idx=pool_idx, ena_status="suppressed"
            )

            args: list = []
            clauses, narrowed = sample_scope_sql(
                alias="roster",
                args=args,
                sequenced_pool_idx=None,
                prep_sample_idx=None,
                visible_to_principal_idx=None,
            )
            assert not narrowed
            rows = await conn.fetch(
                "WITH roster AS ("
                "  SELECT idx AS prep_sample_idx FROM qiita.prep_sample"
                "   WHERE idx = ANY($1::bigint[])"
                f") SELECT prep_sample_idx FROM roster WHERE 1=1 {clauses}",
                [healthy_ps, flagged_ps],
                *args,
            )
            assert [r["prep_sample_idx"] for r in rows] == [healthy_ps]
        finally:
            await tr.rollback()


async def test_enumerate_pool_samples_excludes_flagged(postgres_pool):
    from qiita_control_plane.block_planner import _enumerate_pool_samples

    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            healthy_ps, _ = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            flagged_ps, _ = await _seed_sample(
                conn, owner, sequenced_pool_idx=pool_idx, ena_status="suppressed"
            )
            for ps_idx in (healthy_ps, flagged_ps):
                await mint_sequence_range(
                    conn,
                    prep_sample_idx=ps_idx,
                    count=10,
                    principal_idx=owner,
                    work_ticket_idx=None,
                )

            samples = await _enumerate_pool_samples(conn, pool_idx)
            assert [s.prep_sample_idx for s in samples] == [healthy_ps]
        finally:
            await tr.rollback()


async def test_fetch_pool_members_excludes_flagged(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, pool_idx = await _seed_pool(conn, owner)
            healthy_ps, _ = await _seed_sample(conn, owner, sequenced_pool_idx=pool_idx)
            flagged_ps, _ = await _seed_sample(
                conn, owner, sequenced_pool_idx=pool_idx, ena_status="suppressed"
            )
            for ps_idx in (healthy_ps, flagged_ps):
                await mint_sequence_range(
                    conn,
                    prep_sample_idx=ps_idx,
                    count=10,
                    principal_idx=owner,
                    work_ticket_idx=None,
                )

            members = await fetch_pool_members(conn, pool_idx)
            assert [m[0] for m in members] == [healthy_ps]
        finally:
            await tr.rollback()
