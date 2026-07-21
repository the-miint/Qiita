"""Tests for the ENA-import provenance surface on qiita.sequenced_sample
(T02-4): the source_archive/resolver_kind/transport kwargs on
insert_sequenced_sample, and fetch_sequenced_sample_idxs_by_ena_run_accession
(T02-5's idempotent-re-import lookup).

Pattern 1 (transaction-rollback per test): all seed and assertions happen
inside one rolled-back transaction, mirroring test_study.py /
test_sequencing_run_pool_lookup.py.
"""

import secrets

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.prep_sample import insert_prep_sample
from qiita_control_plane.repositories.sequenced_sample import (
    fetch_sequenced_sample_idxs_by_ena_run_accession,
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


async def _seed_sequenced_sample(conn, owner, **provenance_kwargs) -> tuple[int, int]:
    """Seed a full chain (biosample -> prep_sample -> sequenced_sample) and
    return (prep_sample_idx, sequenced_sample_idx)."""
    biosample_idx = await insert_biosample(conn, owner_idx=owner, created_by_idx=owner)
    protocol_idx = await conn.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = 'short_read_metagenomics'"
    )
    run_idx, _ = await insert_sequencing_run(
        conn, instrument_run_id=_suffix("RUN"), platform="illumina", created_by_idx=owner
    )
    pool_idx, _ = await insert_sequenced_pool(
        conn, sequencing_run_idx=run_idx, created_by_idx=owner
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
        sequenced_pool_idx=pool_idx,
        sequenced_pool_item_id=_suffix("ITEM"),
        created_by_idx=owner,
        **provenance_kwargs,
    )
    return ps_idx, ss_idx


async def test_insert_sequenced_sample_provenance_columns_default_null(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, ss_idx = await _seed_sequenced_sample(conn, owner)

            row = await conn.fetchrow(
                "SELECT source_archive, resolver_kind, transport"
                " FROM qiita.sequenced_sample WHERE idx = $1",
                ss_idx,
            )
            assert row["source_archive"] is None
            assert row["resolver_kind"] is None
            assert row["transport"] is None
        finally:
            await tr.rollback()


async def test_insert_sequenced_sample_provenance_columns_round_trip(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            _, ss_idx = await _seed_sequenced_sample(
                conn,
                owner,
                ena_experiment_accession=_suffix("SRX"),
                ena_run_accession=_suffix("SRR"),
                source_archive="ena",
                resolver_kind="miint",
            )

            row = await conn.fetchrow(
                "SELECT source_archive, resolver_kind, transport"
                " FROM qiita.sequenced_sample WHERE idx = $1",
                ss_idx,
            )
            assert row["source_archive"] == "ena"
            assert row["resolver_kind"] == "miint"
            assert row["transport"] is None
        finally:
            await tr.rollback()


async def test_insert_sequenced_sample_rejects_bad_source_archive(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            with pytest.raises(asyncpg.CheckViolationError):
                await _seed_sequenced_sample(conn, owner, source_archive="not-a-real-archive")
        finally:
            await tr.rollback()


async def test_fetch_sequenced_sample_idxs_by_ena_run_accession_resolves_and_omits_misses(
    postgres_pool,
):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            run_accession = _suffix("SRR")
            _, ss_idx = await _seed_sequenced_sample(conn, owner, ena_run_accession=run_accession)

            resolved = await fetch_sequenced_sample_idxs_by_ena_run_accession(
                conn, values=[run_accession, "SRR-absent"]
            )

            assert resolved == {run_accession: ss_idx}
        finally:
            await tr.rollback()


async def test_fetch_sequenced_sample_idxs_by_ena_run_accession_empty_input(postgres_pool):
    assert await fetch_sequenced_sample_idxs_by_ena_run_accession(postgres_pool, values=[]) == {}
