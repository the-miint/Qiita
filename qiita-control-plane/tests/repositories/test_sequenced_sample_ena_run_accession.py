"""Tests for `fetch_sequenced_sample_idxs_by_ena_run_accession`, the lookup that
makes an ENA re-import idempotent. Each test seeds and asserts inside one
rolled-back transaction.
"""

import secrets

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


async def _seed_sequenced_sample(conn, owner, *, ena_run_accession=None) -> tuple[int, int]:
    """Seed a full chain (biosample -> prep_sample -> sequenced_sample); return
    (prep_sample_idx, sequenced_sample_idx)."""
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
        ena_run_accession=ena_run_accession,
    )
    return ps_idx, ss_idx


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
