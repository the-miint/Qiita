"""Tests for repositories.prep_protocol.fetch_prep_protocol_idx_by_name.

qiita.prep_protocol is a system-admin curated, seed-migration-populated
table (db/migrations/20260501000010_prep_protocol_prep_sample_field.sql
seeds the five curated names); this repo module has no write surface.
"""

import pytest

from qiita_control_plane.repositories.prep_protocol import (
    PrepProtocolUnknownError,
    fetch_prep_protocol_idx_by_name,
)

pytestmark = pytest.mark.db


async def test_fetch_prep_protocol_idx_by_name_resolves_seeded_protocol(postgres_pool):
    idx = await fetch_prep_protocol_idx_by_name(postgres_pool, "short_read_metagenomics")
    assert isinstance(idx, int)
    name = await postgres_pool.fetchval("SELECT name FROM qiita.prep_protocol WHERE idx = $1", idx)
    assert name == "short_read_metagenomics"


async def test_fetch_prep_protocol_idx_by_name_all_five_curated_names_resolve(postgres_pool):
    for name in (
        "short_read_metagenomics",
        "short_read_transcriptomics",
        "long_read_metagenomics",
        "short_read_amplicon",
        "long_read_amplicon",
    ):
        assert await fetch_prep_protocol_idx_by_name(postgres_pool, name) is not None


async def test_fetch_prep_protocol_idx_by_name_unknown_raises(postgres_pool):
    with pytest.raises(PrepProtocolUnknownError, match="made-up-protocol"):
        await fetch_prep_protocol_idx_by_name(postgres_pool, "made-up-protocol")


async def test_fetch_prep_protocol_idx_by_name_retired_protocol_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            name = "retired_test_protocol"
            await conn.execute(
                "INSERT INTO qiita.prep_protocol (name, created_by_idx) VALUES ($1, 1)",
                name,
            )
            await conn.execute(
                "UPDATE qiita.prep_protocol SET retired = true, retired_by_idx = 1,"
                " retired_at = now() WHERE name = $1",
                name,
            )
            with pytest.raises(PrepProtocolUnknownError, match=name):
                await fetch_prep_protocol_idx_by_name(conn, name)
        finally:
            await tr.rollback()
