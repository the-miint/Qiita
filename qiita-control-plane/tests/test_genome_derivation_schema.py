"""Schema-level invariants for qiita.genome_derivation: one row per derived genome,
naming the NCBI assembly it was derived from, and the reference that asserts it.

Talks to Postgres directly (no routes). Every test runs inside a transaction that is
rolled back, so the seeded reference and genomes never outlive the test.
"""

import re
import uuid
from contextlib import asynccontextmanager

import asyncpg
import pytest
from qiita_common.models.reference import GenomeSource

from qiita_control_plane.backfill.wol_genome_repair import DERIVATION_METHOD

pytestmark = pytest.mark.db


@asynccontextmanager
async def _rolled_back(pool):
    conn = await pool.acquire()
    tr = conn.transaction()
    await tr.start()
    try:
        yield conn
    finally:
        await tr.rollback()
        await pool.release(conn)


async def _seed(conn) -> tuple[int, int]:
    """One reference and one WoL-form genome; returns (reference_idx, genome_idx)."""
    reference_idx = await conn.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', (SELECT MIN(idx) FROM qiita.principal))"
        " RETURNING reference_idx",
        f"derivation-schema-{uuid.uuid4()}",
    )
    genome_idx = await conn.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2) RETURNING genome_idx",
        GenomeSource.GENBANK,
        f"G{uuid.uuid4().int % 10**9:09d}",
    )
    return reference_idx, genome_idx


async def _insert(
    conn, genome_idx, parent_source, parent_source_id, reference_idx, method=DERIVATION_METHOD
):
    await conn.execute(
        "INSERT INTO qiita.genome_derivation"
        " (genome_idx, parent_source, parent_source_id, method, asserted_by_reference_idx)"
        " VALUES ($1, $2, $3, $4, $5)",
        genome_idx,
        parent_source,
        parent_source_id,
        method,
        reference_idx,
    )


async def test_genome_derivation_columns(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita' AND c.relname = 'genome_derivation'"
        "   AND a.attnum > 0 AND NOT a.attisdropped"
    )
    assert {r["attname"]: (r["type"], r["attnotnull"]) for r in rows} == {
        "genome_idx": ("bigint", True),
        "parent_source": ("text", True),
        "parent_source_id": ("text", True),
        "method": ("text", True),
        "asserted_by_reference_idx": ("bigint", True),
        "created_at": ("timestamp with time zone", True),
    }


async def test_genome_derivation_primary_key_is_genome_idx(postgres_pool):
    pk = await postgres_pool.fetchval(
        "SELECT pg_get_constraintdef(c.oid)"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'p' AND cn.nspname = 'qiita' AND ct.relname = 'genome_derivation'"
    )
    assert pk == "PRIMARY KEY (genome_idx)"


async def test_genome_derivation_accepts_genbank_and_refseq_parents(postgres_pool):
    async with _rolled_back(postgres_pool) as conn:
        reference_idx, genome_a = await _seed(conn)
        _, genome_b = await _seed(conn)
        await _insert(conn, genome_a, GenomeSource.GENBANK, "GCA_000005825.2", reference_idx)
        await _insert(conn, genome_b, GenomeSource.REFSEQ, "GCF_000005825.12", reference_idx)
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM qiita.genome_derivation WHERE genome_idx = ANY($1::bigint[])",
                [genome_a, genome_b],
            )
            == 2
        )


async def test_genome_derivation_one_row_per_genome(postgres_pool):
    async with _rolled_back(postgres_pool) as conn:
        reference_idx, genome_idx = await _seed(conn)
        await _insert(conn, genome_idx, GenomeSource.GENBANK, "GCA_000005825.2", reference_idx)
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert(conn, genome_idx, GenomeSource.REFSEQ, "GCF_000005825.2", reference_idx)


async def test_genome_derivation_cascades_with_its_genome(postgres_pool):
    async with _rolled_back(postgres_pool) as conn:
        reference_idx, genome_idx = await _seed(conn)
        await _insert(conn, genome_idx, GenomeSource.GENBANK, "GCA_000005825.2", reference_idx)
        await conn.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM qiita.genome_derivation WHERE genome_idx = $1", genome_idx
            )
            == 0
        )


async def test_genome_derivation_restricts_deleting_its_asserting_reference(postgres_pool):
    """The asserting reference is RESTRICT (the column's COMMENT says why)."""
    async with _rolled_back(postgres_pool) as conn:
        reference_idx, genome_idx = await _seed(conn)
        await _insert(conn, genome_idx, GenomeSource.GENBANK, "GCA_000005825.2", reference_idx)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
            )


async def test_genome_derivation_rejects_unknown_reference(postgres_pool):
    async with _rolled_back(postgres_pool) as conn:
        _, genome_idx = await _seed(conn)
        missing = await conn.fetchval(
            "SELECT COALESCE(MAX(reference_idx), 0) + 1000000 FROM qiita.reference"
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await _insert(conn, genome_idx, GenomeSource.GENBANK, "GCA_000005825.2", missing)


@pytest.mark.parametrize(
    ("parent_source", "parent_source_id"),
    [
        pytest.param(GenomeSource.QIITA, "GCA_000005825.2", id="qiita-source"),
        pytest.param("wol", "GCA_000005825.2", id="wol-source"),
        pytest.param(GenomeSource.GENBANK, "GCF_000005825.2", id="refseq-id-under-genbank"),
        pytest.param(GenomeSource.REFSEQ, "GCA_000005825.2", id="genbank-id-under-refseq"),
        pytest.param(GenomeSource.GENBANK, "GCA_000005825", id="no-version"),
        pytest.param(GenomeSource.GENBANK, "GCA_00000582.2", id="eight-digits"),
        pytest.param(GenomeSource.GENBANK, "GCA_0000058250.2", id="ten-digits"),
        pytest.param(GenomeSource.GENBANK, "GCA_000005825.", id="empty-version"),
        pytest.param(GenomeSource.GENBANK, "G000005825", id="wol-id"),
        pytest.param(GenomeSource.GENBANK, "GCA_G000005825", id="gca-g-form"),
        pytest.param(GenomeSource.REFSEQ, "GCF_000005825.2 ", id="trailing-space"),
    ],
)
async def test_genome_derivation_rejects_malformed_parent(
    postgres_pool, parent_source, parent_source_id
):
    async with _rolled_back(postgres_pool) as conn:
        reference_idx, genome_idx = await _seed(conn)
        with pytest.raises(asyncpg.CheckViolationError):
            await _insert(conn, genome_idx, parent_source, parent_source_id, reference_idx)


async def test_genome_derivation_rejects_unknown_method(postgres_pool):
    async with _rolled_back(postgres_pool) as conn:
        reference_idx, genome_idx = await _seed(conn)
        with pytest.raises(asyncpg.CheckViolationError):
            await _insert(
                conn,
                genome_idx,
                GenomeSource.GENBANK,
                "GCA_000005825.2",
                reference_idx,
                method="linearized",
            )


async def test_method_check_matches_the_repair_constant(postgres_pool):
    """The only writer's constant and the CHECK are the same one-value set."""
    check_def = await postgres_pool.fetchval(
        "SELECT pg_get_constraintdef(c.oid)"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE cn.nspname = 'qiita' AND ct.relname = 'genome_derivation'"
        "   AND c.conname = 'genome_derivation_method_check'"
    )
    assert check_def is not None
    assert set(re.findall(r"'([a-z_]+)'", check_def)) == {DERIVATION_METHOD}
