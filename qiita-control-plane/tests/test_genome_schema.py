"""Schema-level invariants for qiita.genome's controlled genome_source
vocabulary and the qiita-origin sample linkage, plus a StrEnum <-> CHECK
parity guard.

The `source` CHECK is TEXT/CHECK (not a Postgres ENUM, per the carve-out), so
it is out of scope for ENUM_PAIRS. This file guards the same drift a light way:
it reads pg_get_constraintdef and asserts the values match
qiita_common.models.GenomeSource exactly. Pattern copied from
test_email_receipt_schema.py.
"""

import re
import uuid

import asyncpg
import pytest
from qiita_common.models import GenomeSource

pytestmark = pytest.mark.db


async def test_genome_source_check_matches_strenum(postgres_pool):
    defs = await postgres_pool.fetch(
        "SELECT c.conname, pg_get_constraintdef(c.oid) AS def"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'c'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'genome'"
        "   AND c.conname = 'genome_source_check'"
    )
    assert len(defs) == 1, defs
    check_def = defs[0]["def"]
    # Every StrEnum value appears in the CHECK...
    for value in GenomeSource:
        assert f"'{value.value}'" in check_def, (value, check_def)
    # ...and the CHECK introduces no value the StrEnum lacks.
    quoted = set(re.findall(r"'([a-z_]+)'", check_def))
    assert quoted == {v.value for v in GenomeSource}


async def test_genome_prep_sample_idx_column_is_nullable_bigint_fk(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita' AND c.relname = 'genome'"
        "   AND a.attname = 'prep_sample_idx'"
    )
    assert len(rows) == 1, "qiita.genome.prep_sample_idx is missing"
    assert rows[0]["type"] == "bigint"
    assert rows[0]["attnotnull"] is False  # nullable

    fk = await postgres_pool.fetchval(
        "SELECT pg_get_constraintdef(c.oid)"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'f' AND cn.nspname = 'qiita' AND ct.relname = 'genome'"
        "   AND pg_get_constraintdef(c.oid) LIKE '%prep_sample_idx%'"
    )
    assert fk is not None, "genome.prep_sample_idx FK is missing"
    assert "prep_sample" in fk


async def test_genome_qiita_origin_check_present(postgres_pool):
    """The biconditional CHECK: prep_sample_idx is set iff source = 'qiita'."""
    check = await postgres_pool.fetchval(
        "SELECT pg_get_constraintdef(c.oid)"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'c' AND cn.nspname = 'qiita' AND ct.relname = 'genome'"
        "   AND c.conname = 'genome_qiita_origin_check'"
    )
    assert check is not None, "genome_qiita_origin_check is missing"
    assert "qiita" in check and "prep_sample_idx" in check


async def test_genome_source_check_rejects_out_of_vocab(postgres_pool):
    """The DB CHECK (not just the app-layer validation) rejects an
    out-of-vocabulary source."""
    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute(
            "INSERT INTO qiita.genome (source, source_id) VALUES ('ncbi', $1)",
            f"BAD-{uuid.uuid4()}",
        )


async def test_genome_qiita_without_prep_sample_rejected_by_check(postgres_pool):
    """source='qiita' with a NULL prep_sample_idx violates the biconditional
    CHECK — the direction a malformed `OR`-typo constraint would wrongly accept."""
    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute(
            "INSERT INTO qiita.genome (source, source_id, prep_sample_idx)"
            " VALUES ('qiita', $1, NULL)",
            f"Q-{uuid.uuid4()}",
        )


async def test_genome_external_with_null_prep_sample_accepted_by_check(postgres_pool):
    """An external genome with a NULL prep_sample_idx satisfies the biconditional."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        gid = await conn.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ('genbank', $1)"
            " RETURNING genome_idx",
            f"OK-{uuid.uuid4()}",
        )
        assert gid is not None
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)
