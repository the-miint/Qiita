"""Tests for reference database schema — five tables with constraints."""

import asyncpg
import pytest

EXPECTED_TABLES = [
    "reference",
    "genomes",
    "features",
    "reference_membership",
    "feature_genome",
]


async def test_all_reference_tables_exist(postgres_pool):
    """All five reference tables must exist in the qiita schema after migrations."""
    for table in EXPECTED_TABLES:
        exists = await postgres_pool.fetchval(
            "SELECT EXISTS("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = 'qiita' AND table_name = $1"
            ")",
            table,
        )
        assert exists, f"Table qiita.{table} does not exist"


async def test_reference_auto_generates_idx(postgres_pool):
    """Inserting without reference_idx should auto-generate an identity value."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        row = await conn.fetchrow(
            "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
            " VALUES ($1, $2, $3, $4, 1) RETURNING reference_idx",
            "test-ref",
            "1.0",
            "sequence_reference",
            "pending",
        )
        assert row["reference_idx"] is not None
        assert isinstance(row["reference_idx"], int)
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_status_defaults_to_pending(postgres_pool):
    """Omitting status from INSERT should default to 'pending'."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        row = await conn.fetchrow(
            "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
            " VALUES ($1, $2, $3, 1) RETURNING status",
            "default-status-test",
            "1.0",
            "sequence_reference",
        )
        assert row["status"] == "pending"
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_rejects_duplicate_name_version(postgres_pool):
    """Duplicate (name, version) must raise a unique violation."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        await conn.execute(
            "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
            " VALUES ($1, $2, $3, $4, 1)",
            "dup-ref",
            "1.0",
            "sequence_reference",
            "pending",
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
                " VALUES ($1, $2, $3, $4, 1)",
                "dup-ref",
                "1.0",
                "sequence_reference",
                "pending",
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_rejects_invalid_kind(postgres_pool):
    """kind must be one of the allowed values."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
                " VALUES ($1, $2, $3, $4, 1)",
                "bad-kind",
                "1.0",
                "invalid_kind",
                "pending",
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_rejects_invalid_status(postgres_pool):
    """status must be one of the allowed values."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
                " VALUES ($1, $2, $3, $4, 1)",
                "bad-status",
                "1.0",
                "sequence_reference",
                "bogus",
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_feature_rejects_duplicate_hash(postgres_pool):
    """Duplicate sequence_hash must raise a unique violation."""
    test_hash = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        await conn.execute(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid)", test_hash
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid)",
                test_hash,
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_membership_fk_enforcement(postgres_pool):
    """reference_membership must reject non-existent reference_idx."""
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await postgres_pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
            " VALUES ($1, $2)",
            999999,
            999999,
        )


async def test_reference_membership_rejects_duplicate(postgres_pool):
    """Duplicate (reference_idx, feature_idx) must raise a unique/PK violation."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        ref_idx = await conn.fetchval(
            "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
            " VALUES ($1, $2, $3, 1) RETURNING reference_idx",
            "membership-dup-test",
            "1.0",
            "sequence_reference",
        )
        feat_idx = await conn.fetchval(
            "INSERT INTO qiita.feature (sequence_hash)"
            " VALUES ($1::uuid) RETURNING feature_idx",
            "b0000000-0000-0000-0000-000000000001",
        )
        await conn.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
            " VALUES ($1, $2)",
            ref_idx,
            feat_idx,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
                " VALUES ($1, $2)",
                ref_idx,
                feat_idx,
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_feature_genome_fk_on_feature(postgres_pool):
    """feature_genome must reject non-existent feature_idx."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        genome_idx = await conn.fetchval(
            "INSERT INTO qiita.genome (source, source_id)"
            " VALUES ($1, $2) RETURNING genome_idx",
            "genbank",
            "GCF_fk_test_feat",
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx)"
                " VALUES ($1, $2)",
                999999,
                genome_idx,
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_feature_genome_fk_on_genome(postgres_pool):
    """feature_genome must reject non-existent genome_idx."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        feat_idx = await conn.fetchval(
            "INSERT INTO qiita.feature (sequence_hash)"
            " VALUES ($1::uuid) RETURNING feature_idx",
            "c0000000-0000-0000-0000-000000000001",
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx)"
                " VALUES ($1, $2)",
                feat_idx,
                999999,
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_feature_genome_unique_feature(postgres_pool):
    """A feature can belong to at most one genome (UNIQUE on feature_idx)."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        feat_idx = await conn.fetchval(
            "INSERT INTO qiita.feature (sequence_hash)"
            " VALUES ($1::uuid) RETURNING feature_idx",
            "d0000000-0000-0000-0000-000000000001",
        )
        g1 = await conn.fetchval(
            "INSERT INTO qiita.genome (source, source_id)"
            " VALUES ($1, $2) RETURNING genome_idx",
            "genbank",
            "GCF_unique_test_1",
        )
        g2 = await conn.fetchval(
            "INSERT INTO qiita.genome (source, source_id)"
            " VALUES ($1, $2) RETURNING genome_idx",
            "genbank",
            "GCF_unique_test_2",
        )
        await conn.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feat_idx,
            g1,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
                feat_idx,
                g2,
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_genome_rejects_duplicate_source(postgres_pool):
    """Duplicate (source, source_id) must raise a unique violation."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        await conn.execute(
            "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2)",
            "genbank",
            "GCF_000123456.1",
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2)",
                "genbank",
                "GCF_000123456.1",
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)
