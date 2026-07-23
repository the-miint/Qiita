"""Tests for reference database schema — five tables with constraints."""

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

pytestmark = pytest.mark.db

EXPECTED_TABLES = [
    "reference",
    "genome",
    "feature",
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
            " VALUES ($1, $2, $3, $4, $5) RETURNING reference_idx",
            "test-ref",
            "1.0",
            "sequence_reference",
            "pending",
            SYSTEM_PRINCIPAL_IDX,
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
            " VALUES ($1, $2, $3, $4) RETURNING status",
            "default-status-test",
            "1.0",
            "sequence_reference",
            SYSTEM_PRINCIPAL_IDX,
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
            " VALUES ($1, $2, $3, $4, $5)",
            "dup-ref",
            "1.0",
            "sequence_reference",
            "pending",
            SYSTEM_PRINCIPAL_IDX,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
                " VALUES ($1, $2, $3, $4, $5)",
                "dup-ref",
                "1.0",
                "sequence_reference",
                "pending",
                SYSTEM_PRINCIPAL_IDX,
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
                " VALUES ($1, $2, $3, $4, $5)",
                "bad-kind",
                "1.0",
                "invalid_kind",
                "pending",
                SYSTEM_PRINCIPAL_IDX,
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
                " VALUES ($1, $2, $3, $4, $5)",
                "bad-status",
                "1.0",
                "sequence_reference",
                "bogus",
                SYSTEM_PRINCIPAL_IDX,
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
        await conn.execute("INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid)", test_hash)
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
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
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
            " VALUES ($1, $2, $3, $4) RETURNING reference_idx",
            "membership-dup-test",
            "1.0",
            "sequence_reference",
            SYSTEM_PRINCIPAL_IDX,
        )
        feat_idx = await conn.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid) RETURNING feature_idx",
            "b0000000-0000-0000-0000-000000000001",
        )
        await conn.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
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


async def _make_membership_row(conn, feature_hash):
    """Insert a reference + feature + their membership row; return
    (reference_idx, feature_idx). Caller runs inside a rolled-back transaction."""
    ref_idx = await conn.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', $2) RETURNING reference_idx",
        f"shard-membership-{feature_hash[:8]}",
        SYSTEM_PRINCIPAL_IDX,
    )
    feat_idx = await conn.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid) RETURNING feature_idx",
        feature_hash,
    )
    await conn.execute(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
        ref_idx,
        feat_idx,
    )
    return ref_idx, feat_idx


async def test_reference_membership_shard_id_defaults_null(postgres_pool):
    """The pre-existing 2-column membership INSERT leaves shard_id NULL — a
    feature not yet assigned to a shard (unsharded reference)."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        ref_idx, feat_idx = await _make_membership_row(conn, "e0000000-0000-0000-0000-000000000001")
        shard_id = await conn.fetchval(
            "SELECT shard_id FROM qiita.reference_membership"
            " WHERE reference_idx = $1 AND feature_idx = $2",
            ref_idx,
            feat_idx,
        )
        assert shard_id is None
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_membership_shard_id_round_trips(postgres_pool):
    """A shard assignment records the lineage-sorted shard index verbatim."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        ref_idx, feat_idx = await _make_membership_row(conn, "e0000000-0000-0000-0000-000000000002")
        await conn.execute(
            "UPDATE qiita.reference_membership SET shard_id = 5"
            " WHERE reference_idx = $1 AND feature_idx = $2",
            ref_idx,
            feat_idx,
        )
        shard_id = await conn.fetchval(
            "SELECT shard_id FROM qiita.reference_membership"
            " WHERE reference_idx = $1 AND feature_idx = $2",
            ref_idx,
            feat_idx,
        )
        assert shard_id == 5
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_membership_rejects_negative_shard_id(postgres_pool):
    """The reference_membership_shard_id_nonneg CHECK rejects a negative shard_id."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        ref_idx, feat_idx = await _make_membership_row(conn, "e0000000-0000-0000-0000-000000000003")
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE qiita.reference_membership SET shard_id = -1"
                " WHERE reference_idx = $1 AND feature_idx = $2",
                ref_idx,
                feat_idx,
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_reference_membership_accession_defaults_null(postgres_pool):
    """The pre-existing 2-column membership INSERT leaves accession NULL — a
    row from before the column existed, or a non-FASTA ingest with no header."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        ref_idx, feat_idx = await _make_membership_row(conn, "e0000000-0000-0000-0000-0000000000a1")
        accession = await conn.fetchval(
            "SELECT accession FROM qiita.reference_membership"
            " WHERE reference_idx = $1 AND feature_idx = $2",
            ref_idx,
            feat_idx,
        )
        assert accession is None
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_write_membership_stores_representative_accession(postgres_pool, tmp_path):
    """write_membership joins the manifest (read_id -> sequence_hash) to the
    feature_map (sequence_hash -> feature_idx) and stores the representative
    accession — the lex-smallest FASTA-header read_id — on each membership row.
    Identical bytes shared under two headers collapse to one feature_idx."""
    import uuid

    import duckdb

    from qiita_control_plane.actions import library as lib

    h1 = str(uuid.UUID(int=0xACCE5501))
    h2 = str(uuid.UUID(int=0xACCE5502))

    ref_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', $2) RETURNING reference_idx",
        "accession-membership",
        SYSTEM_PRINCIPAL_IDX,
    )
    feat1 = await postgres_pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid) RETURNING feature_idx", h1
    )
    feat2 = await postgres_pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid) RETURNING feature_idx", h2
    )

    def _write(path, schema, rows):
        with duckdb.connect(":memory:") as c:
            c.execute(f"CREATE TEMP TABLE t ({schema})")
            c.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
            c.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")

    try:
        manifest = tmp_path / "manifest.parquet"
        feature_map = tmp_path / "feature_map.parquet"
        _write(
            manifest,
            "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
            [("ACC_A", h1, 10), ("ACC_B2", h2, 20), ("ACC_B1", h2, 20)],
        )
        _write(feature_map, "sequence_hash UUID, feature_idx BIGINT", [(h1, feat1), (h2, feat2)])

        linked, already = await lib.write_membership(postgres_pool, ref_idx, manifest, feature_map)
        assert (linked, already) == (2, 0)

        rows = await postgres_pool.fetch(
            "SELECT feature_idx, accession FROM qiita.reference_membership"
            " WHERE reference_idx = $1",
            ref_idx,
        )
        assert {r["feature_idx"]: r["accession"] for r in rows} == {feat1: "ACC_A", feat2: "ACC_B1"}
    finally:
        # write_membership commits, so clean up explicitly — this suite does not
        # auto-reset between tests.
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", ref_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", [feat1, feat2]
        )
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)


async def test_feature_genome_fk_on_feature(postgres_pool):
    """feature_genome must reject non-existent feature_idx."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        genome_idx = await conn.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2) RETURNING genome_idx",
            "genbank",
            "GCF_fk_test_feat",
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
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
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid) RETURNING feature_idx",
            "c0000000-0000-0000-0000-000000000001",
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
                feat_idx,
                999999,
            )
        await tr.rollback()
    finally:
        await postgres_pool.release(conn)


async def test_feature_genome_allows_a_feature_in_multiple_genomes(postgres_pool):
    """A feature can belong to MULTIPLE genomes — two organisms sharing an
    identical mobile element (e.g. a plasmid) resolve to the same feature_idx
    (content-hash-global) under different genome_idx. The composite PK
    (feature_idx, genome_idx) models this many-to-many; the old standalone
    UNIQUE(feature_idx) that silently dropped the second genome's row is gone. An
    exact-duplicate (feature_idx, genome_idx) is still rejected by the PK."""
    conn = await postgres_pool.acquire()
    try:
        tr = conn.transaction()
        await tr.start()
        feat_idx = await conn.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid) RETURNING feature_idx",
            "d0000000-0000-0000-0000-000000000001",
        )
        g1 = await conn.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2) RETURNING genome_idx",
            "genbank",
            "GCF_shared_plasmid_1",
        )
        g2 = await conn.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2) RETURNING genome_idx",
            "genbank",
            "GCF_shared_plasmid_2",
        )
        await conn.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feat_idx,
            g1,
        )
        # The second genome's association must survive — no UniqueViolationError.
        await conn.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feat_idx,
            g2,
        )
        genomes = await conn.fetch(
            "SELECT genome_idx FROM qiita.feature_genome WHERE feature_idx = $1"
            " ORDER BY genome_idx",
            feat_idx,
        )
        assert [r["genome_idx"] for r in genomes] == sorted([g1, g2])
        # The composite PK still rejects an exact (feature_idx, genome_idx) dup.
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
                feat_idx,
                g1,
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
