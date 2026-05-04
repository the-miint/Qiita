"""Integration tests for the action-library primitives via the LIBRARY
name lookup — the same dispatch path a workflow runner will use.

Library functions take Parquet paths under the new on-disk contract; tests
write small fixture Parquet files to tmp_path and pass those in.
"""

import hashlib
import uuid

import duckdb
import pytest

_TEST_SALT = uuid.uuid4().hex


def _md5_uuid(seq: str) -> uuid.UUID:
    return uuid.UUID(hashlib.md5(f"{_TEST_SALT}{seq}".encode()).hexdigest())


def _write_manifest(path, hashes: list[uuid.UUID]) -> None:
    """Materialize a manifest.parquet with (read_id, sequence_hash, length).
    The read_id is synthesised; only sequence_hash matters for mint-features."""
    rows = [(f"seq{i}", str(h), 32 + i) for i, h in enumerate(hashes)]
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE m (read_id VARCHAR, sequence_hash UUID, length BIGINT)"
        )
        conn.executemany("INSERT INTO m VALUES (?, ?::uuid, ?)", rows)
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")


def _read_feature_map(path) -> dict[str, int]:
    """Read a feature_map.parquet into a {sequence_hash → feature_idx} dict."""
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            "SELECT CAST(sequence_hash AS VARCHAR), feature_idx FROM read_parquet(?)",
            [str(path)],
        ).fetchall()
    return {r[0]: r[1] for r in rows}


@pytest.fixture
async def fresh_reference(postgres_pool, human_admin_session):
    """Create a reference owned by the session admin and yield its idx,
    transitioning it to status='minting' so write-membership accepts
    feature_idxs. Cleans up at the end."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'minting', $2)"
        " RETURNING reference_idx",
        f"library-test-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_library_mint_features_dispatch(postgres_pool, tmp_path):
    """LIBRARY['mint-features'](pool, manifest, output_dir) writes
    qiita.feature rows and produces a feature_map.parquet."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    hashes = [_md5_uuid(f"LIB{i}") for i in range(5)]
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, hashes)

    feature_map_path, minted, reused = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path
    )
    assert minted == 5
    assert reused == 0
    assert feature_map_path == tmp_path / "feature_map.parquet"

    mapping = _read_feature_map(feature_map_path)
    assert set(mapping.keys()) == {str(h) for h in hashes}

    # Idempotent re-dispatch: same hashes return reused=5.
    feature_map_path2, minted2, reused2 = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path
    )
    assert minted2 == 0
    assert reused2 == 5
    # Same feature_idx values on the second call.
    assert _read_feature_map(feature_map_path2) == mapping


async def test_library_write_membership_dispatch(postgres_pool, tmp_path, fresh_reference):
    """LIBRARY['write-membership'](pool, idx, feature_map_path) inserts
    qiita.reference_membership rows and returns (linked, already_linked)."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    hashes = [_md5_uuid(f"MEM{i}") for i in range(3)]
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, hashes)
    feature_map_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path
    )

    linked, already_linked = await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
        postgres_pool, fresh_reference, feature_map_path
    )
    assert linked == 3
    assert already_linked == 0

    expected_idxs = sorted(_read_feature_map(feature_map_path).values())
    rows = await postgres_pool.fetch(
        "SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1",
        fresh_reference,
    )
    assert sorted(r["feature_idx"] for r in rows) == expected_idxs

    # Re-dispatch reports already_linked=3.
    linked2, already_linked2 = await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
        postgres_pool, fresh_reference, feature_map_path
    )
    assert linked2 == 0
    assert already_linked2 == 3


async def test_library_mint_features_writes_genome_associations(postgres_pool, tmp_path):
    """When `genome_map_path` is supplied, mint-features additionally
    populates qiita.genome and qiita.feature_genome.

    Schema: genome_map Parquet has (read_id, genome_source, genome_source_id)
    keyed by the FASTA-level read_id. The library JOINs against the
    manifest's read_id to resolve sequence_hash → feature_idx. Reads not
    present in the genome map are silently dropped (an INNER JOIN).
    """
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    # Three reads; the genome map covers the first two only — exercises
    # the "subset of FASTA" case (mixed amplicon + full genome references).
    hashes = [_md5_uuid(f"GEN{i}") for i in range(3)]
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, hashes)

    genome_source = f"src-{uuid.uuid4()}"
    source_ids = [f"GENOME_{uuid.uuid4()}" for _ in range(2)]
    genome_map = tmp_path / "genome_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE gm "
            "(read_id VARCHAR, genome_source VARCHAR, genome_source_id VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO gm VALUES (?, ?, ?)",
            [(f"seq{i}", genome_source, source_ids[i]) for i in range(2)],
        )
        conn.execute(f"COPY gm TO '{genome_map}' (FORMAT PARQUET)")

    feature_map_path, minted, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path, genome_map
    )
    assert minted == 3

    mapping = _read_feature_map(feature_map_path)
    expected_feat_idxs = sorted(mapping[str(hashes[i])] for i in range(2))
    rows = await postgres_pool.fetch(
        "SELECT fg.feature_idx, g.source, g.source_id"
        " FROM qiita.feature_genome fg"
        " JOIN qiita.genome g USING (genome_idx)"
        " WHERE fg.feature_idx = ANY($1::bigint[])"
        " ORDER BY fg.feature_idx",
        expected_feat_idxs,
    )
    assert [r["feature_idx"] for r in rows] == expected_feat_idxs
    assert {r["source"] for r in rows} == {genome_source}
    assert sorted(r["source_id"] for r in rows) == sorted(source_ids)

    # The third read (seq2) has no genome row.
    no_genome = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.feature_genome WHERE feature_idx = $1",
        mapping[str(hashes[2])],
    )
    assert no_genome == 0


async def test_library_write_membership_raises_on_unknown_feature_idx(
    postgres_pool, tmp_path, fresh_reference
):
    """An unknown feature_idx in the feature_map Parquet surfaces as
    ValueError (the FK violation is caught and re-raised as a structured
    error). Routes catch this and map to HTTP 422."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    bogus_map = tmp_path / "bogus.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE fm (sequence_hash UUID, feature_idx BIGINT)"
        )
        conn.execute(
            "INSERT INTO fm VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 9999999999)"
        )
        conn.execute(f"COPY fm TO '{bogus_map}' (FORMAT PARQUET)")

    with pytest.raises(ValueError, match="feature_idx"):
        await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
            postgres_pool, fresh_reference, bogus_map
        )
