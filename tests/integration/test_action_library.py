"""Integration tests for the action-library primitives via the LIBRARY
name lookup — the same dispatch path a workflow runner will use.

Library functions take Parquet paths under the new on-disk contract; tests
write small fixture Parquet files to tmp_path and pass those in.
"""

import hashlib
import uuid

import asyncpg
import duckdb
import pytest

_TEST_SALT = uuid.uuid4().hex


def _md5_uuid(seq: str) -> uuid.UUID:
    return uuid.UUID(hashlib.md5(f"{_TEST_SALT}{seq}".encode()).hexdigest())


def _write_manifest(
    path, hashes: list[uuid.UUID], read_ids: list[str] | None = None
) -> None:
    """Materialize a manifest.parquet with (read_id, sequence_hash, length).
    `read_ids` defaults to seq0..seqN; pass an explicit list to test cases
    that need duplicate sequence_hashes under distinct read_ids."""
    if read_ids is None:
        read_ids = [f"seq{i}" for i in range(len(hashes))]
    assert len(read_ids) == len(hashes)
    rows = [(read_ids[i], str(hashes[i]), 32 + i) for i in range(len(hashes))]
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE m (read_id VARCHAR, sequence_hash UUID, length BIGINT)"
        )
        if rows:
            conn.executemany("INSERT INTO m VALUES (?, ?::uuid, ?)", rows)
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")


def _write_genome_map(path, entries: list[tuple[str, str | None, str | None]]) -> None:
    """Write a (read_id, genome_source, genome_source_id) Parquet. Tuple
    elements may be None to exercise malformed-input contracts."""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE gm "
            "(read_id VARCHAR, genome_source VARCHAR, genome_source_id VARCHAR)"
        )
        conn.executemany("INSERT INTO gm VALUES (?, ?, ?)", entries)
        conn.execute(f"COPY gm TO '{path}' (FORMAT PARQUET)")


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
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


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


async def test_library_write_membership_dispatch(
    postgres_pool, tmp_path, fresh_reference
):
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


async def test_library_mint_features_writes_genome_associations(
    postgres_pool, tmp_path
):
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
    _write_genome_map(
        genome_map, [(f"seq{i}", genome_source, source_ids[i]) for i in range(2)]
    )

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
        conn.execute("CREATE TEMP TABLE fm (sequence_hash UUID, feature_idx BIGINT)")
        conn.execute(
            "INSERT INTO fm VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 9999999999)"
        )
        conn.execute(f"COPY fm TO '{bogus_map}' (FORMAT PARQUET)")

    with pytest.raises(ValueError, match="feature_idx"):
        await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
            postgres_pool, fresh_reference, bogus_map
        )


# =============================================================================
# Edge-case contracts inherited from the deleted test_feature_minting.py
# =============================================================================
# These cover behaviours the pre-Parquet route exercised explicitly:
# mixed novel/reused counts, cross-call dedup, genome-write idempotency,
# and the new contract for shapes the old route rejected at HTTP 422
# (empty input, within-batch duplicate hashes, half-set genome metadata).


async def test_library_mint_features_mixed_novel_and_reused(postgres_pool, tmp_path):
    """A second mint that mixes already-minted hashes with novel ones
    reports both `minted` (for the novel) and `reused` (for the
    pre-existing) — non-zero in the same call. The reused hashes keep
    their original feature_idx; the new ones get fresh ones."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    existing = [_md5_uuid(f"PREEXIST{i}") for i in range(3)]
    pre_manifest = tmp_path / "pre.parquet"
    _write_manifest(pre_manifest, existing)
    pre_out = tmp_path / "pre_out"
    pre_map_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, pre_manifest, pre_out
    )
    pre_mapping = _read_feature_map(pre_map_path)

    novel = [_md5_uuid(f"NEWAFTER{i}") for i in range(2)]
    mixed_manifest = tmp_path / "mixed.parquet"
    _write_manifest(mixed_manifest, existing + novel)
    mixed_out = tmp_path / "mixed_out"
    mixed_map_path, minted, reused = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, mixed_manifest, mixed_out
    )
    assert minted == 2
    assert reused == 3

    mixed_mapping = _read_feature_map(mixed_map_path)
    assert len(mixed_mapping) == 5
    # Reused hashes resolve to the same feature_idx they got in the first call.
    for h in existing:
        assert mixed_mapping[str(h)] == pre_mapping[str(h)]


async def test_library_mint_features_cross_call_dedup(postgres_pool, tmp_path):
    """Mint is reference-agnostic and globally deduplicating: minting the
    same hash twice in unrelated calls (whatever the caller's intent)
    returns the same feature_idx on the second call. This is the new
    home of the pre-branch `cross_reference_deduplication` contract —
    qiita.feature is the global scope."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    h = _md5_uuid("XCALL_SHARED")

    manifest_a = tmp_path / "a.parquet"
    _write_manifest(manifest_a, [h])
    out_a = tmp_path / "a_out"
    map_a, minted_a, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest_a, out_a
    )
    assert minted_a == 1

    manifest_b = tmp_path / "b.parquet"
    _write_manifest(manifest_b, [h])
    out_b = tmp_path / "b_out"
    map_b, minted_b, reused_b = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest_b, out_b
    )
    assert minted_b == 0
    assert reused_b == 1

    assert _read_feature_map(map_a)[str(h)] == _read_feature_map(map_b)[str(h)]


async def test_library_mint_features_genome_writes_are_idempotent(
    postgres_pool, tmp_path
):
    """Re-calling mint with the same genome_map doesn't create duplicate
    qiita.genome rows or feature_genome junction rows. The genome upsert
    is `ON CONFLICT (source, source_id) DO UPDATE`; feature_genome is
    `ON CONFLICT DO NOTHING`. Together they make re-runs converge."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    h = _md5_uuid("GEN_IDEMP")
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, [h])

    src = f"src-{uuid.uuid4()}"
    sid = f"GID-{uuid.uuid4()}"
    genome_map = tmp_path / "genome_map.parquet"
    _write_genome_map(genome_map, [("seq0", src, sid)])

    map1_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path / "out1", genome_map
    )
    feat_idx = next(iter(_read_feature_map(map1_path).values()))

    async def _counts() -> tuple[int, int]:
        genome_rows = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.genome WHERE source = $1 AND source_id = $2",
            src,
            sid,
        )
        fg_rows = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.feature_genome WHERE feature_idx = $1",
            feat_idx,
        )
        return genome_rows, fg_rows

    assert await _counts() == (1, 1)

    # Second call with identical inputs is a no-op at the DB level.
    await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path / "out2", genome_map
    )
    assert await _counts() == (1, 1)


async def test_library_mint_features_handles_empty_manifest(postgres_pool, tmp_path):
    """Empty manifest is accepted under the path-based contract — pre-branch
    the route returned HTTP 422 ("empty entries list"), but a manifest is
    the FASTA-derived ground truth, and an empty manifest validly means
    "this FASTA had no sequences." Output: minted=0, reused=0, an empty
    feature_map.parquet on disk."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    manifest = tmp_path / "empty.parquet"
    _write_manifest(manifest, [])

    feature_map_path, minted, reused = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path
    )
    assert minted == 0
    assert reused == 0
    assert feature_map_path.exists()
    assert _read_feature_map(feature_map_path) == {}


async def test_library_mint_features_dedupes_within_batch_duplicates(
    postgres_pool, tmp_path
):
    """A manifest with the same sequence_hash on two distinct read_ids
    silently dedupes to one feature row — pre-branch this returned HTTP
    422 ("duplicate hashes in request"), but under the path-based
    contract identical sequences validly share a sequence_hash (two
    reads with identical content). Net effect: minted=1, reused=0, one
    feature_map entry, regardless of how many reads pointed to the hash."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    h = _md5_uuid("DUP_BATCH")
    manifest = tmp_path / "dup.parquet"
    _write_manifest(manifest, [h, h], read_ids=["readA", "readB"])

    feature_map_path, minted, reused = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, manifest, tmp_path
    )
    assert minted == 1
    assert reused == 0
    mapping = _read_feature_map(feature_map_path)
    assert len(mapping) == 1
    assert str(h) in mapping


async def test_library_mint_features_genome_map_with_null_source_id_fails(
    postgres_pool, tmp_path
):
    """A genome_map row with NULL in either genome_source or
    genome_source_id fails at the qiita.genome NOT NULL constraint.
    The path-based contract relies on the genome map being well-formed
    upstream; pre-branch the FeatureHashEntry validator caught this at
    the route layer with HTTP 422."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    h = _md5_uuid("HALF_GENOME")
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, [h])

    genome_map = tmp_path / "genome_map.parquet"
    _write_genome_map(genome_map, [("seq0", "somesrc", None)])

    with pytest.raises(asyncpg.NotNullViolationError):
        await LIBRARY[LibraryPrimitive.MINT_FEATURES](
            postgres_pool, manifest, tmp_path, genome_map
        )
