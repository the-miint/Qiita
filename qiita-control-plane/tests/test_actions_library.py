"""Unit tests for the action-library registry shape (no DB).

Catches drift between qiita_common.api_paths.LibraryPrimitive (the closed
set of names workflow YAML can reference via `action:` entries) and the
qiita_control_plane.actions.library.LIBRARY dispatch dict (what the
runner actually calls).
"""

import inspect


def test_library_exposes_every_named_primitive():
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import LIBRARY

    assert set(LIBRARY.keys()) == set(LibraryPrimitive)


def test_library_primitives_are_async_callables():
    """The runner does `await LIBRARY[name](...)` uniformly — every entry
    must be an async callable."""
    from qiita_control_plane.actions import LIBRARY

    for name, fn in LIBRARY.items():
        assert callable(fn), f"{name!r} entry is not callable"
        assert inspect.iscoroutinefunction(fn), f"{name!r} is not async"


def test_library_re_exports_match_module_callables():
    """The names in LIBRARY map 1:1 to the module-level functions of the
    same role — adding a named primitive without a same-named function
    (or vice-versa) is a smell."""
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import LIBRARY
    from qiita_control_plane.actions import library as lib

    assert LIBRARY[LibraryPrimitive.MINT_FEATURES] is lib.mint_features
    assert LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP] is lib.write_membership
    assert LIBRARY[LibraryPrimitive.WRITE_ASSEMBLY_MEMBERSHIP] is lib.write_assembly_membership
    assert LIBRARY[LibraryPrimitive.REGISTER_FILES] is lib.register_files
    assert LIBRARY[LibraryPrimitive.REGISTER_INDEX] is lib.register_index
    assert LIBRARY[LibraryPrimitive.PERSIST_READ_METRICS] is lib.persist_read_metrics
    assert LIBRARY[LibraryPrimitive.PERSIST_QC_REPORT] is lib.persist_qc_report
    assert LIBRARY[LibraryPrimitive.DELETE_READ_MASK_BLOCK] is lib.delete_read_mask_block
    assert LIBRARY[LibraryPrimitive.RECONCILE_BLOCK] is lib.reconcile_block


async def test_delete_pool_reads_data_empty_set_short_circuits():
    """An empty prep_sample set returns {} without a Flight call — so an
    empty pool delete never touches the data plane."""
    from qiita_control_plane.actions import library as lib

    result = await lib.delete_pool_reads_data(
        prep_sample_idxs=[],
        hmac_secret=b"\x00" * 32,
        data_plane_url="grpc://unreachable:1",
    )
    assert result == {}


async def test_delete_read_mask_block_data_empty_members_short_circuits():
    """An empty members list returns 0 without a Flight call — the idempotent
    block-replace wrapper never touches the data plane for an empty block."""
    from qiita_control_plane.actions import library as lib

    rows = await lib.delete_read_mask_block_data(
        mask_idx=7,
        members=[],
        hmac_secret=b"\x00" * 32,
        data_plane_url="grpc://unreachable:1",
    )
    assert rows == 0


def test_assembly_membership_join_resolves_contigs_to_bins_and_features(tmp_path):
    """The DuckDB join behind write-assembly-membership resolves each contig's
    synthetic read_id through bin_map (kind, bin_id) and manifest -> feature_map
    (sequence_hash -> feature_idx) to one (kind, bin_id, feature_idx) row per
    contig. Two contigs that collapse to the same feature_idx (identical bytes)
    stay distinct rows because their bin/kind differ."""
    import uuid

    import duckdb

    from qiita_control_plane.actions.library import ASSEMBLY_MEMBERSHIP_JOIN_SQL

    h1 = uuid.UUID(int=1)
    h2 = uuid.UUID(int=2)

    def _write(path, schema, rows):
        with duckdb.connect(":memory:") as c:
            c.execute(f"CREATE TEMP TABLE t ({schema})")
            c.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
            c.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")

    bin_map = tmp_path / "bin_map.parquet"
    manifest = tmp_path / "manifest.parquet"
    feature_map = tmp_path / "feature_map.parquet"
    _write(
        bin_map,
        "read_id VARCHAR, kind VARCHAR, bin_id VARCHAR",
        [
            ("LCG:circ1:c1", "LCG", "circ1"),
            ("MAG:bin.1:x1", "MAG", "bin.1"),
            ("MAG:bin.2:y1", "MAG", "bin.2"),
        ],
    )
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [
            ("LCG:circ1:c1", str(h1), 10),
            ("MAG:bin.1:x1", str(h2), 20),
            # bin.2 shares bytes with bin.1 -> same hash -> same feature_idx.
            ("MAG:bin.2:y1", str(h2), 20),
        ],
    )
    _write(feature_map, "sequence_hash UUID, feature_idx BIGINT", [(str(h1), 100), (str(h2), 200)])

    with duckdb.connect(":memory:") as c:
        rows = c.execute(
            ASSEMBLY_MEMBERSHIP_JOIN_SQL, [str(bin_map), str(manifest), str(feature_map)]
        ).fetchall()
    assert sorted(rows) == sorted(
        [("LCG", "circ1", 100), ("MAG", "bin.1", 200), ("MAG", "bin.2", 200)]
    )


def test_reap_staged_reads_none_root_is_noop():
    """CP-only/dev (no shared scratch) reaps nothing and never raises."""
    from qiita_control_plane.actions.sequenced_pool import reap_staged_reads

    assert reap_staged_reads(None, [1, 2, 3]) == 0


def test_reap_staged_reads_removes_files_and_empty_dirs(tmp_path):
    from qiita_common.api_paths import compute_reads_staging_path

    from qiita_control_plane.actions.sequenced_pool import reap_staged_reads

    present = compute_reads_staging_path(tmp_path, 11)
    present.parent.mkdir(parents=True)
    present.write_bytes(b"x")
    # idx 22 has no staged copy — reaper must tolerate the gap (idempotent).
    reaped = reap_staged_reads(tmp_path, [11, 22])
    assert reaped == 1
    assert not present.exists()
    assert not present.parent.exists()
