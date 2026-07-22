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
    assert LIBRARY[LibraryPrimitive.DELETE_ALIGNMENT_BLOCK] is lib.delete_alignment_block
    assert LIBRARY[LibraryPrimitive.RECONCILE_ALIGNMENT_BLOCK] is lib.reconcile_alignment_block
    assert LIBRARY[LibraryPrimitive.SYNC_REFERENCE_EXCLUSION] is lib.sync_reference_exclusion


async def test_delete_pool_reads_data_empty_set_short_circuits():
    """An empty prep_sample set returns {} without a Flight call — so an
    empty pool delete never touches the data plane."""
    from qiita_control_plane.actions import library as lib

    result = await lib.delete_pool_reads_data(
        prep_sample_idxs=[],
        signing_key=b"\x00" * 32,
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
        signing_key=b"\x00" * 32,
        data_plane_url="grpc://unreachable:1",
    )
    assert rows == 0


async def test_delete_alignment_block_data_empty_members_short_circuits():
    """The alignment twin: an empty members list returns 0 without a Flight call —
    the idempotent alignment-block-replace wrapper never touches the data plane for
    an empty block."""
    from qiita_control_plane.actions import library as lib

    rows = await lib.delete_alignment_block_data(
        alignment_idx=7,
        members=[],
        signing_key=b"\x00" * 32,
        data_plane_url="grpc://unreachable:1",
    )
    assert rows == 0


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def to_pybytes(self) -> bytes:
        return self._data


class _FakeResult:
    def __init__(self, data: bytes) -> None:
        self.body = _FakeBody(data)


class _FakeExclusionPool:
    """Minimal async pool satisfying `sync_reference_exclusion_data`'s
    advisory-lock transaction (`async with pool.acquire() as conn,
    conn.transaction(): await conn.execute(...)`). The real advisory-lock
    serialization needs concurrency + a live Postgres to exercise; these unit
    tests stub `resolve_excluded_features` + `_do_action`, so the connection only
    has to accept the lock `execute` and the two async-context enters. It records
    the lock acquisition so a test can assert the sync took the lock."""

    def __init__(self) -> None:
        self.lock_keys: list[int] = []

    class _Conn:
        def __init__(self, pool: _FakeExclusionPool) -> None:
            self._pool = pool

        async def execute(self, sql: str, *args):
            if "pg_advisory_xact_lock" in sql:
                self._pool.lock_keys.append(args[0])
            return "SELECT 1"

        def transaction(self):
            conn = self

            class _Txn:
                async def __aenter__(self):
                    return conn

                async def __aexit__(self, *exc):
                    return False

            return _Txn()

    def acquire(self):
        conn = _FakeExclusionPool._Conn(self)

        class _Acquire:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Acquire()


def _decode_action_payload(token: bytes) -> dict:
    """Recover the JSON payload from a signed action token without the key.

    Wire format (qiita_control_plane.auth.tickets): 1B version, 4B big-endian
    payload_len, then the canonical-JSON payload."""
    import json
    import struct

    (payload_len,) = struct.unpack(">I", token[1:5])
    return json.loads(token[5 : 5 + payload_len])


async def test_sync_reference_exclusion_data_stages_resolved_set_and_signs(tmp_path, monkeypatch):
    """The signer resolves the blocklist to its feature_idx set, writes a
    single-column Parquet at `dest`, and signs a `sync_reference_exclusion`
    action carrying that dest. Returns the data plane's loaded feature_count."""
    import json

    import pyarrow.parquet as pq

    from qiita_control_plane.actions import library as lib

    async def _fake_resolve(pool):
        return [10, 20, 30]

    captured: dict = {}

    def _fake_do_action(action_type, data_plane_url, token):
        captured["action_type"] = action_type
        captured["data_plane_url"] = data_plane_url
        captured["token"] = token
        return [_FakeResult(json.dumps({"feature_count": 3}).encode())]

    monkeypatch.setattr(lib, "resolve_excluded_features", _fake_resolve)
    monkeypatch.setattr(lib, "_do_action", _fake_do_action)

    fake_pool = _FakeExclusionPool()
    dest = tmp_path / "reference_exclusion.parquet"
    count = await lib.sync_reference_exclusion_data(
        pool=fake_pool,
        dest=dest,
        signing_key=b"\x00" * 32,
        data_plane_url="grpc://dp:50051",
    )

    assert count == 3
    # Serialized under the exclusion advisory lock (resolve + replace happen once
    # under it — see sync_reference_exclusion_data).
    assert fake_pool.lock_keys == [lib._EXCLUSION_SYNC_ADVISORY_LOCK_KEY]
    assert captured["action_type"] == "sync_reference_exclusion"
    assert captured["data_plane_url"] == "grpc://dp:50051"
    # The signed payload carries exactly the dest — the data plane reads it.
    assert _decode_action_payload(captured["token"]) == {
        "action": "sync_reference_exclusion",
        "dest": str(dest),
    }
    # The staged Parquet is a single int64 feature_idx column with the set.
    table = pq.read_table(dest)
    assert table.column_names == ["feature_idx"]
    assert table.column("feature_idx").to_pylist() == [10, 20, 30]


async def test_sync_reference_exclusion_data_empty_set_writes_clearing_parquet(
    tmp_path, monkeypatch
):
    """An empty blocklist still writes a valid zero-row Parquet and still calls
    the DoAction — so the data plane's wholesale replace CLEARS its mirror
    (re-enabling everything), rather than short-circuiting and leaving stale
    exclusions in the lake."""
    import json

    import pyarrow.parquet as pq

    from qiita_control_plane.actions import library as lib

    async def _fake_resolve(pool):
        return []

    called = {"n": 0}

    def _fake_do_action(action_type, data_plane_url, token):
        called["n"] += 1
        return [_FakeResult(json.dumps({"feature_count": 0}).encode())]

    monkeypatch.setattr(lib, "resolve_excluded_features", _fake_resolve)
    monkeypatch.setattr(lib, "_do_action", _fake_do_action)

    dest = tmp_path / "empty.parquet"
    count = await lib.sync_reference_exclusion_data(
        pool=_FakeExclusionPool(),
        dest=dest,
        signing_key=b"\x00" * 32,
        data_plane_url="grpc://dp:50051",
    )

    assert count == 0
    assert called["n"] == 1, "the clearing sync still hits the data plane"
    table = pq.read_table(dest)
    assert table.column_names == ["feature_idx"]
    assert table.num_rows == 0


async def test_sync_reference_exclusion_primitive_delegates_to_signer(tmp_path, monkeypatch):
    """The workflow-facing `sync_reference_exclusion` primitive is a thin
    wrapper over `sync_reference_exclusion_data` (mirroring the
    delete_read_mask_block -> delete_read_mask_block_data pattern): it forwards
    pool / dest / signing_key / data_plane_url unchanged and surfaces the data
    plane's loaded feature_count as `synced_feature_count` for the workflow log."""
    from qiita_control_plane.actions import library as lib

    captured: dict = {}

    async def _fake_signer(*, pool, dest, signing_key, data_plane_url):
        captured.update(
            pool=pool, dest=dest, signing_key=signing_key, data_plane_url=data_plane_url
        )
        return 5

    monkeypatch.setattr(lib, "sync_reference_exclusion_data", _fake_signer)

    pool = object()
    dest = tmp_path / "reference_exclusion.parquet"
    out = await lib.sync_reference_exclusion(
        pool, dest=dest, signing_key=b"\x00" * 32, data_plane_url="grpc://dp:50051"
    )

    assert out == {"synced_feature_count": 5}
    assert captured == {
        "pool": pool,
        "dest": dest,
        "signing_key": b"\x00" * 32,
        "data_plane_url": "grpc://dp:50051",
    }


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


def test_membership_accession_join_resolves_representative_read_id(tmp_path):
    """The DuckDB join behind write-membership resolves each feature_idx to a
    representative accession — the FASTA-header read_id — via manifest
    (read_id -> sequence_hash) and the already-minted feature_map
    (sequence_hash -> feature_idx). Identical bytes shared under multiple
    read_ids collapse to one feature_idx, and the lex-smallest read_id wins
    (deterministic, mirroring hash_sequences' DISTINCT-ON convention)."""
    import uuid

    import duckdb

    from qiita_control_plane.actions.library import MEMBERSHIP_ACCESSION_JOIN_SQL

    h1 = uuid.UUID(int=1)
    h2 = uuid.UUID(int=2)

    def _write(path, schema, rows):
        with duckdb.connect(":memory:") as c:
            c.execute(f"CREATE TEMP TABLE t ({schema})")
            c.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
            c.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")

    manifest = tmp_path / "manifest.parquet"
    feature_map = tmp_path / "feature_map.parquet"
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [
            ("NZ_CP0001", str(h1), 10),
            # Same bytes under two headers -> one feature_idx; lex-smallest wins.
            ("NZ_CP0002.2", str(h2), 20),
            ("NZ_CP0002.1", str(h2), 20),
        ],
    )
    _write(feature_map, "sequence_hash UUID, feature_idx BIGINT", [(str(h1), 100), (str(h2), 200)])

    with duckdb.connect(":memory:") as c:
        rows = c.execute(
            MEMBERSHIP_ACCESSION_JOIN_SQL, [str(feature_map), str(manifest)]
        ).fetchall()
    assert sorted(rows) == sorted([(100, "NZ_CP0001"), (200, "NZ_CP0002.1")])


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
