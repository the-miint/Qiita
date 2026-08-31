"""Unit tests for the action library (no DB).

Two subjects. The registry-shape tests catch drift between
qiita_common.api_paths.LibraryPrimitive (the closed set of names workflow YAML
can reference via `action:` entries) and the
qiita_control_plane.actions.library.LIBRARY dispatch dict (what the runner
actually calls). The rest exercise the SQL and reporting the primitives own,
against DuckDB and Parquet fixtures rather than a database.
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
    assert LIBRARY[LibraryPrimitive.FINALIZE_MASK_SAMPLE] is lib.finalize_mask_sample_gate
    assert LIBRARY[LibraryPrimitive.FINALIZE_ASSEMBLY_SAMPLE] is lib.finalize_assembly_sample_gate
    assert LIBRARY[LibraryPrimitive.DELETE_ALIGNMENT_BLOCK] is lib.delete_alignment_block
    assert LIBRARY[LibraryPrimitive.DELETE_ALIGNMENT_SAMPLE] is lib.delete_alignment_sample
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


async def test_delete_alignment_sample_signs_the_pair_and_reports_rows(monkeypatch):
    """The per-sample alignment replace signs a `delete_alignment_sample` action
    carrying exactly the `(alignment_idx, prep_sample_idx)` pair — the wire shape
    the Rust `DeleteAlignmentSamplePayload` accepts, whose `deny_unknown_fields`
    rejects anything more — and returns the data plane's rows_deleted."""
    import json

    from qiita_control_plane.actions import library as lib

    captured: dict = {}

    def _fake_do_action(action_type, data_plane_url, token, timeout_seconds=None):
        captured["action_type"] = action_type
        captured["token"] = token
        return [_FakeResult(json.dumps({"rows_deleted": 12}).encode())]

    monkeypatch.setattr(lib, "_do_action", _fake_do_action)

    result = await lib.delete_alignment_sample(
        alignment_idx=42,
        prep_sample_idx=101,
        signing_key=b"\x00" * 32,
        data_plane_url="grpc://dp:50051",
    )

    assert result == {"prep_sample_idx": 101, "rows_deleted": 12}
    assert captured["action_type"] == "delete_alignment_sample"
    assert _decode_action_payload(captured["token"]) == {
        "action": "delete_alignment_sample",
        "alignment_idx": 42,
        "prep_sample_idx": 101,
    }


async def test_delete_alignment_sample_data_reports_zero_on_empty_result(monkeypatch):
    """A data plane that returns no result body reads as 0 rows deleted, not a
    KeyError — the same read the block twin does."""
    from qiita_control_plane.actions import library as lib

    monkeypatch.setattr(lib, "_do_action", lambda *a, **k: [])

    rows = await lib.delete_alignment_sample_data(
        alignment_idx=7,
        prep_sample_idx=8,
        signing_key=b"\x00" * 32,
        data_plane_url="grpc://dp:50051",
    )
    assert rows == 0


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

    def _fake_do_action(action_type, data_plane_url, token, timeout_seconds=None):
        captured["action_type"] = action_type
        captured["data_plane_url"] = data_plane_url
        captured["token"] = token
        captured["timeout_seconds"] = timeout_seconds
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
    # under it — see sync_reference_exclusion_data), and the data-plane call is
    # bounded so a hung DP can't hold the lock forever.
    assert fake_pool.lock_keys == [lib._EXCLUSION_SYNC_ADVISORY_LOCK_KEY]
    assert captured["timeout_seconds"] == lib._EXCLUSION_SYNC_DO_ACTION_TIMEOUT_S
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

    def _fake_do_action(action_type, data_plane_url, token, timeout_seconds=None):
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

    assert out == {"synced_feature_count": 5, "synced": True}
    assert captured == {
        "pool": pool,
        "dest": dest,
        "signing_key": b"\x00" * 32,
        "data_plane_url": "grpc://dp:50051",
    }


async def test_sync_reference_exclusion_primitive_swallows_transient(tmp_path, monkeypatch):
    """The workflow TAIL primitive must NOT raise on a transient data-plane
    failure: it runs after the reference is fully loaded + index-registered (and
    is at `indexing`/`active`), so a propagated error would drive the workflow
    failure_status PATCH and clobber the built reference to `failed`. A FlightError
    or a concurrent-sync LockNotAvailableError is caught and reported `synced=False`
    (reconciled out of band); the load is unaffected. Contrast the signer/route,
    which surface these as retriable 502/503."""
    import asyncpg
    import pyarrow.flight as _flight

    from qiita_control_plane.actions import library as lib

    for exc in (_flight.FlightError("dp down"), asyncpg.LockNotAvailableError("lock held")):

        async def _boom(*, pool, dest, signing_key, data_plane_url, _exc=exc):
            raise _exc

        monkeypatch.setattr(lib, "sync_reference_exclusion_data", _boom)
        out = await lib.sync_reference_exclusion(
            object(),
            dest=tmp_path / "reference_exclusion.parquet",
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://dp:50051",
        )
        assert out == {"synced_feature_count": None, "synced": False}


async def test_sync_reference_exclusion_primitive_still_raises_non_transient(tmp_path, monkeypatch):
    """A non-transient error (a real bug — e.g. bad SQL, a programming fault) is
    NOT swallowed: it propagates so the ticket fails loudly. Only the two known
    transient classes are contained."""
    import pytest

    from qiita_control_plane.actions import library as lib

    async def _boom(*, pool, dest, signing_key, data_plane_url):
        raise RuntimeError("programming bug")

    monkeypatch.setattr(lib, "sync_reference_exclusion_data", _boom)
    with pytest.raises(RuntimeError, match="programming bug"):
        await lib.sync_reference_exclusion(
            object(),
            dest=tmp_path / "reference_exclusion.parquet",
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://dp:50051",
        )


def test_assembly_membership_join_resolves_contigs_to_bins_and_features(tmp_path):
    """The DuckDB join behind write-assembly-membership resolves each contig's
    synthetic read_id through bin_map (kind, bin_id) and manifest -> feature_map
    (sequence_hash -> feature_idx) to one (kind, bin_id, feature_idx) row per
    contig. Two contigs that collapse to the same feature_idx (identical bytes)
    stay distinct rows because their bin/kind differ.

    Also pins the per-contig attributes riding the same join: they must not add
    rows. The two identical contigs inside bin.1 carry DIFFERENT attributes here,
    which is the case that would break the Postgres upsert if the join emitted one
    row per attribute variant — `insert_assembly_membership_rows` refuses a
    duplicated conflict target. One representative contig is chosen, and all four
    of its values travel together rather than being mixed across the two rows."""
    import uuid

    import duckdb

    from qiita_control_plane.actions.library import (
        ASSEMBLY_MEMBERSHIP_JOIN_SQL,
        _register_contig_attributes,
    )

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
        "read_id VARCHAR, kind VARCHAR, bin_id VARCHAR, contig_id VARCHAR",
        [
            ("LCG:circ1:1", "LCG", "circ1", "u1ctg"),
            ("MAG:bin.1:1", "MAG", "bin.1", "u2ctg"),
            # A SECOND identical contig in the SAME bin. `assembly_hash` composes
            # read_id as kind:bin_id:sequence_index, so duplicated bytes inside one
            # bin arrive as two read_ids resolving to one feature_idx.
            ("MAG:bin.1:2", "MAG", "bin.1", "u3ctg"),
            ("MAG:bin.2:1", "MAG", "bin.2", "u4ctg"),
        ],
    )
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [
            ("LCG:circ1:1", str(h1), 10),
            ("MAG:bin.1:1", str(h2), 20),
            ("MAG:bin.1:2", str(h2), 20),
            # bin.2 shares bytes with bin.1 -> same hash -> same feature_idx.
            ("MAG:bin.2:1", str(h2), 20),
        ],
    )
    _write(feature_map, "sequence_hash UUID, feature_idx BIGINT", [(str(h1), 100), (str(h2), 200)])

    # The two bin.1 contigs disagree on every attribute, so a join that let them
    # through separately would be visible as an extra row below. u4ctg is absent
    # on purpose: a contig with no sidecar row must still produce its membership
    # row, with NULLs.
    attrs = tmp_path / "contig_attributes.tsv"
    attrs.write_text(
        "contig_id\traw_name\tcircularity\tdepth\tmult\n"
        "u1ctg\tu1ctg_len-10_circular-yes\tyes\t30.0\t1.02\n"
        "u2ctg\tu2ctg_len-20_circular-no\tno\t11.0\t1.00\n"
        "u3ctg\tu3ctg_len-20_circular-possibly\tpossibly\t99.0\t9.99\n"
    )

    with duckdb.connect(":memory:") as c:
        _register_contig_attributes(c, attrs)
        rows = c.execute(
            ASSEMBLY_MEMBERSHIP_JOIN_SQL, [str(bin_map), str(manifest), str(feature_map)]
        ).fetchall()
    # THREE rows, not four. The duplicate WITHIN bin.1 collapses (the constant's
    # GROUP BY); the same feature under a DIFFERENT bin does not, because
    # (kind, bin_id, feature_idx) still differs. Without that collapse this returns
    # four rows and the Postgres write raises `cardinality_violation` — ON CONFLICT
    # DO UPDATE refuses to touch one conflict target twice.
    #
    # bin.1 takes u2ctg's attributes, not u3ctg's, and takes ALL FOUR from it:
    # `min(contig_id)` picks the representative and the attributes join onto that
    # one row. A per-column aggregate would be free to return u2ctg's circularity
    # beside u3ctg's depth, describing a contig that does not exist.
    assert sorted(rows) == sorted(
        [
            ("LCG", "circ1", 100, "u1ctg_len-10_circular-yes", "yes", 30.0, 1.02),
            ("MAG", "bin.1", 200, "u2ctg_len-20_circular-no", "no", 11.0, 1.00),
            ("MAG", "bin.2", 200, None, None, None, None),
        ]
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


def test_membership_accession_join_keeps_features_with_no_manifest_match(tmp_path):
    """The accession join is a LEFT JOIN from feature_map: a feature_idx whose
    sequence_hash isn't in the manifest still yields a membership row, with a NULL
    accession — never a silently DROPPED feature. Guards the documented
    manifest ⊇ feature_map invariant so a future break degrades to a NULL
    accession, not a missing member (which would surface far downstream)."""
    import uuid

    import duckdb

    from qiita_control_plane.actions.library import MEMBERSHIP_ACCESSION_JOIN_SQL

    h1 = uuid.UUID(int=1)
    h_orphan = uuid.UUID(int=99)  # in feature_map, absent from the manifest

    def _write(path, schema, rows):
        with duckdb.connect(":memory:") as c:
            c.execute(f"CREATE TEMP TABLE t ({schema})")
            c.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
            c.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")

    manifest = tmp_path / "manifest.parquet"
    feature_map = tmp_path / "feature_map.parquet"
    _write(manifest, "read_id VARCHAR, sequence_hash UUID", [("ACC1", str(h1))])
    _write(
        feature_map,
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(h1), 100), (str(h_orphan), 999)],
    )

    with duckdb.connect(":memory:") as c:
        rows = dict(
            c.execute(MEMBERSHIP_ACCESSION_JOIN_SQL, [str(feature_map), str(manifest)]).fetchall()
        )
    assert rows == {100: "ACC1", 999: None}, "orphan feature survives with NULL accession"


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


async def test_register_files_logs_what_a_load_replaced(monkeypatch, caplog):
    """The data plane replaces some tables on their key rather than appending (its
    REPLACE_KEY_TABLES), so a load can supersede rows an earlier one wrote. The
    per-table counts ride back in `replaced` and are logged."""
    import json
    import logging

    from qiita_control_plane.actions import library as lib

    def _fake_do_action(action_type, data_plane_url, token, timeout_seconds=None):
        return [
            _FakeResult(
                json.dumps(
                    {
                        "registered": ["/lake/assembled_sequence/wt7-assembled_sequence.parquet"],
                        "replaced": {"assembled_sequence": 3},
                    }
                ).encode()
            )
        ]

    monkeypatch.setattr(lib, "_do_action", _fake_do_action)

    with caplog.at_level(logging.INFO, logger=lib.__name__):
        registered = await lib.register_files(
            staging_dir="/staging",
            files={"assembled_sequence.parquet": "assembled_sequence"},
            work_ticket_idx=7,
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://dp:50051",
        )

    assert registered == ["/lake/assembled_sequence/wt7-assembled_sequence.parquet"]
    assert "'assembled_sequence': 3" in caplog.text


async def test_register_files_stays_quiet_when_nothing_was_replaced(monkeypatch, caplog):
    """The ordinary load — every key new to the lake — replaces nothing, and a
    data plane that predates the field returns no `replaced` at all."""
    import json
    import logging

    from qiita_control_plane.actions import library as lib

    for body in ({"registered": [], "replaced": {}}, {"registered": []}):

        def _fake_do_action(action_type, data_plane_url, token, timeout_seconds=None, body=body):
            return [_FakeResult(json.dumps(body).encode())]

        monkeypatch.setattr(lib, "_do_action", _fake_do_action)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=lib.__name__):
            assert (
                await lib.register_files(
                    staging_dir="/staging",
                    files={"reference_membership.parquet": "reference_membership"},
                    work_ticket_idx=7,
                    signing_key=b"\x00" * 32,
                    data_plane_url="grpc://dp:50051",
                )
                == []
            )
        assert "replaced" not in caplog.text


def _miint_conn():
    from qiita_control_plane.miint import connect_with_miint

    return connect_with_miint()


def _canonical_hashes(sequences):
    """The canonical hash of each sequence, one per record (no dedup).

    Evaluates `canonical_sequence_hash_expr` itself rather than mirroring it in
    Python: it folds both case and strand, and a hand-rolled twin drifts without
    the test noticing.
    """
    from qiita_common.chunking import canonical_sequence_hash_expr

    with _miint_conn() as conn:
        conn.execute("CREATE TABLE _seq (i INTEGER, sequence VARCHAR)")
        conn.executemany("INSERT INTO _seq VALUES (?, ?)", list(enumerate(sequences)))
        rows = conn.execute(
            f"SELECT i, {canonical_sequence_hash_expr('sequence')} AS h FROM _seq ORDER BY i"
        ).fetchall()
    return [h for _, h in rows]


def _revcomp(sequence):
    """miint's reverse complement, case preserved.

    `sequence_dna_reverse_complement` preserves case
    (https://the-miint.github.io/duckdb-miint/utilities/), so a mixed-case
    argument yields a mixed-case twin — which is what lets one fixture below
    fold case and strand at the same time.
    """
    with _miint_conn() as conn:
        return conn.execute("SELECT sequence_dna_reverse_complement(?)", [sequence]).fetchone()[0]


def _write_manifest(path, read_ids, sequences):
    import duckdb

    hashes = _canonical_hashes(sequences)
    # Three columns, matching what hash_sequences emits — the warning's own
    # argument leans on sequence_length_bp not discriminating.
    with duckdb.connect(":memory:") as c:
        c.execute(
            "CREATE TEMP TABLE t (read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT)"
        )
        c.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(r, str(h), len(q)) for r, h, q in zip(read_ids, hashes, sequences, strict=True)],
        )
        c.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    return len(set(hashes))


# Non-palindromic on purpose: a sequence equal to its own reverse complement
# cannot tell a strand fold from an identity, so it would pass either way.
_FWD = "ACGTTGCAAGGTCCATTGCA"
_MIXED_CASE = "acgtACGTttccGGaa"
_SOLO = "TTTTGGGGCCCCAAAATTTT"


def test_warn_on_collapsed_records_counts_and_names_the_absorbed_records(tmp_path, caplog):
    """Records folded together by case, by strand, or by both are absent from the
    reference; the warning reports how many and which read_ids shared a hash."""
    import logging

    from qiita_control_plane.actions.library import _warn_on_collapsed_records

    read_ids = ["fwd", "rev", "mixed", "mixed_rc", "mixed_upper", "solo"]
    sequences = [
        _FWD,
        _revcomp(_FWD),
        _MIXED_CASE,
        # Case-preserving, so this one folds case AND strand together — the
        # combination a hand-rolled hash oracle got wrong before.
        _revcomp(_MIXED_CASE),
        _MIXED_CASE.upper(),
        _SOLO,
    ]
    manifest = tmp_path / "manifest.parquet"
    features = _write_manifest(manifest, read_ids, sequences)
    # fwd/rev fold on strand; mixed/mixed_rc/mixed_upper on case, strand, and
    # both at once; solo stands alone.
    assert features == 3

    with caplog.at_level(logging.WARNING):
        collapsed = _warn_on_collapsed_records("reference 42", manifest)

    assert collapsed == 3
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert message.startswith("reference 42: ")
    assert "6 submitted record(s) collapsed to 3 feature(s)" in message
    assert "fwd, rev" in message
    # A three-member group is listed together, ordered within the group.
    assert "mixed, mixed_rc, mixed_upper" in message
    assert "solo" not in message


def test_warn_on_collapsed_records_is_silent_when_nothing_collapsed(tmp_path, caplog):
    """The control: distinct sequences leave record count equal to feature count,
    so the same call warns nothing and reports zero."""
    import logging

    from qiita_control_plane.actions.library import _warn_on_collapsed_records

    manifest = tmp_path / "manifest.parquet"
    features = _write_manifest(manifest, ["fwd", "solo"], [_FWD, _SOLO])
    assert features == 2

    with caplog.at_level(logging.WARNING):
        collapsed = _warn_on_collapsed_records("reference 42", manifest)

    assert collapsed == 0
    assert caplog.records == []


def test_warn_on_collapsed_records_marks_a_truncated_group_list(tmp_path, caplog):
    """Past the cap the message lists that many groups and says it was cut; the
    count itself still covers every collapsed record."""
    import logging

    from qiita_control_plane.actions.library import (
        _MAX_REPORTED,
        _warn_on_collapsed_records,
    )

    pairs = _MAX_REPORTED + 3
    read_ids, sequences = [], []
    for i in range(pairs):
        # Distinct per pair, and non-palindromic so each pair folds on strand.
        seq = f"{'ACGTTGCAAGG' * 2}{'ACGT' * (i + 1)}TTCCA"
        read_ids += [f"r{i:02d}_fwd", f"r{i:02d}_rev"]
        sequences += [seq, _revcomp(seq)]
    manifest = tmp_path / "manifest.parquet"
    features = _write_manifest(manifest, read_ids, sequences)
    assert features == pairs

    with caplog.at_level(logging.WARNING):
        collapsed = _warn_on_collapsed_records("reference 42", manifest)

    assert collapsed == pairs
    message = caplog.records[0].getMessage()
    assert message.endswith("(truncated)")
    assert message.count(";") == _MAX_REPORTED - 1


def test_warn_on_collapsed_records_leads_with_the_caller_s_scope(tmp_path, caplog):
    """The helper serves both membership writers, so the thing being loaded is the
    caller's to name — an assembly run is not a reference."""
    import logging

    from qiita_control_plane.actions.library import _warn_on_collapsed_records

    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, ["c_fwd", "c_rev"], [_FWD, _revcomp(_FWD)])

    with caplog.at_level(logging.WARNING):
        collapsed = _warn_on_collapsed_records(
            "assembly run (prep_sample 7, processing 3)", manifest
        )

    assert collapsed == 1
    message = caplog.records[0].getMessage()
    assert message.startswith("assembly run (prep_sample 7, processing 3): ")
    assert "c_fwd, c_rev" in message
