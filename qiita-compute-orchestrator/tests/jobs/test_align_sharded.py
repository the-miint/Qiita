"""Isolated unit tests for `align_sharded.execute` + `plan`.

The real miint aligner seams — `rype_classify` (read_to_shard build) and
`align_{minimap2,bowtie2}_sharded` — need the extension, real sequence bytes, and
per-shard indexes, so they are exercised by the integration smoke
(`tests/integration/test_sharded_alignment.py`). Here those seams are stubbed and we
assert the orchestration around them:

  - the query is the WHOLE read set as `(read_id = sequence_idx, sequence1,
    sequence2)` — ONE query, no SE/PE split (a read set is uniformly SE or PE by
    construction; the tools handle the mode natively);
  - a SINGLE align call runs (the aligner is dispatched by `Inputs.aligner`, which
    the CP resolves from platform — minimap2 carries the `map-hifi` preset);
  - minimap2 is handed a MATERIALIZED copy of the query relation and bowtie2 the lazy
    view, and that copy exists only across the align (created after routing, dropped
    before the phase-2 sort) — every other minimap2 case here runs through it, so the
    output assertions double as proof the copy changes nothing but storage;
  - the aligner's SAM output is passed through, EXCEPT the raw VARCHAR
    `reference`/`mate_reference` (dropped — `feature_idx`/`mate_feature_idx`, cast
    from them, carry the identity), with `prep_sample_idx` (stamped PER ROW from the
    reads), `feature_idx`, and `mate_feature_idx` added;
  - a paired-end read's two mate rows both survive AND keep their mate columns, so
    the pairing is explicit (not two unrelated rows);
  - cross-shard multiplicity emits one distinct-feature row per shard (no dedup);
  - the identity filter keeps only high-identity placements, with its two dimensions
    kept independent: the FLOORS come from the aligner (0.99 bowtie2 / 0.90 minimap2,
    query coverage minimap2-only) while the POOLING comes from the batch shape — a
    paired-end batch's two mate rows are judged as a unit (never an orphan) and a
    single-end record is judged on its own CIGAR, whichever aligner produced it;
  - a batch mixing single- and paired-end reads is REJECTED, before the routing pass;
  - an empty alignment set is VALID (no fail-fast);
  - a failed align leaves no partial output.

The stub aligner emits =/X CIGARs (as the real bowtie2 does under `xeq := true`),
because the COPY's identity filter runs the REAL `cigar_sequence_identity`, which
returns NULL for a plain `M` CIGAR (it needs the =/X distinction).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from pathlib import Path

import duckdb
import pytest

from qiita_compute_orchestrator.miint import open_miint_conn

# The columns the stubbed align seam materialises, mimicking the real
# align_*_sharded output (a representative subset of the full SAM columns — enough
# to prove the mate columns pass through and the raw subject ids are dropped).
# `reference`/`mate_reference` are VARCHAR subject ids (our feature_idx), matching
# the real function; execute() drops them from the OUTPUT after casting to
# feature_idx / mate_feature_idx.
_ALIGN_COLS = (
    "read_id",
    "flags",
    "reference",
    "position",
    "stop_position",
    "mapq",
    "cigar",
    "mate_reference",
    "mate_position",
    "template_length",
)

# The alignment.parquet output columns (the stub's representative subset): the five
# CP identity columns + the aligner SAM columns MINUS read_id/reference/mate_reference.
_OUTPUT_COLS = [
    "alignment_idx",
    "prep_sample_idx",
    "sequence_idx",
    "feature_idx",
    "mate_feature_idx",
    "flags",
    "position",
    "stop_position",
    "mapq",
    "cigar",
    "mate_position",
    "template_length",
]


# The reads the streamed-block stub serves. `_write_reads_parquet` records the
# last file it wrote here, and `_stub_block_read_stream` streams it — so a test
# still declares its reads by writing them, exactly as before align_sharded
# stopped taking a `reads` input.
_STAGED_READS: list[Path] = []


def _write_reads_parquet(path: Path, rows: list[tuple[int, int, str, str | None]]) -> Path:
    """Write a read-block Parquet with the columns align_sharded reads:
    `(prep_sample_idx BIGINT, sequence_idx BIGINT, sequence1 VARCHAR, sequence2
    VARCHAR)`. `rows` = (prep_sample_idx, sequence_idx, sequence1, sequence2).

    Also records the path for the streamed-block stub (see `_STAGED_READS`)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        if not rows:
            conn.execute(
                "COPY (SELECT CAST(NULL AS BIGINT) AS prep_sample_idx, "
                "CAST(NULL AS BIGINT) AS sequence_idx, CAST(NULL AS VARCHAR) AS sequence1, "
                "CAST(NULL AS VARCHAR) AS sequence2 WHERE false) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
            _STAGED_READS.append(path)
            return path
        values_sql = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR))"
            for _ in rows
        )
        params: list = []
        for ps, sidx, s1, s2 in rows:
            params.extend([ps, sidx, s1, s2])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) "
            "AS t(prep_sample_idx, sequence_idx, sequence1, sequence2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    _STAGED_READS.append(path)
    return path


@pytest.fixture(autouse=True)
def _stub_block_read_stream(monkeypatch):
    """Serve `align_sharded`'s streamed reads from the test's own Parquet.

    The job always streams now (it has no `reads` input), so without this every
    test would try to mint a ticket against a control plane. Stubbed at
    `open_read_block_stream` — the CO→CP/DP seam — rather than at
    `bind_step_reads`, so the real drain-to-Parquet and lazy-view binding stay
    under test.
    """
    from contextlib import asynccontextmanager

    from qiita_compute_orchestrator import read_source

    _STAGED_READS.clear()

    @asynccontextmanager
    async def _fake(conn, *, work_ticket_idx, relation):
        assert _STAGED_READS, "test did not write a reads parquet"
        path = _STAGED_READS[-1]
        conn.execute(f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{path}')")
        try:
            yield relation
        finally:
            conn.execute(f"DROP VIEW IF EXISTS {relation}")

    monkeypatch.setattr(read_source, "open_read_block_stream", _fake)


def _make_indexes(tmp_path):
    """A populated router `.ryxdi` dir + a shard_directory (both just need to be
    non-empty for the validators — the real align is stubbed)."""
    router = tmp_path / "rype-router.ryxdi"
    router.mkdir(parents=True)
    (router / "manifest.toml").write_text("k=64\n")
    shard_dir = tmp_path / "minimap2-shards"
    shard_dir.mkdir(parents=True)
    (shard_dir / "0.mmi").write_bytes(b"MMI")
    return router, shard_dir


def _install_stubs(align_sharded, monkeypatch, *, routing, alignments, calls=None, captured=None):
    """Install QUERY-AWARE stubs for the read_to_shard build + both align seams.

    `routing`: {read_id: [shard_name, ...]} — the read_to_shard build inserts a row
    per (read in the query, shard_name). `alignments`: {read_id: [align_row, ...]}
    where an `align_row` is the tuple `(flags, reference, position, stop_position,
    mapq, cigar, mate_reference, mate_position, template_length)` the align seam
    emits for each read present in the query (one row per mate for a PE read).
    `calls` (optional list) records each align call's aligner, preset, and — for the
    relation it was handed — the name, the columns, the relation KIND (`BASE TABLE`
    vs `VIEW`, so a test can tell the materialized minimap2 copy from the lazy view)
    and its rows; `captured` (optional dict) records the routing `threshold`.

    The align seams return `(sql, params)` rather than materializing a relation, so
    these stubs return a SELECT over a typed VALUES list, semi-joined to the query
    table so only reads actually present in the query emit rows — the same
    query-awareness the old CTAS stub got imperatively. Returning SQL (rather than
    building a table) is what keeps execute()'s real shape under test: the fragment
    is embedded as a subquery inside the staging COPY, exactly as the real aligner
    is.

    The seams no longer receive a connection, so `fake_r2s` stashes the one it is
    given for the align stub's column introspection. That ordering is guaranteed —
    the read_to_shard build always runs before the align seam."""
    conn_box: dict = {}
    # Explicit per-value casts: DuckDB infers a VALUES column's type from the first
    # row, and mate_reference / mate_position are NULL in the single-end fixtures,
    # which would otherwise leave those columns untyped and break the `= '='` /
    # IS NULL decode in execute().
    #
    # The types must match what the REAL aligner emits for each column, not merely
    # something that holds the fixture values: `flags`/`mapq` are USMALLINT/UTINYINT
    # (see `_EMPTY_ALIGNMENT_SELECT`, which mirrors the DuckLake `alignment` table),
    # and a wider stub type would make the non-empty path's schema differ from the
    # empty path's in unit tests only — the exact divergence register-files fails on.
    # `test_align_sharded_empty_and_nonempty_schemas_agree` pins that they agree.
    col_types = (
        "BIGINT",  # read_id
        "USMALLINT",  # flags
        "VARCHAR",  # reference
        "BIGINT",  # position
        "BIGINT",  # stop_position
        "UTINYINT",  # mapq
        "VARCHAR",  # cigar
        "VARCHAR",  # mate_reference
        "BIGINT",  # mate_position
        "BIGINT",  # template_length
    )
    col_names = (
        "read_id, flags, reference, position, stop_position, mapq, cigar, "
        "mate_reference, mate_position, template_length"
    )

    def fake_r2s(conn, router_index_path, query_table, dest_table, *, threshold):
        conn_box["conn"] = conn
        if captured is not None:
            captured["threshold"] = threshold
        read_ids = [r[0] for r in conn.execute(f"SELECT read_id FROM {query_table}").fetchall()]
        for rid in read_ids:
            for shard_name in routing.get(rid, []):
                conn.execute(
                    f"INSERT INTO {dest_table} VALUES (CAST(? AS BIGINT), CAST(? AS VARCHAR))",
                    [rid, shard_name],
                )

    def _align_sql(query_table, *, aligner, preset):
        if calls is not None:
            conn = conn_box["conn"]
            cols = [d[0] for d in conn.execute(f"SELECT * FROM {query_table} LIMIT 0").description]
            # The KIND of the relation the aligner was handed, read from DuckDB's own
            # catalog rather than inferred from its name: `BASE TABLE` for the
            # materialized minimap2 copy, `VIEW` for the lazy Parquet-backed query.
            kind = conn.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
                [query_table],
            ).fetchone()
            calls.append(
                {
                    "aligner": aligner,
                    "cols": cols,
                    "preset": preset,
                    "query_table": query_table,
                    "query_kind": None if kind is None else kind[0],
                    "query_rows": conn.execute(
                        f"SELECT * FROM {query_table} ORDER BY read_id"
                    ).fetchall(),
                }
            )
        rows = []
        for rid, align_rows in alignments.items():
            for row in align_rows:
                literals = ", ".join(
                    f"CAST({_sql_literal(v)} AS {t})"
                    for v, t in zip((rid, *row), col_types, strict=True)
                )
                rows.append(f"({literals})")
        if not rows:
            # No fixture rows: an aligner that emitted nothing. Still has to be a
            # well-typed, empty relation of the right shape.
            typed = ", ".join(
                f"CAST(NULL AS {t}) AS {n}"
                for t, n in zip(col_types, col_names.replace(" ", "").split(","), strict=True)
            )
            return f"SELECT {typed} WHERE false", []
        return (
            f"SELECT * FROM (VALUES {', '.join(rows)}) AS t({col_names}) "
            f"WHERE read_id IN (SELECT read_id FROM {query_table})",
            [],
        )

    def fake_mm2(query_table, shard_directory, read_to_shard_table, *, preset):
        return _align_sql(query_table, aligner="minimap2", preset=preset)

    def fake_bt2(query_table, shard_directory, read_to_shard_table):
        return _align_sql(query_table, aligner="bowtie2", preset=None)

    monkeypatch.setattr(align_sharded, "_build_read_to_shard", fake_r2s)
    monkeypatch.setattr(align_sharded, "_align_minimap2_sharded_sql", fake_mm2)
    monkeypatch.setattr(align_sharded, "_align_bowtie2_sharded_sql", fake_bt2)


def _record_sql(align_sharded, monkeypatch) -> list[str]:
    """Record, in order, every SQL statement `execute()` runs on the job's connection.

    For the LIFETIME assertions about the materialized minimap2 query relation — that
    it is created only after routing has committed to an align, and dropped before the
    phase-2 sort so the sort gets that memory back. Those are memory-lifetime
    properties: they leave no trace in the output rows, so statement order is the only
    thing that can pin them. The recorder WRAPS the real connection (it does not
    replace it), so every statement still runs and the rest of the assertions in a
    test hold as usual. Whitespace is normalized so a reflowed SQL string does not
    break a match."""
    real_open = align_sharded.open_miint_conn
    log: list[str] = []

    class _Recorder:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *args, **kwargs):
            log.append(" ".join(sql.split()))
            return self._inner.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    @contextlib.contextmanager
    def _wrapped(*args, **kwargs):
        with real_open(*args, **kwargs) as conn:
            yield _Recorder(conn)

    monkeypatch.setattr(align_sharded, "open_miint_conn", _wrapped)
    return log


def _sole_index(log: list[str], needle: str, *, what: str) -> int:
    """The index of the ONE recorded statement containing `needle`. Fails loudly on
    zero or multiple matches, so an ordering assertion can never pass by matching the
    wrong statement."""
    hits = [i for i, sql in enumerate(log) if needle in sql]
    assert len(hits) == 1, f"expected exactly one {what} statement, found {len(hits)}"
    return hits[0]


def _sql_literal(value) -> str:
    """A Python fixture value as a SQL literal for the VALUES list above. Only the
    types the align fixtures use (None / int / str); anything else is a fixture bug
    and should fail loudly rather than be coerced."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        raise TypeError("unexpected bool in an align fixture row")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    raise TypeError(f"unsupported align fixture literal: {value!r}")


def _read_alignment(path: Path):
    """Return (columns, rows) of alignment.parquet. Rows project the output columns
    the tests assert on (leading `alignment_idx`, the identity columns, then the
    surviving SAM subset — reference/mate_reference are dropped), in a stable order."""
    with duckdb.connect(":memory:") as conn:
        cols = [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        ]
        rows = conn.execute(
            "SELECT alignment_idx, prep_sample_idx, sequence_idx, feature_idx, mate_feature_idx, "
            "flags, position, stop_position, mapq, cigar, mate_position, template_length "
            f"FROM read_parquet('{path}') "
            "ORDER BY alignment_idx, prep_sample_idx, sequence_idx, feature_idx, position, flags"
        ).fetchall()
    return cols, rows


def _alignment_schema(path: Path) -> dict[str, str]:
    """`{column_name: duckdb_type}` for alignment.parquet, via DESCRIBE so the answer
    is DuckDB's own type names rather than the DBAPI `description` type codes."""
    with duckdb.connect(":memory:") as conn:
        return {
            name: dtype
            for name, dtype, *_ in conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}')"
            ).fetchall()
        }


# An align row for a simple single-end primary hit to `feature`: no mate (mate_*
# NULL, template_length 0). Emits a =/X CIGAR (real aligners do under `xeq`), so the
# identity filter can score it. `(flags, reference, position, stop_position, mapq,
# cigar, mate_reference, mate_position, template_length)`.
def _se_hit(feature, *, flags=0, position=1, stop=41, mapq=60, cigar="40="):
    return (flags, str(feature), position, stop, mapq, cigar, None, None, 0)


def test_align_sharded_single_call_and_passthrough_minimap2(tmp_path, monkeypatch):
    """A uniformly-SE block runs ONE minimap2 call over the whole set (no split),
    and the output carries the SAM columns (minus the raw subject ids) +
    prep_sample_idx + feature_idx + mate_feature_idx, with prep_sample_idx stamped
    per row."""
    from qiita_compute_orchestrator.jobs import align_sharded

    # reads 1 & 3 align (distinct prep_samples), read 2 routes nowhere.
    _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 1, "ACGT", None), (10, 2, "TTGG", None), (20, 3, "GGCC", None)],
    )
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    captured: dict = {}
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 3: ["1"]},
        alignments={
            1: [_se_hit(100, position=5, stop=45)],
            3: [_se_hit(200, position=12, stop=52)],
        },
        calls=calls,
        captured=captured,
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))

    # Exactly ONE align call over the WHOLE set (no SE/PE split), carrying the full
    # query columns; the map-hifi preset + routing threshold reach miint.
    assert [c["aligner"] for c in calls] == ["minimap2"]
    assert calls[0]["preset"] == "map-hifi"
    assert calls[0]["cols"] == ["read_id", "sequence1", "sequence2"]
    assert captured["threshold"] == align_sharded._ROUTING_THRESHOLD

    cols, rows = _read_alignment(Path(out["alignment"]))
    # Leading alignment_idx + the identity columns + the aligner SAM columns MINUS
    # read_id/reference/mate_reference (read_id renamed to sequence_idx).
    assert cols == _OUTPUT_COLS
    # alignment_idx stamped on every row; prep_sample_idx stamped PER ROW (read 1 ->
    # 10, read 3 -> 20); feature_idx is CAST(reference); mate columns NULL for SE.
    assert rows == [
        (555, 10, 1, 100, None, 0, 5, 45, 60, "40=", None, 0),
        (555, 20, 3, 200, None, 0, 12, 52, 60, "40=", None, 0),
    ]


def test_align_sharded_dispatch_bowtie2(tmp_path, monkeypatch):
    """aligner='bowtie2' routes to the bowtie2 seam (no preset kwarg — the param set
    is inlined in the seam), never minimap2, in a single call."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"]},
        alignments={1: [_se_hit(100)]},
        calls=calls,
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    assert [c["aligner"] for c in calls] == ["bowtie2"]
    assert calls[0]["preset"] is None
    _cols, rows = _read_alignment(Path(out["alignment"]))
    assert rows == [(555, 10, 1, 100, None, 0, 1, 41, 60, "40=", None, 0)]


@pytest.mark.parametrize("aligner", ["minimap2", "bowtie2"])
def test_align_sharded_aligner_gets_a_materialized_query(tmp_path, monkeypatch, aligner):
    """BOTH aligners are handed a real TABLE holding the query, not the lazy Parquet view.

    Each sharded aligner re-reads the query relation once per shard, and against a
    Parquet-backed view every one of those reads costs the block's whole sequence BYTES
    (a scan decompresses entire column chunks to yield any row). That is why the copy is
    unconditional: it pays for itself on the short-read block too, not just the long-read
    one — an earlier revision materialized for minimap2 only, on a slope measured at 1M
    reads and then applied to the 10M-read short-read block.

    Asserted on the relation KIND from DuckDB's own catalog rather than the name, since
    the name alone would not prove materialization, and the copy is checked FAITHFUL:
    same columns, same rows, in the query's own shape."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = [(10, 1, "ACGT", None), (10, 2, "TTGG", None), (20, 3, "GGCC", None)]
    _write_reads_parquet(tmp_path / "reads.parquet", reads)
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 2: ["0"], 3: ["1"]},
        alignments={1: [_se_hit(100)]},
        calls=calls,
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner=aligner,
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))

    assert calls[0]["query_table"] == align_sharded._QUERY_MATERIALIZED
    assert calls[0]["query_kind"] == "BASE TABLE"
    # Faithful copy: the query's columns, and every read — the materialization is a
    # storage change only, it must not filter or reshape the query.
    assert calls[0]["cols"] == ["read_id", "sequence1", "sequence2"]
    assert calls[0]["query_rows"] == [
        (sequence_idx, sequence1, sequence2) for _psi, sequence_idx, sequence1, sequence2 in reads
    ]


def test_align_sharded_query_view_is_what_routing_and_the_probe_read(tmp_path, monkeypatch):
    """The materialized copy is for the ALIGNER only — the SE/PE probe and the routing
    pass still read a VIEW, and the copy does not exist yet when they run.

    Both would be actively worse off against a table: the probe is answered from the
    Parquet's row-group statistics without touching the sequence columns, and rype holds
    its accumulated reads plus its minimizer structures for the length of the classify,
    so building ours first would stack a full copy of the block on top of that peak."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", "TTGG")])
    router, shard_dir = _make_indexes(tmp_path)

    routed_query_tables: list[str] = []

    def _record_routing_relation(conn, router_index_path, query_table, dest_table, *, threshold):
        routed_query_tables.append(query_table)
        conn.execute(f"INSERT INTO {dest_table} VALUES (CAST(1 AS BIGINT), CAST('0' AS VARCHAR))")

    _install_stubs(align_sharded, monkeypatch, routing={1: ["0"]}, alignments={1: [_se_hit(100)]})
    monkeypatch.setattr(align_sharded, "_build_read_to_shard", _record_routing_relation)
    sql_log = _record_sql(align_sharded, monkeypatch)

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))

    assert routed_query_tables == [align_sharded._QUERY]
    probe = _sole_index(sql_log, "count(sequence2), count(*)", what="SE/PE probe")
    created = _sole_index(
        sql_log, f"CREATE TABLE {align_sharded._QUERY_MATERIALIZED}", what="materialize"
    )
    assert probe < created


@pytest.mark.parametrize("sequence2", [None, "TTGG"], ids=["single-end", "paired-end"])
def test_align_sharded_routes_from_a_view_carrying_both_mates(tmp_path, monkeypatch, sequence2):
    """The routing pass reads the full `_QUERY`, and reads it as a VIEW.

    Both properties are invisible in the output. A TABLE here would stack a second copy of
    the block's sequences (~15 GB long-read) on top of rype's own peak, so only
    `table_type` can pin it.

    The parametrize is the assertion on the column list: the same three columns for either
    read shape is what says the relation handed to rype is UNCONDITIONAL.

    The aligner assertion is the other half: `sequence2` must survive into
    `_QUERY_MATERIALIZED`, because both sharded aligners need it to align a pair natively.
    Narrowing that relation would silently turn PE alignment into SE."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", sequence2)])
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"]},
        alignments={1: [_se_hit(100)]},
        calls=calls,
    )

    # WRAP the installed routing stub rather than replacing it: the stub is what hands
    # the align seam its connection, so a bare replacement would strand `calls`.
    seen_routing_cols: list[list[str]] = []
    seen_routing_kinds: list[str] = []
    installed_r2s = align_sharded._build_read_to_shard

    def _record_routing_cols(conn, router_index_path, query_table, dest_table, *, threshold):
        seen_routing_cols.append(
            [d[0] for d in conn.execute(f"SELECT * FROM {query_table} LIMIT 0").description]
        )
        # The KIND matters as much as the columns: a TABLE here would hold a second
        # copy of the block's sequences (~15 GB long-read) on top of rype's own peak,
        # which is why routing reads the lazy Parquet scan. See the note at the
        # _QUERY_MATERIALIZED create.
        seen_routing_kinds.append(
            conn.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
                [query_table],
            ).fetchone()[0]
        )
        installed_r2s(conn, router_index_path, query_table, dest_table, threshold=threshold)

    monkeypatch.setattr(align_sharded, "_build_read_to_shard", _record_routing_cols)

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))

    assert seen_routing_cols == [["read_id", "sequence1", "sequence2"]]
    assert seen_routing_kinds == ["VIEW"]
    assert calls[0]["cols"] == ["read_id", "sequence1", "sequence2"]


def test_align_sharded_materialized_query_lives_only_across_the_align(tmp_path, monkeypatch):
    """The materialized query is created only once routing has committed to an align,
    and dropped before the phase-2 sort.

    Both ends are memory-lifetime properties invisible in the output: creating it
    before the routing pass would hold a copy of the block's sequences while rype makes
    its own internal one, and holding it past phase 1 would deny the sort that memory.
    Statement order is the only thing that can pin either."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(align_sharded, monkeypatch, routing={1: ["0"]}, alignments={1: [_se_hit(100)]})
    sql_log = _record_sql(align_sharded, monkeypatch)

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))

    routed_check = _sole_index(
        sql_log, f"SELECT count(*) FROM {align_sharded._READ_TO_SHARD}", what="routed-count"
    )
    created = _sole_index(
        sql_log, f"CREATE TABLE {align_sharded._QUERY_MATERIALIZED}", what="materialize"
    )
    # Phase 1 by its own projection, not by the staging filename — phase 2 names that
    # file too (it reads what phase 1 wrote), and matching both would order nothing.
    phase1 = _sole_index(sql_log, "AS alignment_idx, rm.prep_sample_idx", what="phase-1 COPY")
    dropped = _sole_index(
        sql_log, f"DROP TABLE IF EXISTS {align_sharded._QUERY_MATERIALIZED}", what="drop"
    )
    phase2 = _sole_index(sql_log, "ORDER BY alignment_idx", what="phase-2 sort")
    assert routed_check < created < phase1 < dropped < phase2


def test_align_sharded_no_routed_reads_skips_the_materialization(tmp_path, monkeypatch):
    """A block whose reads route nowhere never pays for the copy: the aligner is not
    called at all on that path, so materializing would buy nothing."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(align_sharded, monkeypatch, routing={}, alignments={})
    sql_log = _record_sql(align_sharded, monkeypatch)

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))

    assert not [
        sql for sql in sql_log if align_sharded._QUERY_MATERIALIZED in sql and "CREATE" in sql
    ]
    # Still the valid empty output the no-routed-reads path is supposed to write.
    _cols, rows = _read_alignment(Path(out["alignment"]))
    assert rows == []


def test_align_sharded_cross_shard_multiplicity_no_dedup(tmp_path, monkeypatch):
    """A read routed to two shards aligns to a DISTINCT feature per shard and emits
    BOTH rows — no cross-shard dedup (a feature is in exactly one shard)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 7, "ACGTTTGG", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={7: ["0", "1"]},  # routes to BOTH shards
        alignments={7: [_se_hit(100, position=1, stop=41), _se_hit(200, position=3, stop=43)]},
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    assert rows == [
        (555, 10, 7, 100, None, 0, 1, 41, 60, "40=", None, 0),
        (555, 10, 7, 200, None, 0, 3, 43, 60, "40=", None, 0),
    ]


def test_align_sharded_pe_pair_keeps_mate_columns(tmp_path, monkeypatch):
    """A paired-end (bowtie2) read aligning within ONE shard emits one SAM row per
    mate — two rows sharing (sequence_idx, feature_idx). BOTH survive AND keep their
    mate columns (mate_position / template_length) so the pairing is EXPLICIT — one
    read's alignment to a feature, not two unrelated rows. Also pins the
    `mate_feature_idx` cast across BOTH SAM RNEXT encodings of a mate on the same
    feature: `'='` and the numeric id (the raw mate_reference is dropped from output,
    but the decode still resolves it). The pair is high-identity so the pooled filter
    keeps it."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 5, "ACGTACGT", "TTGGCCAA")])
    router, shard_dir = _make_indexes(tmp_path)
    # One PE read routed to a single shard; the align seam emits two mate rows to the
    # same feature 100 with a signed template_length, mimicking an fr pair (R1 fwd
    # flags 99, R2 rev flags 147). mate_reference is '=' on R1 and the numeric "100"
    # on R2 so both cast branches resolve to mate_feature_idx 100. =/X CIGARs at 100%
    # identity → the pooled pair clears the filter.
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={5: ["0"]},
        alignments={
            5: [
                (99, "100", 1, 151, 60, "150=", "=", 151, 300),
                (147, "100", 151, 301, 60, "150=", "100", 1, -300),
            ]
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Both mate rows kept (ordered by position), each carrying its mate columns +
    # the decoded mate_feature_idx (100 either way). NOT collapsed.
    assert rows == [
        (555, 10, 5, 100, 100, 99, 1, 151, 60, "150=", 151, 300),
        (555, 10, 5, 100, 100, 147, 151, 301, 60, "150=", 1, -300),
    ]


def test_align_sharded_low_identity_alignment_filtered(tmp_path, monkeypatch):
    """A single-end (minimap2) alignment below the identity threshold is dropped,
    while a high-identity one on the same block survives — the per-record filter."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None), (10, 2, "TTGG", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 2: ["0"]},
        alignments={
            1: [_se_hit(100, cigar="40=")],  # identity 1.0 -> kept
            2: [_se_hit(200, position=3, stop=43, cigar="20=20X")],  # identity 0.5 -> dropped
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Only the high-identity read 1 survives; the 50%-identity read 2 is filtered.
    assert rows == [(555, 10, 1, 100, None, 0, 1, 41, 60, "40=", None, 0)]


def test_align_sharded_minimap2_low_query_coverage_filtered(tmp_path, monkeypatch):
    """A minimap2 placement that is HIGH-IDENTITY but LOW query coverage (a long read
    soft-clipped down to a short aligned span) is dropped by the qcov floor, while a
    fully-covered high-identity one on the same block survives. Identity alone would
    keep both — this pins the separate minimap2 query-coverage gate."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None), (10, 2, "TTGG", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 2: ["0"]},
        alignments={
            # identity 1.0 both; read 1 fully aligns (qcov 1.0 -> kept), read 2 aligns
            # 20 of 100 query bases (80 soft-clipped -> qcov 0.2 < 0.90 -> dropped).
            1: [_se_hit(100, cigar="100=")],
            2: [_se_hit(200, cigar="20=80S")],
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Only the fully-covered read 1 survives; read 2's low-qcov placement is dropped.
    assert [r[2] for r in rows] == [1]


def test_align_sharded_minimap2_identity_floor_is_0_90(tmp_path, monkeypatch):
    """minimap2 (long-read) uses a 0.90 identity floor, NOT bowtie2's 0.99. A 0.95
    placement is KEPT (it would be dropped under 0.99) and a 0.85 one is dropped —
    pinning the per-aligner floor for the more-divergent long-read population."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None), (10, 2, "TTGG", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 2: ["0"]},
        alignments={
            1: [_se_hit(100, cigar="38=2X")],  # identity 0.95 -> kept at the 0.90 floor
            2: [_se_hit(200, cigar="34=6X")],  # identity 0.85 -> dropped
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Read 1 (0.95) survives, read 2 (0.85) is filtered — the 0.90 minimap2 floor.
    assert [r[2] for r in rows] == [1]


def test_align_sharded_bowtie2_low_identity_pair_dropped_as_unit(tmp_path, monkeypatch):
    """A bowtie2 concordant pair whose POOLED identity is below threshold drops BOTH
    mates (never orphans one), while a high-identity pair on the same block is kept
    whole."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 5, "ACGTACGT", "TTGGCCAA"), (10, 6, "GGGGCCCC", "AAAATTTT")],
    )
    router, shard_dir = _make_indexes(tmp_path)
    # Pair 5 (feature 100): both mates 150= -> pooled identity 1.0 -> KEPT.
    # Pair 6 (feature 200): mate A 150=, mate B 100=50X -> pooled 250 matches / 300
    # aligned = 0.833 < 0.99 -> BOTH dropped (as a unit).
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={5: ["0"], 6: ["0"]},
        alignments={
            5: [
                (99, "100", 1, 151, 60, "150=", "=", 151, 300),
                (147, "100", 151, 301, 60, "150=", "=", 1, -300),
            ],
            6: [
                (99, "200", 1, 151, 60, "150=", "=", 151, 300),
                (147, "200", 151, 301, 60, "100=50X", "=", 1, -300),
            ],
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Only pair 5 (feature 100) survives, both mates; pair 6 is gone entirely.
    assert rows == [
        (555, 10, 5, 100, 100, 99, 1, 151, 60, "150=", 151, 300),
        (555, 10, 5, 100, 100, 147, 151, 301, 60, "150=", 1, -300),
    ]


def test_align_sharded_minimap2_pe_pair_pooled_at_minimap2_floor(tmp_path, monkeypatch):
    """A PAIRED-END minimap2 batch pools its mates, at minimap2's floor — the fourth
    (aligner x batch-shape) quadrant, and the only one with no live caller.

    The two dimensions are independent by design: the FLOORS come from the aligner,
    the POOLING from the batch shape. Three quadrants are reachable through the
    control plane, which picks minimap2 for pacbio/nanopore (single-end) and bowtie2
    for short reads (either shape); PE+minimap2 is dead today. It is pinned anyway
    because the alternative to defining it is special-casing it, and an untested
    combination is how the two dimensions silently re-conflate.

    Both minimap2-specific gates are exercised over the POOLED CIGAR here, which no
    other test does (the qcov test is single-end, so it scores one row's `cigar`):
      * pair 6's pooled identity 0.967 is KEPT at 0.90 and would be DROPPED at
        bowtie2's 0.99 — so the floor really is per-aligner, not per-shape;
      * pair 7 is high-identity but low pooled COVERAGE, and is dropped by the
        coverage conjunct applied to `string_agg(cigar)` rather than a single row."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(
        tmp_path / "reads.parquet",
        [
            (10, 5, "ACGTACGT", "TTGGCCAA"),
            (10, 6, "GGGGCCCC", "AAAATTTT"),
            (10, 7, "CCCCGGGG", "TTTTAAAA"),
        ],
    )
    router, shard_dir = _make_indexes(tmp_path)
    # Pair 5 (feature 100): 150= + 150=      -> pooled identity 300/300 = 1.00  -> KEPT
    # Pair 6 (feature 200): 150= + 140=10X   -> pooled identity 290/300 = 0.967 -> KEPT
    #                       at the 0.90 minimap2 floor (would FAIL bowtie2's 0.99)
    # Pair 7 (feature 300): 150= + 20=130S   -> pooled identity 170/170 = 1.00, but
    #                       pooled qcov 170/300 = 0.567 < 0.90 -> BOTH mates DROPPED
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={5: ["0"], 6: ["0"], 7: ["0"]},
        alignments={
            5: [
                (99, "100", 1, 151, 60, "150=", "=", 151, 300),
                (147, "100", 151, 301, 60, "150=", "=", 1, -300),
            ],
            6: [
                (99, "200", 1, 151, 60, "150=", "=", 151, 300),
                (147, "200", 151, 301, 60, "140=10X", "=", 1, -300),
            ],
            7: [
                (99, "300", 1, 151, 60, "150=", "=", 151, 300),
                (147, "300", 151, 301, 60, "20=130S", "=", 1, -300),
            ],
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Pairs 5 and 6 survive whole; pair 7 is gone entirely (never half a pair).
    assert rows == [
        (555, 10, 5, 100, 100, 99, 1, 151, 60, "150=", 151, 300),
        (555, 10, 5, 100, 100, 147, 151, 301, 60, "150=", 1, -300),
        (555, 10, 6, 200, 200, 99, 1, 151, 60, "150=", 151, 300),
        (555, 10, 6, 200, 200, 147, 151, 301, 60, "140=10X", 1, -300),
    ]


def test_align_sharded_bowtie2_single_end_scored_per_record(tmp_path, monkeypatch):
    """A SINGLE-END bowtie2 batch is scored PER RECORD at the bowtie2 floor (0.99).

    The pooling is a property of the BATCH, not of the aligner: an SE bowtie2 run has
    no mate to pool, so each alignment stands on its own CIGAR. This is the shape the
    live short-read data takes, and it was previously UNTESTED — every bowtie2 case
    here was paired.

    Note this asserts a contract, not a bug fix: the old aligner-keyed branch
    produced the SAME rows for SE input (a pooled window over a one-row partition
    returns that row's own CIGAR), it just paid a full blocking sort of every
    alignment to do it. The change is cost, not output — which is exactly why this
    test is worth having, since nothing else pins the SE result."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 1, "ACGTACGT", None), (10, 2, "GGGGCCCC", None), (20, 3, "TTTTAAAA", None)],
    )
    router, shard_dir = _make_indexes(tmp_path)
    # read 1: 150=          -> identity 1.00 >= 0.99 -> KEPT
    # read 2: 100=50X       -> identity 0.667        -> DROPPED
    # read 3: 149=1X        -> identity 0.993        -> KEPT (and a different sample)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 2: ["0"], 3: ["0"]},
        alignments={
            1: [_se_hit(100, position=1, stop=151, cigar="150=")],
            2: [_se_hit(200, position=5, stop=155, cigar="100=50X")],
            3: [_se_hit(300, position=9, stop=159, cigar="149=1X")],
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Reads 1 and 3 survive on their own identity; read 2 is dropped. No mate columns
    # are populated (SE), and prep_sample_idx is stamped per row (10 vs 20).
    assert rows == [
        (555, 10, 1, 100, None, 0, 1, 151, 60, "150=", None, 0),
        (555, 20, 3, 300, None, 0, 9, 159, 60, "149=1X", None, 0),
    ]
    # The phase-1 staging Parquet must NOT survive in the workspace: that directory
    # is `alignment_staging_dir`, and register-files loads every `*.parquet` in it
    # into the DuckLake `alignment` table — a leftover would register the UNSORTED,
    # (for PE) UNFILTERED rows a second time.
    assert [p.name for p in (tmp_path / "ws").glob("*.parquet")] == ["alignment.parquet"]


def test_pooled_cigar_scoring_is_permutation_invariant():
    """CONTRACT (duckdb-miint): `cigar_sequence_identity` and `cigar_query_coverage`
    must be invariant to the ORDER of the concatenated CIGAR they are handed.

    The paired-end filter scores `string_agg(cigar, '')` over a window carrying **no
    `ORDER BY`**, so which mate's CIGAR lands first is unspecified — DuckDB may
    produce `mateA||mateB` on one run and `mateB||mateA` on the next. The gate is only
    well-defined if both score identically. Neither function is documented as
    order-insensitive, so pin it here: a mirror build that made either
    position-dependent would turn the PE gate NONDETERMINISTIC — the same pair kept on
    one run and dropped on the next, with no error and nothing else to catch it.

    Adding `ORDER BY` to the window would remove the dependency outright, at the cost
    of a sort inside every partition. While this contract holds, the cheaper form is
    correct; if this test ever fails, add the `ORDER BY` rather than relaxing it.

    Deliberately covers a HARD case, not just symmetric fragments: an indel and a
    soft clip, whose op mix makes a naive left-to-right accumulator order-dependent."""
    fragments = ["150=", "75=75X", "100=2I48=", "20=130S", "60=1D89="]
    with open_miint_conn() as conn:
        rows = conn.execute(
            "SELECT cigar_sequence_identity(c), cigar_query_coverage(c) "
            "FROM (SELECT UNNEST(?::VARCHAR[]) AS c)",
            [["".join(p) for p in itertools.permutations(fragments)]],
        ).fetchall()
    assert len(rows) == 120, f"expected every permutation to score, got {len(rows)}"
    identities = {r[0] for r in rows}
    coverages = {r[1] for r in rows}
    assert len(identities) == 1, f"cigar_sequence_identity is order-dependent: {identities}"
    assert len(coverages) == 1, f"cigar_query_coverage is order-dependent: {coverages}"
    # And a sanity floor: the probe must actually be scoring, not returning NULL for
    # every permutation (which would make the invariance assertions vacuous).
    assert identities != {None} and coverages != {None}


def test_align_sharded_se_placements_sharing_a_start_are_scored_separately(tmp_path, monkeypatch):
    """Two SE placements of one read on one feature at the SAME start position are
    scored INDEPENDENTLY — the good one survives the other's failure.

    This is the one input shape where the per-record SE filter is not merely cheaper
    than the pooled window it replaced, but gives DIFFERENT rows. The old partition key
    was `(read_id, reference, position)` — `LEAST`/`GREATEST` collapse to `position`
    once `mate_position` is NULL, since both ignore NULLs — so two placements sharing
    all three landed in ONE partition and were judged on their CIGARs concatenated:
    `150=` + `75=75X` pools to identity 225/300 = 0.75, below the 0.99 floor, dropping
    BOTH — including the perfect `150=`.

    Scoring the concatenation of two unrelated placements was never the intended
    semantics, so per-record is the deliberate answer here, not an accident of the
    refactor. Whether bowtie2 `report_all` can actually emit two rows sharing that key
    is not established (it would need a real aligner run); this pins our filter's
    behaviour for the shape either way, because every other fixture in this file has
    one placement per read and so cannot see the difference."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGTACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"]},
        alignments={
            1: [
                # primary, perfect: identity 1.00 >= 0.99 -> KEPT
                _se_hit(100, flags=0, position=1, stop=151, cigar="150="),
                # secondary at the SAME start, poor: identity 0.50 -> DROPPED alone
                _se_hit(100, flags=256, position=1, stop=151, cigar="75=75X"),
            ]
        },
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # The perfect placement survives on its own merits; the poor one is dropped on
    # its own. Under the old pooled form this list was EMPTY.
    assert rows == [(555, 10, 1, 100, None, 0, 1, 151, 60, "150=", None, 0)]


def test_align_sharded_rejects_a_mixed_se_pe_batch(tmp_path, monkeypatch):
    """A batch mixing single- and paired-end reads fails LOUDLY, naming the counts.

    A prep/run is uniformly one or the other by construction, so a mix is invalid
    input. Previously it was left to surface downstream — bowtie2 rejects it at bind
    with an opaque `gpl_boundary` error, and minimap2 TOLERATES a mix, which would
    have silently applied the wrong (mis-pooled) identity filter."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 1, "ACGTACGT", "TTGGCCAA"), (10, 2, "GGGGCCCC", None)],
    )
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 2: ["0"]},
        alignments={1: [_se_hit(100)], 2: [_se_hit(200)]},
    )

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="mixes single- and paired-end"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    # And it leaves no partial output behind.
    assert not (tmp_path / "ws" / "alignment.parquet").exists()


def test_align_sharded_rejects_a_mixed_batch_that_routes_nowhere(tmp_path, monkeypatch):
    """The mixed-batch rejection is UNCONDITIONAL — it does not depend on the reads
    routing somewhere.

    A block whose reads route to no shard is a legitimate empty no-op that returns an
    empty alignment.parquet, so the shape probe has to run BEFORE that early return.
    Otherwise invalid input would exit 0 whenever routing happened to come up empty,
    which is precisely the case where nothing downstream could ever notice."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 1, "ACGTACGT", "TTGGCCAA"), (10, 2, "GGGGCCCC", None)],
    )
    router, shard_dir = _make_indexes(tmp_path)
    # Nothing routes: absent the ordering above, execute() would take the empty-output
    # path and never look at the batch shape.
    _install_stubs(align_sharded, monkeypatch, routing={}, alignments={})

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="mixes single- and paired-end"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    assert not (tmp_path / "ws" / "alignment.parquet").exists()


def test_align_sharded_empty_alignment_is_valid(tmp_path, monkeypatch):
    """A block whose reads align nowhere yields an EMPTY alignment.parquet — valid,
    not a fail-fast — while keeping the full column schema."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(align_sharded, monkeypatch, routing={}, alignments={})

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    alignment = Path(out["alignment"])
    assert alignment.exists()
    cols, rows = _read_alignment(alignment)
    assert rows == []
    # The empty path writes the FULL alignment schema (`_EMPTY_ALIGNMENT_SELECT`,
    # incl. the tag_* columns) — a superset of the stub's representative subset — so
    # assert the leading identity columns, that the surviving SAM subset is present,
    # and that the raw subject ids are absent.
    assert cols[:5] == _OUTPUT_COLS[:5]
    assert {"flags", "position", "cigar", "mate_position", "template_length"} <= set(cols)
    assert "reference" not in cols and "mate_reference" not in cols


def test_align_sharded_empty_and_nonempty_schemas_agree(tmp_path, monkeypatch):
    """The empty and non-empty output paths must agree on COLUMN TYPES, not just names.

    The two paths build their schema by completely different routes: the empty one
    from `_EMPTY_ALIGNMENT_SELECT`'s hand-written CASTs, the non-empty one from
    whatever the aligner's own output types happen to be. A divergence between them
    is invisible here (both write a valid Parquet) and surfaces at register-files,
    against the DuckLake `alignment` table — so it is worth an explicit assertion.

    Scoped to the columns the STUB models, because the stub deliberately emits a
    representative subset rather than the real aligner's full 23 columns (no `tag_*`).
    Full-schema parity against real miint is pinned in
    tests/integration/test_sharded_alignment.py, which runs both paths for real; what
    this catches is a stub whose types have drifted from the empty path's, which would
    otherwise let every other test in this file assert a schema production never
    emits."""
    from qiita_compute_orchestrator.jobs import align_sharded

    router, shard_dir = _make_indexes(tmp_path)

    def _run(ws_name, *, routing, alignments):
        _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
        _install_stubs(align_sharded, monkeypatch, routing=routing, alignments=alignments)
        inputs = align_sharded.Inputs(
            # reads stream (see _stub_block_read_stream)
            reference_idx=42,
            alignment_idx=555,
            aligner="minimap2",
            router_index_path=router,
            shard_directory=shard_dir,
            work_ticket_idx=1,
        )
        out = asyncio.run(align_sharded.execute(inputs, tmp_path / ws_name))
        return _alignment_schema(Path(out["alignment"]))

    # routing={} takes the no-routed-reads path (_EMPTY_ALIGNMENT_SELECT); a routed
    # read with a placement takes the two-phase aligner path.
    empty = _run("ws_empty", routing={}, alignments={})
    nonempty = _run("ws_rows", routing={1: ["0"]}, alignments={1: [_se_hit(100)]})

    # Every column the aligner path emits must exist in the empty path with the SAME
    # type — flags USMALLINT / mapq UTINYINT being the ones a careless stub widens.
    assert set(nonempty) <= set(empty)
    assert {c: empty[c] for c in nonempty} == nonempty
    assert nonempty["flags"] == "USMALLINT"
    assert nonempty["mapq"] == "UTINYINT"


def test_align_sharded_partial_output_removed_on_failure(tmp_path, monkeypatch):
    """A failed align leaves no partial alignment.parquet (the manifest walker must
    not promote it)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(align_sharded, monkeypatch, routing={1: ["0"]}, alignments={})

    def boom(query_table, shard_directory, read_to_shard_table, *, preset):
        raise RuntimeError("align blew up")

    monkeypatch.setattr(align_sharded, "_align_minimap2_sharded_sql", boom)

    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=42,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(RuntimeError, match="align blew up"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    assert not (tmp_path / "ws" / "alignment.parquet").exists()


def test_align_sharded_takes_no_reads_input():
    """align_sharded ALWAYS streams its reads; there is no `reads` field.

    Pinned because the absence is the contract: the `align` workflow stages
    nothing, and the control plane decides from the work ticket that the stream
    is the block's MASKED reads. A `reads` input would reintroduce a way to hand
    this job raw, un-host-depleted reads.
    """
    from qiita_compute_orchestrator.jobs import align_sharded

    assert "reads" not in align_sharded.Inputs.model_fields


def test_align_sharded_missing_router_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    _router, shard_dir = _make_indexes(tmp_path)
    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=1,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=tmp_path / "absent.ryxdi",
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError, match="router_index_path"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))


def test_align_sharded_empty_router_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    _router, shard_dir = _make_indexes(tmp_path)
    empty_router = tmp_path / "empty.ryxdi"
    empty_router.mkdir()
    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=1,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=empty_router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="populated .ryxdi"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))


def test_align_sharded_missing_shard_directory_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import align_sharded

    _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, _shard_dir = _make_indexes(tmp_path)
    inputs = align_sharded.Inputs(
        # reads stream (see _stub_block_read_stream)
        reference_idx=1,
        alignment_idx=555,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=tmp_path / "absent-shards",
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError, match="shard_directory"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))


def test_align_sharded_rejects_unknown_aligner(tmp_path):
    """Inputs validation (Literal) rejects an aligner other than minimap2/bowtie2."""
    from pydantic import ValidationError

    from qiita_compute_orchestrator.jobs import align_sharded

    with pytest.raises(ValidationError):
        align_sharded.Inputs(
            reads=tmp_path / "reads.parquet",
            reference_idx=1,
            alignment_idx=555,
            aligner="bwa",
            router_index_path=tmp_path / "r.ryxdi",
            shard_directory=tmp_path / "shards",
            work_ticket_idx=1,
        )


def test_align_sharded_has_no_plan():
    """align_sharded deliberately exposes NO plan().

    Its reads stream from the data plane, and the walltime model it used to carry
    was driven by a Parquet-footer read count that a stream cannot supply — and
    plan() runs at submit time in the orchestrator process, so it must not open a
    Flight stream to find one. Sizing falls back to the workflow YAML baseline
    with TIMEOUT escalation as the backstop, matching estimate_feature_table.

    Pinned as a test because the absence is a decision, not an omission: the
    block's exact read count IS derivable control-plane-side from the block
    members, so restoring sizing means threading that count through the workflow
    `params:` — not reinstating a footer read here.
    """
    from qiita_compute_orchestrator.jobs import align_sharded

    assert not hasattr(align_sharded, "plan")
