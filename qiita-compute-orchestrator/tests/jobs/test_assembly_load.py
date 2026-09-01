"""Tests for the assembly_load native job.

Calls execute() directly. Synthesizes assembly_hash's outputs (manifest,
hash-keyed assembly_chunks, bin_map) + mint-features' feature_map + the container
CheckM / DAS_Tool TSVs, and asserts the four DuckLake-shape staging Parquets:
assembled_sequence (reused reference_load writer), assembled_sequence_chunks
(reused writer, re-keyed to feature_idx), assembly_membership (the DuckLake copy),
and bin_quality (CSV-read). No FASTA and no real hashing here — fixtures carry the
sequence_hash/feature_idx directly so the test isolates the re-key + lift logic.
"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from uuid import UUID

import duckdb
import pytest
from qiita_common.assembly_constants import (
    CONTIG_ATTRIBUTE_COLUMNS,
    CONTIG_ATTRIBUTES_FILE,
)
from qiita_common.chunking import reassemble_chunks_expr

# Three contigs across two bins, one circular genome, and one unbinned-residue
# contig, keyed by assembly_hash's synthetic `kind:bin_id:sequence_index`. bin.1 has
# two contigs; bin.2's contig shares bin.1's first contig's bytes (same hash -> same
# feature_idx) to exercise the distinct-membership / dedup path. The UNBINNED row
# carries its own contig id as bin_id, the shape assembly_hash emits, and has no
# CheckM counterpart — bin_quality holds MAG rows alone.
_SEQUENCES = {
    "LCG:circ1:1": ("AAAACCCCGGGGTTTT", 100),
    "MAG:bin.1:1": ("ACGTACGTACGTACGT", 200),
    "MAG:bin.1:2": ("TTTTGGGGCCCCAAAA", 300),
    "MAG:bin.2:1": ("ACGTACGTACGTACGT", 200),  # identical bytes to bin.1's first
    "UNBINNED:ctgU:1": ("GGGGCCCCAAAATTTT", 400),
}


def _hash(seq: str) -> UUID:
    return UUID(hashlib.md5(seq.encode()).hexdigest())


def _bin_kind(read_id: str) -> tuple[str, str]:
    """`kind:bin_id:sequence_index` -> (kind, bin_id). Neither `kind` nor
    `sequence_index` holds a `:`, so the FIRST and the LAST one are the two
    separators; a bin_id may hold any number in between."""
    kind, rest = read_id.split(":", 1)
    return kind, rest.rsplit(":", 1)[0]


def _write(path: Path, schema: str, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TEMP TABLE t ({schema})")
        if rows:
            placeholders = ", ".join("?" for _ in rows[0])
            conn.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


def _run(inputs, workspace) -> dict:
    from qiita_compute_orchestrator.jobs.assembly_load import execute

    return asyncio.run(execute(inputs, workspace))


@pytest.fixture
def staging_inputs(tmp_path):
    """Synthesize assembly_hash's manifest / assembly_chunks / bin_map +
    mint-features' feature_map. Returns the dict spread into Inputs."""
    manifest = tmp_path / "manifest.parquet"
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [(rid, str(_hash(seq)), len(seq)) for rid, (seq, _f) in _SEQUENCES.items()],
    )

    # feature_map: one row per DISTINCT sequence_hash -> feature_idx.
    distinct = {_hash(seq): fidx for _rid, (seq, fidx) in _SEQUENCES.items()}
    feature_map = tmp_path / "feature_map.parquet"
    _write(
        feature_map,
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(h), fidx) for h, fidx in distinct.items()],
    )

    # assembly_chunks: hash-keyed dir of part_*.parquet, one row per distinct hash.
    chunks = tmp_path / "assembly_chunks"
    chunks.mkdir()
    _write(
        chunks / "part_00000.parquet",
        "sequence_hash UUID, chunk_index INTEGER, chunk_data VARCHAR",
        [
            (str(h), 0, seq)
            for seq, h in {seq: _hash(seq) for seq, _ in _SEQUENCES.values()}.items()
        ],
    )

    bin_map = tmp_path / "bin_map.parquet"
    _write(
        bin_map,
        "read_id VARCHAR, kind VARCHAR, bin_id VARCHAR, contig_id VARCHAR",
        # contig_id is the assembler's own id, which assembly_hash carries through
        # and the attribute join keys on. Derived from read_id so each is distinct.
        [(rid, *_bin_kind(rid), f"ctg_{rid}") for rid in _SEQUENCES],
    )

    return {
        "manifest": manifest,
        "feature_map": feature_map,
        "assembly_chunks": chunks,
        "bin_map": bin_map,
    }


def _tsv(path: Path, header: list[str], rows: list[tuple]) -> None:
    """Write a RAW tab-delimited tool table (the shape the containers now emit
    verbatim) — assembly_load parses it with DuckDB read_csv, never Python csv."""
    path.write_text(
        "\t".join(header) + "\n" + "".join("\t".join(str(c) for c in r) + "\n" for r in rows)
    )


def _inputs(tmp_path, staging_inputs, *, checkm_rows=None, das_rows=None, attr_rows=None):
    from qiita_compute_orchestrator.jobs.assembly_load import Inputs

    genomes_dir = tmp_path / "genomes"
    genomes_dir.mkdir(exist_ok=True)
    # attr_rows is a 5-tuple matching the entrypoints' sidecar columns. Left None
    # by default so the existing cases exercise the absent-sidecar path, which is
    # what a run assembled before the sidecar existed hands this job.
    if attr_rows is not None:
        _tsv(
            genomes_dir / CONTIG_ATTRIBUTES_FILE,
            list(CONTIG_ATTRIBUTE_COLUMNS),
            list(attr_rows),
        )
    checkm_dir = tmp_path / "checkm"
    refined_dir = tmp_path / "refined"
    checkm_dir.mkdir(exist_ok=True)
    refined_dir.mkdir(exist_ok=True)
    # checkm_rows is a 7-tuple (bin_id, marker, completeness, contamination,
    # strain, genome_size, n_contigs). It is split across CheckM's two RAW
    # --tab_table outputs, exactly as the checkm.sh container now emits them:
    # lineage.tsv (lineage_wf) carries the quality columns, qa.tsv (qa -o 2) the
    # genome stats. Headers are CheckM 1.x's verbatim spaced/parenthesized names.
    if checkm_rows is not None:
        _tsv(
            checkm_dir / "lineage.tsv",
            ["Bin Id", "Marker lineage", "Completeness", "Contamination", "Strain heterogeneity"],
            [(r[0], r[1], r[2], r[3], r[4]) for r in checkm_rows],
        )
        _tsv(
            checkm_dir / "qa.tsv",
            ["Bin Id", "Genome size (bp)", "# contigs"],
            [(r[0], r[5], r[6]) for r in checkm_rows],
        )
    # das_rows is a 3-tuple (bin, bin_score, bin_set) written as DAS_Tool's RAW
    # summary columns (DAS_Tool 1.1.x names).
    if das_rows is not None:
        _tsv(
            refined_dir / "das_tool_summary.tsv",
            ["bin", "bin_score", "bin_set"],
            list(das_rows),
        )
    return Inputs(
        genomes_dir=genomes_dir,
        checkm_dir=checkm_dir,
        refined_bins_dir=refined_dir,
        processing_idx=77,
        prep_sample_idx=42,
        work_ticket_idx=7,
        **staging_inputs,
    )


def _rows(pq, cols, order):
    with duckdb.connect(":memory:") as con:
        return con.execute(f"SELECT {cols} FROM read_parquet('{pq}') ORDER BY {order}").fetchall()


def _schema(pq):
    with duckdb.connect(":memory:") as con:
        return {c[0]: c[1] for c in con.execute(f"DESCRIBE SELECT * FROM '{pq}'").fetchall()}


def test_reused_writers_emit_feature_keyed_sequences_and_chunks(tmp_path, staging_inputs):
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=[("bin.1", 0.87, "metabat2")],
    )
    out = _run(inputs, tmp_path / "ws")
    staging = out["staging_dir"]

    # assembled_sequence.parquet — one row per DISTINCT feature_idx (reused writer).
    seq = _rows(
        staging / "assembled_sequence.parquet",
        "feature_idx, CAST(sequence_hash AS VARCHAR), sequence_length_bp",
        "feature_idx",
    )
    assert seq == [
        (100, str(_hash("AAAACCCCGGGGTTTT")), 16),
        (200, str(_hash("ACGTACGTACGTACGT")), 16),
        (300, str(_hash("TTTTGGGGCCCCAAAA")), 16),
        (400, str(_hash("GGGGCCCCAAAATTTT")), 16),
    ]

    # assembled_sequence_chunks/ — directory of part files, keyed by feature_idx.
    chunks_dir = staging / "assembled_sequence_chunks"
    assert chunks_dir.is_dir()
    glob = str(chunks_dir / "part_*.parquet")
    assert _schema(chunks_dir / "part_00000.parquet") == {
        "feature_idx": "BIGINT",
        "chunk_index": "INTEGER",
        "chunk_data": "VARCHAR",
    }
    with duckdb.connect(":memory:") as con:
        reassembled = dict(
            con.execute(
                f"SELECT feature_idx, {reassemble_chunks_expr()} "
                "FROM read_parquet(?) GROUP BY feature_idx",
                [glob],
            ).fetchall()
        )
    assert reassembled == {
        100: "AAAACCCCGGGGTTTT",
        200: "ACGTACGTACGTACGT",
        300: "TTTTGGGGCCCCAAAA",
        400: "GGGGCCCCAAAATTTT",
    }


def test_assembly_membership_parquet_lifts_bins_to_feature_idx(tmp_path, staging_inputs):
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=[("bin.1", 0.87, "metabat2")],
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "assembly_membership.parquet"
    assert _schema(pq) == {
        "prep_sample_idx": "BIGINT",
        "processing_idx": "BIGINT",
        "kind": "VARCHAR",
        "bin_id": "VARCHAR",
        "feature_idx": "BIGINT",
        "raw_name": "VARCHAR",
        "circularity": "VARCHAR",
        "depth": "DOUBLE",
        "mult": "DOUBLE",
    }
    # This case passes no sidecar, so every attribute is NULL — and the row count
    # below is unchanged by that. The columns exist whether or not the assemble
    # step recorded them, which is what lets one query read runs from both sides
    # of this change.
    assert _rows(pq, "DISTINCT raw_name, circularity, depth, mult", "1") == [
        (None, None, None, None)
    ]
    rows = _rows(pq, "kind, bin_id, feature_idx", "kind, bin_id, feature_idx")
    # bin.2 shares x1's feature (200) but keeps its own distinct membership row.
    # The unbinned contig lifts the same way, its own contig id as bin_id.
    assert rows == [
        ("LCG", "circ1", 100),
        ("MAG", "bin.1", 200),
        ("MAG", "bin.1", 300),
        ("MAG", "bin.2", 200),
        ("UNBINNED", "ctgU", 400),
    ]
    stamps = _rows(pq, "DISTINCT prep_sample_idx, processing_idx", "1")
    assert stamps == [(42, 77)]


def test_membership_collapses_duplicate_contigs_to_one_coherent_row(tmp_path):
    """Two IDENTICAL contigs in one bin become one row, carrying one contig's
    attributes rather than a mix of both.

    The key `(kind, bin_id, feature_idx)` collapses them, but their attribute
    values differ, so a `SELECT DISTINCT` over the widened column list would emit
    one row per variant -- two rows on a key the Postgres twin upserts on, which
    is SQLSTATE 21000 there and a duplicated row here. The general fixture above
    cannot catch that: no two of its contigs share a key.
    """
    dup = "ACGTACGTACGTACGT"
    seqs = {"MAG:bin.1:1": (dup, 200), "MAG:bin.1:2": (dup, 200)}
    manifest = tmp_path / "m.parquet"
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [(rid, str(_hash(s)), len(s)) for rid, (s, _f) in seqs.items()],
    )
    feature_map = tmp_path / "fm.parquet"
    _write(feature_map, "sequence_hash UUID, feature_idx BIGINT", [(str(_hash(dup)), 200)])
    chunks = tmp_path / "ch"
    chunks.mkdir()
    _write(
        chunks / "part_00000.parquet",
        "sequence_hash UUID, chunk_index INTEGER, chunk_data VARCHAR",
        [(str(_hash(dup)), 0, dup)],
    )
    bin_map = tmp_path / "bm.parquet"
    _write(
        bin_map,
        "read_id VARCHAR, kind VARCHAR, bin_id VARCHAR, contig_id VARCHAR",
        [("MAG:bin.1:1", "MAG", "bin.1", "ctgA"), ("MAG:bin.1:2", "MAG", "bin.1", "ctgB")],
    )
    inputs = _inputs(
        tmp_path,
        {
            "manifest": manifest,
            "feature_map": feature_map,
            "assembly_chunks": chunks,
            "bin_map": bin_map,
        },
        # Deliberately disagreeing, so a row mixing the two is distinguishable
        # from a row taking all four from one contig.
        attr_rows=[
            ("ctgA", "rawA", "yes", 5.0, 1.1),
            ("ctgB", "rawB", "no", 9.0, 2.2),
        ],
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "assembly_membership.parquet"
    rows = _rows(pq, "kind, bin_id, feature_idx, raw_name, circularity, depth, mult", "1")
    assert rows == [("MAG", "bin.1", 200, "rawA", "yes", 5.0, 1.1)], (
        "the two contigs must collapse to ONE row whose four attributes all come "
        "from the same contig"
    )


def test_membership_carries_the_assembler_attributes(tmp_path, staging_inputs):
    """The sidecar's values reach the membership Parquet, joined on contig_id.

    Row count is asserted against the no-sidecar case above: the join must not
    add or drop a membership row, only decorate it. A contig the sidecar does not
    mention keeps its row with NULLs — the state every MAG contig is in when the
    bin FASTA renamed it, and every run assembled before the sidecar existed.
    """
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        attr_rows=[
            ("ctg_LCG:circ1:1", "circ1_raw", "yes", 30.5, 1.02),
            ("ctg_MAG:bin.1:1", "b11_raw", "possibly", 12.0, 1.44),
            # ctg_MAG:bin.1:2, ctg_MAG:bin.2:1 and ctg_UNBINNED:ctgU:1 are absent
            # on purpose: their rows must survive with NULLs.
        ],
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "assembly_membership.parquet"
    rows = _rows(
        pq,
        "kind, bin_id, feature_idx, raw_name, circularity, depth, mult",
        "kind, bin_id, feature_idx",
    )
    assert rows == [
        ("LCG", "circ1", 100, "circ1_raw", "yes", 30.5, 1.02),
        ("MAG", "bin.1", 200, "b11_raw", "possibly", 12.0, 1.44),
        ("MAG", "bin.1", 300, None, None, None, None),
        ("MAG", "bin.2", 200, None, None, None, None),
        ("UNBINNED", "ctgU", 400, None, None, None, None),
    ]


def test_a_hifiasm_shaped_sidecar_lands_empty_cells_as_null_doubles(tmp_path, staging_inputs):
    """The shape the DEFAULT assembler always writes reaches the lake correctly.

    hifiasm_meta emits `mult` empty on EVERY row and `depth` empty for any S-line
    with no `dp:f` tag, so the sidecar's two numeric columns can be entirely or
    partly blank. Left to `auto_detect` an all-empty column reads as VARCHAR and
    the Parquet's type would depend on which assembler ran — which is what the
    declared types in `register_contig_attribute_table` prevent. Empty strings,
    not `None`, because `_tsv` stringifies and would write the literal "None".
    """
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        attr_rows=[
            ("ctg_LCG:circ1:1", "s0.ctg000001c", "yes", 29, ""),
            ("ctg_MAG:bin.1:1", "s1.utg000002l", "no", "", ""),
        ],
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "assembly_membership.parquet"
    assert _schema(pq)["depth"] == "DOUBLE"
    assert _schema(pq)["mult"] == "DOUBLE"
    assert _rows(pq, "bin_id, feature_idx, circularity, depth, mult", "bin_id, feature_idx") == [
        # depth read, mult blank — the tag-carrying hifiasm segment.
        ("bin.1", 200, "no", None, None),
        ("bin.1", 300, None, None, None),
        ("bin.2", 200, None, None, None),
        ("circ1", 100, "yes", 29.0, None),
        ("ctgU", 400, None, None, None),
    ]


def test_bin_quality_joins_checkm_and_das(tmp_path, staging_inputs):
    """CheckM x DAS_Tool -> one row per refined bin, and per refined bin only.

    The fixture's LCG and UNBINNED memberships have no CheckM row, so the
    exact-equality assertion below pins that bin_quality holds MAG rows alone.
    """
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=[("bin.1", 0.87, "metabat2")],
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "bin_quality.parquet"
    assert _schema(pq) == {
        "prep_sample_idx": "BIGINT",
        "processing_idx": "BIGINT",
        "kind": "VARCHAR",
        "bin_id": "VARCHAR",
        "marker_lineage": "VARCHAR",
        "completeness": "DOUBLE",
        "contamination": "DOUBLE",
        "strain_heterogeneity": "DOUBLE",
        "genome_size": "BIGINT",
        "n_contigs": "BIGINT",
        "das_tool_score": "DOUBLE",
        "source_binner": "VARCHAR",
    }
    rows = _rows(
        pq,
        "prep_sample_idx, processing_idx, kind, bin_id, completeness, contamination, "
        "genome_size, n_contigs, das_tool_score, source_binner",
        "bin_id",
    )
    assert rows == [(42, 77, "MAG", "bin.1", 95.5, 1.2, 10000, 2, 0.87, "metabat2")]


def test_bin_quality_without_das_scores_is_null(tmp_path, staging_inputs):
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=None,  # no das_tool_summary.tsv
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "bin_quality.parquet"
    rows = _rows(pq, "bin_id, completeness, das_tool_score, source_binner", "bin_id")
    assert rows == [("bin.1", 95.5, None, None)]


def test_no_checkm_table_writes_empty_bin_quality(tmp_path, staging_inputs):
    """No CheckM table (a sample with no MAG, or a CheckM DB that was absent) ->
    valid empty bin_quality with the right schema; the sequences/membership still
    store."""
    inputs = _inputs(tmp_path, staging_inputs, checkm_rows=None, das_rows=None)
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "bin_quality.parquet"
    # Schema present, zero rows.
    assert _schema(pq)["completeness"] == "DOUBLE"
    with duckdb.connect(":memory:") as con:
        n = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
    assert n == 0
    # Membership still written.
    assert (out["staging_dir"] / "assembly_membership.parquet").exists()


def test_missing_manifest_raises_file_not_found(tmp_path, staging_inputs):
    si = dict(staging_inputs)
    si["manifest"] = tmp_path / "nope.parquet"
    with pytest.raises(FileNotFoundError):
        _run(_inputs(tmp_path, si), tmp_path / "ws")
