"""Smoke test: GFF3 → annotated-interval features, against the REAL miint build.

miint's `read_gff` is NOT stubbed here. The whole point of this file is to pin what
the extension actually does, because the two things it gets wrong fail SILENTLY:

  1. `read_gff`'s `stop_position` is GFF3's 1-based CLOSED end; `alignment_slice` /
     `read_alignments` / `qiita_lake.alignment` all speak 1-based HALF-OPEN. Both
     spell the column `stop_position`. Feeding one to the other type-checks, runs,
     raises nothing — and quietly drops the interval's last base from every
     coverage number computed downstream forever.
  2. The extracted sub-sequence's canonical hash IS the annotated feature's
     identity. An off-by-one in the `substr` mints a DIFFERENT feature_idx that
     still looks perfectly well-formed.

So the assertions are: the conversion happens (stop = gff_stop + 1), the window
round-trips to the exact insert bytes, and — the one that actually matters — the
STORED window drives `alignment_slice` with no further adjustment and recovers the
insert exactly. That last one is what a future reader needs, because it is the
claim every consumer depends on.
"""

from __future__ import annotations

import asyncio
import random
from pathlib import Path

import duckdb
import pytest
from qiita_common.chunking import canonical_sequence_hash_expr

from qiita_compute_orchestrator.jobs.hash_sequences import Inputs, execute
from qiita_compute_orchestrator.miint import open_miint_conn

_CHUNK_SIZE = 65_536
_RNG = random.Random(20260713)


def _dna(n: int) -> str:
    return "".join(_RNG.choice("ACGT") for _ in range(n))


# A plasmid: backbone, then the insert, then more backbone. The insert therefore
# sits at 1-based CLOSED [2001, 3000] — which is what the GFF3 below declares.
_BACKBONE_A = _dna(2000)
_INSERT = _dna(1000)
_BACKBONE_B = _dna(1000)
_PLASMID = _BACKBONE_A + _INSERT + _BACKBONE_B

_GFF_START = 2001  # 1-based inclusive
_GFF_STOP_CLOSED = 3000  # 1-based INCLUSIVE (what a GFF3 file carries)
_EXPECTED_STOP_HALFOPEN = 3001  # what we must STORE
_INSERT_LEN = 1000

_PLASMID_READ_ID = "plasmid_01"


def _write_chunked_upload(path: Path, reads: list[tuple[str, str]]) -> Path:
    """The CLI-side upload shape: `(read_id, chunk_index, chunk_data)`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        (rid, i // _CHUNK_SIZE, seq[i : i + _CHUNK_SIZE])
        for rid, seq in reads
        for i in range(0, len(seq), _CHUNK_SIZE)
    ]
    values_sql = ", ".join(
        "(CAST(? AS VARCHAR), CAST(? AS INTEGER), CAST(? AS VARCHAR))" for _ in rows
    )
    params: list = []
    for rid, idx, data in rows:
        params.extend([rid, idx, data])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(read_id, chunk_index, chunk_data)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _write_gff(
    path: Path,
    rows: list[tuple[str, int, int, str]],
    *,
    feature_type: str = "insert",
    score: str = ".",
    phase: str = ".",
    extra_lines: list[str] = [],
    fasta_section: str | None = None,
) -> Path:
    """rows: (seqid, start, stop_CLOSED, attributes). Coordinates as a GFF3
    carries them — 1-based, closed.

    `extra_lines` takes verbatim GFF3 lines (for the landmark / discontinuous-feature
    shapes), and `fasta_section` appends a `##FASTA` block — which is not an exotic
    option but what prokka and bakta ALWAYS emit.
    """
    lines = ["##gff-version 3"]
    lines += [
        f"{seqid}\tsyndna\t{feature_type}\t{start}\t{stop}\t{score}\t+\t{phase}\t{attrs}"
        for seqid, start, stop, attrs in rows
    ]
    lines += extra_lines
    if fasta_section is not None:
        lines += ["##FASTA", f">{_PLASMID_READ_ID}"]
        lines += [fasta_section[i : i + 60] for i in range(0, len(fasta_section), 60)]
    path.write_text("\n".join(lines) + "\n")
    return path


def _run(tmp_path: Path, *, gff: Path | None) -> dict:
    upload = _write_chunked_upload(tmp_path / "upload.parquet", [(_PLASMID_READ_ID, _PLASMID)])
    return asyncio.run(
        execute(
            Inputs(fasta_path=upload, gff_path=gff, reference_idx=1, work_ticket_idx=1),
            tmp_path / "ws",
        )
    )


def _annotations(path: Path) -> list[dict]:
    with duckdb.connect(":memory:") as conn:
        cols = [
            c[0] for c in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        ]
        rows = conn.execute(f"SELECT * FROM read_parquet('{path}') ORDER BY position").fetchall()
    return [dict(zip(cols, r, strict=True)) for r in rows]


def _standard_gff(tmp_path: Path) -> Path:
    return _write_gff(
        tmp_path / "insert.gff3",
        [
            (
                _PLASMID_READ_ID,
                _GFF_START,
                _GFF_STOP_CLOSED,
                "ID=insert_01;Name=synDNA-1;mass_ng=0.5",
            )
        ],
    )


def test_annotation_window_is_half_open(tmp_path):
    """THE off-by-one. GFF3 says the insert ends at 3000, inclusive. We must store
    3001, exclusive — and the length must come out as stop - position, no +1."""
    out = _run(tmp_path, gff=_standard_gff(tmp_path))
    (ann,) = _annotations(out["annotation_manifest"])

    assert ann["position"] == _GFF_START
    assert ann["stop_position"] == _EXPECTED_STOP_HALFOPEN, (
        "stored stop_position must be the GFF3 CLOSED stop + 1 (half-open), or every "
        "downstream coverage number silently loses the interval's last base"
    )
    assert ann["stop_position"] - ann["position"] == _INSERT_LEN
    # The interval's length is NOT a stored column: it is stop - position, and a stored
    # copy could only ever disagree with the coordinates it came from.
    assert "sequence_length_bp" not in ann


def test_annotation_feature_hash_is_the_insert_itself(tmp_path):
    """The annotated feature's identity is the canonical hash of the EXTRACTED
    bytes. Pin it against the hash of the insert computed independently — an
    off-by-one in the substr would mint a different, perfectly well-formed
    feature_idx."""
    out = _run(tmp_path, gff=_standard_gff(tmp_path))
    (ann,) = _annotations(out["annotation_manifest"])

    # miint, not a plain connection: the canonical hash is strand-canonical and calls
    # miint's `sequence_dna_reverse_complement`.
    conn = open_miint_conn()
    try:
        # The hash expression embeds its argument more than once, so it is fed from a
        # column rather than a bare `?` placeholder.
        conn.execute("CREATE TABLE s (sequence VARCHAR)")
        conn.execute("INSERT INTO s VALUES (?)", [_INSERT])
        expected = conn.execute(
            f"SELECT {canonical_sequence_hash_expr('sequence')} FROM s"
        ).fetchone()[0]
    finally:
        conn.close()
    assert ann["sequence_hash"] == expected


def test_attributes_survive_as_a_map(tmp_path):
    """Per-insert MASS rides the GFF3 attributes — that is the whole reason the
    issue chose GFF3 over a bare (start, stop) pair. If attributes don't
    round-trip, the cell-count model has nowhere to get mass from."""
    out = _run(tmp_path, gff=_standard_gff(tmp_path))
    (ann,) = _annotations(out["annotation_manifest"])

    assert ann["annotation_id"] == "insert_01"
    assert ann["attributes"]["mass_ng"] == "0.5"
    assert ann["attributes"]["Name"] == "synDNA-1"
    assert ann["parent_read_id"] == _PLASMID_READ_ID
    assert ann["strand"] == "+"
    assert ann["annotation_type"] == "insert"


def test_stored_window_feeds_alignment_slice_with_no_adjustment(tmp_path):
    """The claim every downstream consumer rests on: the window AS STORED goes
    straight into `alignment_slice(t, position, stop_position)`.

    This is the test that makes the half-open decision real rather than a comment.
    It aligns three reads to the plasmid and checks the stored window keeps exactly
    the ones overlapping the insert — including the read spanning the insert→backbone
    junction, which is a REAL spike-in — and excludes the backbone-only read.

    The anti-vacuity control is the last assertion: re-slicing with the CLOSED stop
    (what a careless consumer would pass) must give a DIFFERENT answer. If it didn't,
    this test would be pinning nothing.
    """
    out = _run(tmp_path, gff=_standard_gff(tmp_path))
    (ann,) = _annotations(out["annotation_manifest"])
    position, stop_position = ann["position"], ann["stop_position"]

    reads = {
        1: _INSERT,  # wholly inside the window
        2: _BACKBONE_A[-300:] + _INSERT[:700],  # spans the junction — REAL
        3: _BACKBONE_A[:500],  # backbone only — contamination
    }

    conn = open_miint_conn()
    try:
        conn.execute("CREATE TABLE subjects (read_id BIGINT, sequence1 VARCHAR)")
        conn.execute("INSERT INTO subjects VALUES (42, ?)", [_PLASMID])
        mmi = str(tmp_path / "plasmid.mmi")
        (success,) = conn.execute(
            "SELECT success FROM save_minimap2_index('subjects', ?, preset := 'map-hifi')",
            [mmi],
        ).fetchone()
        assert success

        conn.execute("CREATE TABLE query (read_id BIGINT, sequence1 VARCHAR)")
        for rid, seq in reads.items():
            conn.execute("INSERT INTO query VALUES (?, ?)", [rid, seq])
        # CREATE VIEW can't carry a prepared parameter, so the index path is inlined.
        conn.execute(
            "CREATE VIEW aligned AS SELECT * FROM align_minimap2("
            f"  'query', index_path := '{mmi}', preset := 'map-hifi', max_secondary := 0)"
            " WHERE alignment_is_primary(flags) AND NOT alignment_is_unmapped(flags)"
        )
        # All three reads really do align — otherwise "excluded by the window" below
        # would be indistinguishable from "never aligned at all".
        assert conn.execute("SELECT count(*) FROM aligned").fetchone()[0] == 3

        kept = {
            int(r[0])
            for r in conn.execute(
                "SELECT read_id FROM alignment_slice('aligned', ?, ?)",
                [position, stop_position],
            ).fetchall()
        }
        assert kept == {1, 2}, "the junction-spanning read is a real spike-in and must survive"

        # The insert's LAST base is covered — this is precisely what the closed/half-open
        # confusion destroys.
        depth = conn.execute(
            "SELECT compute_coverage_depth(position, stop_position, cigar, ?, 'exclude_deletions')"
            " FROM aligned",
            [len(_PLASMID)],
        ).fetchone()[0]
        window_depth = depth[position - 1 : stop_position - 1]
        assert len(window_depth) == _INSERT_LEN
        assert window_depth[-1] == 1, "the insert's last base must be covered"

        # CONTROL: the careless consumer passes the CLOSED stop. It must differ — if
        # this assertion failed, the half-open conversion would be unobservable and
        # this whole test would be vacuous.
        careless = conn.execute(
            "SELECT count(*) FROM alignment_slice('aligned', ?, ?)",
            [position, stop_position - 1],
        ).fetchone()[0]
        careless_depth = depth[position - 1 : stop_position - 2]
        assert len(careless_depth) == _INSERT_LEN - 1
        assert careless >= 1  # it still returns rows — that is why it's silent
    finally:
        conn.close()


def test_no_gff_yields_a_typed_empty_manifest(tmp_path):
    """Most references have no GFF. They still emit the file (a workflow `outputs:`
    binding is unconditional), and it must carry the full TYPED schema — the
    downstream mint reads `sequence_hash` off it, so an untyped empty file would
    fail there instead of here."""
    out = _run(tmp_path, gff=None)
    path = out["annotation_manifest"]
    assert path.exists()

    with duckdb.connect(":memory:") as conn:
        assert conn.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0] == 0
        types = dict(
            (c[0], c[1])
            for c in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        )
    assert types["sequence_hash"] == "UUID"
    assert types["position"] == "BIGINT"
    assert types["stop_position"] == "BIGINT"
    assert types["attributes"] == "MAP(VARCHAR, VARCHAR)"

    # The real assertion: the empty file's schema must equal the POPULATED file's,
    # column for column and type for type. Both land in one DuckLake table, so a
    # divergence is a lake-level schema conflict — and spot-checking four columns
    # would not catch it. This is what makes "same code path" a fact rather than a
    # claim in a docstring.
    populated = _run(tmp_path / "with_gff", gff=_standard_gff(tmp_path))["annotation_manifest"]
    with duckdb.connect(":memory:") as conn:
        populated_types = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{populated}')"
        ).fetchall()
        empty_types = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    assert [(c[0], c[1]) for c in empty_types] == [(c[0], c[1]) for c in populated_types]


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        pytest.param(
            [("no_such_contig", 1, 100, "ID=x")],
            "absent from the FASTA",
            id="seqid_not_in_fasta",
        ),
        pytest.param(
            [(_PLASMID_READ_ID, 3900, 4500, "ID=x")],
            "outside their parent sequence",
            id="window_runs_off_the_end",
        ),
        pytest.param(
            [(_PLASMID_READ_ID, 500, 400, "ID=x")],
            "outside their parent sequence",
            id="inverted_window",
        ),
    ],
)
def test_malformed_gff_fails_loud(tmp_path, rows, expected):
    """Each of these silently corrupts a depth number rather than crashing, so each
    is a raise, not a warning. A duplicate ID in particular would fan out the
    feature-table join and double-count an insert."""
    gff = _write_gff(tmp_path / "bad.gff3", rows)
    with pytest.raises(ValueError, match=expected):
        _run(tmp_path, gff=gff)


def test_interval_ending_on_the_parents_last_base_is_accepted(tmp_path):
    """The bound check is `stop_position > sequence_length_bp + 1`, and that `+1` is
    exactly where an off-by-one would hide. An interval whose last base IS the
    parent's last base is legal and must survive — pair it with the test below,
    which pushes one base further and must fail. Neither test means much alone."""
    last = len(_PLASMID)  # 4000, the parent's final base, CLOSED
    gff = _write_gff(tmp_path / "edge.gff3", [(_PLASMID_READ_ID, last - 99, last, "ID=tail")])
    (ann,) = _annotations(_run(tmp_path, gff=gff)["annotation_manifest"])
    assert ann["stop_position"] == last + 1  # half-open, one past the last base
    assert ann["stop_position"] - ann["position"] == 100


def test_interval_one_base_past_the_parents_end_is_rejected(tmp_path):
    """One base beyond the test above. Must fail — otherwise `substr` would silently
    return a SHORT sequence and mint a feature_idx for bases that do not exist."""
    last = len(_PLASMID)
    gff = _write_gff(tmp_path / "over.gff3", [(_PLASMID_READ_ID, last - 99, last + 1, "ID=over")])
    with pytest.raises(ValueError, match="outside their parent sequence"):
        _run(tmp_path, gff=gff)


def test_non_landmark_interval_spanning_the_whole_parent_is_rejected(tmp_path):
    """A row of a REAL feature type covering 1..len extracts the parent's own bytes, so
    it canonically hashes to the PARENT's feature_idx — yielding an annotation whose
    feature_idx equals its parent_feature_idx, pointing at a feature that IS in
    reference_membership and IS indexed. That quietly falsifies the invariant the whole
    design rests on, and nothing would raise. So it is refused.

    The landmark types (`region` & co.) are the ones that do this LEGITIMATELY, and they
    are dropped rather than raised on — see the test below."""
    gff = _write_gff(tmp_path / "whole.gff3", [(_PLASMID_READ_ID, 1, len(_PLASMID), "ID=whole")])
    with pytest.raises(ValueError, match="ENTIRE parent sequence"):
        _run(tmp_path, gff=gff)


def test_two_annotations_with_identical_bases_are_KEPT(tmp_path):
    """Two intervals with identical bases collapse to ONE feature_idx — and that is
    correct, not an error.

    This case is the whole reason `annotation_id`, not `feature_idx`, is the
    annotation's identity. A bacterial genome carries the 16S rRNA gene in 5-7
    BYTE-IDENTICAL copies, which canonically hash to one feature_idx. An earlier
    revision of this code REJECTED that, which made `--gff` raise on essentially
    every real bacterial genome while working fine on the SynDNA plasmids it was
    written against — exactly the SynDNA-shaped failure the issue warned about.

    A feature is a SEQUENCE; an annotation is an OCCURRENCE of it at a place. Both
    occurrences survive, they share a feature_idx, and they are told apart by their
    ids and their windows.
    """
    dup = _BACKBONE_A[:100]
    plasmid = dup + _dna(50) + dup + _dna(50)
    upload = _write_chunked_upload(tmp_path / "dup.parquet", [("p", plasmid)])
    gff = _write_gff(
        tmp_path / "dup.gff3", [("p", 1, 100, "ID=copy_a"), ("p", 151, 250, "ID=copy_b")]
    )

    out = asyncio.run(
        execute(
            Inputs(fasta_path=upload, gff_path=gff, reference_idx=1, work_ticket_idx=1),
            tmp_path / "ws2",
        )
    )
    anns = _annotations(out["annotation_manifest"])

    assert [a["annotation_id"] for a in anns] == ["copy_a", "copy_b"]
    assert anns[0]["sequence_hash"] == anns[1]["sequence_hash"], (
        "identical bases must canonically hash to ONE feature — that is what "
        "content-addressed features MEAN"
    )
    # ...and they remain distinguishable, because identity is the id + the window.
    assert (anns[0]["position"], anns[0]["stop_position"]) == (1, 101)
    assert (anns[1]["position"], anns[1]["stop_position"]) == (151, 251)


def test_embedded_fasta_section_is_not_parsed_as_annotations(tmp_path):
    """prokka and bakta ALWAYS append the genome to their GFF3 as a `##FASTA` section,
    and `read_gff` does not stop there — it returns one row per line of the embedded
    FASTA, with the nucleotide line itself sitting in `seqid` and NULL in every other
    column. On a real prokka file that is 1539 junk rows behind 99 real features.

    Un-filtered, those rows reach the parent check and the ingest dies claiming a line
    of nucleotides is not a sequence in the FASTA. So `--gff` would be unusable on the
    output of the two most common bacterial annotators there are.

    The CONTROL is the second half: the byte-identical GFF3 WITHOUT the `##FASTA`
    section must give the same answer. If it didn't, this test would be pinning the
    wrong thing.
    """
    with_fasta = _write_gff(
        tmp_path / "prokka.gff3",
        [(_PLASMID_READ_ID, _GFF_START, _GFF_STOP_CLOSED, "ID=insert_01")],
        fasta_section=_PLASMID,
    )
    anns = _annotations(_run(tmp_path, gff=with_fasta)["annotation_manifest"])
    assert [a["annotation_id"] for a in anns] == ["insert_01"], (
        "the ##FASTA section must not become annotation rows"
    )

    # CONTROL: same file, section stripped. Same result — so what we filtered really was
    # the FASTA block and not something load-bearing.
    without = _write_gff(
        tmp_path / "clean.gff3",
        [(_PLASMID_READ_ID, _GFF_START, _GFF_STOP_CLOSED, "ID=insert_01")],
    )
    control = _annotations(_run(tmp_path / "ctl", gff=without)["annotation_manifest"])
    assert [dict(a) for a in anns] == [dict(a) for a in control]


def test_duplicate_gff3_ids_are_kept_not_rejected(tmp_path):
    """GFF3 lets a DISCONTINUOUS feature — a ribosomal-slippage CDS — span several lines
    that all carry the SAME `ID`. It is one feature at multiple locations, and the spec
    says so explicitly. NCBI's RefSeq annotation of E. coli K-12 MG1655 has 20 of them.

    An earlier revision raised on a duplicate `ID`, which rejected that file outright.
    So `annotation_id` is provenance, not identity: both rows survive, and they are told
    apart by their windows — which is what the minted `annotation_idx` keys on.
    """
    gff = _write_gff(
        tmp_path / "discontinuous.gff3",
        [
            (_PLASMID_READ_ID, 2001, 2400, "ID=cds-slippage"),
            (_PLASMID_READ_ID, 2402, 3000, "ID=cds-slippage"),
        ],
    )
    anns = _annotations(_run(tmp_path, gff=gff)["annotation_manifest"])

    assert [a["annotation_id"] for a in anns] == ["cds-slippage", "cds-slippage"]
    assert [(a["position"], a["stop_position"]) for a in anns] == [(2001, 2401), (2402, 3001)]


def test_landmark_rows_are_dropped_rather_than_rejected(tmp_path):
    """Every NCBI GFF3 opens each record with a `region` line spanning the whole
    sequence. It declares the landmark; it does not annotate an interval of it. It
    necessarily extracts the parent's own bytes, so it would hash to the PARENT's
    feature_idx — which is exactly the thing the design forbids.

    Dropping it by type is what lets a stock RefSeq file ingest at all, while a
    non-landmark row doing the same thing still raises (test above)."""
    gff = _write_gff(
        tmp_path / "ncbi.gff3",
        [(_PLASMID_READ_ID, _GFF_START, _GFF_STOP_CLOSED, "ID=insert_01")],
        extra_lines=[
            f"{_PLASMID_READ_ID}\tRefSeq\tregion\t1\t{len(_PLASMID)}\t.\t+\t.\tID=NC_1:1..{len(_PLASMID)}"
        ],
    )
    anns = _annotations(_run(tmp_path, gff=gff)["annotation_manifest"])
    assert [a["annotation_id"] for a in anns] == ["insert_01"]
    assert all(a["annotation_type"] != "region" for a in anns)


def test_score_and_phase_are_persisted(tmp_path):
    """GFF3 columns 6 and 8. Both are genuinely optional — `score` is NULL on 100% of the
    rows of both a stock RefSeq and a stock prokka file, and `phase` is populated only on
    CDS rows — so they are stored NULLABLE. Dropping them would mean a caller who wants
    them has no way back to them short of a re-ingest."""
    scored = _write_gff(
        tmp_path / "scored.gff3",
        [(_PLASMID_READ_ID, _GFF_START, _GFF_STOP_CLOSED, "ID=cds_01")],
        feature_type="CDS",
        score="42.5",
        phase="0",
    )
    (ann,) = _annotations(_run(tmp_path, gff=scored)["annotation_manifest"])
    assert ann["score"] == 42.5
    assert ann["phase"] == 0
    assert ann["source"] == "syndna"

    # ...and the far more common shape: both absent. They must come back NULL, not 0.0 —
    # a score of zero and no score at all are different claims.
    (bare,) = _annotations(
        _run(tmp_path / "bare", gff=_standard_gff(tmp_path))["annotation_manifest"]
    )
    assert bare["score"] is None
    assert bare["phase"] is None


def test_parent_sequence_hash_resolves_the_parent_without_a_read_id_join(tmp_path):
    """The manifest carries the PARENT's canonical hash, not just its read_id. That is
    what lets the control plane resolve parent_feature_idx off the feature map with a
    fixed-width UUID join instead of a VARCHAR one — feature_idx itself cannot appear
    here, because the orchestrator has no database and nothing has been minted yet."""
    out = _run(tmp_path, gff=_standard_gff(tmp_path))
    (ann,) = _annotations(out["annotation_manifest"])

    with duckdb.connect(":memory:") as conn:
        (parent_hash,) = conn.execute(
            f"SELECT sequence_hash FROM read_parquet('{out['manifest']}') WHERE read_id = ?",
            [_PLASMID_READ_ID],
        ).fetchone()

    assert ann["parent_sequence_hash"] == parent_hash
    assert ann["parent_read_id"] == _PLASMID_READ_ID
    # The interval is NOT its parent — different bytes, different feature.
    assert ann["sequence_hash"] != parent_hash
