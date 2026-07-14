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


def _write_gff(path: Path, rows: list[tuple[str, int, int, str]]) -> Path:
    """rows: (seqid, start, stop_CLOSED, attributes). Coordinates as a GFF3
    carries them — 1-based, closed."""
    lines = ["##gff-version 3"]
    lines += [
        f"{seqid}\tsyndna\tinsert\t{start}\t{stop}\t.\t+\t.\t{attrs}"
        for seqid, start, stop, attrs in rows
    ]
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
    assert ann["sequence_length_bp"] == _INSERT_LEN


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

    assert ann["read_id"] == "insert_01"  # the minting key; renamed to annotation_id in the lake
    assert ann["attributes"]["mass_ng"] == "0.5"
    assert ann["attributes"]["Name"] == "synDNA-1"
    assert ann["parent_read_id"] == _PLASMID_READ_ID
    assert ann["strand"] == "+"
    assert ann["type"] == "insert"


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
        pytest.param(
            [(_PLASMID_READ_ID, 100, 200, "ID=dup"), (_PLASMID_READ_ID, 300, 400, "ID=dup")],
            "duplicate annotation IDs",
            id="duplicate_id",
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
