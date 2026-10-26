"""`qiita assembly export` with the HTTP reads and the Flight streams faked — real
miint, real SQL, real FASTA writer.

The fixture run has one contig in two subjects (``bin.1`` and ``bin.2``), so a join
that keyed on the contig alone would write it twice into one file or drop it from
one; and its chunks are shuffled and cut short of the chunk size, so reassembly order
is exercised. The live data plane path is `tests/integration/test_assembly_export_e2e.py`.
"""

import argparse
import csv
import random

import pyarrow as pa
import pytest
from qiita_common.assembly_constants import BIN_QUALITY_COLUMN_NAMES

from qiita_control_plane.cli.user import assembly as ax
from qiita_control_plane.miint import connect_with_miint

_RUN = 5
_CHUNK = 7

# feature_idx -> sequence. Mixed composition, none a palindrome.
_SEQ = {
    101: "ATGCGTACGTTAGCCGATAGGCTTACGATCGA",  # LCG, 32 bp
    102: "GGGCCCATATATGCGCTTTTAAGC",  # bin.1
    103: "ACGTACCAGT",  # bin.1 and bin.2
    104: "TTTTGgggNNCCAAAAT",  # bin.2; an ambiguity code and soft-masked bases
    105: "GATTACA",  # UNBINNED
}

# (feature_idx, kind, bin_id, raw_name, circularity, depth, mult)
_MEMBERSHIP = [
    (101, "LCG", "u7ctg", "u7ctg_circular-yes", "yes", 30.0, 2.0),
    (102, "MAG", "bin.1", "u1ctg", "no", 10.0, None),
    (103, "MAG", "bin.1", "u2ctg", "no", 20.0, None),
    (103, "MAG", "bin.2", "u2ctg", "no", 20.0, None),
    (104, "MAG", "bin.2", "u4ctg", "possibly", None, None),
    (105, "UNBINNED", "u9ctg", "u9ctg", "no", 1.0, None),
]

# (kind, bin_id, completeness, contamination)
_QUALITY = [("LCG", "u7ctg", 90.0, 0.5), ("MAG", "bin.1", 95.0, 1.0), ("MAG", "bin.2", 40.0, 9.0)]


def _membership_table(rows=_MEMBERSHIP) -> pa.Table:
    names = ["feature_idx", "kind", "bin_id", "raw_name", "circularity", "depth", "mult"]
    return pa.table(
        {n: [r[i] for r in rows] for i, n in enumerate(names)},
        schema=pa.schema(
            [
                ("feature_idx", pa.int64()),
                ("kind", pa.string()),
                ("bin_id", pa.string()),
                ("raw_name", pa.string()),
                ("circularity", pa.string()),
                ("depth", pa.float64()),
                ("mult", pa.float64()),
            ]
        ),
    )


def _lengths(seqs: dict[int, str]) -> pa.Table:
    return pa.table(
        {
            "feature_idx": pa.array(list(seqs), pa.int64()),
            "sequence_hash": pa.array([b"\0" * 16] * len(seqs), pa.binary(16)),
            "sequence_length_bp": pa.array([len(s) for s in seqs.values()], pa.int64()),
        }
    )


def _chunks(seqs: dict[int, str], *, repeat: int | None = None) -> pa.Table:
    rows = [
        (f, i, s[i * _CHUNK : (i + 1) * _CHUNK])
        for f, s in seqs.items()
        for i in range((len(s) + _CHUNK - 1) // _CHUNK)
    ]
    if repeat is not None:
        rows += [r for r in rows if r[0] == repeat]
    random.Random(7).shuffle(rows)
    return pa.table(
        {
            "feature_idx": pa.array([r[0] for r in rows], pa.int64()),
            "chunk_index": pa.array([r[1] for r in rows], pa.int32()),
            "chunk_data": pa.array([r[2] for r in rows], pa.string()),
        }
    )


def _quality(rows=_QUALITY, *, prep_sample_idx: int) -> pa.Table:
    cols = {name: [None] * len(rows) for name in BIN_QUALITY_COLUMN_NAMES}
    cols["prep_sample_idx"] = [prep_sample_idx] * len(rows)
    cols["processing_idx"] = [_RUN] * len(rows)
    for i, (kind, bin_id, completeness, contamination) in enumerate(rows):
        cols["kind"][i], cols["bin_id"][i] = kind, bin_id
        cols["completeness"][i], cols["contamination"][i] = completeness, contamination
        cols["marker_lineage"][i] = "k__Bacteria"
    return pa.table(
        {
            n: pa.array(
                v,
                pa.string()
                if n in ("kind", "bin_id", "marker_lineage", "source_binner")
                else pa.float64()
                if n in ("completeness", "contamination", "strain_heterogeneity", "das_tool_score")
                else pa.int64(),
            )
            for n, v in cols.items()
        }
    )


class _Stream:
    def __init__(self, table: pa.Table):
        self._table = table

    def to_reader(self):
        return self._table.to_reader()


class _FakeFlight:
    """Serves each ticket from a table keyed by the `(prep_sample_idx, table)` the
    fake mint signed it for."""

    def __init__(self, tables: dict[tuple[int, str], pa.Table]):
        self._tables = tables
        self.served: list[tuple[int, str]] = []

    def do_get(self, ticket, *options):
        ps, table = ticket.ticket.decode().split(":")
        self.served.append((int(ps), table))
        return _Stream(self._tables[(int(ps), table)])


def _sample(ps: int, accession: str | None = "SAMEA1", state: str = "completed") -> dict:
    return {"prep_sample_idx": ps, "biosample_accession": accession, "assembly_state": state}


def _tables(ps: int, *, seqs=_SEQ, streamed=None, repeat=None, quality=_QUALITY):
    return {
        (ps, "assembled_sequence"): _lengths(streamed or seqs),
        (ps, "assembled_sequence_chunks"): _chunks(streamed or seqs, repeat=repeat),
        (ps, "bin_quality"): _quality(quality, prep_sample_idx=ps),
    }


@pytest.fixture
def served(monkeypatch):
    """Install the fakes; the test fills `roster`, `membership` and `tables`."""
    state = {"roster": [_sample(11)], "membership": {11: _membership_table()}, "tables": {}}
    state["tables"].update(_tables(11))
    monkeypatch.setattr(ax, "_fetch_roster", lambda *a, **k: state["roster"])
    monkeypatch.setattr(
        ax,
        "_fetch_membership",
        lambda *a, prep_sample_idx, **k: state["membership"][prep_sample_idx],
    )
    monkeypatch.setattr(
        ax,
        "_mint_run_ticket",
        lambda *a, prep_sample_idx, table, **k: f"{prep_sample_idx}:{table}".encode(),
    )
    return state


def _args(tmp_path, **overrides) -> argparse.Namespace:
    fields = {
        "base_url": "http://cp",
        "processing_idx": _RUN,
        "prep_sample_idx": None,
        "sequenced_pool_idx": 1,
        "study_idx": None,
        "kind": ax.DEFAULT_EXPORT_KINDS,
        "min_bp": None,
        "max_bp": None,
        "min_completeness": None,
        "max_contamination": None,
        "output_dir": tmp_path,
    }
    return argparse.Namespace(**(fields | overrides))


def _export(served, tmp_path, **overrides):
    with connect_with_miint() as con:
        return ax.run_export(
            _args(tmp_path, **overrides), "tok", con, _FakeFlight(served["tables"])
        )


def _fasta(path) -> list[tuple[str, str]]:
    with connect_with_miint() as con:
        return con.execute(
            f"SELECT read_id, sequence1 FROM read_fastx('{path}') ORDER BY read_id"
        ).fetchall()


def _tsv(path) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def _gc(seq: str) -> float:
    """G+C over the A/C/G/T bases, case-insensitive; an ambiguity code is in neither.
    The export's own definition, restated because miint has no composition scalar
    to call instead (duckdb-miint#282)."""
    upper = seq.upper()
    return sum(upper.count(b) for b in "GC") / sum(upper.count(b) for b in "ACGT")


def test_each_genome_gets_its_contigs_once_and_whole(served, tmp_path):
    genomes, samples, empty = _export(served, tmp_path)

    assert (genomes, samples, empty) == (3, 1, [])
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "SAMEA1_bin.1.fasta.gz",
        "SAMEA1_bin.2.fasta.gz",
        "SAMEA1_u7ctg.fasta.gz",
        "contigs.tsv",
        "genomes.tsv",
    ]
    # Longest first; 103 is in both bins and appears once in each.
    assert _fasta(tmp_path / "SAMEA1_bin.1.fasta.gz") == [
        ("SAMEA1_bin.1_1", _SEQ[102]),
        ("SAMEA1_bin.1_2", _SEQ[103]),
    ]
    assert _fasta(tmp_path / "SAMEA1_bin.2.fasta.gz") == [
        ("SAMEA1_bin.2_1", _SEQ[104]),
        ("SAMEA1_bin.2_2", _SEQ[103]),
    ]
    assert _fasta(tmp_path / "SAMEA1_u7ctg.fasta.gz") == [("SAMEA1_u7ctg_1", _SEQ[101])]


def test_metadata_describes_each_genome_without_internal_identifiers(served, tmp_path):
    _export(served, tmp_path)

    genomes = {g["genome"]: g for g in _tsv(tmp_path / "genomes.tsv")}
    assert list(genomes) == ["SAMEA1_bin.1", "SAMEA1_bin.2", "SAMEA1_u7ctg"]
    bin1 = genomes["SAMEA1_bin.1"]
    assert (bin1["biosample_accession"], bin1["kind"], bin1["bin_id"]) == ("SAMEA1", "MAG", "bin.1")
    assert int(bin1["length_bp"]) == len(_SEQ[102]) + len(_SEQ[103])
    assert (int(bin1["n_contigs"]), int(bin1["n_circular"])) == (2, 0)
    assert float(bin1["gc"]) == pytest.approx(_gc(_SEQ[102] + _SEQ[103]))
    weighted = (10.0 * len(_SEQ[102]) + 20.0 * len(_SEQ[103])) / (len(_SEQ[102]) + len(_SEQ[103]))
    assert float(bin1["depth"]) == pytest.approx(weighted)
    assert (float(bin1["completeness"]), float(bin1["contamination"])) == (95.0, 1.0)
    assert bin1["fasta"] == "SAMEA1_bin.1.fasta.gz"
    assert int(genomes["SAMEA1_u7ctg"]["n_circular"]) == 1
    assert float(genomes["SAMEA1_bin.2"]["gc"]) == pytest.approx(_gc(_SEQ[104] + _SEQ[103]))
    # 104 reports no depth, so bin.2's depth is 103's alone rather than diluted by it.
    assert float(genomes["SAMEA1_bin.2"]["depth"]) == 20.0

    contigs = _tsv(tmp_path / "contigs.tsv")
    assert [c["contig"] for c in contigs if c["genome"] == "SAMEA1_bin.2"] == [
        "SAMEA1_bin.2_1",
        "SAMEA1_bin.2_2",
    ]
    lcg = next(c for c in contigs if c["genome"] == "SAMEA1_u7ctg")
    assert (lcg["raw_name"], lcg["circularity"], int(lcg["length_bp"])) == (
        "u7ctg_circular-yes",
        "yes",
        len(_SEQ[101]),
    )
    for path in (tmp_path / "genomes.tsv", tmp_path / "contigs.tsv"):
        header = path.read_text().splitlines()[0].split("\t")
        assert not [c for c in header if c.endswith("_idx")], header


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param({"kind": ("UNBINNED",)}, ["SAMEA1_u9ctg"], id="kind"),
        # bin.2 scored 40; UNBINNED is excluded by kind, and would be by being unscored.
        pytest.param({"min_completeness": 50.0}, ["SAMEA1_bin.1", "SAMEA1_u7ctg"], id="complete"),
        pytest.param({"max_contamination": 5.0}, ["SAMEA1_bin.1", "SAMEA1_u7ctg"], id="contam"),
        pytest.param(
            {"kind": ("UNBINNED", "MAG"), "min_completeness": 0.0},
            ["SAMEA1_bin.1", "SAMEA1_bin.2"],
            id="unscored-excluded",
        ),
        pytest.param({"max_bp": 30}, ["SAMEA1_bin.2"], id="max-bp"),
        pytest.param({"min_bp": 31}, ["SAMEA1_bin.1", "SAMEA1_u7ctg"], id="min-bp"),
    ],
)
def test_filters(served, tmp_path, overrides, expected):
    _export(served, tmp_path, **overrides)
    assert [g["genome"] for g in _tsv(tmp_path / "genomes.tsv")] == expected
    assert sorted(p.name for p in tmp_path.glob("*.fasta.gz")) == [
        f"{g}.fasta.gz" for g in expected
    ]


def test_a_sample_with_nothing_selected_streams_no_chunks(served, tmp_path):
    flight = _FakeFlight(served["tables"])
    with connect_with_miint() as con:
        ax.run_export(_args(tmp_path, min_bp=10**6), "tok", con, flight)
    assert (11, "assembled_sequence_chunks") not in flight.served
    assert _tsv(tmp_path / "genomes.tsv") == []


def _assert_nothing_written(tmp_path):
    assert list(tmp_path.iterdir()) == []


def test_a_doubled_chunk_is_refused_and_nothing_is_written(served, tmp_path):
    """A chunk registered twice reassembles to a record longer than the registered
    length under one header; the export refuses rather than writing it."""
    served["tables"].update(_tables(11, repeat=103))
    with pytest.raises(ax.ExportRefused, match="length other than the registered"):
        _export(served, tmp_path)
    _assert_nothing_written(tmp_path)


def test_membership_and_stream_disagreeing_is_refused(served, tmp_path):
    """A contig the membership lists and the data plane does not serve — what a
    superseded Postgres row looks like."""
    streamed = {f: s for f, s in _SEQ.items() if f != 104}
    served["tables"].update(_tables(11, streamed=streamed))
    with pytest.raises(ax.ExportRefused, match="lists 1 contig"):
        _export(served, tmp_path)
    _assert_nothing_written(tmp_path)


def test_a_repeated_quality_row_is_refused(served, tmp_path):
    served["tables"].update(_tables(11, quality=[*_QUALITY, ("MAG", "bin.1", 10.0, 50.0)]))
    with pytest.raises(ax.ExportRefused, match="more than one"):
        _export(served, tmp_path)


@pytest.mark.parametrize(
    ("roster", "match"),
    [
        pytest.param([_sample(11), _sample(12, state="pending")], "not completed", id="pending"),
        pytest.param([_sample(11, state="invalidated")], "not completed", id="invalidated"),
        # A state the export does not know is refused, not silently left out.
        pytest.param([_sample(11, state="archived")], "not completed", id="unknown-state"),
        pytest.param([_sample(11, accession=None)], "no biosample accession", id="unnamed"),
        pytest.param([_sample(11, accession="SAM/1")], "file name", id="unsafe-name"),
        pytest.param([], "no prep_sample", id="empty"),
    ],
)
def test_roster_refusals(served, tmp_path, roster, match):
    served["roster"] = roster
    with pytest.raises(ax.ExportRefused, match=match):
        _export(served, tmp_path)
    _assert_nothing_written(tmp_path)


def test_a_sample_that_assembled_nothing_is_reported_not_exported(served, tmp_path):
    served["roster"] = [_sample(11), _sample(12, accession="SAMEA2", state="no_data")]
    genomes, samples, empty = _export(served, tmp_path)
    assert (genomes, samples) == (3, 1)
    assert [s["prep_sample_idx"] for s in empty] == [12]


def test_two_genomes_with_one_name_are_refused_and_nothing_is_written(served, tmp_path):
    """Two prep_samples of one biosample in one export name their subjects alike;
    the first genome written, the LCG, is the first to collide."""
    served["roster"] = [_sample(11), _sample(12)]
    served["membership"][12] = _membership_table()
    served["tables"].update(_tables(12))
    with pytest.raises(ax.ExportRefused, match="both named 'SAMEA1_u7ctg'"):
        _export(served, tmp_path)
    _assert_nothing_written(tmp_path)


def test_an_existing_file_is_not_overwritten(served, tmp_path):
    (tmp_path / "SAMEA1_bin.2.fasta.gz").write_text("keep")
    with pytest.raises(FileExistsError):
        _export(served, tmp_path)
    assert [p.name for p in tmp_path.iterdir()] == ["SAMEA1_bin.2.fasta.gz"]
    assert (tmp_path / "SAMEA1_bin.2.fasta.gz").read_text() == "keep"


def test_the_command_exits_1_on_a_refusal_and_names_it(served, tmp_path, monkeypatch, capsys):
    import pyarrow.flight

    from qiita_control_plane.cli.user import main

    served["roster"] = [_sample(11, state="pending")]
    monkeypatch.setattr(ax._common, "read_token", lambda *a, **k: "tok")
    monkeypatch.setattr(
        pyarrow.flight, "FlightClient", lambda url: _Closing(_FakeFlight(served["tables"]))
    )
    argv = ["--base-url", "https://cp", "assembly", "export", "--processing-idx", str(_RUN)]
    argv += ["--study-idx", "3", "--output-dir", str(tmp_path), "--data-plane-url", "grpc://x"]
    assert main(argv) == 1
    assert "not completed" in capsys.readouterr().err

    served["roster"] = [_sample(11)]
    assert main(argv) == 0
    assert "wrote 3 genome(s) from 1 prep_sample(s)" in capsys.readouterr().out


class _Closing:
    def __init__(self, inner):
        self._inner = inner

    def __enter__(self):
        return self._inner

    def __exit__(self, *exc):
        return False
