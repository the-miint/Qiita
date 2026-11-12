"""Unit tests for `gene_call.execute`: which subjects it calls, the two call sets, the
canonical-strand coordinates, the CheckM files, and the column contract with the lake.

Runs `predict_genes` against the team-mirror miint build and the GPL-boundary the
conftest installs. The fixture sequences are random DNA from a fixed seed: Prodigal
calls genes on them in both modes, which is all these tests need.
"""

from __future__ import annotations

import asyncio
import random
import re
from pathlib import Path

import duckdb
import pytest
from qiita_common.chunking import canonical_sequence_hash_expr, normalized_sequence_expr
from qiita_common.gene_call_constants import (
    CALL_SET_CHECKM,
    CALL_SET_META_CLOSED,
    GENE_CALL_ATTRIBUTE_KEYS,
    GENE_CALL_COLUMNS,
    GENE_CALL_CONTIG_COLUMNS,
    GENE_CALL_RUN_KEY,
)

from qiita_compute_orchestrator.jobs import gene_call
from qiita_compute_orchestrator.miint import open_miint_conn

_REPO = Path(__file__).resolve().parents[3]


def _dna(rng: random.Random, n: int) -> str:
    return "".join(rng.choice("ACGT") for _ in range(n))


def _fasta(path: Path, records: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f">{name}\n{seq}\n" for name, seq in records.items()))


def _layout(root: Path) -> dict[str, dict[str, str]]:
    """A MAG over 100 kb (single mode), a MAG under it (metagenomic mode), one circular
    contig, and noLCG holding the first MAG's contigs, a contig above the residue cut
    and one below it. Returns each FASTA's records."""
    rng = random.Random(7)
    bin1 = {"c1": _dna(rng, 60_000), "c2": _dna(rng, 50_000), "c3": _dna(rng, 40_000)}
    files = {
        "refined/bin.1.fa": bin1,
        # Lowercase, as a soft-masked contig arrives: the canonical strand and the hash
        # are taken over the upper-cased bases.
        "refined/bin.2.fa": {"s1": _dna(rng, 30_000).lower()},
        "genomes/circular.fa": {"lcg1": _dna(rng, 120_000)},
        "genomes/noLCG.fa": {
            **bin1,
            "long1": _dna(rng, gene_call.MIN_RESIDUE_LENGTH_BP + 10_000),
            "short1": _dna(rng, 20_000),
        },
    }
    for rel, records in files.items():
        _fasta(root / rel, records)
    return files


def _run(root: Path) -> Path:
    inputs = gene_call.Inputs(
        genomes_dir=root / "genomes",
        refined_bins_dir=root / "refined",
        prep_sample_idx=1,
        work_ticket_idx=1,
    )
    out = asyncio.run(gene_call.execute(inputs, root / "ws"))
    return out["gene_calls"]


@pytest.fixture(scope="module")
def called(tmp_path_factory) -> tuple[Path, dict[str, dict[str, str]]]:
    root = tmp_path_factory.mktemp("gene_call")
    files = _layout(root)
    return _run(root), files


def _rows(path: Path, sql: str = "SELECT * FROM t") -> list[tuple]:
    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE t AS FROM '{path}'")
    return conn.execute(sql).fetchall()


def test_subjects_are_the_ones_checkm_scores(called):
    """Each refined bin, each circular contig, and the unbinned residue at or above the
    cut: a binned noLCG contig and a short one are not subjects."""
    out, _ = called
    subjects = _rows(
        out / gene_call.CONTIG_FILE, "SELECT DISTINCT kind, bin_id FROM t ORDER BY ALL"
    )
    assert subjects == [
        ("LCG", "lcg1"),
        ("MAG", "bin.1"),
        ("MAG", "bin.2"),
        ("UNBINNED", "long1"),
    ]


def test_every_contig_of_a_subject_has_a_row_per_call_set(called):
    out, files = called
    counts = dict(
        _rows(
            out / gene_call.CONTIG_FILE,
            "SELECT call_set || ':' || bin_id, count(*) FROM t GROUP BY ALL",
        )
    )
    for call_set in (CALL_SET_CHECKM, CALL_SET_META_CLOSED):
        assert counts[f"{call_set}:bin.1"] == len(files["refined/bin.1.fa"])
        assert counts[f"{call_set}:lcg1"] == 1


def test_checkm_mode_follows_the_subject_length(called):
    """Single mode at 100 kb or more, metagenomic below; the other call set is
    metagenomic throughout."""
    out, _ = called
    modes = dict(
        _rows(
            out / gene_call.CONTIG_FILE,
            f"SELECT bin_id, bool_and(meta_mode) FROM t WHERE call_set = '{CALL_SET_CHECKM}' "
            "GROUP BY ALL",
        )
    )
    assert modes == {"bin.1": False, "bin.2": True, "lcg1": False, "long1": False}
    assert _rows(
        out / gene_call.CONTIG_FILE,
        f"SELECT bool_and(meta_mode AND closed_ends AND NOT mask_regions) FROM t "
        f"WHERE call_set = '{CALL_SET_META_CLOSED}'",
    ) == [(True,)]


def test_closed_ends_leaves_no_partial_gene(called):
    out, _ = called
    partial = dict(
        _rows(
            out / gene_call.GENE_FILE,
            "SELECT call_set, count(*) FILTER (WHERE partial_left OR partial_right) FROM t "
            "GROUP BY ALL",
        )
    )
    assert partial[CALL_SET_META_CLOSED] == 0
    # The control: without closed ends, genes do run off these contigs' edges.
    assert partial[CALL_SET_CHECKM] > 0


def test_coordinates_cut_the_gene_from_the_canonical_strand(called):
    """The bases at each gene's stored window, on the contig's canonical strand, are the
    gene's own: they hash to its `sequence_hash`, and a complete gene starts with a
    start codon on its own strand. The layout's contigs are random, so about half are
    read on the non-canonical strand, and the test checks that some are."""
    out, files = called
    contigs = {name: seq for records in files.values() for name, seq in records.items()}
    with open_miint_conn() as conn:
        conn.execute("CREATE TABLE contig (name VARCHAR, sequence1 VARCHAR)")
        conn.executemany("INSERT INTO contig VALUES (?, ?)", list(contigs.items()))
        norm = normalized_sequence_expr("sequence1")
        conn.execute(
            f"""CREATE TABLE canon AS SELECT DISTINCT
                    {canonical_sequence_hash_expr("sequence1")} AS h,
                    md5({norm})::uuid <> {canonical_sequence_hash_expr("sequence1")} AS flipped,
                    CASE WHEN md5({norm})::uuid = {canonical_sequence_hash_expr("sequence1")}
                         THEN {norm} ELSE sequence_dna_reverse_complement({norm}) END AS seq
                FROM contig"""
        )
        conn.execute(f"CREATE TABLE g AS FROM '{out / gene_call.GENE_FILE}'")
        window = "substring(c.seq, g.position, g.stop_position - g.position)"
        on_strand = (
            f"CASE g.strand WHEN '+' THEN {window} "
            f"ELSE sequence_dna_reverse_complement({window}) END"
        )
        genes, wrong_hash, bad_start, on_flipped = conn.execute(
            f"""SELECT count(*),
                       count(*) FILTER (WHERE {canonical_sequence_hash_expr(window)}
                                        <> g.sequence_hash),
                       count(*) FILTER (WHERE NOT g.partial_left AND NOT g.partial_right
                                        AND left({on_strand}, 3) NOT IN ('ATG', 'GTG', 'TTG')),
                       count(*) FILTER (WHERE c.flipped)
                FROM g JOIN canon c ON c.h = g.parent_sequence_hash"""
        ).fetchone()
    assert genes == _rows(out / gene_call.GENE_FILE, "SELECT count(*) FROM t")[0][0]
    assert (wrong_hash, bad_start) == (0, 0)
    assert on_flipped > 0
    # The lowercase contig is read on its canonical strand and carries genes, so a
    # strand test that skipped normalization would flip it and fail the counts above.
    s1 = files["refined/bin.2.fa"]["s1"]
    with open_miint_conn() as conn:
        conn.execute("CREATE TABLE s1 AS SELECT ? AS s", [s1])
        flipped, genes_on_s1 = conn.execute(
            f"""SELECT md5({normalized_sequence_expr("s")})::uuid
                           <> {canonical_sequence_hash_expr("s")},
                       (SELECT count(*) FROM '{out / gene_call.GENE_FILE}' g
                        WHERE g.parent_sequence_hash = {canonical_sequence_hash_expr("s1.s")})
                FROM s1"""
        ).fetchone()
    assert (flipped, genes_on_s1 > 0) == (False, True)


def test_checkm_files_are_written_per_subject_with_contig_names_as_read(called):
    out, files = called
    checkm = out / gene_call.CHECKM_DIR
    written = sorted(str(p.relative_to(checkm)) for p in checkm.rglob("genes.*"))
    assert written == sorted(
        f"{kind}/{bin_id}/genes.{ext}"
        for kind, bin_id in (
            ("LCG", "lcg1"),
            ("MAG", "bin.1"),
            ("MAG", "bin.2"),
            ("UNBINNED", "long1"),
        )
        for ext in ("faa", "gff")
    )
    headers = re.findall(r"^>(\S+)", (checkm / "MAG/bin.1/genes.faa").read_text(), re.M)
    assert {h.rsplit("_", 1)[0] for h in headers} <= set(files["refined/bin.1.fa"])
    n_genes = _rows(
        out / gene_call.GENE_FILE,
        f"SELECT count(*) FROM t WHERE call_set = '{CALL_SET_CHECKM}' AND bin_id = 'bin.1'",
    )[0][0]
    assert len(headers) == n_genes


def test_outputs_are_the_lake_columns_with_hashes_for_the_identifiers(called):
    """What the load needs: every lake column but the run key, with each `feature_idx`
    still a canonical hash, to be minted."""
    out, _ = called
    swap = {
        "parent_feature_idx": ["parent_sequence_hash"],
        "feature_idx": ["sequence_hash", "sequence_length_bp"],
    }

    def expected(columns):
        names = []
        for name, _ in columns:
            if name not in GENE_CALL_RUN_KEY:
                names.extend(swap.get(name, [name]))
        return names

    conn = duckdb.connect()
    for file, columns in (
        (gene_call.GENE_FILE, GENE_CALL_COLUMNS),
        (gene_call.CONTIG_FILE, GENE_CALL_CONTIG_COLUMNS),
    ):
        got = [r[0] for r in conn.execute(f"DESCRIBE FROM '{out / file}'").fetchall()]
        assert got == expected(columns), file


def test_attribute_keys_are_the_contracts():
    assert tuple(gene_call.ATTRIBUTE_EXPR) == GENE_CALL_ATTRIBUTE_KEYS


def test_residue_cut_is_the_one_checkm_applies():
    """`residue_split.py` chooses which unbinned contigs CheckM scores; a contig it
    scores that this step did not call is one CheckM calls Prodigal on itself."""
    src = (_REPO / "workflows/long-read-assembly/residue_split.py").read_text()
    (value,) = re.findall(r"^_MIN_RESIDUE_LENGTH_BP = ([\d_]+)$", src, re.M)
    assert int(value) == gene_call.MIN_RESIDUE_LENGTH_BP


def test_no_subjects_writes_empty_outputs(tmp_path):
    (tmp_path / "genomes").mkdir()
    (tmp_path / "refined").mkdir()
    (tmp_path / "genomes/circular.fa").write_text("")
    out = _run(tmp_path)
    assert _rows(out / gene_call.GENE_FILE, "SELECT count(*) FROM t") == [(0,)]
    assert _rows(out / gene_call.CONTIG_FILE, "SELECT count(*) FROM t") == [(0,)]
    assert not any((out / gene_call.CHECKM_DIR).iterdir())


def test_an_absent_bins_directory_is_refused(tmp_path):
    """Read as no bins, it would make every long noLCG contig an UNBINNED subject."""
    _layout(tmp_path)
    for f in (tmp_path / "refined").iterdir():
        f.unlink()
    (tmp_path / "refined").rmdir()
    with pytest.raises(FileNotFoundError, match="refined_bins_dir"):
        _run(tmp_path)
    assert not (tmp_path / "ws" / gene_call.OUTPUT_DIR).exists()


@pytest.mark.parametrize("bin_id", ["", ".", "..", "a/b", "a\0b"])
def test_a_bin_id_that_is_not_a_plain_name_is_refused(bin_id):
    with pytest.raises(ValueError, match="cannot name a directory"):
        gene_call._dir_name(bin_id)


@pytest.mark.parametrize("status", ["unheard_of", None])
def test_a_contig_status_outside_the_contract_fails(status):
    """A status `predict_genes`' docs do not list, or a contig with no status at all,
    stops the step rather than reaching the lake. The control row passes."""
    with open_miint_conn() as conn:
        conn.execute(
            """CREATE TEMP TABLE contig AS SELECT * FROM (VALUES
                   (1::BIGINT, 'MAG', 'b', 'good', 'h1'::VARCHAR, 1),
                   (1::BIGINT, 'MAG', 'b', 'bad', 'h2'::VARCHAR, 2))
               t(genome_id, kind, bin_id, contig_id, sequence_hash, contig_order)"""
        )
        conn.execute(
            """CREATE TEMP TABLE calls (genome_id BIGINT, read_id VARCHAR, meta_mode BOOLEAN,
                   gene_index INTEGER, status VARCHAR, translation_table INTEGER,
                   uses_sd BOOLEAN, meta_bin INTEGER, model_desc VARCHAR,
                   model_gc_content DOUBLE)"""
        )
        conn.execute(
            "INSERT INTO calls VALUES (1, 'good', true, NULL, 'no_genes', 11, true, 0, 'm', 0.5)"
        )
        if status is not None:
            conn.execute(
                "INSERT INTO calls VALUES (1, 'bad', true, NULL, ?, 11, true, 0, 'm', 0.5)",
                [status],
            )
        sql = gene_call._contig_rows_sql("calls", CALL_SET_META_CLOSED, True, False)
        with pytest.raises(duckdb.Error, match="has status"):
            conn.execute(sql).fetchall()
        conn.execute("DELETE FROM contig WHERE contig_id = 'bad'")
        assert conn.execute(f"SELECT status FROM ({sql})").fetchall() == [("no_genes",)]
