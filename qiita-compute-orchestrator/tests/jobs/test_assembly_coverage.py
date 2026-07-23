"""Real-miint contract test for `assembly_coverage` (align_minimap2 NOT stubbed).

This step's correctness rests on behaviours of miint's BAM writer that are not in
its docs, so they are pinned here rather than described in a comment. What each
case would catch:

  - **@SQ is reversed from the REFERENCE_LENGTHS table.** The step builds that
    table DESC so @SQ lands ASC, which is the only reason `ORDER BY reference,
    position` is a real coordinate sort — jgi_summarize_bam_contig_depths checks
    the @SQ index (tid), not the contig name, and rejects the file outright
    otherwise. `test_reflen_order_is_reversed_in_sq` pins the reversal against
    miint DIRECTLY, with both orderings, so a version bump that "fixes" it fails
    here instead of producing a BAM metaWRAP rejects two steps downstream.
  - **Zero-coverage contigs still get an @SQ line.** That is what makes jgi report
    them at depth 0 rather than dropping them, so the fixture deliberately leaves
    one contig unaligned.
  - **`FORMAT BAM` requires REFERENCE_LENGTHS.** Pinned because the plain form is
    the obvious thing to write and fails only at runtime.
  - **`SEQUENCE_DATA` actually put SEQ in the file.** Drop it and every other case
    here still passes while jgi silently under-reports depth — see
    `test_bam_carries_real_sequences`.

Not pinned here, because it needs metabat2 which the test env does not have: that
jgi accepts the BAM and agrees with a samtools-written one. Established by probe
against jgi_summarize_bam_contig_depths 2.15 — with `SEQUENCE_DATA`, both depth and
variance from miint's BAM equal those from a real `minimap2 | samtools sort` BAM to
every printed digit (per contig: depth 6.29783 / 7.99987, variance 14.4156 /
14.4126) with zero warnings. The sortedness and SEQ assertions below are the
in-repo proxy for it.
"""

from __future__ import annotations

import asyncio
import random
from pathlib import Path

import pytest

from qiita_compute_orchestrator.jobs import assembly_coverage
from qiita_compute_orchestrator.jobs.assembly_coverage import (
    Inputs,
    _read_bam_reference_names,
    execute,
)
from qiita_compute_orchestrator.miint import open_miint_conn

# Names chosen so ASCII order and insertion order differ from length order — a
# fixture where all three coincide could not tell the orderings apart.
_CTG_A = "s1.ctg000001l"
_CTG_B = "s1.ctg000002l"
_CTG_UNCOVERED = "s1.ctg000003c"


def _rand_seq(rng: random.Random, n: int) -> str:
    return "".join(rng.choice("ACGT") for _ in range(n))


def _empty_alignment_table(conn, name: str) -> None:
    """Create a 0-row table with align_minimap2's REAL output schema.

    Derived from the function rather than hand-declared: miint's BAM writer
    validates its input columns before anything else, so a hand-written stub
    fails on a schema complaint and masks the behaviour under test.
    """
    seq = _rand_seq(random.Random(7), 400)
    conn.execute(
        f"CREATE OR REPLACE TABLE {name}_subj AS SELECT 'c1' AS read_id, ? AS sequence1", [seq]
    )
    conn.execute(
        f"CREATE OR REPLACE TABLE {name}_q AS SELECT 'r1' AS read_id, ? AS sequence1", [seq]
    )
    conn.execute(
        f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM align_minimap2("
        f"'{name}_q', subject_table := '{name}_subj') LIMIT 0"
    )


def _mutate(rng: random.Random, seq: str, rate: float = 0.001) -> str:
    return "".join(
        rng.choice([c for c in "ACGT" if c != b]) if rng.random() < rate else b for b in seq
    )


@pytest.fixture
def assembly(tmp_path: Path) -> dict[str, Path | dict[str, int]]:
    """A 3-contig assembly + HiFi-like reads drawn from only TWO of them.

    `_CTG_UNCOVERED` is left with zero reads on purpose: it is the case that
    distinguishes "the header reflects the assembly" from "the header reflects
    whatever happened to align".
    """
    rng = random.Random(20260722)
    lengths = {_CTG_A: 20000, _CTG_B: 15000, _CTG_UNCOVERED: 8000}
    contigs = {name: _rand_seq(rng, n) for name, n in lengths.items()}

    genomes_dir = tmp_path / "genomes"
    genomes_dir.mkdir()
    (genomes_dir / "noLCG.fa").write_text(
        "".join(f">{name}\n{seq}\n" for name, seq in contigs.items())
    )

    reads_fastq = tmp_path / "masked_reads.fastq"
    with reads_fastq.open("w") as fh:
        for name in (_CTG_A, _CTG_B):
            seq = contigs[name]
            for i in range(8):
                length = rng.randint(8000, 12000)
                start = rng.randint(0, len(seq) - length)
                body = _mutate(rng, seq[start : start + length])
                fh.write(f"@read_{name}_{i}\n{body}\n+\n{'~' * len(body)}\n")

    return {"genomes_dir": genomes_dir, "reads": reads_fastq, "lengths": lengths}


def _run(assembly, tmp_path: Path) -> Path:
    outputs = asyncio.run(
        execute(
            Inputs(
                genomes_dir=assembly["genomes_dir"],
                masked_reads_fastq=assembly["reads"],
                prep_sample_idx=1,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
    )
    return outputs["coverage_bam"]


def test_sq_header_is_ascending_and_covers_every_contig(assembly, tmp_path):
    """@SQ carries all three contigs, ascending — including the unaligned one.

    Ascending order is the load-bearing part: it is what makes the step's
    `ORDER BY reference, position` a genuine coordinate sort.
    """
    bam = _run(assembly, tmp_path)
    assert _read_bam_reference_names(bam) == [_CTG_A, _CTG_B, _CTG_UNCOVERED]


def test_records_are_coordinate_sorted(assembly, tmp_path):
    """Records are non-decreasing in (tid, position).

    This is what jgi checks. Read back through miint's own reader so the test
    needs no samtools.
    """
    bam = _run(assembly, tmp_path)
    order = {name: i for i, name in enumerate(_read_bam_reference_names(bam))}

    with open_miint_conn() as conn:
        rows = conn.execute(
            "SELECT reference, position FROM read_alignments(?)", [str(bam)]
        ).fetchall()

    assert rows, "fixture produced no alignments — the assertions below are vacuous"
    keys = [(order[ref], pos) for ref, pos in rows]
    assert keys == sorted(keys)


def test_bam_carries_real_sequences(assembly, tmp_path):
    """SEQ is written, not `*`.

    The load-bearing test of the whole `SEQUENCE_DATA` argument. Drop that
    argument and the BAM still writes, still sorts, still passes every other case
    here — and jgi silently reports a length-dependent under-estimate of depth,
    because it sizes its contig-end exclusion window from the read length it
    reads out of SEQ. Nothing else in this file would notice.

    Verified non-vacuous against a SEQ-less BAM: `read_sequences_sam` does not
    return blanks there, it RAISES ("Primary/unmapped read missing sequence
    (SEQ='*')"), so this case fails either way.
    """
    bam = _run(assembly, tmp_path)
    with open_miint_conn() as conn:
        # Join the BAM's SEQ back to the source FASTQ and compare LENGTHS per
        # read_id. Asserting merely "some record has ACGT in it" would pass if the
        # writer emitted the wrong read, a truncated one, or the subject sequence
        # — and the property jgi actually consumes is the length.
        rows = conn.execute(
            "SELECT b.read_id, length(b.sequence1), length(f.sequence1) "
            "FROM read_sequences_sam(?) b "
            "JOIN read_fastx(?) f USING (read_id)",
            [str(bam), str(assembly["reads"])],
        ).fetchall()

    assert rows, "BAM carries no SEQ — was SEQUENCE_DATA dropped from the COPY?"
    # Every record here is primary and unclipped (the fixture's reads are exact
    # substrings of one contig), so SEQ must be the full source read. A hard-clipped
    # supplementary would legitimately be shorter — probed separately, not here.
    mismatched = [(rid, got, want) for rid, got, want in rows if got != want]
    assert not mismatched, f"SEQ length != source read length: {mismatched[:3]}"


def test_chimeric_and_reverse_reads_produce_a_valid_bam(tmp_path):
    """Hard-clipped supplementary and reverse-strand records survive SEQUENCE_DATA.

    The `assembly` fixture is three unrelated contigs, so it makes neither a
    chimera (→ supplementary alignment, which minimap2 hard-clips) nor a
    reverse-strand hit. A hard clip consumes fewer query bases than the
    full-length read the lookup supplies, and a reverse hit needs the sequence
    reverse-complemented — either handled wrong yields a SEQ/CIGAR length
    mismatch, which is an invalid record. This fixture forces both: reads that
    span two contigs, and one read given as the reverse complement of a contig
    slice.

    The strict per-record check (SEQ length == CIGAR query-consuming length) needs
    samtools, which the test env lacks, so it lives in the probe (which showed
    `samtools quickcheck` clean, zero mismatches). What is checkable here without
    samtools is nearly as strong: `read_sequences_sam` reads EVERY record back —
    it raises on a `*` SEQ and on a malformed record — and returns one row per
    alignment record with a non-empty ACGTN sequence, while `read_alignments`
    confirms the hard-clipped supplementary is actually present.
    """
    rng = random.Random(31337)
    c1 = _rand_seq(rng, 20000)
    c2 = _rand_seq(rng, 15000)
    genomes_dir = tmp_path / "genomes"
    genomes_dir.mkdir()
    (genomes_dir / "noLCG.fa").write_text(f">ctgA\n{c1}\n>ctgB\n{c2}\n")

    comp = {"A": "T", "C": "G", "G": "C", "T": "A"}
    reads = tmp_path / "reads.fastq"
    with reads.open("w") as fh:
        for i in range(4):  # chimeras: half ctgA + half ctgB → supplementary + hard clip
            s = c1[1000:6000] + c2[2000:7000]
            fh.write(f"@chim_{i}\n{s}\n+\n{'~' * len(s)}\n")
        for i in range(4):  # reverse-complement of a ctgA slice → reverse-strand hit
            s = "".join(comp[b] for b in reversed(c1[i * 900 : i * 900 + 9000]))
            fh.write(f"@rev_{i}\n{s}\n+\n{'~' * len(s)}\n")

    outputs = asyncio.run(
        execute(
            Inputs(
                genomes_dir=genomes_dir,
                masked_reads_fastq=reads,
                prep_sample_idx=1,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
    )
    bam = outputs["coverage_bam"]

    with open_miint_conn() as conn:
        # Reads every record back; raises on a `*` SEQ or a malformed record.
        seqs = conn.execute("SELECT sequence1 FROM read_sequences_sam(?)", [str(bam)]).fetchall()
        aln_count = conn.execute("SELECT count(*) FROM read_alignments(?)", [str(bam)]).fetchone()[
            0
        ]
        has_hardclip = conn.execute(
            "SELECT bool_or(cigar LIKE '%H%') FROM read_alignments(?)", [str(bam)]
        ).fetchone()[0]

    assert seqs, "fixture produced no records — nothing was exercised"
    assert has_hardclip, (
        "fixture made no hard-clipped record; it is no longer testing supplementaries"
    )
    # Every alignment record read back with a real sequence (none dropped, none `*`).
    assert len(seqs) == aln_count
    assert all(s and set(s) <= set("ACGTN") for (s,) in seqs)


def test_uncovered_contig_has_no_alignments(assembly, tmp_path):
    """The zero-coverage contig really is zero-coverage.

    Without this, `test_sq_header_...` could pass on a fixture where every contig
    happened to be hit, and would no longer be testing the header's provenance.
    """
    bam = _run(assembly, tmp_path)
    with open_miint_conn() as conn:
        refs = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT reference FROM read_alignments(?)", [str(bam)]
            ).fetchall()
        }
    assert refs == {_CTG_A, _CTG_B}


def test_missing_sequence_data_entry_raises(assembly, tmp_path):
    """A SEQUENCE_DATA lookup miss fails loudly, it does not fall back to `*`.

    Load-bearing: a partial lookup that silently wrote `*` for the missing reads
    would reintroduce the depth under-report for a SUBSET of reads, which no
    downstream check would catch. Pinned against miint directly, with the reads
    table deliberately short one entry.
    """
    with open_miint_conn() as conn:
        conn.execute(
            "CREATE TABLE subj AS SELECT read_id, sequence1 FROM read_fastx(?)",
            [str(assembly["genomes_dir"] / "noLCG.fa")],
        )
        # Interpolated: DuckDB rejects a bound parameter inside a VIEW body.
        conn.execute(
            f"CREATE VIEW q AS SELECT read_id, sequence1 FROM read_fastx('{assembly['reads']}')"
        )
        conn.execute(
            "CREATE TABLE aln AS SELECT * FROM align_minimap2("
            "'q', subject_table := 'subj', preset := 'map-hifi')"
        )
        conn.execute(
            "CREATE TABLE reflen AS SELECT read_id AS reference, "
            "length(sequence1) AS length FROM subj ORDER BY read_id DESC"
        )
        # Drop exactly one aligned read from the lookup.
        victim = conn.execute("SELECT read_id FROM aln LIMIT 1").fetchone()[0]
        conn.execute(
            "CREATE TABLE sd AS SELECT read_id, sequence1, qual1 FROM read_fastx(?) "
            "WHERE read_id <> ?",
            [str(assembly["reads"]), victim],
        )
        with pytest.raises(Exception, match="SEQUENCE_DATA"):
            conn.execute(
                f"COPY (SELECT * FROM aln) TO '{tmp_path / 'partial.bam'}' "
                "(FORMAT BAM, REFERENCE_LENGTHS 'reflen', SEQUENCE_DATA 'sd')"
            )


def test_empty_nolcg_yields_an_empty_bam(tmp_path):
    """No contigs is a valid pipeline outcome, not a step failure.

    binning.sh short-circuits on the same condition and never stages this file.
    """
    genomes_dir = tmp_path / "genomes"
    genomes_dir.mkdir()
    (genomes_dir / "noLCG.fa").write_text("")
    reads = tmp_path / "reads.fastq"
    reads.write_text("@r\nACGT\n+\n~~~~\n")

    outputs = asyncio.run(
        execute(
            Inputs(
                genomes_dir=genomes_dir,
                masked_reads_fastq=reads,
                prep_sample_idx=1,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
    )
    assert outputs["coverage_bam"].read_bytes() == b""


def test_reflen_order_is_reversed_in_sq(tmp_path):
    """miint reverses the REFERENCE_LENGTHS table when writing @SQ.

    Pinned against miint DIRECTLY, both directions, because the step's entire sort
    strategy is built on it and it is undocumented. If a miint bump stops
    reversing, this fails with a clear cause — rather than
    `test_sq_header_is_ascending...` failing with a confusing one.
    """
    bam = tmp_path / "order.bam"
    names = [_CTG_A, _CTG_B, _CTG_UNCOVERED]

    with open_miint_conn() as conn:
        _empty_alignment_table(conn, "aln")
        for order, expected in ((names, list(reversed(names))), (list(reversed(names)), names)):
            values = ", ".join(f"('{n}', 1000)" for n in order)
            conn.execute(
                f"CREATE OR REPLACE TABLE reflen AS SELECT * FROM (VALUES {values}) "
                "t(reference, length)"
            )
            conn.execute(
                f"COPY (SELECT * FROM aln) TO '{bam}' (FORMAT BAM, REFERENCE_LENGTHS 'reflen')"
            )
            assert _read_bam_reference_names(bam) == expected, (
                f"reflen order {order} produced @SQ {_read_bam_reference_names(bam)}; "
                "assembly_coverage assumes miint reverses it"
            )


def test_format_bam_requires_reference_lengths(tmp_path):
    """The plain `(FORMAT BAM)` form is rejected.

    Pinned so the step's REFERENCE_LENGTHS argument is not "tidied away" by
    someone who assumes the header can be inferred from the alignments.
    """
    with open_miint_conn() as conn:
        _empty_alignment_table(conn, "aln")
        with pytest.raises(Exception, match="REFERENCE_LENGTHS"):
            conn.execute(f"COPY aln TO '{tmp_path / 'x.bam'}' (FORMAT BAM)")


def test_workflow_wires_this_module_and_feeds_binning():
    """The YAML actually routes `assembly_coverage` to this module, and `binning`
    consumes its output.

    Reads 1.0.0.yaml rather than asserting a constant against its own literal:
    the drift worth catching is between the module and the workflow, and a
    self-comparison cannot see it. Also pins the ORDERING — binning must come
    after the step producing its `coverage_bam`, or metaWRAP silently falls back
    to bwa self-alignment.
    """
    import yaml

    repo_root = Path(__file__).resolve().parents[3]
    spec = yaml.safe_load(
        (repo_root / "workflows" / "long-read-assembly" / "1.0.0.yaml").read_text()
    )
    names = [e.get("step") or e.get("action") for e in spec["steps"]]

    coverage = next(e for e in spec["steps"] if e.get("step") == assembly_coverage.YAML_STEP_NAME)
    assert coverage["module"] == "qiita_compute_orchestrator.jobs.assembly_coverage"
    assert "coverage_bam" in coverage["outputs"]

    binning = next(e for e in spec["steps"] if e.get("step") == "binning")
    assert "coverage_bam" in binning["inputs"]
    assert names.index(assembly_coverage.YAML_STEP_NAME) < names.index("binning")
