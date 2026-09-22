"""Real-miint contract test for `assembly_coverage` (align_minimap2 NOT stubbed).

Behaviours of miint's BAM writer this step DEPENDS on, pinned here because they are
not in its docs:

  - **Zero-coverage contigs still get an @SQ line.** That is what makes jgi report
    them at depth 0 rather than dropping them, so the fixture deliberately leaves
    one contig unaligned.
  - **`FORMAT BAM` requires REFERENCE_LENGTHS.** Pinned because the plain form is
    the obvious thing to write and fails only at runtime.
  - **`SEQUENCE_DATA` actually put SEQ in the file.** Drop it and every other case
    here still passes while jgi silently under-reports depth — see
    `test_bam_carries_real_sequences`.

And the @SQ-order chain the step's coordinate sort now rests on, in three pinned
steps: **@SQ is sorted by reference name** (`test_sq_order_is_reference_name_sorted`,
duckdb-miint#173) — whatever order REFERENCE_LENGTHS is in, which is the half that
cost a production ticket when a previous version of the step assumed the reflen
table steered it; **so name order is tid order** (`test_contig_name_order_is_tid_order`);
**so the BAM this step writes is coordinate sorted**
(`test_written_bam_is_tid_monotonic`), which is what lets `binning.sh` stage it for
metaWRAP with no `samtools sort` of its own. Break any link and that entrypoint
silently hands jgi an unsorted file.

`binning.sh` still reorders the assembly FASTA to @SQ order. That is a separate
requirement (metabat2 wants the depth matrix and the assembly in the same contig
order) which record ordering does not touch — see its comment there.

Not pinned here, because it needs metabat2 which the test env does not have: that
jgi accepts the BAM and agrees with a samtools-written one. Established by probe
against jgi_summarize_bam_contig_depths 2.15 — with `SEQUENCE_DATA`, both depth and
variance from miint's BAM equal those from a real `minimap2 | samtools sort` BAM to
every printed digit (per contig: depth 6.29783 / 7.99987, variance 14.4156 /
14.4126) with zero warnings. The SEQ assertions below are the in-repo proxy for it.
Note that probe compared against a `samtools sort`ed BAM and used a handful of
contigs, which is why it did not expose the @SQ-order defect above.
"""

from __future__ import annotations

import asyncio
import random
import shutil
import subprocess
from pathlib import Path

import pytest

from qiita_compute_orchestrator.jobs import assembly_coverage
from qiita_compute_orchestrator.jobs.assembly_coverage import (
    Inputs,
    execute,
)
from qiita_compute_orchestrator.miint import open_miint_conn


def _sq_reference_names(path) -> list[str]:
    """The @SQ reference names in tid order.

    Lives in the TEST, not the job: the step never reads its own header back.
    `read_alignment_header` (duckdb-miint#174) is the only way to see tid from SQL;
    it replaced a hand-rolled BGZF header parser here when it landed.
    """
    with open_miint_conn() as conn:
        return [
            row[0]
            for row in conn.execute(
                "SELECT reference FROM read_alignment_header(?) ORDER BY tid", [str(path)]
            ).fetchall()
        ]


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


# Enough contigs that NUMERIC order (s0, s1, s2 …) and LEXICOGRAPHIC order
# (s0, s1, s10 … s2) differ, which takes at least eleven. The `assembly` fixture
# above orders identically whichever way you look at it, which is precisely why
# the original @SQ-order defect survived it and reached production.
_SCRAMBLE_N = 13


@pytest.fixture
def scrambled_assembly(tmp_path: Path) -> dict[str, Path | dict[str, int] | list[str]]:
    """A 13-contig assembly whose reads are emitted SHUFFLED, as a read set arrives.

    Written to noLCG.fa in numeric order, the way the assembler emits it — so the
    file order, the read order and the @SQ order are three different orders.
    One contig is left uncovered, as in `assembly`, to keep @SQ completeness under
    test on the shape that matters for the depth table.
    """
    rng = random.Random(20260803)
    names = [f"s{i}.ctg{i:06d}l" for i in range(_SCRAMBLE_N)]
    lengths = {name: 12000 for name in names}
    contigs = {name: _rand_seq(rng, lengths[name]) for name in names}

    genomes_dir = tmp_path / "scrambled_genomes"
    genomes_dir.mkdir()
    (genomes_dir / "noLCG.fa").write_text("".join(f">{name}\n{contigs[name]}\n" for name in names))

    reads: list[tuple[str, str]] = []
    for name in names[:-1]:
        for i in range(6):
            start = rng.randint(0, lengths[name] - 4000)
            body = _mutate(rng, contigs[name][start : start + 4000])
            reads.append((name, f"@read_{name}_{i}\n{body}\n+\n{'~' * len(body)}\n"))
    rng.shuffle(reads)

    reads_fastq = tmp_path / "scrambled_reads.fastq"
    reads_fastq.write_text("".join(record for _, record in reads))

    return {
        "genomes_dir": genomes_dir,
        "reads": reads_fastq,
        "lengths": lengths,
        # Source contig of each read, in FASTQ order — the anti-vacuity control.
        "read_contigs": [name for name, _ in reads],
    }


def test_sq_header_covers_every_contig(assembly, tmp_path):
    """@SQ carries all three contigs — including the unaligned one.

    The SET is what matters here: a contig with an @SQ line is reported by jgi at
    depth 0 instead of being dropped. The ORDER is asserted separately, by
    `test_sq_order_is_reference_name_sorted` — this fixture is three `s1.`-prefixed
    contigs, too few to tell one ordering from another.
    """
    bam = _run(assembly, tmp_path)
    assert sorted(_sq_reference_names(bam)) == [_CTG_A, _CTG_B, _CTG_UNCOVERED]


def _record_sort_keys(bam) -> list[tuple[int, int]]:
    """Each record's coordinate key — (tid, position) — in FILE order.

    Both halves, because both are load-bearing and each fails a different consumer:
    a BAM ordered by reference alone still has positions stepping backwards inside a
    contig, which jgi rejects as unsorted and `samtools index` rejects with
    "unsorted positions on sequence #1" (measured, 255 of 520 records).

    Rests on `read_alignments` returning rows in file order (not re-sorting), else
    an unsorted BAM could pass a sortedness check silently. Probed against the
    shipped build: a BAM written deliberately in reverse (reference, position)
    order reads back reversed, not re-sorted.
    """
    tid = {name: i for i, name in enumerate(_sq_reference_names(bam))}
    with open_miint_conn() as conn:
        rows = conn.execute(
            "SELECT reference, position FROM read_alignments(?)", [str(bam)]
        ).fetchall()
    return [(tid[ref], pos) for ref, pos in rows]


def test_contig_name_order_is_tid_order(tmp_path):
    """Ordering records by contig NAME now orders them by tid too.

    The consequence of `test_sq_order_is_reference_name_sorted`. It was false
    before duckdb-miint#173: `assembly_coverage` wrote `ORDER BY reference ASC,
    position ASC` believing it was a coordinate sort, and on the production
    assembly (20,975 contigs) 11,390 of 925,483 records stepped backwards in tid,
    which `jgi_summarize_bam_contig_depths` rejects outright.

    This is what makes the step's `ORDER BY reference, position` a coordinate
    sort; `test_written_bam_is_tid_monotonic` pins the result end to end. It says
    nothing about `binning.sh`'s FASTA reordering, which exists because metabat2
    needs the depth matrix and the assembly in the same contig order.
    """
    bam = tmp_path / "nameorder.bam"
    names = sorted(f"s{i}.ctg{i:06d}l" for i in range(1, _SQ_PROBE_N + 1))

    with open_miint_conn() as conn:
        _empty_alignment_table(conn, "aln")
        sq_order = _write_sq_probe_bam(conn, names, bam)

    tid = {name: i for i, name in enumerate(sq_order)}
    tids_in_name_order = [tid[name] for name in names]
    assert tids_in_name_order == sorted(tids_in_name_order), (
        "walking contigs in ascending name order no longer gives ascending tids"
    )


def test_written_bam_is_tid_monotonic(scrambled_assembly, tmp_path):
    """The step's OWN output is coordinate sorted — nothing re-sorts it downstream.

    `binning.sh` stages this file into metaWRAP's alignment cache as-is, which
    skips metaWRAP's `samtools sort` along with its `bwa mem`, so this property is
    what `jgi_summarize_bam_contig_depths` (and the `samtools index` in metaWRAP's
    concoct block) accepts the file on. What those two do with an ordered vs
    unordered file is recorded in `docs/duckdb-miint.md`'s `FORMAT BAM` writer
    section; it needs their binaries, so it is not pinned here.

    Runs the real `execute`, so the connection settings are covered too — every job
    of this shape sets `preserve_insertion_order=false`, and the `ORDER BY` has to
    survive it. That was also measured at production scale; `docs/duckdb-miint.md`'s
    `FORMAT BAM` writer section has the numbers.

    At this fixture's size a single write batch could hide a sink that reorders
    batches, which is why that scale measurement exists. This test covers the step's
    own SQL: deleting either half of the `ORDER BY` fails it.
    """
    bam = _run(scrambled_assembly, tmp_path)

    tid = {name: i for i, name in enumerate(_sq_reference_names(bam))}
    assert sorted(tid) == sorted(scrambled_assembly["lengths"]), (
        "@SQ dropped a contig; the zero-coverage one is the point"
    )

    # ANTI-VACUITY: a read set that arrived already tid-monotonic would satisfy the
    # assertion below with no sort at all, so pin that this one does not.
    tids_in_read_order = [tid[name] for name in scrambled_assembly["read_contigs"]]
    assert tids_in_read_order != sorted(tids_in_read_order), (
        "the fixture's reads are already in tid order, so the sort assertion below "
        "cannot fail — shuffle them, or this test proves nothing"
    )

    keys = _record_sort_keys(bam)
    assert keys, "fixture produced no alignments — the assertion below is vacuous"
    assert keys == sorted(keys), (
        f"the written BAM is not coordinate-ordered: "
        f"{sum(1 for a, b in zip(keys, keys[1:]) if b < a)} of {len(keys)} records "
        "step backwards in (tid, position). jgi rejects such a file with 'the bam "
        "file is not sorted!', and binning.sh runs no samtools sort that would "
        "absorb it."
    )
    # Both halves of the sort key, separately: dropping `, position` leaves the tid
    # sequence monotonic, so the assertion above alone would still pass.
    within_contig = [(a, b) for a, b in zip(keys, keys[1:]) if a[0] == b[0] and b[1] < a[1]]
    assert not within_contig, (
        f"positions step backwards inside a contig ({len(within_contig)} times) — "
        "the `, position` half of the ORDER BY is not reaching the writer"
    )


@pytest.mark.skipif(
    shutil.which("samtools") is None,
    reason="needs samtools; the behaviour it pins is relied on by binning.sh's "
    "contig-set-drift guard, which runs inside the long-read-assembly binning image",
)
def test_faidx_exits_nonzero_on_a_missing_region(tmp_path):
    """binning.sh's drift guard leans on `samtools faidx` FAILING on an absent name.

    The reorder pipes the BAM's @SQ names into `xargs ... samtools faidx <assembly>`.
    If an @SQ name is not in the assembly, faidx must exit non-zero so `xargs` (and
    the entrypoint's `set -e`) fail the step — that is the guard's "@SQ name not in
    noLCG" direction, which the static count check cannot catch (faidx pads a header
    for the missing name, so `grep -c '^>'` counts stay equal). Pin that faidx
    behaviour directly, rather than leaving it a comment. SKIPPED without samtools
    (CI, dev boxes); it holds inside the binning image where samtools is present.
    """
    fa = tmp_path / "asm.fa"
    fa.write_text(">c1\nACGTACGTACGT\n>c2\nTTTTGGGGCCCC\n")
    subprocess.run(["samtools", "faidx", str(fa)], check=True, capture_output=True)

    present = subprocess.run(["samtools", "faidx", str(fa), "c1"], capture_output=True)
    assert present.returncode == 0, "faidx failed on a present region — fixture is wrong"

    missing = subprocess.run(["samtools", "faidx", str(fa), "c1", "cNOPE"], capture_output=True)
    assert missing.returncode != 0, (
        "samtools faidx returned 0 for an absent region — binning.sh's drift guard "
        "would then NOT fail the step when the @SQ set has a name absent from noLCG"
    )


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

    Without this, `test_sq_header_covers_every_contig` could pass on a fixture
    where every contig happened to be hit, and would no longer be testing that a
    zero-coverage contig still earns an @SQ line.
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


# Contig count for the @SQ-order probe. Deliberately well past the handful this
# file's `assembly` fixture uses: the reflen->@SQ "reversal" this file used to
# assert is a SMALL-n COINCIDENCE, and how small depends on the names. Measured
# (miint v1.5.4, 2026-07-24): with `s1.ctg%06dl`-style names a DESC reflen yields
# ascending @SQ up to n=5 and diverges from n=6; with the `s<i>.ctg%06dl` names
# used below it already diverges at n=3. A real assembly has thousands.
_SQ_PROBE_N = 64


def _write_sq_probe_bam(conn, order: list[str], bam) -> list[str]:
    """Write a record-less BAM whose REFERENCE_LENGTHS rows are in `order`."""
    values = ", ".join(f"('{n}', 1000)" for n in order)
    conn.execute(
        f"CREATE OR REPLACE TABLE reflen AS SELECT * FROM (VALUES {values}) t(reference, length)"
    )
    conn.execute(f"COPY (SELECT * FROM aln) TO '{bam}' (FORMAT BAM, REFERENCE_LENGTHS 'reflen')")
    return _sq_reference_names(bam)


def test_sq_order_is_reference_name_sorted(tmp_path):
    """@SQ comes out sorted by reference NAME, whatever order REFERENCE_LENGTHS is in.

    duckdb-miint#173: @SQ was previously emitted in `unordered_map` hash-bucket
    order, matching neither the reflen table nor its reverse nor name order, which
    is how `assembly_coverage` shipped a tid-scrambled BAM to production. It is now
    a defined order, so this pins which one — the reflen row order still does not
    steer it, and code must not assume it does.
    """
    bam = tmp_path / "order.bam"
    names = sorted(f"s{i}.ctg{i:06d}l" for i in range(1, _SQ_PROBE_N + 1))

    with open_miint_conn() as conn:
        _empty_alignment_table(conn, "aln")
        for label, order in (("ASC", names), ("DESC", list(reversed(names)))):
            got = _write_sq_probe_bam(conn, order, bam)
            assert got == names, f"reflen order {label}: @SQ is not name-sorted"


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
