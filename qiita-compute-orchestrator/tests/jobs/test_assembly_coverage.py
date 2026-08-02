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

And one behaviour this step deliberately does NOT depend on, pinned because a
previous version of the step DID and it cost a production ticket: **the @SQ order
is not controlled by the REFERENCE_LENGTHS table.** It is the reference NAME order
— upstream gave it a defined layout in response to duckdb-miint#173, having
previously emitted an order derivable from nothing.
`test_sq_order_is_name_sorted_regardless_of_reflen` pins the writer's behaviour and
`test_contig_name_order_is_tid_order` pins the consequence.

That tid order and name order now agree means a record-side name sort WOULD be a
coordinate sort — but this step still applies no ORDER BY and `binning.sh` still
runs `samtools sort`, because retiring it needs the record side checked against jgi
rather than inferred from the header. docs/duckdb-miint.md's "Open upstream gaps"
table tracks that removal.

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
    """The @SQ reference names in header order, via miint's `read_alignment_header`.

    Lives in the TEST, not the job: the step never reads its own header back
    (nothing in the job depends on the @SQ order — see the module docstring), but
    pinning what the writer does with @SQ requires reading the header.

    This used to hand-parse the BGZF container (magic, `l_text`, `n_ref`, then the
    per-reference name/length records) because miint exposed no header reader.
    `read_alignment_header(path)` now does, returning one row per reference as
    `(tid, name, length)` in header order — qiita-verified on mirror build
    `2b2841e`. Ordering by `tid` rather than trusting row order keeps this a
    statement about the header rather than about the reader's scan order.
    """
    with open_miint_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM read_alignment_header(?) ORDER BY 1", [str(path)]
        ).fetchall()
    return [name for _tid, name, _length in rows]


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


def test_sq_header_covers_every_contig(assembly, tmp_path):
    """@SQ carries all three contigs — including the unaligned one.

    The SET is what matters and is all this can assert: a contig with an @SQ line
    is reported by jgi at depth 0 instead of being dropped. The ORDER is
    deliberately NOT asserted here — it is the writer's, and
    `test_sq_order_is_name_sorted_regardless_of_reflen` is where it is pinned, on a
    fixture built to discriminate. An older version of this test asserted ascending
    name order and passed only because this fixture is three `s1.`-prefixed contigs;
    it happens to agree with the current writer, but it would not have caught the
    build where it did not.
    """
    bam = _run(assembly, tmp_path)
    assert sorted(_sq_reference_names(bam)) == [_CTG_A, _CTG_B, _CTG_UNCOVERED]


def _record_tids(bam) -> list[int]:
    """Each record's tid — its reference's @SQ index — in FILE order.

    Rests on `read_alignments` returning rows in file order (not re-sorting), else
    an unsorted BAM could pass a sortedness check silently. Probed against the
    shipped build: a BAM written deliberately in reverse (reference, position)
    order reads back reversed, not re-sorted.
    """
    tid = {name: i for i, name in enumerate(_sq_reference_names(bam))}
    with open_miint_conn() as conn:
        refs = conn.execute("SELECT reference FROM read_alignments(?)", [str(bam)]).fetchall()
    return [tid[ref] for (ref,) in refs]


def test_contig_name_order_is_tid_order(tmp_path):
    """Ordering records by contig NAME now DOES order them by tid.

    The inverse of what this test asserted before, and the change is upstream's:
    miint's BAM writer used to lay @SQ out in an order derivable from nothing we
    could name, so `ORDER BY reference ASC` produced a name-grouped but
    tid-scrambled file, which `jgi_summarize_bam_contig_depths` rejects outright
    as not coordinate-sorted. On the production assembly (20,975 contigs) 11,390
    of 925,483 records stepped backwards in tid; after `binning.sh`'s
    `samtools sort`, zero.

    @SQ is now name-sorted (see `test_sq_order_is_name_sorted_regardless_of_reflen`),
    so walking contigs in ascending name order reads off ascending tids and a name
    sort IS a coordinate sort.

    **This does not by itself retire `binning.sh`'s `samtools sort`.** That needs
    the record side checked too, against the tool that consumes the file, not
    inferred from the header — tracked separately with its own exit criteria.
    Checked from the header alone; no records needed.

    Fed a DESCENDING reflen table on purpose. With an ascending one the assertion
    would also pass against a writer that merely echoed its input, which is the
    coincidence this file's history is made of — the reflen order must not be the
    thing under test.
    """
    bam = tmp_path / "nameorder.bam"
    names = sorted(f"s{i}.ctg{i:06d}l" for i in range(1, _SQ_PROBE_N + 1))
    lengths = _sq_probe_lengths(names)

    with open_miint_conn() as conn:
        _empty_alignment_table(conn, "aln")
        sq_order = _write_sq_probe_bam(conn, list(reversed(names)), bam, lengths)

    tid = {name: i for i, name in enumerate(sq_order)}
    tids_in_name_order = [tid[name] for name in names]
    assert tids_in_name_order == sorted(tids_in_name_order), (
        "walking contigs in ascending name order no longer gives ascending tids — "
        "miint's @SQ layout has stopped being name-sorted, so tid order and name "
        "order no longer agree — re-read binning.sh's samtools sort and the "
        "assembly_coverage module docstring before relying on either"
    )


@pytest.mark.skipif(
    shutil.which("samtools") is None,
    reason="needs samtools; the fix it pins lives in binning.sh, which runs inside "
    "the long-read-assembly binning image where samtools is present",
)
def test_samtools_sort_makes_the_bam_tid_monotonic(assembly, tmp_path):
    """`samtools sort` is what actually produces the coordinate sort jgi demands.

    The positive half of `test_contig_name_order_is_tid_order`: whatever the
    writer emitted, the file `binning.sh` hands metaWRAP is tid-monotonic and still
    carries an @SQ line for every contig (including the zero-coverage one — losing
    those would silently drop contigs from the depth table).

    SKIPPED wherever samtools is absent, which includes CI and a stock dev box —
    so on those, this property is NOT covered in-repo. It was established on the
    deploy host instead, inside the binning image, against the 2.0 GB BAM from the
    ticket that exposed the bug: 11,390 out-of-order tid transitions over 925,483
    records before the sort and 0 after, @SQ name sets identical (20,975 either
    way, `cmp` clean), and jgi then reporting every contig — 20,925 of them at
    depth 0 on a subset built to have zero-coverage contigs.
    """
    bam = _run(assembly, tmp_path)
    sorted_bam = tmp_path / "sorted.bam"
    subprocess.run(
        ["samtools", "sort", "-o", str(sorted_bam), str(bam)],
        check=True,
        capture_output=True,
    )

    tids = _record_tids(sorted_bam)
    assert tids, "fixture produced no alignments — the assertion below is vacuous"
    assert tids == sorted(tids), "samtools sort did not leave the records tid-ordered"
    assert sorted(_sq_reference_names(sorted_bam)) == [_CTG_A, _CTG_B, _CTG_UNCOVERED], (
        "the sort dropped a contig from @SQ; the zero-coverage one is the point"
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
# file's `assembly` fixture uses, because this file's history is a sequence of
# small-n coincidences read as contracts: a 3-contig fixture "proved" @SQ reversed
# the reflen order, and it does not. The current name-sorted finding was re-probed
# at n = 3/5/10/64/300 and holds at every one — but n is kept high so a future
# claim cannot be established on a handful again. A real assembly has thousands.
#
# The names below are `s<i>.ctg%06dl`, which also separates LEXICOGRAPHIC from
# NUMERIC ordering (`s10.` sorts before `s2.`), so a natural-sort writer would not
# pass as a lexicographic one.
_SQ_PROBE_N = 64


def _sq_probe_lengths(names: list[str]) -> dict[str, int]:
    """Distinct per-contig lengths whose ordering matches NEITHER the name order
    nor its reverse.

    A constant length would leave the @SQ tests unable to tell "sorted by name"
    from "sorted by (length, name)" — and a real assembly's lengths vary over
    orders of magnitude, which is exactly the dimension a constant holds still.
    Asserted rather than assumed, so a change to the names or the seed cannot
    quietly restore the coincidence."""
    lengths = [1000 + 37 * i for i in range(len(names))]
    random.Random(20260802).shuffle(lengths)
    by_name = dict(zip(names, lengths, strict=True))
    ascending = sorted(names, key=lambda n: by_name[n])
    assert ascending != names, "length-ascending coincides with the name order"
    assert ascending != list(reversed(names)), "length-descending coincides with the name order"
    return by_name


def _write_sq_probe_bam(conn, order: list[str], bam, lengths: dict[str, int]) -> list[str]:
    """Write a record-less BAM whose REFERENCE_LENGTHS rows are in `order`."""
    values = ", ".join(f"('{n}', {lengths[n]})" for n in order)
    conn.execute(
        f"CREATE OR REPLACE TABLE reflen AS SELECT * FROM (VALUES {values}) t(reference, length)"
    )
    conn.execute(f"COPY (SELECT * FROM aln) TO '{bam}' (FORMAT BAM, REFERENCE_LENGTHS 'reflen')")
    return _sq_reference_names(bam)


def test_sq_order_is_name_sorted_regardless_of_reflen(tmp_path):
    """@SQ comes out sorted by contig NAME, whatever order REFERENCE_LENGTHS is in.

    This replaces an assertion that @SQ was derivable from nothing — that the
    order matched neither the table's row order, nor its reverse, nor the name
    order, and that the rule was unknown (qiita-probed 2026-07-24 on miint v1.5.4,
    reproduced standalone on the deploy host). That WAS true of that build.

    Re-probed on the current mirror build across n = 3, 5, 10, 64, 300 with the
    reflen table built ASC, DESC and shuffled: every input order yields the SAME
    @SQ, and it is `sorted(names)`. Feeding an ASC table and observing "@SQ ==
    input" is therefore a coincidence of ASC already being sorted — which is why
    the shuffled arm is here, and why this test asserts sortedness rather than
    input-tracking. The three input orders are compared against EACH OTHER for
    exactly that reason: an implementation that echoed the input would pass a
    sorted-input check and fail this one.

    Why a test rather than a comment: `assembly_coverage` once built its reflen
    DESC believing miint reversed it, and shipped a BAM whose records were
    name-ordered but tid-scrambled, which `jgi_summarize_bam_contig_depths`
    rejected in production. The order matters, so it gets pinned — in whichever
    direction is currently true.
    """
    names = sorted(f"s{i}.ctg{i:06d}l" for i in range(1, _SQ_PROBE_N + 1))
    descending = list(reversed(names))
    shuffled = names[:]
    random.Random(20260802).shuffle(shuffled)
    assert shuffled not in (names, descending), (
        "the shuffled arm landed on the sorted or reversed order, which would make "
        "it a duplicate of another arm rather than a control"
    )
    lengths = _sq_probe_lengths(names)

    seen: dict[str, list[str]] = {}
    with open_miint_conn() as conn:
        _empty_alignment_table(conn, "aln")
        for label, order in (("ASC", names), ("DESC", descending), ("SHUFFLED", shuffled)):
            # A path per arm: sharing one would let "all three agree" be satisfied
            # by a file that was never rewritten.
            got = _write_sq_probe_bam(conn, order, tmp_path / f"order_{label}.bam", lengths)
            seen[label] = got
            assert sorted(got) == names, (
                f"reflen order {label}: @SQ dropped or invented contigs — "
                "the SET is the one part of the header that has always been guaranteed"
            )
            assert got == names, (
                f"reflen order {label} did not come back name-sorted in @SQ; miint's "
                "BAM writer has changed its layout again, which silently un-does the "
                "tid/name agreement `test_contig_name_order_is_tid_order` depends on"
            )

    # Redundant while the three assertions above hold — and kept for what it says
    # when one of them does NOT: it distinguishes "the writer echoes its input"
    # from "the writer sorts", which is the distinction the DESC and SHUFFLED arms
    # exist to draw and the one an ASC-only test silently gets wrong.
    assert seen["ASC"] == seen["DESC"] == seen["SHUFFLED"], (
        "@SQ differed between reflen input orders, so the layout tracks the input "
        "rather than the names"
    )

    # Deterministic across CONNECTIONS, not merely twice within one. That is what
    # makes the assertions above a statement about miint rather than run-to-run
    # noise — the weaker same-connection form would not distinguish the two.
    with open_miint_conn() as conn:
        _empty_alignment_table(conn, "aln")
        again = _write_sq_probe_bam(conn, descending, tmp_path / "order2.bam", lengths)
    assert again == seen["DESC"], (
        "@SQ order changed between connections; the order assertions above would "
        "then be about noise, not about miint"
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
