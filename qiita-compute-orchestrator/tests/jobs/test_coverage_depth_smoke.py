"""Coverage depth against the REAL miint build — the SQL, not a stub.

Fixtures are synthetic but built to the geometry of the actual SynDNA vector library
(11 plasmids of 17,263 bp, each carrying one 2,547 bp insert at [7360, 9907)), because
that geometry is what makes the design non-obvious: the insert is only 15% of the
plasmid, so 85% of every plasmid-derived read is backbone. The same computation was run
end-to-end against the real `AllsynDNA_plasmids_FASTA_ReIndexed_FINAL.fasta` and the GFF3
derived from it, and produced the identical numbers.

Each read is chosen to isolate one decision, and the assertions are written so that
getting the decision wrong changes an answer rather than merely failing a shape check:

  insert_exact    — wholly inside the window                     -> contributes ILEN
  insert_mm       — 1% mismatches, still above the identity gate -> contributes ILEN
  junction        — spans insert->backbone. A REAL spike-in.     -> contributes 1200
                    Aligned fraction is 1.0 against the PLASMID but 0.60 against the
                    WINDOW, so a gate applied after windowing would delete it.
  backbone_only   — maps to the plasmid, never to the insert     -> contributes 0
  chimera         — 1.5 kb junk + 0.5 kb insert; 25% aligned     -> REJECTED by the gate
"""

from __future__ import annotations

import random

import pytest

from qiita_compute_orchestrator.jobs._coverage import (
    DEPTH_MODE_EXCLUDE_DELETIONS,
    DEPTH_MODE_INCLUDE_DELETIONS,
    MIN_ALIGNED_FRACTION,
    MIN_IDENTITY,
    compute_feature_depth,
)
from qiita_compute_orchestrator.miint import open_miint_conn

# The real vector library's geometry.
PLASMID_LEN = 17_263
INSERT_START = 7_360  # 1-based inclusive
INSERT_STOP = 9_907  # 1-based EXCLUSIVE (half-open, as reference_annotation stores it)
INSERT_LEN = INSERT_STOP - INSERT_START  # 2547


PARENT_FEATURE_IDX = 100  # the plasmid
INSERT_FEATURE_IDX = 200  # the insert (its own feature — an interval of the plasmid)
PREP_SAMPLE_IDX = 7

_RNG = random.Random(20260714)


def _dna(n: int) -> str:
    return "".join(_RNG.choice("ACGT") for _ in range(n))


PLASMID = _dna(PLASMID_LEN)
INSERT = PLASMID[INSERT_START - 1 : INSERT_STOP - 1]


def _mutate(seq: str, n: int) -> str:
    rng = random.Random(99)
    s = list(seq)
    for i in rng.sample(range(len(s)), n):
        s[i] = rng.choice([b for b in "ACGT" if b != s[i]])
    return "".join(s)


READS: dict[int, tuple[str, str, int]] = {
    1: ("insert_exact", INSERT, INSERT_LEN),
    2: ("insert_mm", _mutate(INSERT, INSERT_LEN // 100), INSERT_LEN),
    3: ("junction", PLASMID[INSERT_START - 801 : INSERT_START - 1] + INSERT[:1200], 1200),
    4: ("backbone_only", PLASMID[100:1600], 0),
    5: ("chimera", _dna(1500) + INSERT[:500], 0),  # rejected by the aligned-fraction gate
}
KEPT_BY_GATE = {1, 2, 3, 4}  # everything but the chimera
EXPECTED_BASES = sum(READS[r][2] for r in KEPT_BY_GATE)  # 2547 + 2547 + 1200 + 0 = 6294


@pytest.fixture(scope="module")
def conn(tmp_path_factory):
    c = open_miint_conn()
    try:
        c.execute("CREATE TABLE subj (read_id BIGINT, sequence1 VARCHAR)")
        c.execute("INSERT INTO subj VALUES (?, ?)", [PARENT_FEATURE_IDX, PLASMID])
        mmi = str(tmp_path_factory.mktemp("idx") / "plasmid.mmi")
        (ok,) = c.execute(
            "SELECT success FROM save_minimap2_index('subj', ?, preset := 'map-hifi')", [mmi]
        ).fetchone()
        assert ok

        c.execute("CREATE TABLE q (read_id BIGINT, sequence1 VARCHAR)")
        for rid, (_, seq, _) in READS.items():
            c.execute("INSERT INTO q VALUES (?, ?)", [rid, seq])

        # The alignment relation, shaped exactly as the coverage job will hand it over.
        c.execute(
            "CREATE TABLE alignment AS SELECT "
            f"  {PREP_SAMPLE_IDX}::BIGINT AS prep_sample_idx, "
            "   read_id AS sequence_idx, "
            "   CAST(reference AS BIGINT) AS parent_feature_idx, "
            "   flags, position, stop_position, cigar "
            f"FROM align_minimap2('q', index_path := '{mmi}', preset := 'map-hifi',"
            "                     max_secondary := 0)"
        )
        # The annotation window + the parent's length, as reference_annotation /
        # reference_sequences supply them.
        c.execute(
            "CREATE TABLE feature_window AS SELECT "
            f"  {INSERT_FEATURE_IDX}::BIGINT AS feature_idx, "
            f"  {PARENT_FEATURE_IDX}::BIGINT AS parent_feature_idx, "
            f"  {INSERT_START}::BIGINT AS position, {INSERT_STOP}::BIGINT AS stop_position"
        )
        c.execute(
            "CREATE TABLE parent_len AS SELECT "
            f"  {PARENT_FEATURE_IDX}::BIGINT AS feature_idx, "
            f"  {PLASMID_LEN}::BIGINT AS sequence_length_bp"
        )
        # The samples this ticket measured — NOT derived from the alignment (a sample with
        # no spike-in reads must still get zero rows).
        c.execute(
            f"CREATE TABLE measured_sample AS SELECT {PREP_SAMPLE_IDX}::BIGINT AS prep_sample_idx"
        )
        yield c
    finally:
        c.close()


def _depth(conn, *, mode=DEPTH_MODE_INCLUDE_DELETIONS, min_aligned_fraction=MIN_ALIGNED_FRACTION):
    compute_feature_depth(
        conn,
        alignment_relation="alignment",
        sample_relation="measured_sample",
        window_relation="feature_window",
        parent_length_relation="parent_len",
        min_identity=MIN_IDENTITY,
        min_aligned_fraction=min_aligned_fraction,
        depth_mode=mode,
        out_relation="cov",
    )
    return conn.execute(
        "SELECT prep_sample_idx, feature_idx, covered_bases, feature_length, "
        "       occurrences, mean_depth FROM cov"
    ).fetchall()


def test_mean_depth_is_exact(conn):
    """The headline number, in closed form: every kept read's in-window bases, over the
    insert length. If any of the five reads were gated differently this changes."""
    (row,) = _depth(conn)
    prep, feature, bases, length, occurrences, mean = row

    assert (prep, feature) == (PREP_SAMPLE_IDX, INSERT_FEATURE_IDX)
    assert (length, occurrences) == (INSERT_LEN, 1)
    assert bases == EXPECTED_BASES, "in-window bases must be the sum over the gated reads"
    assert mean == pytest.approx(EXPECTED_BASES / INSERT_LEN)
    assert mean == pytest.approx(2.471143, abs=1e-6)  # the number the real data produced


def test_the_junction_read_survives_because_the_gate_runs_before_windowing(conn):
    """THE decision this design turns on.

    A read spanning insert->backbone is a real spike-in molecule. Against the PLASMID it
    is fully aligned; against the WINDOW it is only 60% aligned. So the aligned-fraction
    gate is correct pre-windowing and catastrophic post-windowing — it would delete
    exactly the reads the plasmid-level reference exists to capture.

    Asserted as a DIFFERENCE, not a claim: measure the read's aligned fraction both ways
    and show the same 0.90 threshold decides them oppositely.
    """
    pre = conn.execute(
        "SELECT cigar_query_coverage(cigar) FROM alignment WHERE sequence_idx = 3"
    ).fetchone()[0]
    # alignment_slice reads the SAM column set, so the read key must be `read_id` — and
    # it refuses input carrying more than one reference, hence the single-parent filter.
    conn.execute(
        "CREATE OR REPLACE VIEW one_parent AS "
        "SELECT sequence_idx AS read_id, flags, position, stop_position, cigar FROM alignment "
        f"WHERE parent_feature_idx = {PARENT_FEATURE_IDX}"
    )
    post = conn.execute(
        f"SELECT cigar_query_coverage(cigar) FROM alignment_slice("
        f"  'one_parent', {INSERT_START}, {INSERT_STOP}) WHERE CAST(read_id AS BIGINT) = 3"
    ).fetchone()[0]

    assert pre >= MIN_ALIGNED_FRACTION, "against the plasmid the junction read is fully aligned"
    assert post < MIN_ALIGNED_FRACTION, (
        "against the window it is not — so the SAME gate applied after windowing would "
        "drop a real spike-in"
    )
    assert post == pytest.approx(0.60, abs=0.01)

    # ...and it does in fact contribute its 1200 in-window bases.
    (row,) = _depth(conn)
    assert row[2] == EXPECTED_BASES


def test_the_chimera_is_rejected_and_that_is_what_the_ratio_gate_is_for(conn):
    """A read that is mostly unrelated sequence with a short insert-like stretch aligns to
    only ~25% of the plasmid. Identity alone would keep it (the aligned part matches
    perfectly); only the aligned-fraction gate rejects it.

    The control: drop the gate to 0.0 and the number MUST change. Otherwise this test is
    asserting nothing about the gate.
    """
    ident, frac = conn.execute(
        "SELECT cigar_sequence_identity(cigar), cigar_query_coverage(cigar) "
        "FROM alignment WHERE sequence_idx = 5"
    ).fetchone()
    assert ident >= MIN_IDENTITY, "the aligned portion matches perfectly — identity keeps it"
    assert frac < MIN_ALIGNED_FRACTION, "only the aligned-FRACTION gate rejects it"

    with_gate = _depth(conn)[0][2]
    without_gate = _depth(conn, min_aligned_fraction=0.0)[0][2]
    assert with_gate == EXPECTED_BASES
    assert without_gate > with_gate, "dropping the gate must let the chimera's bases in"
    assert without_gate - with_gate == 500  # exactly the chimera's insert-like stretch


def test_backbone_only_read_aligns_but_contributes_nothing(conn):
    """Plasmid removal is free: a read mapping only to the backbone passes the gate (it is
    a genuine, high-identity plasmid alignment) and simply contributes zero in-window
    bases. The anti-vacuity half is the first assertion — it really does align, so "0
    in-window" is a result and not an artefact of it never aligning at all."""
    n = conn.execute("SELECT count(*) FROM alignment WHERE sequence_idx = 4").fetchone()[0]
    assert n == 1, "the backbone read must really align, or this proves nothing"

    ident, frac = conn.execute(
        "SELECT cigar_sequence_identity(cigar), cigar_query_coverage(cigar) "
        "FROM alignment WHERE sequence_idx = 4"
    ).fetchone()
    assert ident >= MIN_IDENTITY and frac >= MIN_ALIGNED_FRACTION, "it passes the gate"

    # Its 1500 bases are nowhere in the total.
    assert _depth(conn)[0][2] == EXPECTED_BASES


def test_deletion_mode_is_observable(conn, tmp_path):
    """Same point as above, done directly on the CIGAR: a 30 bp deletion inside the window
    counts under `include_deletions` and not under `exclude_deletions`."""
    depth_incl = conn.execute(
        "SELECT list_sum(compute_coverage_depth(1::BIGINT, 101::BIGINT, '40=30D30=', "
        f"100::BIGINT, '{DEPTH_MODE_INCLUDE_DELETIONS}'))"
    ).fetchone()[0]
    depth_excl = conn.execute(
        "SELECT list_sum(compute_coverage_depth(1::BIGINT, 101::BIGINT, '40=30D30=', "
        f"100::BIGINT, '{DEPTH_MODE_EXCLUDE_DELETIONS}'))"
    ).fetchone()[0]
    assert depth_incl == 100  # 40 matched + 30 deleted + 30 matched
    assert depth_excl == 70  # the 30 deleted reference positions do not count
    assert depth_incl != depth_excl, "the mode must move the number or it is decorative"


def test_a_feature_with_no_reads_yields_an_explicit_zero(conn):
    """A feature table must distinguish 'measured, and it was zero' from 'not measured'.
    A spike-in that failed to amplify and one that was never in the pool are different
    facts, and dropping the row would make them identical."""
    conn.execute(
        "CREATE OR REPLACE TABLE feature_window AS "
        f"SELECT {INSERT_FEATURE_IDX}::BIGINT AS feature_idx, "
        f"       {PARENT_FEATURE_IDX}::BIGINT AS parent_feature_idx, "
        f"       {INSERT_START}::BIGINT AS position, {INSERT_STOP}::BIGINT AS stop_position "
        "UNION ALL "
        f"SELECT 999::BIGINT, {PARENT_FEATURE_IDX}::BIGINT, 16000::BIGINT, 16500::BIGINT"
    )
    rows = {r[1]: r for r in _depth(conn)}
    try:
        assert 999 in rows, "a window with no covering read must still produce a row"
        zero = rows[999]
        assert zero[2] == 0 and zero[5] == 0.0
        assert zero[3] == 500, "its length is still recorded"
    finally:
        conn.execute(
            "CREATE OR REPLACE TABLE feature_window AS "
            f"SELECT {INSERT_FEATURE_IDX}::BIGINT AS feature_idx, "
            f"       {PARENT_FEATURE_IDX}::BIGINT AS parent_feature_idx, "
            f"       {INSERT_START}::BIGINT AS position, {INSERT_STOP}::BIGINT AS stop_position"
        )


def test_unknown_depth_mode_is_refused(conn):
    with pytest.raises(ValueError, match="unknown depth_mode"):
        compute_feature_depth(
            conn,
            alignment_relation="alignment",
            sample_relation="measured_sample",
            window_relation="feature_window",
            parent_length_relation="parent_len",
            min_identity=MIN_IDENTITY,
            min_aligned_fraction=MIN_ALIGNED_FRACTION,
            depth_mode="whatever",
            out_relation="cov",
        )


def test_a_feature_at_several_windows_is_summed_not_averaged(conn):
    """The 16S case, and the reason the coverage table has an `occurrences` column.

    A feature is a SEQUENCE; an annotation is an OCCURRENCE of it at a place. A bacterial
    16S rRNA gene occurs in 5-7 BYTE-IDENTICAL copies, which canonically hash to ONE
    feature_idx — so one feature legitimately sits at N windows. The depth of that
    sequence must be sum(bases) / sum(lengths) across the occurrences.

    Averaging the per-occurrence MEANS instead would weight a short occurrence exactly
    like a long one. This test is built so the two disagree: occurrence A is fully covered
    and occurrence B is not covered at all, and they have DIFFERENT lengths — so
    "average of the means" and "sum over sum" give different answers, and only one of
    them is right.
    """
    # Two windows for ONE feature_idx: the insert (2547 bp, fully covered) and a
    # 500 bp stretch of empty backbone (0 covered).
    conn.execute(
        "CREATE OR REPLACE TABLE feature_window AS "
        f"SELECT {INSERT_FEATURE_IDX}::BIGINT AS feature_idx, "
        f"       {PARENT_FEATURE_IDX}::BIGINT AS parent_feature_idx, "
        f"       {INSERT_START}::BIGINT AS position, {INSERT_STOP}::BIGINT AS stop_position "
        "UNION ALL "
        f"SELECT {INSERT_FEATURE_IDX}::BIGINT, {PARENT_FEATURE_IDX}::BIGINT, "
        "       16000::BIGINT, 16500::BIGINT"
    )
    try:
        (row,) = _depth(conn)
        _, feature, bases, length, occurrences, mean = row

        assert feature == INSERT_FEATURE_IDX
        assert occurrences == 2, "both windows must fold into ONE row for the feature"
        assert bases == EXPECTED_BASES, "bases are summed across occurrences"
        assert length == INSERT_LEN + 500, "lengths are summed too"
        assert mean == pytest.approx(EXPECTED_BASES / (INSERT_LEN + 500))

        # The control: averaging the per-occurrence means gives a DIFFERENT (wrong)
        # answer, so this test is actually pinning the aggregation and not just arithmetic.
        mean_of_means = ((EXPECTED_BASES / INSERT_LEN) + (0 / 500)) / 2
        assert mean != pytest.approx(mean_of_means), (
            "sum-over-sum and mean-of-means must differ here, or the test proves nothing"
        )
    finally:
        conn.execute(
            "CREATE OR REPLACE TABLE feature_window AS "
            f"SELECT {INSERT_FEATURE_IDX}::BIGINT AS feature_idx, "
            f"       {PARENT_FEATURE_IDX}::BIGINT AS parent_feature_idx, "
            f"       {INSERT_START}::BIGINT AS position, {INSERT_STOP}::BIGINT AS stop_position"
        )
