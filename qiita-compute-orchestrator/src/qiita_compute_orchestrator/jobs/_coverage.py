"""Per-feature mean coverage depth, from an alignment and a set of feature windows.

The mechanism the SynDNA cell-count work needs, built generic on purpose: it takes
*alignments* and *feature windows* and produces a *feature table*. SynDNA and a genome
differ only in which reference they point at — a SynDNA insert is a window on a plasmid,
a gene is a window on a chromosome — so nothing here knows what SynDNA is.

    depth(feature) = bases of aligned read covering the feature's window
                     ------------------------------------------------------
                                    length of that window

**Why coverage depth and not a read count.** A read count is not a unit the mass model
can use: a single 20 kb HiFi read and a 150 bp short read are both "one read" but differ
by two orders of magnitude in the DNA they represent. The mass model is over base-pairs,
so the quantity has to be base-pairs.

**The gate runs on the UNSLICED alignment.** Reads are aligned to the PARENT (the whole
plasmid, not the bare insert), and the identity + aligned-fraction filters are applied
there, before any windowing. This is not a detail:

  * A read spanning the insert->backbone junction is a REAL spike-in molecule. Against
    the plasmid it aligns end-to-end (aligned fraction 1.0) and passes. Against the
    window it is only ~60% aligned — so the same 0.90 filter applied AFTER windowing
    would delete exactly the reads the plasmid-level design exists to keep. (Measured on
    the real SynDNA plasmids: 0.60.)
  * A chimeric read carrying a short insert-like stretch aligns to only ~25% of the
    plasmid and is rejected. That is what the aligned-fraction gate is *for*.
  * A read that maps only to the plasmid BACKBONE passes the gate, contributes zero
    in-window bases, and so drops out by construction. Plasmid removal is free.

**Windowing is done by aggregate, not by `alignment_slice`.** `compute_coverage_depth` is
an AGGREGATE returning a per-base array sized to the reference length, so grouping by the
parent gives one array per parent and every window on it is then a `list_slice` of that
array — all of a plasmid's inserts in a single pass. `alignment_slice` would force one
call per (parent, window): it REFUSES input carrying more than one reference ("Filter to a
single reference before slicing").

The cost of that choice is that the array is PARENT-length. A 17 kb plasmid is free and a
microbial genome (~1e6) is ~4 MB. A human chromosome (~2.5e8) would be ~1 GB per group and
needs the slice-and-shift path instead — deliberately not built, because nothing asks for
it yet and an untested path is worse than an absent one.
"""

from __future__ import annotations

import duckdb

# Scoring the alignment. Both are computed from the CIGAR alone — no NM/MD tags — which
# is what makes them usable on a sliced alignment too (`alignment_slice` NULLs the tags on
# any read it trims, so `alignment_seq_identity` returns NULL for exactly the
# boundary-spanning reads). On minimap2's eqx CIGAR, `cigar_sequence_identity` reproduces
# blast identity exactly.
IDENTITY_EXPR = "cigar_sequence_identity(cigar)"
ALIGNED_FRACTION_EXPR = "cigar_query_coverage(cigar)"

# The gate's THRESHOLDS, single-sourced here alongside its expressions. Settled with the
# assay owner: a read contributes iff it is >= 95% identical AND >= 90% of it aligns, both
# measured against the whole PLASMID (pre-window). This is THE gate — the reason it lives
# in one module is that syndna masks reads as spike-in and coverage_depth counts them
# toward depth, and if the two ever used different cutoffs a read could be called a
# spike-in and yet contribute no depth (or vice versa), silently. Both jobs import these;
# neither redefines them. (They are still passed as parameters into `compute_feature_depth`
# so a test can vary them, but the production callers pass THESE.)
MIN_IDENTITY = 0.95
MIN_ALIGNED_FRACTION = 0.90

# Only primary, mapped alignments contribute. Primary-only is load-bearing rather than
# tidy: the plasmid BACKBONE is identical across every SynDNA plasmid, so one read can
# align to several of them at high identity. Counting secondaries would multi-count the
# same molecule across inserts.
#
# `alignment_is_primary` is TRUE for an UNMAPPED read (the SAM spec makes unmapped
# implicitly primary), so the second conjunct is not redundant. miint has since added
# `alignment_is_mapped_primary` for exactly this confusion; adopt it once the mirror build
# carries it.
MAPPED_PRIMARY_EXPR = "alignment_is_primary(flags) AND NOT alignment_is_unmapped(flags)"

# `include_deletions` (samtools depth -J): a deleted reference position inside the feature
# still counts as covered. Settled with the assay owner. At >= 0.95 identity indels are
# rare, and the alternative asks an awkward question about the mirror case (an insertion in
# the read). It measurably moves the number, so it is a hashed knob, not a constant.
DEPTH_MODE_INCLUDE_DELETIONS = "include_deletions"
DEPTH_MODE_EXCLUDE_DELETIONS = "exclude_deletions"


def compute_feature_depth(
    conn: duckdb.DuckDBPyConnection,
    *,
    alignment_relation: str,
    sample_relation: str,
    window_relation: str,
    parent_length_relation: str,
    min_identity: float,
    min_aligned_fraction: float,
    depth_mode: str,
    out_relation: str,
) -> None:
    """Materialise `out_relation` with one row per (prep_sample_idx, feature_idx).

    Input relations (names, not paths — the caller registers them):

    * `alignment_relation`   — `(prep_sample_idx, parent_feature_idx, flags, position,
                                stop_position, cigar)`. `position`/`stop_position` are the
                               alignment's coordinates on the PARENT, 1-based, half-open.
    * `sample_relation`      — `(prep_sample_idx)`. The samples this ticket MEASURED, which
                               is NOT the same as the samples that produced an alignment: a
                               sample with no spike-in reads at all must still appear, with
                               zeros. Taking the sample set from the alignment would make
                               "no reads" indistinguishable from "not measured".
    * `window_relation`      — `(feature_idx, parent_feature_idx, position, stop_position)`.
                               The annotation windows, half-open, exactly as
                               `reference_annotation` stores them.
    * `parent_length_relation` — `(feature_idx, sequence_length_bp)` for the PARENTS.

    Output columns: `prep_sample_idx, feature_idx, covered_bases, feature_length,
    occurrences, mean_depth`. ONE row per (sample, feature) — see the aggregation note
    below; `parent_feature_idx` is deliberately NOT an output column, because a feature
    with several occurrences can sit on more than one parent and the value would not be
    single-valued. It is reference metadata, recoverable from `reference_annotation`, not
    part of the measurement.

    **A feature may have MORE THAN ONE window, and they are summed, not averaged.** A
    feature is a SEQUENCE and an annotation is an OCCURRENCE of it at a place, so one
    feature_idx legitimately appears at N windows: a bacterial 16S rRNA gene occurs in
    5-7 byte-identical copies, which canonically hash to one feature_idx. The depth of
    "that sequence" in the sample is then

        sum(covered_bases over occurrences) / sum(feature_length over occurrences)

    and NOT the mean of the per-occurrence means — averaging means would weight a 200 bp
    occurrence the same as a 2000 bp one. `occurrences` is emitted so the multiplicity is
    visible rather than silently folded away.

    The output is DENSE: every (sample, feature) pair gets a row, and one with no covering
    read carries `covered_bases = 0` / `mean_depth = 0.0` — an explicit zero, not an absent
    row. A feature table must distinguish "measured, and it was zero" from "not measured":
    a spike-in that failed to amplify and one that was never in the pool are different
    facts, and dropping the row makes them identical.
    """
    if depth_mode not in (DEPTH_MODE_INCLUDE_DELETIONS, DEPTH_MODE_EXCLUDE_DELETIONS):
        raise ValueError(f"unknown depth_mode {depth_mode!r}")

    # 1. Gate the alignment on the PARENT, before any windowing. See the module docstring
    #    — doing this after windowing deletes the junction-spanning reads.
    conn.execute(
        f"CREATE OR REPLACE TEMP VIEW _cov_gated AS "
        f"SELECT * FROM {alignment_relation} "
        f"WHERE {MAPPED_PRIMARY_EXPR} "
        f"  AND {IDENTITY_EXPR} >= {min_identity} "
        f"  AND {ALIGNED_FRACTION_EXPR} >= {min_aligned_fraction}"
    )

    # 2. One per-base depth array per (sample, parent). `reference_length` must be a plain
    #    per-row expression — aggregates cannot nest — and it is constant within the group,
    #    so it rides along in the GROUP BY.
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW _cov_parent_depth AS "
        "SELECT g.prep_sample_idx, "
        "       g.parent_feature_idx, "
        "       compute_coverage_depth(g.position, g.stop_position, g.cigar, "
        f"                             pl.sequence_length_bp, '{depth_mode}') AS depth "
        f"FROM _cov_gated g "
        f"JOIN {parent_length_relation} pl ON pl.feature_idx = g.parent_feature_idx "
        "GROUP BY g.prep_sample_idx, g.parent_feature_idx, pl.sequence_length_bp"
    )

    # 3. Cut each window out of its parent's array.
    #
    #    `list_slice` is 1-based INCLUSIVE; our windows are half-open, so the end index is
    #    `stop_position - 1`. Getting this wrong drops the feature's last base and raises
    #    nothing — the same half-open/closed trap the annotation ingest converts once, at
    #    the boundary, to avoid.
    #
    #    (sample x window) is the CROSS product — that is what makes the table dense — and
    #    the depth is LEFT JOINed onto it. An inner join here would silently drop every
    #    (sample, feature) with no covering read, which is precisely the "measured zero"
    #    vs "not measured" distinction the docstring promises to keep.
    conn.execute(
        f"CREATE OR REPLACE TABLE {out_relation} AS "
        # (sample x window) is the CROSS product, and the depth is LEFT JOINed onto it.
        # An inner join would silently drop every (sample, feature) with no covering read.
        "WITH per_window AS ("
        "  SELECT s.prep_sample_idx, "
        "         w.feature_idx, "
        "         coalesce("
        "             list_sum(list_slice(pd.depth, w.position, w.stop_position - 1)), 0"
        "         )::BIGINT AS covered_bases, "
        "         (w.stop_position - w.position)::BIGINT AS window_length "
        f"  FROM {sample_relation} s "
        f"  CROSS JOIN {window_relation} w "
        "  LEFT JOIN _cov_parent_depth pd "
        "         ON pd.parent_feature_idx = w.parent_feature_idx "
        "        AND pd.prep_sample_idx = s.prep_sample_idx"
        ") "
        # Fold the occurrences of one feature together: SUM the bases and SUM the lengths,
        # then divide. Averaging the per-occurrence means would be wrong (it weights a
        # short occurrence like a long one).
        "SELECT prep_sample_idx, "
        "       feature_idx, "
        "       sum(covered_bases)::BIGINT AS covered_bases, "
        "       sum(window_length)::BIGINT AS feature_length, "
        "       count(*)::BIGINT AS occurrences, "
        "       sum(covered_bases)::DOUBLE / sum(window_length) AS mean_depth "
        "FROM per_window "
        "GROUP BY prep_sample_idx, feature_idx "
        "ORDER BY prep_sample_idx, feature_idx"
    )
