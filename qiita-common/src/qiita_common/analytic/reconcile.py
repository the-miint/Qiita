"""The de novo arm, and how it is reconciled with the reference arm.

A **combined** table estimates over two alignment runs at once: the cohort aligned
against a reference, and each sample aligned against its own assembled contigs. This
module owns what is true only of the second arm and of the join between them, with
one exception noted at `coverage._denovo_survivor_parts`, which an import cycle
keeps where it is. Absent it, every relation and every statement here is skipped and the analytic
is the reference-only one.

**Precedence: the de novo arm wins a read outright.** A read the de novo arm placed
on a contig of its own sample contributes only there; the reference arm contributes
exactly the reads the de novo arm did not place. That is one DELETE
(`denovo_alignment_statements`), applied to the reference slice as it is staged, so
every reader after it — the coverage filter, the roll-up diagnostics, woltka's input
— sees the reconciled arm without knowing a reconciliation happened.

**Three consequences of precedence, each of which looks like a result rather than
an error:**

* Reference genomes lose breadth. Reads removed from the reference arm no longer
  cover it, so a genome that clears `coverage_threshold` in a reference-only table
  over the same cohort can drop out of the combined one.
* The two arms were not gated alike, so precedence decides between placements two
  different admission rules produced. `align_sharded` filters per SAM record at a
  floor that varies by aligner (`_MIN_SEQUENCE_IDENTITY_*`, which carries why);
  `align_denovo` filters a read POOLED over its records against one contig, at a
  threshold its workflow takes as a parameter. Nothing here re-judges either — both
  ran before a row was persisted — so a read absent from one arm is absent because
  that producer did not admit it, not because this module dropped it.

* A read the de novo arm won can end up counted on NEITHER arm. Precedence runs at
  staging and the breadth filter runs after it, and both arms inner-join the survivor
  set — so if the qiita genome that won a read then fails `coverage_threshold`, the
  read is gone from the de novo arm by the filter and from the reference arm by
  precedence, even where a reference-only table would have kept it on a surviving
  reference genome. Reordering the two would not fix it, it would just move which arm
  loses the read; what decides it is that precedence is about placements and the
  filter is about genomes. Filtering FIRST would not undo it either — it would keep
  the read on the reference arm and leave that genome's breadth computed from reads
  it no longer holds, which is a different anomaly rather than this one moved.

**What is counted is unchanged by any of this.** `woltka_ogu` splits a read across
its distinct `reference` values after the map join, so a read's several rows within
one arm — secondaries, supplementaries, shard fan-out — collapse to what that read
contributed before, against the genome they share. (What a read contributes is per
SEGMENT, so a mate pair is 2.0, not 1.0; `docs/duckdb-miint.md` carries that.)
Precedence removes a read's rows from one arm entirely, never some of them, which is
what keeps the total the same.
"""

from __future__ import annotations

from ..assembly_constants import BIN_QUALITY_SCORE_COLUMNS
from .relations import (
    ALIGNMENT_TABLE,
    DENOVO_ALIGNMENT_TABLE,
    DENOVO_CONTIG_LENGTHS_TABLE,
    DENOVO_COVERAGE_ALIGNMENTS_VIEW,
    DENOVO_GENOME_QUALITY_TABLE,
    DENOVO_MAP_TABLE,
    GENOME_LENGTHS_TABLE,
)
from .stage import ALIGNMENT_COLUMNS


def denovo_map_join(alias: str) -> str:
    """The extra ON term a join against `DENOVO_MAP_TABLE` carries when its left side
    has a sample: the SAMPLE, as well as the contig. `alias` is that side's table
    alias; the map side is always `m`.

    The one join that omits it is `denovo_genome_lengths_insert_sql`, whose left side
    is the deduplicated lengths and has no sample column — deliberately, and it says
    why.

    A function rather than a literal at each site, and taking the alias rather than
    baking one in, because the three call sites alias their left relation differently
    and a constant that fixed the spelling would be copied out by hand at the others
    — which is the drift it exists to prevent.

    Dropping the term is not a bind error, which is why it is shared at all: two
    cohort samples that assemble byte-identical contigs share one content-addressed
    `feature_idx` under two genomes, so the contig-only join returns both rows and
    credits half of one sample's read to the other sample's genome.
    """
    return f" AND {alias}.prep_sample_idx = m.prep_sample_idx"


def denovo_map_table_sql(source: str) -> str:
    """Stage the de novo feature->genome map from `source` — a relation keyed
    `(prep_sample_idx, feature_idx, genome_idx)` — into `DENOVO_MAP_TABLE`.

    The rename is `map_table_sql`'s, to the column names `genome_coverage` requires,
    plus the sample key that makes this map a different join from the reference one.

    **`source` must already be scoped to ONE assembly run.** A contig assembled by two
    runs of the same sample is one `feature_idx` under two genomes — one per run, which
    is what an assembly identity per run means — so an unscoped map double-counts a
    sample against itself the same way the unscoped join double-counts it against its
    neighbours.

    DISTINCT collapses only EXACT `(sample, contig, genome)` repeats. The membership
    key carries `(kind, bin_id)`, so two rows can share a contig; they are deduped
    here only when they also agree on the genome, which — the mint being keyed on
    `(prep_sample_idx, processing_idx, kind, bin_id)` — means only when a stored
    `genome_idx` disagrees with the `(kind, bin_id)` it was minted from. Nothing
    constrains that column against its own key, so this is the cheap guard for it.

    **It does NOT collapse a contig that belongs to two genomes of one run**, and that
    is not a defect: an assembler can emit one sequence as both a circular LCG record
    and a member of a refined bin, and `assembly_hash` keeps both rows deliberately.
    Content-addressing gives them one `feature_idx` and the mint gives them two
    genomes, so a read there fans out and `woltka_ogu` splits it — the same treatment
    `ogu` describes for a plasmid under several reference genomes. Whether that split
    is the right assay answer for an LCG/MAG overlap is a question for the assay
    owner, not something this staging step should quietly decide.
    """
    return (
        f"CREATE TABLE {DENOVO_MAP_TABLE} AS "
        f"SELECT DISTINCT prep_sample_idx, feature_idx AS contig_id, "
        f"genome_idx AS genome_id "
        f"FROM {source}"
    )


def denovo_genome_quality_table_sql(source: str) -> str:
    """Stage the de novo arm's per-genome CheckM scores from `source` — a relation
    keyed `(prep_sample_idx, genome_idx)` and carrying `BIN_QUALITY_SCORE_COLUMNS` —
    into `DENOVO_GENOME_QUALITY_TABLE`.

    The rename is `denovo_map_table_sql`'s, to the `genome_id` the other de novo
    relations key on, so a consumer joins this to the map without a second spelling
    for the same column. The score columns come from the shared constant rather than
    a second spelling here, so widening what the resolver stages cannot leave this
    silently projecting the old pair.

    **The scores pass through untouched, and a NULL is not a zero.** The resolver
    LEFT-joins, so a genome CheckM did not score arrives with both scores NULL and
    keeps them. A predicate over these columns has to say what it does with the
    unscored: SQL three-valued logic drops them from a bare `completeness >= x`,
    which is a decision about genomes nobody measured, taken by omission.

    **`source` must already be scoped to ONE assembly run**, for the reason
    `denovo_map_table_sql` gives: a subject key is per-run, and a contig assembled
    by two runs is one feature under two genomes.

    No `DISTINCT`, where that sibling carries one: its source is per contig and can
    hold exact repeats, this one is already per genome. A duplicate here would mean
    the resolver's join fanned out, which is a load to fix rather than a repeat to
    collapse — the resolver states that invariant at the join.
    """
    scores = ", ".join(BIN_QUALITY_SCORE_COLUMNS)
    return (
        f"CREATE TABLE {DENOVO_GENOME_QUALITY_TABLE} AS "
        f"SELECT prep_sample_idx, genome_idx AS genome_id, {scores} "
        f"FROM {source}"
    )


def denovo_contig_lengths_table_sql() -> str:
    """Create `DENOVO_CONTIG_LENGTHS_TABLE` empty, for the cohort's contig lengths.

    Empty and then inserted into, rather than created from the first stream, because
    the de novo lengths arrive as **one stream per cohort sample** — the assembly
    read-back is scoped to a single `(prep_sample_idx, processing_idx)` run — where
    the reference arm has one whole-reference stream. A create-from-first shape would
    need a different statement for the first sample than for the rest, and would have
    no form at all for a cohort whose samples all assembled nothing.
    """
    return (
        f"CREATE TABLE {DENOVO_CONTIG_LENGTHS_TABLE} "
        f"(feature_idx BIGINT, sequence_length_bp BIGINT)"
    )


def denovo_contig_lengths_insert_sql(source: str) -> str:
    """Append one sample's assembled-contig lengths from `source` (the caller's
    per-run `assembled_sequence` stream) to `DENOVO_CONTIG_LENGTHS_TABLE`.

    **Whole-run, with no narrowing to the contigs that sample aligned to**, for the
    reason `genome_lengths_table_sql` gives: the denominator is the genome's full
    length, and narrowing it raises every genome's breadth.
    """
    return (
        f"INSERT INTO {DENOVO_CONTIG_LENGTHS_TABLE} "
        f"SELECT feature_idx, sequence_length_bp FROM {source}"
    )


def denovo_genome_lengths_insert_sql() -> str:
    """Roll the cohort's contig lengths up to per-genome denominators and append them
    to `GENOME_LENGTHS_TABLE`, beside the reference arm's.

    Appended rather than unioned into a relation of its own: a qiita genome and a
    reference genome are both rows of `qiita.genome`, so the two arms' `genome_id`
    values are drawn from one space and cannot collide, and every reader downstream
    wants one denominator per genome regardless of which arm produced it.

    **Deduplicated first.** The lengths arrive as one stream per sample and a contig
    assembled by two of them is one content-addressed feature, so it is streamed twice;
    summing the raw union would give the genomes holding it a denominator inflated by
    an entire duplicate contig, depressing their breadth. Deduplicating before the
    join, rather than distinct-ing after it, is what lets a contig shared by two
    samples still count once for EACH of the two genomes that legitimately contain it.

    The DISTINCT is over `(feature_idx, sequence_length_bp)` rather than the feature
    alone, so two rows for one contig collapse only if they agree on its length. They
    do — `feature_idx` is the content hash and the length is that content's — and a
    row that ever disagreed would be a contradiction this is the wrong place to
    resolve silently.
    """
    return (
        f"INSERT INTO {GENOME_LENGTHS_TABLE} "
        f"SELECT m.genome_id AS genome_id, SUM(l.sequence_length_bp) AS total_length "
        f"FROM (SELECT DISTINCT feature_idx, sequence_length_bp "
        f"FROM {DENOVO_CONTIG_LENGTHS_TABLE}) l "
        f"JOIN {DENOVO_MAP_TABLE} m ON l.feature_idx = m.contig_id "
        f"GROUP BY m.genome_id"
    )


def denovo_alignment_statements(source: str) -> tuple[str, ...]:
    """Stage the de novo slice from `source` and apply precedence, in that order.

    One sequence rather than two builders, for the reason `ogu_input_statements` is
    one: staging the second arm without applying precedence does not fail, it
    double-counts every read both arms placed — once against its contig and once
    against the reference genome — and returns a table whose totals are simply too
    large.

    Requires `ALIGNMENT_TABLE` and `DENOVO_MAP_TABLE` to exist already; both are named
    in the DELETE, so calling this too early is a bind error rather than a silent
    no-op.

    **The DELETE matches on `sequence_idx` alone**, which the lake mints globally
    unique across samples, so it is the read that is superseded rather than one
    sample's copy of a read id.

    **A de novo placement supersedes only if it can be rolled up.** The DELETE reads
    the slice through the map, so a read placed on a contig with no genome — a run
    whose membership predates the genome mint — falls back to its reference placement
    instead of being lost from both arms. Reads with no placement in either arm were
    never in either slice.
    """
    return (
        f"CREATE TABLE {DENOVO_ALIGNMENT_TABLE} AS "
        f"SELECT {', '.join(ALIGNMENT_COLUMNS)} FROM {source}",
        f"DELETE FROM {ALIGNMENT_TABLE} WHERE sequence_idx IN ("
        f"SELECT p.sequence_idx FROM {DENOVO_ALIGNMENT_TABLE} p "
        f"JOIN {DENOVO_MAP_TABLE} m ON p.feature_idx = m.contig_id"
        f"{denovo_map_join('p')})",
    )


def denovo_coverage_alignments_view_sql() -> str:
    """The de novo arm's aligned intervals, in the shape `coverage_alignments_view_sql`
    produces for the reference arm and for the same reasons — NULL coordinates excluded
    where the exclusion is visible, `prep_sample_idx` carried so a scope can group by it.

    A separate view rather than a `UNION ALL` with the reference one, because the two
    reach their genome through different maps; the union happens in the survivor set,
    after each arm has been rolled up through its own.
    """
    return (
        f"CREATE VIEW {DENOVO_COVERAGE_ALIGNMENTS_VIEW} AS "
        f"SELECT prep_sample_idx, feature_idx AS reference, position, stop_position "
        f"FROM {DENOVO_ALIGNMENT_TABLE} "
        f"WHERE position IS NOT NULL AND stop_position IS NOT NULL"
    )


def denovo_ogu_input_select_sql(*, survivor_relation: str | None, by_sample: bool) -> str:
    """The de novo arm's contribution to woltka's input, in the columns
    `ogu_input_table_sql` selects for the reference arm.

    `survivor_relation` is the already-built survivor set to join, or `None` when no
    threshold applies. `by_sample` adds the sample term that a per-sample survivor set
    requires — both mirror the reference arm's join exactly, because the survivor set
    is one relation covering both arms.
    """
    sql = (
        f"SELECT a.sequence_idx, a.prep_sample_idx, a.flags, m.genome_id AS reference "
        f"FROM {DENOVO_ALIGNMENT_TABLE} a "
        f"JOIN {DENOVO_MAP_TABLE} m ON a.feature_idx = m.contig_id"
        f"{denovo_map_join('a')}"
    )
    if survivor_relation is not None:
        sql += f" JOIN {survivor_relation} s ON m.genome_id = s.genome_id"
        if by_sample:
            sql += " AND a.prep_sample_idx = s.prep_sample_idx"
    return sql
