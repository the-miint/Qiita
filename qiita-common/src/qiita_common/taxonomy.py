"""The taxonomy rank shape, and the per-genome reduction its two consumers share.

**SQL text only, no `duckdb` import** — the same discipline `feature_table.py` keeps,
so `qiita-common` stays importable by anything that only needs the contract.

Two callers reduce a reference's per-feature taxonomy to one row per genome, for
different reasons: the shard planner tiles genomes by lineage so a shard holds
related organisms, and the client-side feature table writes a taxonomy sidecar keyed
by the same public handle as the table. They must agree about *which* member feature
speaks for a genome, because a genome that tiled under one lineage and published
another would be two different claims about one organism.

That representative is **the lowest `feature_idx` among the genome's classified
members**. Lowest for determinism (scan order must not decide it) and *classified*
because a genome is one organism whose contigs share one lineage: an exclusion that
blocks the lowest contig removes its taxonomy row, and letting that drag the healthy
siblings to unclassified would relocate a whole genome on the strength of one
curation decision.
"""

# Coarsest → finest. This is simultaneously the DuckLake `reference_taxonomy` column
# order, the order a lineage string concatenates in, and the order `RANK_PREFIXES`
# pairs with, so the three cannot be reordered independently.
RANK_COLUMNS = (
    "domain",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
    "strain",
)

# The prefix each rank carries in a source lineage, paired with RANK_COLUMNS by
# index. Restoring these is faithful rather than invented: reference ingest
# *enforces* this exact sequence by rank position and then strips exactly three
# characters when writing the columns, so the prefix is a function of position and
# putting it back recovers what was dropped. See the reference-load job's rank
# validation for the enforcement.
RANK_PREFIXES = ("d__", "p__", "c__", "o__", "f__", "g__", "s__", "t__")

# `class` and `order` are SQL keywords, so every reference to them as a column has to
# be quoted. Quoting all eight instead of just those two would be uniform but would
# make the identifiers case-sensitive against a schema that spells them lowercase —
# so the set is explicit.
_KEYWORD_RANKS = frozenset({"class", "order"})


def quote_rank(column: str) -> str:
    """`column` as it must be written in SQL — quoted only where it is a keyword."""
    return f'"{column}"' if column in _KEYWORD_RANKS else column


QUOTED_RANK_COLUMNS = tuple(quote_rank(column) for column in RANK_COLUMNS)


def rank_columns_sql(*, alias: str = "") -> str:
    """The eight rank columns as a select list, optionally table-qualified."""
    prefix = f"{alias}." if alias else ""
    return ", ".join(f"{prefix}{column}" for column in QUOTED_RANK_COLUMNS)


def prefixed_rank_columns_sql(*, alias: str = "") -> str:
    """The eight rank columns with their source prefixes restored, aliased back to
    their own names — `domain` becomes `'d__' || domain AS domain`.

    NULL survives as NULL (an absent rank is not `'d__'`), while an EMPTY rank
    becomes the bare prefix, which is what it was before ingest stripped it: a
    lineage may carry `p__` with nothing after it, and that is a different statement
    from "no phylum was reported".
    """
    prefix = f"{alias}." if alias else ""
    return ", ".join(
        f"CASE WHEN {prefix}{quoted} IS NULL THEN NULL"
        f" ELSE '{rank_prefix}' || {prefix}{quoted} END AS {quoted}"
        for quoted, rank_prefix in zip(QUOTED_RANK_COLUMNS, RANK_PREFIXES, strict=True)
    )


def genome_representative_taxonomy_select_sql(*, member_genome: str, taxonomy: str) -> str:
    """One row per genome: `genome_idx` plus the eight ranks of its representative
    member (see the module docstring for which member that is, and why).

    Both arguments are **relation expressions**, not table names — a name, a
    `read_parquet(...)`, or a parenthesized SELECT that renames columns — so a caller
    whose map is keyed differently converts at the call site rather than passing
    column names through. `member_genome` must yield `(feature_idx, genome_idx)` and
    `taxonomy` must yield `feature_idx` plus the eight rank columns.

    **Every genome in `member_genome` appears in the output**, including one no member
    of which is classified: it comes back with all eight ranks NULL rather than
    vanishing. A genome missing from a taxonomy artifact is indistinguishable from a
    genome absent from the table, so it has to be present and empty.

    The LEFT JOIN to `taxonomy` is deliberate in both places it appears: a feature
    with no taxonomy row is a *member* that cannot speak for the genome, not a
    member that disappears.
    """
    ranks = rank_columns_sql(alias="t")
    lineage = f"concat_ws(';', {ranks})"
    return (
        f"WITH member_rank AS ("
        f"  SELECT mg.genome_idx, mg.feature_idx, {ranks}, {lineage} AS lineage"
        f"    FROM {member_genome} mg"
        f"    LEFT JOIN {taxonomy} t ON t.feature_idx = mg.feature_idx"
        f"), representative AS ("
        f"  SELECT genome_idx, min(feature_idx) FILTER (WHERE lineage <> '') AS feature_idx"
        f"    FROM member_rank GROUP BY genome_idx"
        f") SELECT r.genome_idx, {rank_columns_sql(alias='m')}"
        f"    FROM representative r"
        f"    LEFT JOIN member_rank m"
        f"      ON m.genome_idx = r.genome_idx AND m.feature_idx = r.feature_idx"
    )


def genome_lineage_select_sql(*, member_genome: str, taxonomy: str) -> str:
    """`(genome_idx, lineage)` — the representative's ranks joined with `';'`.

    The shard planner's projection of the reduction above. A genome with no
    classified member yields NULL, which its caller reads as unclassified.

    **The string form loses information the column form keeps, which is why the
    published sidecar does not use it:** `concat_ws` *skips* NULLs rather than
    emitting an empty field, so a lineage missing a middle rank silently promotes
    every rank below it — `domain;family` is indistinguishable from a genome whose
    phylum and class really are those names. That is harmless for tiling, which only
    needs a stable sort key that groups relatives, and wrong for anything a reader
    interprets.
    """
    representative = genome_representative_taxonomy_select_sql(
        member_genome=member_genome, taxonomy=taxonomy
    )
    return (
        f"SELECT genome_idx, concat_ws(';', {rank_columns_sql()}) AS lineage"
        f"  FROM ({representative})"
    )
