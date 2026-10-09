"""Teardown helpers: the entity-graph sweep, the by-idx primitive, and the
teardowns composed from them.

The sweep deletes by parent FK rather than by row idx, so a caller needs no
per-row bookkeeping and a row nothing recorded — one a trigger or a cascade
produced — goes with the rest. It covers the study / biosample / prep_sample
graph only, and the caller supplies those three idx lists itself, because an
entity may legitimately carry no study link.

A caller that owns parents above that graph — pools, runs, principals — either
deletes them with delete_idxs once the sweep has returned, or calls one of the
composed teardowns here, which take a whole shape at once: delete_principal for
a principal and its user row, teardown_ena_study_graph for an ENA import's
studies together with the runs and pools they registered.
"""

from collections.abc import Iterable

import asyncpg

STUDY = "study"
BIOSAMPLE = "biosample"
PREP_SAMPLE = "prep_sample"

# A column keyed this way names a genome rather than an entity, and is matched
# through the genomes that belong to the caller's prep_samples. Only a
# qiita-sourced genome carries a prep_sample_idx, so an external genbank or
# refseq genome is never in range.
GENOME_OF_PREP_SAMPLE = "genome_of_prep_sample"

# The sweep, in delete order. Each entry is a table and the columns it is keyed
# on; a table with several keys is matched on any of them. The tier-1 tables are
# referenced by the tier-0 ones, so the tiers run in this order; order within a
# tier is free.
#
# Hard-coded rather than derived from the catalog so a reader can see exactly
# what a teardown touches. A migration adding a table that carries one of the key
# columns below registers it here in the same PR, or names it in
# UNSWEPT_ENTITY_TABLES with the reason: a parity test compares this list against
# the live schema, so one forgotten fails on the schema alone rather than waiting
# for something to seed a row into it. A table that reaches an entity through some
# other column name is outside what that test sees.
SWEEP_TIERS = (
    (
        ("alignment_sample", (("prep_sample_idx", PREP_SAMPLE),)),
        (
            "assembly_membership",
            (("genome_idx", GENOME_OF_PREP_SAMPLE), ("prep_sample_idx", PREP_SAMPLE)),
        ),
        ("assembly_sample", (("prep_sample_idx", PREP_SAMPLE),)),
        ("biosample_field_exception", (("biosample_idx", BIOSAMPLE),)),
        ("biosample_metadata", (("biosample_idx", BIOSAMPLE),)),
        ("biosample_to_study", (("biosample_idx", BIOSAMPLE), ("study_idx", STUDY))),
        ("block_member", (("prep_sample_idx", PREP_SAMPLE),)),
        ("ena_import_batch_item", (("study_idx", STUDY),)),
        ("exported_feature", (("genome_idx", GENOME_OF_PREP_SAMPLE),)),
        ("exported_identifier", (("prep_sample_idx", PREP_SAMPLE),)),
        ("feature_genome", (("genome_idx", GENOME_OF_PREP_SAMPLE),)),
        ("mask_sample", (("prep_sample_idx", PREP_SAMPLE),)),
        ("prep_sample_field_exception", (("prep_sample_idx", PREP_SAMPLE),)),
        ("prep_sample_metadata", (("prep_sample_idx", PREP_SAMPLE),)),
        ("prep_sample_to_study", (("prep_sample_idx", PREP_SAMPLE), ("study_idx", STUDY))),
        ("sequence_range", (("prep_sample_idx", PREP_SAMPLE),)),
        ("sequenced_sample", (("prep_sample_idx", PREP_SAMPLE),)),
        ("study_access", (("study_idx", STUDY),)),
        ("study_tag_to_study", (("study_idx", STUDY),)),
        ("syndna_read_count", (("prep_sample_idx", PREP_SAMPLE),)),
    ),
    (
        ("biosample_study_field", (("study_idx", STUDY),)),
        ("genome", (("prep_sample_idx", PREP_SAMPLE),)),
        ("prep_sample_study_field", (("study_idx", STUDY),)),
    ),
)

# prep_sample references biosample, and the link rows referencing study are gone
# by the time this runs, so the entities go in this order.
ENTITY_DELETE_ORDER = (PREP_SAMPLE, BIOSAMPLE, STUDY)

# Not swept, each for its own reason. A work ticket references a study or a
# prep_sample, so a standing one fails the entity delete; teardown_entity_graph's
# docstring states when the caller has to clear it. An exclusion names its genome
# with a bare BIGINT and is meant to outlive it, but it does reference the
# principals who recorded and unblocked it, so a caller that touched one must
# clear the exclusion before delete_principal.
UNSWEPT_ENTITY_TABLES = frozenset({"work_ticket", "reference_exclusion"})

_ENTITY_KEY_COLUMNS = {
    STUDY: "study_idx",
    BIOSAMPLE: "biosample_idx",
    PREP_SAMPLE: "prep_sample_idx",
}
_GENOME_KEY_COLUMN = "genome_idx"

# An ENA import names each sequencing run "<study accession>:<platform>", which
# ena_import/registration.py composes, so a study's runs are matched on the
# accession and that separator.
_ENA_RUN_ID_LIKE = "{accession}:%"

_KEY_BY_COLUMN = {column: key for key, column in _ENTITY_KEY_COLUMNS.items()}
_KEY_BY_COLUMN[_GENOME_KEY_COLUMN] = GENOME_OF_PREP_SAMPLE
_ENTITY_TABLES = frozenset(_ENTITY_KEY_COLUMNS)


class EntityGraphNotSweptError(AssertionError):
    """Rows survived a teardown for the entities it was given.

    Carries the table and the surviving count, so a missed table is named
    rather than surfacing later as a foreign-key violation. The tables in
    UNSWEPT_ENTITY_TABLES sit outside the range checked; what leaving one of
    them standing costs differs per table, and its comment says which.
    """

    def __init__(self, table: str, column: str, surviving: int) -> None:
        self.table = table
        self.column = column
        self.surviving = surviving
        super().__init__(
            f"{surviving} row(s) survive in qiita.{table} for the swept entities"
            f" (matched on {column}); the sweep list is missing this table"
        )


def _reject_non_identifiers(caller: str, *names: str) -> None:
    """Raise unless every name is a bare identifier.

    These names are interpolated into the statement rather than bound, which
    no placeholder can do for a table or a column.
    """
    for name in names:
        if not name.isidentifier():
            raise ValueError(f"{caller} rejects a non-identifier name: {name!r}")


def _as_idx_list(idxs: int | Iterable[int]) -> list[int]:
    """Normalise a scalar idx, or an iterable of them, into a list.

    Materialising here is what lets an empty run be told from a non-empty one:
    an unconsumed iterator reads as truthy whatever it holds.
    """
    if isinstance(idxs, int):
        return [idxs]
    return list(idxs)


async def delete_idxs(pool: asyncpg.Pool, table: str, idxs: int | Iterable[int]) -> None:
    """Delete rows by idx from qiita.<table>.

    `idxs` may be a scalar int or an iterable of ints; an empty iterable is a
    no-op. The scalar form is normalised so callers can pass a single
    auto-seeded idx without wrapping it in a list. `table` is interpolated into
    the statement, so it must be a literal the caller wrote, never input.
    """
    _reject_non_identifiers("delete_idxs", table)
    named = _as_idx_list(idxs)
    if not named:
        return
    await pool.execute(
        f"DELETE FROM qiita.{table} WHERE idx = ANY($1::bigint[])",
        named,
    )


async def _fetch_genome_idxs(pool: asyncpg.Pool, prep_sample_idxs: list[int]) -> list[int]:
    """Return the idxs of the genomes these prep_samples produced.

    Resolved before the sweep runs, because the sweep deletes qiita.genome and
    the genome-keyed tables can no longer be matched once it has.
    """
    if not prep_sample_idxs:
        return []
    rows = await pool.fetch(
        "SELECT genome_idx FROM qiita.genome WHERE prep_sample_idx = ANY($1::bigint[])",
        prep_sample_idxs,
    )
    genome_idxs = [row["genome_idx"] for row in rows]
    return genome_idxs


async def _entity_keyed_candidates(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    """Return every (table_name, column_name) in qiita keyed on an entity or a genome.

    Base tables only: a view carrying one of these columns would report its rows
    as survivors, and the entity skip matches on table name, so a view over an
    entity table carries a different one and slips past it.

    Ordered by table name, then column name, so that a caller raising on the
    first survivor names the same table and column every run when several
    survive at once.
    """
    rows = await pool.fetch(
        "SELECT c.table_name, c.column_name FROM information_schema.columns c"
        "  JOIN information_schema.tables t"
        "    ON t.table_schema = c.table_schema AND t.table_name = c.table_name"
        " WHERE c.table_schema = 'qiita' AND t.table_type = 'BASE TABLE'"
        "   AND c.column_name = ANY($1::text[])"
        " ORDER BY c.table_name, c.column_name",
        [*_ENTITY_KEY_COLUMNS.values(), _GENOME_KEY_COLUMN],
    )
    return rows


async def _sweep_table(
    pool: asyncpg.Pool,
    table: str,
    keys: tuple[tuple[str, str], ...],
    idxs: dict[str, list[int]],
) -> None:
    """Delete one table's rows for the named entities, matching any of its keys."""
    _reject_non_identifiers("_sweep_table", table, *(column for column, _key in keys))
    clauses: list[str] = []
    args: list[list[int]] = []
    for column, key in keys:
        named = idxs[key]
        if not named:
            continue
        args.append(named)
        clauses.append(f"{column} = ANY(${len(args)}::bigint[])")
    if not clauses:
        return
    await pool.execute(f"DELETE FROM qiita.{table} WHERE " + " OR ".join(clauses), *args)


async def assert_entity_graph_swept(
    pool: asyncpg.Pool,
    *,
    study_idxs: list[int],
    biosample_idxs: list[int],
    prep_sample_idxs: list[int],
    genome_idxs: list[int] | None = None,
) -> None:
    """Raise if any table keyed on an entity or a genome still holds rows for them.

    Discovers the tables from the catalog rather than the sweep list, matching on
    the four key column names above, so a table carrying one of them and
    forgotten in the sweep list fails the first time a test touches it. A table
    that reaches an entity through some other column name is not checked. The
    entity tables themselves are skipped, since they are deleted after this
    runs.

    `genome_idxs` names the genomes whose derived rows are checked too. They
    cannot be looked up here: qiita.genome is already gone by the time this
    runs, so the caller resolves them first.
    """
    idxs = {
        STUDY: study_idxs,
        BIOSAMPLE: biosample_idxs,
        PREP_SAMPLE: prep_sample_idxs,
        GENOME_OF_PREP_SAMPLE: list(genome_idxs or ()),
    }
    candidates = await _entity_keyed_candidates(pool)
    for row in candidates:
        table, column = row["table_name"], row["column_name"]
        # prep_sample is the only entity table this query reaches — study and
        # biosample key on a column named `idx`, which it does not ask for —
        # and it carries a biosample_idx of its own, which would read as a
        # survivor though a caller that named it goes on to delete it. One the
        # caller did not name is skipped here and fails the biosample delete
        # instead. qiita.genome is not skipped: the sweep deletes it, so a row
        # left behind is exactly what this is here to catch.
        if table in _ENTITY_TABLES or table in UNSWEPT_ENTITY_TABLES:
            continue
        _reject_non_identifiers("assert_entity_graph_swept", table, column)
        named = idxs[_KEY_BY_COLUMN[column]]
        if not named:
            continue
        surviving = await pool.fetchval(
            f"SELECT count(*) FROM qiita.{table} WHERE {column} = ANY($1::bigint[])",
            named,
        )
        if surviving:
            raise EntityGraphNotSweptError(table, column, surviving)


async def teardown_entity_graph(
    pool: asyncpg.Pool,
    *,
    study_idxs: list[int],
    biosample_idxs: list[int],
    prep_sample_idxs: list[int],
) -> None:
    """Delete these entities and everything hanging off them.

    Sweeps each tier in order, verifies nothing survived, then deletes the
    entities themselves. The caller keeps its own parents and deletes them
    after this returns.

    Work tickets go the other way: a ticket references its study or its
    prep_sample under RESTRICT, so the caller must clear its own tickets
    before calling this, or the entity delete at the end raises.
    """
    idxs = {
        STUDY: list(study_idxs),
        BIOSAMPLE: list(biosample_idxs),
        PREP_SAMPLE: list(prep_sample_idxs),
    }
    # Resolved up front: the tier that deletes qiita.genome runs below, and the
    # genome-keyed tables cannot be matched once it has.
    idxs[GENOME_OF_PREP_SAMPLE] = await _fetch_genome_idxs(pool, idxs[PREP_SAMPLE])
    for tier in SWEEP_TIERS:
        for table, keys in tier:
            await _sweep_table(pool, table, keys, idxs)
    await assert_entity_graph_swept(
        pool,
        study_idxs=idxs[STUDY],
        biosample_idxs=idxs[BIOSAMPLE],
        prep_sample_idxs=idxs[PREP_SAMPLE],
        genome_idxs=idxs[GENOME_OF_PREP_SAMPLE],
    )
    for table in ENTITY_DELETE_ORDER:
        await delete_idxs(pool, table, idxs[table])


async def delete_principal(pool: asyncpg.Pool, principal_idxs: int | Iterable[int]) -> None:
    """Delete these principals and their user rows.

    Runs last in a teardown: every table the caller created referencing a
    principal must already be gone, since those references are RESTRICT.
    """
    named = _as_idx_list(principal_idxs)
    if not named:
        return
    await pool.execute("DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])", named)
    await delete_idxs(pool, "principal", named)


async def resolve_ena_study_idxs(
    pool: asyncpg.Pool, accessions: Iterable[str], *, by_ena_accession: bool = False
) -> list[int]:
    """Return the idxs of the studies registered under these accessions.

    Matches bioproject_accession, and ena_study_accession as well when
    `by_ena_accession` is set, for a caller that holds only the accession the
    import echoed back. An accession no study carries contributes nothing.
    """
    named = list(accessions)
    if not named:
        return []
    rows = await pool.fetch(
        "SELECT idx FROM qiita.study"
        " WHERE bioproject_accession = ANY($1::text[])"
        "    OR ($2 AND ena_study_accession = ANY($1::text[]))",
        named,
        by_ena_accession,
    )
    study_idxs = [row["idx"] for row in rows]
    return study_idxs


async def teardown_ena_study_graph(
    pool: asyncpg.Pool,
    *,
    study_idxs: Iterable[int],
    run_accessions: Iterable[str],
    extra_biosample_idxs: Iterable[int] = (),
) -> None:
    """Delete these studies, the entities linked to them, and their ENA runs.

    `run_accessions` names the study accessions whose sequencing runs go too,
    matched on the instrument_run_id prefix an import composes from them; a
    study whose runs are not wanted passes none. Resolving no study at all
    leaves the runs standing, since nothing is then in range to clear the
    sequenced_samples that reference their pools. `extra_biosample_idxs` names
    biosamples that must go although no link to these studies survives, which
    is how a biosample two studies share is dropped once. Every prep_sample on
    such a biosample has to be reachable from `study_idxs`: the sweep is given
    no prep_samples of its own for it, so one belonging to a study outside the
    call fails the biosample delete with a bare foreign-key error.
    """
    named_studies = list(study_idxs)
    # The link rows go with the sweep, so the entities they name are resolved
    # while those rows still exist.
    ps_rows = await pool.fetch(
        "SELECT DISTINCT prep_sample_idx FROM qiita.prep_sample_to_study"
        " WHERE study_idx = ANY($1::bigint[])",
        named_studies,
    )
    bs_rows = await pool.fetch(
        "SELECT DISTINCT biosample_idx FROM qiita.biosample_to_study"
        " WHERE study_idx = ANY($1::bigint[])",
        named_studies,
    )
    # The runs go only when a study does: their pools are referenced by the
    # sequenced_samples the sweep removes, and it removes none when no
    # prep_sample is in range, so the pool delete below would fail on them.
    run_idxs: list[int] = []
    if named_studies:
        run_rows = await pool.fetch(
            "SELECT idx FROM qiita.sequencing_run WHERE instrument_run_id LIKE ANY($1::text[])",
            [_ENA_RUN_ID_LIKE.format(accession=accession) for accession in run_accessions],
        )
        run_idxs = [row["idx"] for row in run_rows]
    # A work ticket is never swept. This clears the pool-scoped ones, which
    # reference the pool under RESTRICT, ahead of the pool delete below. A ticket
    # carries exactly one scope target, so a study- or prep_sample-scoped one is
    # not matched here; teardown_entity_graph's docstring names whose job it is.
    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE sequenced_pool_idx IN"
        " (SELECT idx FROM qiita.sequenced_pool WHERE sequencing_run_idx = ANY($1::bigint[]))",
        run_idxs,
    )
    biosample_idxs = {row["biosample_idx"] for row in bs_rows} | set(extra_biosample_idxs)
    await teardown_entity_graph(
        pool,
        study_idxs=named_studies,
        biosample_idxs=sorted(biosample_idxs),
        prep_sample_idxs=sorted({row["prep_sample_idx"] for row in ps_rows}),
    )
    await pool.execute(
        "DELETE FROM qiita.sequenced_pool WHERE sequencing_run_idx = ANY($1::bigint[])",
        run_idxs,
    )
    await delete_idxs(pool, "sequencing_run", run_idxs)
