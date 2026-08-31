"""Repository functions for qiita.assembly_membership and qiita.assembly_sample.

The assembly analogue of the reference_membership INSERT that
`qiita_control_plane.actions.library.write_membership` performs inline: a bulk,
idempotent link of a prep_sample's assembly RUN (processing_idx) contigs — each a
qiita.feature minted via the SHARED mint-features path — to the bin they belong to
(kind, bin_id; the kind value set is enumerated in
`qiita_common.assembly_constants`). The DuckDB-side JOIN (bin_map x manifest x
feature_map -> (kind, bin_id, feature_idx)) and the batch streaming live in the
library primitive; this module owns only the raw bulk-insert SQL, mirroring
repositories.processing owning qiita.mint_processing's call. It also owns
`assembly_genome_source_id`, the one definition of which qiita.genome an assembled
subject mints as — the inline write and the backfill both call it.

It also owns qiita.assembly_sample, the per-(processing_idx, prep_sample)
completion gate — the assembly twin of qiita.mask_sample / qiita.alignment_sample.
Its contract lives on `fetch_assembly_sample_state` below.
"""

import asyncpg
from qiita_common.api_paths import URL_PROCESSING_SAMPLE_STATUS
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import AssemblySampleState

from . import gate_state_literal, require_transaction

# The two `assembly_sample` states a consumer of contigs may proceed on, asserted
# against the Literal so a renamed member fails at import rather than matching no
# rows. Every other value, and absence, is a refusal — `fetch_assembly_sample_state`
# is the contract that says which is which.
ASSEMBLY_SAMPLE_COMPLETED = gate_state_literal("completed", AssemblySampleState)
ASSEMBLY_SAMPLE_NO_DATA = gate_state_literal("no_data", AssemblySampleState)


def assembly_genome_source_id(
    *, prep_sample_idx: int, processing_idx: int, kind: str, bin_id: str
) -> str:
    """The `qiita.genome.source_id` for one assembled subject, shared by the inline
    mint and the backfill so the two cannot disagree about which genome a contig
    belongs to.

    The tuple is the subject's identity minus the contig: one genome per
    ``(prep_sample_idx, processing_idx, kind, bin_id)``, which is
    `assembly_membership`'s PRIMARY KEY without ``feature_idx``. `processing_idx` is
    in it because a prep_sample re-assembled under different params yields different
    contigs, so its bins are different genomes.

    A hash of the tuple rather than the contig bytes: `qiita.genome.prep_sample_idx`
    is a scalar FK, so two prep_samples assembling a byte-identical single-contig
    genome would collapse onto one row that can record only one origin.

    `(source, source_id)` is UNIQUE, so a future change to this tuple mints NEW
    genomes rather than corrupting existing ones — the same property
    `alignment_definition` relies on. That is why there is no scheme discriminator.
    """
    return canonical_params_hash(
        {
            "prep_sample_idx": prep_sample_idx,
            "processing_idx": processing_idx,
            "kind": kind,
            "bin_id": bin_id,
        }
    ).hex()


async def insert_assembly_membership_rows(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    processing_idx: int,
    kinds: list[str],
    bin_ids: list[str],
    feature_idxs: list[int],
    genome_idxs: list[int],
) -> int:
    """Bulk-insert one chunk of assembly_membership rows; return the count of rows
    written or re-stamped.

    The four lists are positionally aligned: row i links
    ``(prep_sample_idx, processing_idx, kinds[i], bin_ids[i], feature_idxs[i])`` to
    ``genome_idxs[i]``, the genome its subject was minted as.

    ``DO UPDATE`` on the natural PK. The conflict fires on a workflow retried from
    the start and on any run over rows a backfill already created; under
    ``DO NOTHING`` those rows would keep whatever ``genome_idx`` they had, including
    the NULL a pre-mint row carries. What a NULL costs a reader is on the column's
    own comment. Re-stamping converges instead.

    **The caller must not hand this a duplicated key.** Postgres refuses to let one
    ``ON CONFLICT DO UPDATE`` touch a conflict target twice, which the ``DO NOTHING``
    form tolerated; `ASSEMBLY_MEMBERSHIP_JOIN_SQL` carries the ``DISTINCT`` that
    guarantees it. Caught below rather than left to leak: it arrives as SQLSTATE
    21000 (qiita-probed), which is outside `IntegrityConstraintViolationError`
    entirely, so the FK arm cannot cover it.

    That also decides what the return counts. ``RETURNING`` fires for updated rows
    too, so this is "rows this chunk wrote or re-stamped", NOT "rows newly linked" —
    a replay reports its full chunk rather than zero. `write_assembly_membership`
    derives its collapse report from the manifest, not from this, so nothing reads
    it as a novelty count.

    Wraps asyncpg.ForeignKeyViolationError into a ValueError so the caller surfaces
    a structured error instead of leaking the asyncpg exception (a feature_idx that
    isn't in qiita.feature, or a genome_idx that isn't in qiita.genome, means the
    caller's upstream produced inconsistent inputs).
    """
    if not feature_idxs:
        return 0
    try:
        rows = await conn.fetch(
            "INSERT INTO qiita.assembly_membership"
            " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx, genome_idx)"
            " SELECT $1, $2, k, b, f, g"
            " FROM unnest($3::text[], $4::text[], $5::bigint[], $6::bigint[])"
            "      AS t(k, b, f, g)"
            " ON CONFLICT (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)"
            " DO UPDATE SET genome_idx = EXCLUDED.genome_idx"
            " RETURNING feature_idx",
            prep_sample_idx,
            processing_idx,
            kinds,
            bin_ids,
            feature_idxs,
            genome_idxs,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        raise ValueError(
            "One or more feature_idx / genome_idx / prep_sample_idx / processing_idx "
            "values do not exist"
        ) from exc
    except asyncpg.CardinalityViolationError as exc:
        raise ValueError(
            "the same (kind, bin_id, feature_idx) was submitted twice in one chunk, "
            "which this upsert cannot resolve — the caller's query is missing its "
            "DISTINCT (see ASSEMBLY_MEMBERSHIP_JOIN_SQL)"
        ) from exc
    return len(rows)


# =============================================================================
# assembly_sample gate (per-(processing_idx, prep_sample) completion)
# =============================================================================
# One row per (run, sample). Read contract: `fetch_assembly_sample_state`.


async def create_assembly_sample_pending(
    conn: asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idx: int,
) -> None:
    """Materialize the `(processing_idx, prep_sample_idx)` gate row at 'pending'.

    Idempotent: the same params re-resolve to the same processing_idx, so a
    resumed or redriven ticket runs this again over whatever the last run left. It
    reopens 'no_data' and leaves 'completed' — the same asymmetry
    `upsert_assembly_sample_no_data` is guarded on, for the reason stated there.
    'no_data' says the run that wrote it assembled nothing; a new run of the same
    identity has not finished, so leaving it would have the gate answer for a run
    that is still going — and answer 'no_data' about a run that may go on to FAIL.

    'invalidated' is left standing for a third reason: a withdrawal is a judgement
    someone made about a stored contig set, and a redrive does not quietly
    overturn it. The re-run proceeds, and its terminal write is what refuses —
    see `upsert_assembly_sample_completed`.

    The reopen composes with 'pending' never being closed on a failure path: a run
    that ends 'no_data', followed by one that reopens the row and then FAILs,
    leaves 'pending' standing with the earlier terminal answer overwritten and no
    sweeper to restore it. Another re-run under the same params re-resolves the
    same key and can close it; nothing else will.

    Twin of `repositories.block.create_mask_sample_pending`, one sample at a time
    — assembly is per-sample-ticket, not a block fan-out. Those twins have no
    'no_data' state, so their unconditional DO NOTHING is the same rule with one
    arm.
    """
    require_transaction(conn)
    await conn.execute(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'pending')"
        " ON CONFLICT (processing_idx, prep_sample_idx) DO UPDATE SET state = 'pending'"
        "   WHERE qiita.assembly_sample.state = 'no_data'",
        processing_idx,
        prep_sample_idx,
    )


class AssemblySampleInvalidated(Exception):
    """Raised when an assembly run tries to complete a `(processing_idx,
    prep_sample)` pair whose previous run was withdrawn.

    Withdrawal is a judgement someone made about a stored contig set, so a
    redrive does not quietly overturn it: the fresh contigs land in DuckLake, the
    gate stays 'invalidated', and consumers keep refusing until someone restores
    the pair via PATCH /processing/{processing_idx}/sample-status. The
    alternative is a re-run under the same params — the params are what the
    identity hashes, so it IS the same run — re-completing what it was withdrawn
    for, with nothing said. Twin of
    `repositories.block.MaskSampleInvalidated`."""

    def __init__(self, *, processing_idx: int, prep_sample_idx: int) -> None:
        self.processing_idx = processing_idx
        self.prep_sample_idx = prep_sample_idx
        super().__init__(
            f"assembly run {processing_idx} for prep_sample {prep_sample_idx} was "
            "invalidated; an assembly run cannot complete it. Restore it via "
            f"PATCH {URL_PROCESSING_SAMPLE_STATUS.format(processing_idx=processing_idx)} "
            "once the output is trusted."
        )


async def upsert_assembly_sample_completed(
    conn: asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idx: int,
) -> None:
    """Write the `(processing_idx, prep_sample_idx)` gate row to 'completed'.

    The `finalize-assembly-sample` terminal action's writer. An upsert rather
    than an UPDATE so it stands alone on a ticket whose pending row was never
    written, and re-affirms 'completed' on a workflow retried from the start.

    Guarded only against 'invalidated', where `upsert_assembly_sample_no_data` is
    also guarded against 'completed': 'completed' is reachable from every state a
    RUN produces, a prior run's 'no_data' included. It is not reachable from the
    one state a PERSON produces — raises `AssemblySampleInvalidated` there, since
    the DO UPDATE leaves such a row alone rather than silently re-completing it.

    Raises on the empty return directly, where the mask twin
    (`repositories.block.upsert_mask_sample_completed`) re-reads the row first:
    this guard names one state, so an unmoved row has one explanation, while the
    twin's writers guard on two and have to tell withdrawal from idempotence.
    """
    require_transaction(conn)
    written = await conn.fetchval(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')"
        " ON CONFLICT (processing_idx, prep_sample_idx) DO UPDATE SET state = 'completed'"
        "   WHERE qiita.assembly_sample.state <> 'invalidated'"
        " RETURNING prep_sample_idx",
        processing_idx,
        prep_sample_idx,
    )
    if written is None:
        raise AssemblySampleInvalidated(
            processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )


async def upsert_assembly_sample_no_data(
    conn: asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idx: int,
) -> str | None:
    """Write the `(processing_idx, prep_sample_idx)` gate row to 'no_data'; return
    None when it was written, else the state the guard left standing.

    The other terminal writer: assembly_hash raised StepNoData, so the workflow
    stops before `finalize-assembly-sample` and the runner closes the gate here.

    The DO UPDATE is guarded on the row holding neither of the two states this
    write must not overturn, which is the asymmetry with
    `upsert_assembly_sample_completed` above:

      * 'completed' — an earlier run under the same processing_idx left contigs
        in DuckLake; a later run of that identity finding none does not remove
        them, so writing 'no_data' over it would make the gate deny rows that are
        there. The gate then reads 'completed' while the ticket that just ended
        reads NO_DATA over the same (run, sample).
      * 'invalidated' — someone withdrew this pair, and a re-run does not
        overturn that judgement (`AssemblySampleInvalidated` states why for the
        completing twin).

    Unlike the mask twin (`repositories.block.upsert_mask_sample_completed`) this
    does not raise on either: both standing states are still the answer a
    consumer asking for contigs needs, and this write is on a path that has
    already ended. The return value is what lets the caller say which one held —
    `runner._processing._record_assembly_gate_no_data` logs it.
    """
    require_transaction(conn)
    written = await conn.fetchval(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'no_data')"
        " ON CONFLICT (processing_idx, prep_sample_idx) DO UPDATE SET state = 'no_data'"
        "   WHERE qiita.assembly_sample.state NOT IN ('completed', 'invalidated')"
        " RETURNING prep_sample_idx",
        processing_idx,
        prep_sample_idx,
    )
    if written is not None:
        return None
    # Re-read rather than infer: the guard names two states and the caller's log
    # line has to say which one actually stands.
    return await fetch_assembly_sample_state(
        conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
    )


async def fetch_assembly_sample_state(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    processing_idx: int,
    prep_sample_idx: int,
) -> str | None:
    """Return the `(processing_idx, prep_sample_idx)` gate row's state, or None
    when no row exists.

    The assembly_sample gate contract (canonical statement; other consumers point
    here rather than restate it). Three states are written first-class by the
    assembly workflow: 'pending' when the runner mints the run identity, then
    'completed' at the terminal `finalize-assembly-sample` action, or 'no_data'
    when assembly_hash found no contig of any kind and the ticket ended NO_DATA.
    A fourth, 'invalidated', is written by an operator withdrawing a completed
    run whose contigs are not trustworthy — a judgement about one RUN, distinct
    from deprecating the CONFIG (`ProcessingStatus`).

    The invariant: completion is read from this state, never from the presence of
    qiita.assembly_membership or DuckLake rows — the assembly tail writes those
    across several entries, so a partial footprint is indistinguishable from a
    finished one by row presence alone. A consumer that needs contigs proceeds
    ONLY on 'completed', rejecting None (absence is never "assembled"), 'pending',
    'no_data' (there are none) and 'invalidated' (there are, and they are
    withdrawn). Expressing withdrawal as a state value rather than a column beside
    it is what makes that hold without every consumer growing a second check.
    {'completed', 'no_data', 'invalidated'} is the terminal set, for a consumer
    asking only whether the run is over.

    Terminal is per-run, not per-key: a re-run of the same identity can reopen a
    row this read reported terminal, so a consumer holding an earlier 'no_data'
    can read 'pending' on the next call. Which state reopens, and why, is on
    `create_assembly_sample_pending`.

    None also covers a sample the run never reached: a ticket whose masked
    pass-set is empty raises StepNoData in the runner's pre-loop input resolver,
    which runs before the processing_idx mint, so there is no key to write a row
    under. The identity itself may still exist — another sample's ticket under the
    same params mints the same processing_idx — so a consumer holding one can ask
    about such a sample and get None. The ticket carries that outcome; this gate
    does not.

    'pending' likewise outlives a ticket that FAILED or was cancelled. The only
    terminal writers are `finalize-assembly-sample` and the runner's StepNoData
    handler, and nothing sweeps, so the row goes on reading "not over" for a run
    that has ended — read the work_ticket for that, as in the None case above. It
    is a stale row rather than a wrong answer: a consumer that needs contigs reads
    'completed' alone, and no submit-time site refuses a re-run over a 'pending'
    row (qiita.alignment_sample's gate rows do gate the align planner; this gate
    has no such consumer), so a resubmission under the same params re-resolves the
    same key and closes it.

    Point-in-time read: no FOR UPDATE, no transaction requirement, and it accepts
    a pool or a connection — it gates a read, it does not finalize.
    """
    return await conn.fetchval(
        "SELECT state FROM qiita.assembly_sample"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        processing_idx,
        prep_sample_idx,
    )


async def fetch_assembly_sample_states(
    db: asyncpg.Pool | asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idx: list[int],
) -> dict[int, str]:
    """The gate state of every named sample under one run, as
    ``{prep_sample_idx: state}``. Samples with no row are ABSENT from the result,
    which is the None `fetch_assembly_sample_state` returns, spelled for a cohort.

    The bulk form, for a consumer deciding a whole cohort at once rather than
    asking per sample. The contract those states carry is on
    `fetch_assembly_sample_state` and is not repeated here.
    """
    rows = await db.fetch(
        "SELECT prep_sample_idx, state FROM qiita.assembly_sample"
        " WHERE processing_idx = $1 AND prep_sample_idx = ANY($2)",
        processing_idx,
        prep_sample_idx,
    )
    return {r["prep_sample_idx"]: r["state"] for r in rows}


# The de novo arm's feature -> genome row set, shared verbatim between the Parquet
# the compute job reads and the REST map a client reads — the assembly twin of
# `reference_membership.GENOME_MAP_PAIRS_SQL`, and shared for the same reason:
# the two drivers must not disagree about which contigs have a genome.
#
# `$1` is the prep_sample_idx COHORT (an array — the REST read passes one sample,
# the cohort export passes the lot), `$2` the processing_idx. **Both terms,
# always.** The pair is the assembly RUN, and a contig is content-addressed across
# runs and samples alike: dropping processing_idx returns one contig once per run
# that produced it, dropping prep_sample_idx returns it once per sample. Either way
# a consumer joining on the contig gets rows it did not ask for.
#
# `genome_idx IS NOT NULL` is the completeness filter, and the reason
# `count_assembly_membership_without_genome` exists beside it: a NULL is a run whose
# memberships predate the genome mint, and silently dropping those contigs would
# give their genomes a short denominator rather than an error.
_ASSEMBLY_GENOME_MAP_FROM = " FROM qiita.assembly_membership am"
_ASSEMBLY_GENOME_MAP_WHERE = (
    " WHERE am.prep_sample_idx = ANY($1) AND am.processing_idx = $2 AND am.genome_idx IS NOT NULL"
)
ASSEMBLY_GENOME_MAP_PAIRS_SQL = _ASSEMBLY_GENOME_MAP_FROM + _ASSEMBLY_GENOME_MAP_WHERE

# The fetch alone needs the genome row, for the provenance columns the REST entry
# carries. Not folded into the fragment above, for the reason its reference twin
# gives: `genome_idx` is FK'd and filtered NOT NULL here, so the join can neither
# drop nor duplicate a row, and a count that carries it pays for an answer that
# cannot differ.
_ASSEMBLY_GENOME_SOURCE_JOIN = " JOIN qiita.genome g ON g.genome_idx = am.genome_idx"


async def fetch_assembly_genome_map(
    db: asyncpg.Pool | asyncpg.Connection,
    *,
    prep_sample_idx: int,
    processing_idx: int,
    limit: int,
) -> list[asyncpg.Record]:
    """One assembly run's contig → genome lookup: one row per (feature, genome)
    pair with the genome's `source` / `source_id`, ordered by (feature_idx,
    genome_idx), at most `limit` rows.

    DISTINCT, unlike its reference twin: the membership key carries `(kind, bin_id)`,
    so one contig can appear on several rows of one run. Repeats collapse only when
    they also agree on the genome; a contig that belongs to two genomes of the run
    keeps both pairs, which `analytic.reconcile.denovo_map_table_sql` explains.
    """
    return await db.fetch(
        "SELECT DISTINCT am.feature_idx, am.genome_idx, g.source, g.source_id"
        + _ASSEMBLY_GENOME_MAP_FROM
        + _ASSEMBLY_GENOME_SOURCE_JOIN
        + _ASSEMBLY_GENOME_MAP_WHERE
        + " ORDER BY am.feature_idx, am.genome_idx LIMIT $3",
        [prep_sample_idx],
        processing_idx,
        limit,
    )


async def count_assembly_genome_map(
    db: asyncpg.Pool | asyncpg.Connection, *, prep_sample_idx: int, processing_idx: int
) -> int:
    """How many (feature, genome) pairs `fetch_assembly_genome_map` would return
    uncapped — the size a refusal names."""
    return await db.fetchval(
        "SELECT count(*) FROM (SELECT DISTINCT am.feature_idx, am.genome_idx"
        + ASSEMBLY_GENOME_MAP_PAIRS_SQL
        + ")",
        [prep_sample_idx],
        processing_idx,
    )


async def count_assembly_membership_without_genome(
    db: asyncpg.Pool | asyncpg.Connection,
    *,
    prep_sample_idx: list[int],
    processing_idx: int,
) -> dict[int, int]:
    """How many membership rows carry no `genome_idx`, per named prep_sample.
    prep_samples with none are ABSENT from the result, so an empty dict is a clean
    run.

    The completeness check the map's `IS NOT NULL` filter needs behind it. A run
    whose memberships predate the genome mint yields a map that silently omits
    those contigs, and the omission is not visible in the result: the genomes they
    belong to keep their other contigs, so their length denominators come back
    short and their breadth of coverage comes back high. A caller staging this map
    refuses on a non-empty result rather than building a table over it — the
    assembly-genome backfill is what makes it empty.

    Cohort-shaped like the two reads above, so a caller checking a whole cohort
    makes one round trip and its refusal can name every offender rather than the
    first.
    """
    rows = await db.fetch(
        "SELECT prep_sample_idx, count(*) AS n FROM qiita.assembly_membership"
        " WHERE prep_sample_idx = ANY($1) AND processing_idx = $2 AND genome_idx IS NULL"
        " GROUP BY prep_sample_idx ORDER BY prep_sample_idx",
        prep_sample_idx,
        processing_idx,
    )
    return {r["prep_sample_idx"]: r["n"] for r in rows}
