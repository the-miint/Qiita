"""Repository functions for qiita.assembly_membership and qiita.assembly_sample.

The assembly analogue of the reference_membership INSERT that
`qiita_control_plane.actions.library.write_membership` performs inline: a bulk,
idempotent link of a prep_sample's assembly RUN (processing_idx) contigs — each a
qiita.feature minted via the SHARED mint-features path — to the bin they belong to
(kind, bin_id; the kind value set is enumerated in
`qiita_common.assembly_constants`). The DuckDB-side JOIN (bin_map x manifest x
feature_map -> (kind, bin_id, feature_idx)) and the batch streaming live in the
library primitive; this module owns only the raw bulk-insert SQL, mirroring
repositories.processing owning qiita.mint_processing's call.

It also owns qiita.assembly_sample, the per-(processing_idx, prep_sample)
completion gate — the assembly twin of qiita.mask_sample / qiita.alignment_sample.
Its contract lives on `fetch_assembly_sample_state` below.
"""

import asyncpg

from . import require_transaction


async def insert_assembly_membership_rows(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    processing_idx: int,
    kinds: list[str],
    bin_ids: list[str],
    feature_idxs: list[int],
) -> int:
    """Bulk-insert one chunk of assembly_membership rows; return the count of
    newly-linked rows.

    The three lists are positionally aligned: row i links
    ``(prep_sample_idx, processing_idx, kinds[i], bin_ids[i], feature_idxs[i])``.
    ``ON CONFLICT DO NOTHING`` on the natural PK makes the write idempotent /
    replay-safe — a workflow retried from the start re-runs this primitive and
    re-inserting the same rows links nothing new. Wraps
    asyncpg.ForeignKeyViolationError into a ValueError so the caller surfaces a
    structured error instead of leaking the asyncpg exception (a feature_idx that
    isn't in qiita.feature means upstream mint-features produced inconsistent
    inputs).
    """
    if not feature_idxs:
        return 0
    try:
        rows = await conn.fetch(
            "INSERT INTO qiita.assembly_membership"
            " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)"
            " SELECT $1, $2, k, b, f"
            " FROM unnest($3::text[], $4::text[], $5::bigint[]) AS t(k, b, f)"
            " ON CONFLICT DO NOTHING"
            " RETURNING feature_idx",
            prep_sample_idx,
            processing_idx,
            kinds,
            bin_ids,
            feature_idxs,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        raise ValueError(
            "One or more feature_idx / prep_sample_idx / processing_idx values do not exist"
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
    Unguarded, where `upsert_assembly_sample_no_data` is guarded: 'completed' is
    reachable from every other state, a prior run's 'no_data' included.
    """
    require_transaction(conn)
    await conn.execute(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')"
        " ON CONFLICT (processing_idx, prep_sample_idx) DO UPDATE SET state = 'completed'",
        processing_idx,
        prep_sample_idx,
    )


async def upsert_assembly_sample_no_data(
    conn: asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idx: int,
) -> None:
    """Write the `(processing_idx, prep_sample_idx)` gate row to 'no_data'.

    The other terminal writer: assembly_hash raised StepNoData, so the workflow
    stops before `finalize-assembly-sample` and the runner closes the gate here.

    The DO UPDATE is guarded on the row not already being 'completed', which is
    the asymmetry with `upsert_assembly_sample_completed` above. An earlier run
    under the same processing_idx that reached 'completed' left contigs in
    DuckLake; a later run of that identity finding none does not remove them, so
    writing 'no_data' over it would make the gate deny rows that are there.
    """
    require_transaction(conn)
    await conn.execute(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'no_data')"
        " ON CONFLICT (processing_idx, prep_sample_idx) DO UPDATE SET state = 'no_data'"
        "   WHERE qiita.assembly_sample.state <> 'completed'",
        processing_idx,
        prep_sample_idx,
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
    here rather than restate it). The gate has three states, all written
    first-class by the assembly workflow: 'pending' when the runner mints the run
    identity, then 'completed' at the terminal `finalize-assembly-sample` action,
    or 'no_data' when assembly_hash found no contig of any kind and the ticket
    ended NO_DATA.

    The invariant: completion is read from this state, never from the presence of
    qiita.assembly_membership or DuckLake rows — the assembly tail writes those
    across several entries, so a partial footprint is indistinguishable from a
    finished one by row presence alone. {'completed', 'no_data'} is the terminal
    set: a consumer asking whether the run is over reads both, while a consumer
    that needs contigs proceeds on 'completed' alone (under 'no_data' there are
    none). 'pending' and None both mean "not over"; None is never "assembled".

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
