"""Repository functions for the qiita.alignment_definition table, plus the reads
that answer questions ABOUT alignments from the sample side.

Those reads query qiita.alignment_sample (and join sequenced_sample /
prep_sample) rather than alignment_definition alone — "which alignments touch
these samples", "which of this cohort is not finished". They live here because
what they return is alignments; the table they lead with is an implementation
detail of that answer.

An alignment's identity is its CONFIG: the sharded reference it aligns against,
the sharded aligner, the host-depletion mask its input reads carry, and the
reference's current shard-set. The mint path is a thin wrapper around the
qiita.mint_alignment_definition plpgsql function, which upserts on params_hash
so the same config always resolves to the same alignment_idx fleet-wide
(idempotent) — the exact discipline qiita.mask_definition / mint_mask_definition
use (this module mirrors repositories/mask_definition.py).

The params_hash is computed control-plane-side via
qiita_common.hashing.canonical_params_hash (SHA-256 of the canonical config
JSON) — no pgcrypto dependency on the database. The function only enforces the
dedup and returns the row; asyncpg.ForeignKeyViolationError (unknown
principal_idx), asyncpg.InvalidParameterValueError (SQLSTATE 22023, a
non-32-byte hash — unreachable via this helper) and
ParamsDoNotSurviveStorageError (a config whose jsonb round trip would change
the digest) propagate to the caller.
"""

import json
from collections.abc import Sequence
from typing import Literal

import asyncpg
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    AlignmentDefinitionStatus,
    AlignmentSampleState,
)

from . import gate_state_literal, require_transaction

# The `alignment_sample` states, asserted against the Literal so a renamed member
# fails at import rather than matching no rows. A consumer proceeds on `completed`
# alone; `list_completed_alignment_samples` is the one copy of that predicate.
ALIGNMENT_SAMPLE_PENDING = gate_state_literal("pending", AlignmentSampleState)
ALIGNMENT_SAMPLE_COMPLETED = gate_state_literal("completed", AlignmentSampleState)
ALIGNMENT_SAMPLE_INVALIDATED = gate_state_literal("invalidated", AlignmentSampleState)

ALIGNMENT_DEFINITION_RETURNING = (
    "alignment_idx, params_hash, params, created_by_idx, created_at, status,"
    " deprecated_at, deprecated_by_idx, deprecation_reason, superseded_by"
)


class AlignmentDefinitionDeprecated(Exception):
    """Raised when a mint resolves to an alignment whose config is deprecated.

    Carries the SQLSTATE 23514 the plpgsql mint raises, translated here so routes
    and the runner match on a type. A deprecated config is void: no new data is
    aligned under it, and because a block identity carries no version, re-planning
    the same reference, aligner and mask always lands on this row."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


class AlignmentDefinitionNotFound(Exception):
    """Raised when the alignment_idx does not exist."""

    def __init__(self, alignment_idx: int) -> None:
        self.alignment_idx = alignment_idx
        super().__init__(f"no alignment_definition with alignment_idx={alignment_idx}")


class ParamsDoNotSurviveStorageError(RuntimeError):
    """`params` contains a value whose jsonb round trip changes its canonical JSON.

    Postgres stores a JSON number as `numeric` and renders it back in plain decimal,
    so a float Python spells with an exponent can come back as an integer literal
    (1.5e30 → 1500000000000000000000000000000, which re-parses as an `int`), and -0.0
    comes back as 0.0. Either way `canonical_params_hash` of the row we read is not the
    digest we stored beside it.

    That would be invisible — and permanent for the affected `alignment_idx` — if
    nothing re-hashed the params. Something does: the pool alignment listing publishes
    `params_hash` and the CLI recomputes it from the `params` beside it and refuses to
    build on a mismatch (`cli/user/alignment.py`). So this is caught here, at the write
    that would create it, rather than surfacing as an unexplainable refusal in someone
    else's build months later.

    A config needing a fractional value should carry it as a string, or as a scaled
    integer. The sibling mints (`mask_definition`, `processing`) store params the same
    way and have the same latent exposure; they are not guarded because nothing
    re-hashes their stored params yet. If that changes, this check is what to reuse.
    """


async def _assert_params_survive_storage(
    conn: asyncpg.Connection, params: dict, params_hash: bytes
) -> None:
    """Refuse `params` whose canonical hash changes across a jsonb round trip.

    Asks Postgres to normalize the blob rather than predicting how it will: the rule is
    `numeric`'s output format, and a Python-side approximation of it would be both
    fiddly and wrong in the cases that matter. One extra statement per *definition*
    mint, which happens once per unique config, not once per sample.
    """
    stored = await conn.fetchval("SELECT ($1::jsonb)::text", json.dumps(params))
    if canonical_params_hash(json.loads(stored)) != params_hash:
        raise ParamsDoNotSurviveStorageError(
            f"these alignment params do not survive jsonb storage unchanged: stored as "
            f"{stored!r}, which no longer hashes to the digest computed for them. See "
            f"ParamsDoNotSurviveStorageError."
        )


async def mint_alignment_definition(
    conn: asyncpg.Connection,
    *,
    params: dict,
    principal_idx: int,
) -> asyncpg.Record:
    """Mint (or return the existing) alignment_definition row for a config.

    Deduplicates on the canonical-JSON SHA-256 of `params` — the dedup key is
    the config blob, so the same config resolves to the same `alignment_idx`
    fleet-wide. `params` is the canonical alignment config:
    `{reference_idx, aligner, mask_idx, shard_ids: sorted[int]}`. The sorted
    shard-set is part of the hash, but growing a reference is NOT supported today
    and this is not yet a working growth foundation: the shard COUNT is fixed at
    _SHARD_COUNT so the set is [0..N-1] regardless of reference size, and shard
    assignment is re-plan-safe (not append-only), so the set is not a stable
    growth discriminator. See the 20260712000000_alignment_definition migration
    comment for the properties real growth support would need.

    Returns the qiita.alignment_definition row as an asyncpg.Record. Raises
    AlignmentDefinitionDeprecated when the config resolves to a deprecated row,
    asyncpg.ForeignKeyViolationError when principal_idx does not exist, and
    ParamsDoNotSurviveStorageError for a config whose stored form would stop matching
    the digest stored beside it.

    No `require_transaction(conn)` guard: the qiita.mint_alignment_definition
    plpgsql body (the SELECT/INSERT upsert loop) executes as a single SQL
    statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    await _assert_params_survive_storage(conn, params, params_hash)
    # asyncpg encodes a dict bound to a jsonb parameter via the JSON codec; pass
    # the serialized string explicitly so the behaviour is independent of
    # whether a JSON codec is registered on the connection.
    try:
        return await conn.fetchrow(
            f"SELECT {ALIGNMENT_DEFINITION_RETURNING}"
            "  FROM qiita.mint_alignment_definition($1, $2::jsonb, $3)",
            params_hash,
            json.dumps(params),
            principal_idx,
        )
    except asyncpg.CheckViolationError as exc:
        # SQLSTATE 23514 is also what a genuine table CHECK raises, so match on the
        # message the function sets and re-raise anything else untouched.
        if "is deprecated" not in str(exc):
            raise
        raise AlignmentDefinitionDeprecated(str(exc)) from exc


async def lookup_alignment_idx_by_params(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    params: dict,
) -> int | None:
    """Return the alignment_idx whose params_hash matches ``params``, or None.

    A pure LOOKUP — it computes the same canonical-JSON SHA-256 the mint path
    uses (`canonical_params_hash`) and SELECTs the existing row; it never mints.
    Accepts either a pool or a connection so it composes standalone or inside a
    transaction.
    """
    params_hash = canonical_params_hash(params)
    return await pool_or_conn.fetchval(
        "SELECT alignment_idx FROM qiita.alignment_definition WHERE params_hash = $1",
        params_hash,
    )


async def fetch_alignment_definition_by_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    alignment_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.alignment_definition row for alignment_idx, or None.

    Accepts either a pool or a connection so the helper composes inside an open
    transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        f"SELECT {ALIGNMENT_DEFINITION_RETURNING}"
        "  FROM qiita.alignment_definition"
        " WHERE alignment_idx = $1",
        alignment_idx,
    )


async def alignment_definition_exists(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    alignment_idx: int,
) -> bool:
    """Does this alignment_idx exist? Same round trip as
    `fetch_alignment_definition_by_idx` without dragging the `params` JSONB back
    for a caller that only wants to 404. Twin of `fetch_prep_sample_exists`.
    """
    return (
        await pool_or_conn.fetchval(
            "SELECT 1 FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
        )
    ) is not None


async def list_pool_prep_sample_idxs(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> list[int]:
    """The pool's non-retired, non-flagged sequenced samples, as prep_sample_idx.

    Same sample set as the other pool rollups (`ps.retired IS NOT TRUE` and
    `ss.ena_status IS NULL`), so a pool's alignments are counted over the same
    population its completion and QC reports describe.
    """
    rows = await pool_or_conn.fetch(
        "SELECT ss.prep_sample_idx"
        "  FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1 AND ps.retired IS NOT TRUE"
        "   AND ss.ena_status IS NULL"
        " ORDER BY ss.prep_sample_idx",
        sequenced_pool_idx,
    )
    return [r["prep_sample_idx"] for r in rows]


async def list_alignments_over_prep_samples(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    prep_sample_idxs: Sequence[int],
) -> list[asyncpg.Record]:
    """Every alignment touching any of `prep_sample_idxs`, with its config and
    completed/total counts **over exactly those samples**.

    The caller passes the set it may read, so the counts come back already
    scoped to it — the alignment's real membership never reaches the response.
    That is what keeps this route's answer consistent with what the all-or-
    nothing mint will accept.

    An alignment none of these samples belongs to does not appear at all, which
    is the narrowing the discovery contract wants: a zero-count row would still
    disclose that the alignment exists. Empty input short-circuits.

    **Aggregate first, join second — not the other way round.** Postgres has no
    eager aggregation, so joining `alignment_definition` up front carries its
    `params` JSONB through the join and into the GROUP BY sort key once per
    `alignment_sample` row rather than once per alignment. Measured on a
    2,000-sample cohort over 40 alignments (~1.5 KB params, `work_mem=4MB`):
    291 ms with a 134 MB external merge sort, against 10.8 ms and no spill in
    this shape. The sort carries one row per (sample, alignment) pair, so the
    spill threshold is roughly
    `samples × alignments × sizeof(params) > work_mem` — about 2,700 pairs at
    those settings, which a 2,000-sample pool crosses on its SECOND alignment.
    Not a large-pool problem, in other words: it is the default shape, and this
    route is open to any authenticated user over a table that grows without
    bound.
    """
    if not prep_sample_idxs:
        return []
    return await pool_or_conn.fetch(
        "WITH counted AS ("
        "  SELECT alignment_idx,"
        "         count(*) FILTER (WHERE state = 'completed') AS samples_completed,"
        "         count(*) AS samples_total"
        "    FROM qiita.alignment_sample"
        "   WHERE prep_sample_idx = ANY($1::bigint[])"
        "   GROUP BY alignment_idx"
        ")"
        " SELECT c.alignment_idx, ad.params, ad.params_hash,"
        "        c.samples_completed, c.samples_total,"
        "        ad.status, ad.deprecation_reason, ad.superseded_by"
        "   FROM counted c"
        "   JOIN qiita.alignment_definition ad ON ad.alignment_idx = c.alignment_idx"
        "  ORDER BY c.alignment_idx",
        list(prep_sample_idxs),
    )


async def list_completed_alignment_samples(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    alignment_idx: int,
    prep_sample_idxs: Sequence[int],
) -> list[int]:
    """The subset of `prep_sample_idxs` that are `'completed'` for this alignment.

    The one definition of the completion predicate. `block.list_incomplete_alignment_samples`
    — which the mint uses to refuse a cohort — is this function's complement and
    calls it rather than re-issuing the query. Alignment rows are NOT 1:1 with
    reads, so the presence of rows must never be read as done; `state` is a
    first-class column for exactly that reason.

    Sorted, because the result becomes a signed ticket's cohort and an unstable
    order would make two identical requests sign different payload bytes.
    """
    if not prep_sample_idxs:
        return []
    rows = await pool_or_conn.fetch(
        "SELECT prep_sample_idx FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = ANY($2::bigint[])"
        "   AND state = $3"
        " ORDER BY prep_sample_idx",
        alignment_idx,
        list(prep_sample_idxs),
        ALIGNMENT_SAMPLE_COMPLETED,
    )
    return [r["prep_sample_idx"] for r in rows]


async def transition_alignment_definition_status(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    alignment_idx: int,
    status: AlignmentDefinitionStatus,
    reason: str | None,
    superseded_by: int | None,
    principal_idx: int,
) -> asyncpg.Record:
    """Set an alignment CONFIG's lifecycle status and return the updated row.

    The twin of `transition_mask_definition_status`: the provenance columns move
    with `status` in one UPDATE, so the CHECK tying them together never sees a
    half-applied transition, and re-deprecating restamps who/when/why.

    Raises AlignmentDefinitionNotFound when the row does not exist, and
    asyncpg.ForeignKeyViolationError when `superseded_by` names no alignment.
    """
    deprecating = status is AlignmentDefinitionStatus.DEPRECATED
    row = await conn.fetchrow(
        "UPDATE qiita.alignment_definition"
        "    SET status = $2,"
        "        deprecated_at = CASE WHEN $3::boolean THEN now() END,"
        "        deprecated_by_idx = CASE WHEN $3::boolean THEN $4::bigint END,"
        "        deprecation_reason = CASE WHEN $3::boolean THEN $5::text END,"
        "        superseded_by = CASE WHEN $3::boolean THEN $6::bigint END"
        "  WHERE alignment_idx = $1"
        f" RETURNING {ALIGNMENT_DEFINITION_RETURNING}",
        alignment_idx,
        status.value,
        deprecating,
        principal_idx,
        reason,
        superseded_by,
    )
    if row is None:
        raise AlignmentDefinitionNotFound(alignment_idx)
    return row


async def set_alignment_sample_states(
    conn: asyncpg.Connection,
    *,
    alignment_idx: int,
    prep_sample_idxs: list[int],
    state: Literal["completed", "invalidated"],
    reason: str | None,
    principal_idx: int,
) -> dict[str, list[int]]:
    """Withdraw (or restore) specific RUNS of one alignment.

    The twin of `set_mask_sample_states`, and the same rules: a `'pending'` row is
    skipped (the align pipeline owns that value and would overwrite a withdrawal),
    and each row's `FOR UPDATE` lock is taken before classifying, the lock every
    other `alignment_sample` writer takes (`lock_alignment_sample`).

    Returns `{"updated", "unchanged", "not_found", "skipped_pending"}`.
    """
    require_transaction(conn)
    present = {
        r["prep_sample_idx"]: r["state"]
        for r in await conn.fetch(
            "SELECT prep_sample_idx, state FROM qiita.alignment_sample"
            " WHERE alignment_idx = $1 AND prep_sample_idx = ANY($2::bigint[])"
            " ORDER BY prep_sample_idx"
            " FOR UPDATE",
            alignment_idx,
            prep_sample_idxs,
        )
    }
    not_found = [idx for idx in prep_sample_idxs if idx not in present]
    skipped_pending = [idx for idx, st in present.items() if st == ALIGNMENT_SAMPLE_PENDING]
    unchanged = [idx for idx, st in present.items() if st == state]
    to_update = [idx for idx, st in present.items() if st not in (state, ALIGNMENT_SAMPLE_PENDING)]
    if to_update:
        invalidating = state == ALIGNMENT_SAMPLE_INVALIDATED
        await conn.execute(
            "UPDATE qiita.alignment_sample"
            "    SET state = $3,"
            "        invalidated_at = CASE WHEN $4::boolean THEN now() END,"
            "        invalidated_by_idx = CASE WHEN $4::boolean THEN $5::bigint END,"
            "        invalidation_reason = CASE WHEN $4::boolean THEN $6::text END"
            "  WHERE alignment_idx = $1 AND prep_sample_idx = ANY($2::bigint[])",
            alignment_idx,
            to_update,
            state,
            invalidating,
            principal_idx,
            reason,
        )
    return {
        "updated": sorted(to_update),
        "unchanged": sorted(unchanged),
        "not_found": sorted(not_found),
        "skipped_pending": sorted(skipped_pending),
    }


class AlignmentHasLiveWork(Exception):
    """Raised when deprecating an alignment that still has unfinished work tickets.
    Deprecation is refused until they finish or are cancelled, so no block or de novo
    run of a void config is still to write rows after it."""

    def __init__(self, alignment_idx: int, work_ticket_idxs: list[int]) -> None:
        self.alignment_idx = alignment_idx
        self.work_ticket_idxs = work_ticket_idxs
        super().__init__(
            f"alignment {alignment_idx} has {len(work_ticket_idxs)} unfinished work"
            f" ticket(s): {work_ticket_idxs}. Wait for them to finish, or cancel them,"
            " then deprecate"
        )


async def refuse_deprecating_with_live_work(conn: asyncpg.Connection, alignment_idx: int) -> None:
    """Raise AlignmentHasLiveWork if `alignment_idx` is active and any work ticket
    under it is not terminal (held tickets included: the fan-out would release them).

    Takes the definition row `FOR UPDATE`. Each writer of `work_ticket.alignment_idx`
    takes it `FOR SHARE` (`lock_active_alignment`) in the transaction that writes the
    column, so a plan either commits its tickets before this count or finds the row
    deprecated. A de novo ticket the runner has not yet stamped with its alignment
    is not counted; its stamp takes the share lock and refuses once the alignment is
    deprecated. An already-deprecated row is not checked, so its reason can still be
    corrected."""
    require_transaction(conn)
    status = await conn.fetchval(
        "SELECT status FROM qiita.alignment_definition WHERE alignment_idx = $1 FOR UPDATE",
        alignment_idx,
    )
    if status is None:
        raise AlignmentDefinitionNotFound(alignment_idx)
    if status != AlignmentDefinitionStatus.ACTIVE.value:
        return
    live = [
        r["work_ticket_idx"]
        for r in await conn.fetch(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE alignment_idx = $1 AND state::text = ANY($2::text[])"
            " ORDER BY work_ticket_idx",
            alignment_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    ]
    if live:
        raise AlignmentHasLiveWork(alignment_idx, live)


async def lock_active_alignment(conn: asyncpg.Connection, alignment_idx: int) -> None:
    """Hold the definition row `FOR SHARE` for the caller's transaction and raise
    AlignmentDefinitionDeprecated if it is deprecated. Called by every transaction
    that writes `work_ticket.alignment_idx`, so a deprecation cannot commit between
    the mint and the ticket recording it (`refuse_deprecating_with_live_work`)."""
    require_transaction(conn)
    row = await conn.fetchrow(
        "SELECT status, deprecation_reason FROM qiita.alignment_definition"
        " WHERE alignment_idx = $1 FOR SHARE",
        alignment_idx,
    )
    if row is None:
        raise AlignmentDefinitionNotFound(alignment_idx)
    if row["status"] == AlignmentDefinitionStatus.DEPRECATED.value:
        raise AlignmentDefinitionDeprecated(
            f"alignment_definition {alignment_idx} is deprecated: {row['deprecation_reason']}"
        )
