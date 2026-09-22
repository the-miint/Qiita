"""Repository functions for the qiita.processing identity table.

A processing_idx is the identity of a per-sample processing RUN — its canonical
parameters (workflow + version + result-affecting knobs like the mask whose
pass-set is assembled, and the assembler). The mint path wraps the
qiita.mint_processing plpgsql function, which upserts on params_hash so the same
params always resolve to the same processing_idx fleet-wide (idempotent re-run)
and different params get a distinct id (run disambiguation). The params_hash is
computed control-plane-side via qiita_common.hashing.canonical_params_hash
(SHA-256 of the canonical params JSON) — no pgcrypto dependency on the database.
Mirrors repositories.mask_definition.

The read side answers three questions a client otherwise needs a psql shell for:
which runs exist (`list_processing`), what one run encodes
(`fetch_processing_by_idx`), and which samples it assembled
(`fetch_processing_prep_samples`). Both list reads narrow to the samples the
caller may see through the shared `_sample_scope.sample_scope_sql`.

The lifecycle writes (`transition_processing_status`, `set_assembly_sample_states`)
are the twin of the mask ones in `repositories.mask_definition`, and are deliberately
not merged with them: the two gates carry different state sets, so the skip rules and
the response buckets differ, and a merged writer would have to interpolate the table
and key column into a lifecycle UPDATE for a saving of about thirty lines. What IS
shared is factored out — the narrowing predicate (`_sample_scope`) and the state-member
assertion (`gate_state_literal`).
"""

import json
from typing import Literal

import asyncpg
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import AssemblySampleState, ProcessingStatus

from . import gate_state_literal, require_transaction
from ._sample_scope import sample_scope_sql

# Column projection backing every Processing response. Defined once because three
# readers (the mint, the by-idx fetch, and the list) return the same shape, and a
# lifecycle column added to one of them and not the others reaches the API as the
# wire model's default — silent for the four nullable provenance fields, which is
# the case an error would not catch. `params_hash` is not projected: it is the
# dedup key, derivable from `params`, and has no wire model field.
PROCESSING_RETURNING = (
    "processing_idx, workflow, version, params, created_at, status,"
    " deprecated_at, deprecated_by_idx, deprecation_reason, superseded_by"
)

# The same projection, qualified for the list query's join against the tally CTE.
# Derived from it rather than retyped so a lifecycle column cannot reach one
# reader and miss the other.
_PROCESSING_LIST_COLUMNS = ", ".join(f"p.{col.strip()}" for col in PROCESSING_RETURNING.split(","))

# The alias both roster queries below give qiita.assembly_sample.
# `sample_scope_sql` correlates its clauses on `<alias>.prep_sample_idx`, so the
# two have to agree.
_ROSTER_ALIAS = "asm"


# The assembly_sample states this module binds as query parameters, each asserted
# against the wire type so a renamed member fails at import rather than matching
# no rows (`gate_state_literal` carries why).
_STATE_PENDING, _STATE_COMPLETED, _STATE_NO_DATA, _STATE_INVALIDATED = (
    gate_state_literal("pending", AssemblySampleState),
    gate_state_literal("completed", AssemblySampleState),
    gate_state_literal("no_data", AssemblySampleState),
    gate_state_literal("invalidated", AssemblySampleState),
)


class ProcessingDeprecated(Exception):
    """Raised when a mint resolves to a run whose config has been deprecated.

    Carries the SQLSTATE 23514 the plpgsql mint raises, translated at this layer
    so the runner matches on a type rather than on a Postgres error string. A
    deprecated config is void: it must not assemble new data."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


class ProcessingNotFound(Exception):
    """Raised when the processing_idx does not exist."""

    def __init__(self, processing_idx: int) -> None:
        self.processing_idx = processing_idx
        super().__init__(f"no processing with processing_idx={processing_idx}")


async def mint_processing(
    conn: asyncpg.Connection,
    *,
    workflow: str,
    version: str,
    params: dict,
) -> asyncpg.Record:
    """Mint (or return the existing) qiita.processing row for a params set.

    Deduplicates on the canonical-JSON SHA-256 of `params` — the dedup key is the
    full params blob, so the same params resolve to the same `processing_idx`.
    `workflow` / `version` are stored as descriptive columns; they are expected to
    also appear inside `params` so the hash covers them.

    Returns the qiita.processing row as an asyncpg.Record. Raises
    `ProcessingDeprecated` when the params resolve to a deprecated run.

    No `require_transaction(conn)` guard: the plpgsql SELECT/INSERT upsert loop
    runs as a single statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    try:
        return await conn.fetchrow(
            f"SELECT {PROCESSING_RETURNING}  FROM qiita.mint_processing($1, $2, $3, $4::jsonb)",
            params_hash,
            workflow,
            version,
            json.dumps(params),
        )
    except asyncpg.CheckViolationError as exc:
        # The mint raises SQLSTATE 23514 for a deprecated config. asyncpg maps
        # that class to CheckViolationError, which a genuine table CHECK would
        # also raise — so match on the message the function sets, and re-raise
        # anything else untouched rather than reporting an unrelated constraint
        # as a deprecation.
        if "is deprecated" not in str(exc):
            raise
        raise ProcessingDeprecated(str(exc)) from exc


async def fetch_processing_by_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    processing_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.processing row for processing_idx, or None.

    Accepts either a pool or a connection so the helper composes inside an open
    transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        f"SELECT {PROCESSING_RETURNING} FROM qiita.processing WHERE processing_idx = $1",
        processing_idx,
    )


async def list_processing(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    visible_to_principal_idx: int | None,
    sequenced_pool_idx: int | None = None,
    prep_sample_idx: int | None = None,
    status: ProcessingStatus | None = None,
    limit: int,
) -> list[asyncpg.Record]:
    """Return up to `limit` qiita.processing rows, newest first, each with its
    per-run sample tally under the same narrowings.

    `visible_to_principal_idx` has no default: None means "see every sample", so a
    caller that omitted it by accident would get fleet-wide visibility silently.

    The tally counts qiita.assembly_sample rows by state. Unlike the mask tally
    there is no work_ticket arm to union in: no work_ticket column carries a
    processing_idx, so a ticket cannot be attributed to a run and the gate is the
    only source. A sample whose assembly ticket died before the runner minted the
    identity therefore has no row and is counted nowhere — read the work_ticket
    for that, as `fetch_assembly_sample_state` states.

    Which runs come back:

      * A bypass-role caller with no filters gets every run, including one that
        has no gate rows yet (tally 0).
      * Any narrowing — a filter, or a `visible_to_principal_idx` — restricts the
        result to runs with at least one matching sample. A plain user therefore
        sees the runs that touched their own samples, and a zero-tally row never
        reveals a run the narrowing excluded.

    `status` narrows to one config lifecycle. Omitted, BOTH are returned: a
    deprecated run stays listed so "what assembled this published contig set?"
    keeps an answer.

    Callers that need to detect truncation pass `limit = cap + 1`; a returned
    length > cap means the set exceeded the cap.
    """
    args: list = [_STATE_COMPLETED, _STATE_PENDING, _STATE_NO_DATA, _STATE_INVALIDATED]
    scope, narrowed = sample_scope_sql(
        alias=_ROSTER_ALIAS,
        args=args,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=prep_sample_idx,
        visible_to_principal_idx=visible_to_principal_idx,
    )
    status_predicate = ""
    if status is not None:
        args.append(status.value)
        status_predicate = f"p.status = ${len(args)}"
    args.append(limit)
    limit_param = f"${len(args)}"
    # `narrowed` and `status` are independent WHERE sources, so the connective is
    # computed rather than baked into either fragment.
    where_parts = [
        part
        for part in ("t.processing_idx IS NOT NULL" if narrowed else "", status_predicate)
        if part
    ]
    where_sql = ("  WHERE " + " AND ".join(where_parts)) if where_parts else ""
    query = (
        "WITH tally AS ("
        f"    SELECT {_ROSTER_ALIAS}.processing_idx,"
        f"           count(*) FILTER (WHERE {_ROSTER_ALIAS}.state = $1) AS samples_completed,"
        f"           count(*) FILTER (WHERE {_ROSTER_ALIAS}.state = $2) AS samples_pending,"
        f"           count(*) FILTER (WHERE {_ROSTER_ALIAS}.state = $3) AS samples_no_data,"
        f"           count(*) FILTER (WHERE {_ROSTER_ALIAS}.state = $4) AS samples_invalidated"
        f"      FROM qiita.assembly_sample {_ROSTER_ALIAS}"
        f"     WHERE true{scope}"
        f"     GROUP BY {_ROSTER_ALIAS}.processing_idx"
        ")"
        f" SELECT {_PROCESSING_LIST_COLUMNS},"
        "        COALESCE(t.samples_completed, 0) AS samples_completed,"
        "        COALESCE(t.samples_pending, 0) AS samples_pending,"
        "        COALESCE(t.samples_no_data, 0) AS samples_no_data,"
        "        COALESCE(t.samples_invalidated, 0) AS samples_invalidated"
        "   FROM qiita.processing p"
        "   LEFT JOIN tally t ON t.processing_idx = p.processing_idx"
        + where_sql
        + "  ORDER BY p.processing_idx DESC"
        f"  LIMIT {limit_param}"
    )
    return list(await pool_or_conn.fetch(query, *args))


async def fetch_processing_prep_samples(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    processing_idx: int,
    *,
    visible_to_principal_idx: int | None,
    sequenced_pool_idx: int | None = None,
    limit: int,
) -> list[asyncpg.Record]:
    """Return up to `limit` samples assembled under `processing_idx`, ascending by
    prep_sample_idx, each with its gate state.

    `visible_to_principal_idx` has no default, for the reason given on
    `list_processing`.

    Joins qiita.biosample for the accession so a caller can name the sample
    without a second read; the accession is NULL until the biosample is submitted
    to NCBI. Retired prep_samples are excluded, matching this run's tally in the
    list read.

    An empty list for a run that exists means no sample the caller may see is
    gated under it — not that the run is absent. The route checks existence
    separately so a typo'd processing_idx 404s rather than reading as "no samples".

    Callers that need to detect truncation pass `limit = cap + 1`.
    """
    args: list = [processing_idx]
    scope, _narrowed = sample_scope_sql(
        alias=_ROSTER_ALIAS,
        args=args,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=None,
        visible_to_principal_idx=visible_to_principal_idx,
    )
    args.append(limit)
    query = (
        f"SELECT {_ROSTER_ALIAS}.prep_sample_idx,"
        f"       {_ROSTER_ALIAS}.state AS assembly_state,"
        "        bs.biosample_accession"
        f"   FROM qiita.assembly_sample {_ROSTER_ALIAS}"
        f"   JOIN qiita.prep_sample ps ON ps.idx = {_ROSTER_ALIAS}.prep_sample_idx"
        "   JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
        f"  WHERE {_ROSTER_ALIAS}.processing_idx = $1"
        f"{scope}"
        f"  ORDER BY {_ROSTER_ALIAS}.prep_sample_idx"
        f"  LIMIT ${len(args)}"
    )
    return list(await pool_or_conn.fetch(query, *args))


async def transition_processing_status(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    processing_idx: int,
    status: ProcessingStatus,
    reason: str | None,
    superseded_by: int | None,
    principal_idx: int,
) -> asyncpg.Record:
    """Set an assembly run CONFIG's lifecycle status and return the updated row.

    The three provenance columns move with `status` in one UPDATE, so the
    biconditional CHECK on the table can never see a half-applied transition.
    Idempotent: re-deprecating an already-deprecated run restamps who/when/why
    rather than refusing, which is what a corrected reason wants.

    Raises ProcessingNotFound when the row does not exist, and
    asyncpg.ForeignKeyViolationError when `superseded_by` names no run.
    """
    deprecating = status is ProcessingStatus.DEPRECATED
    row = await conn.fetchrow(
        "UPDATE qiita.processing"
        "    SET status = $2,"
        "        deprecated_at = CASE WHEN $3::boolean THEN now() END,"
        "        deprecated_by_idx = CASE WHEN $3::boolean THEN $4::bigint END,"
        "        deprecation_reason = CASE WHEN $3::boolean THEN $5::text END,"
        "        superseded_by = CASE WHEN $3::boolean THEN $6::bigint END"
        "  WHERE processing_idx = $1"
        f" RETURNING {PROCESSING_RETURNING}",
        processing_idx,
        status.value,
        deprecating,
        principal_idx,
        reason,
        superseded_by,
    )
    if row is None:
        raise ProcessingNotFound(processing_idx)
    return row


async def set_assembly_sample_states(
    conn: asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idxs: list[int],
    state: Literal["completed", "invalidated"],
    reason: str | None,
    principal_idx: int,
) -> dict[str, list[int]]:
    """Withdraw (or restore) specific RUNS of one assembly identity.

    Returns `{"updated", "unchanged", "not_found", "skipped_pending",
    "skipped_no_data"}` over the requested prep_samples: rows whose state
    changed, rows that already held the requested state, prep_samples with no gate
    row under this run, and the two states this route does not write over.

    A `'pending'` row is skipped rather than written. `'pending'` is the assembly
    pipeline's value — `finalize-assembly-sample` and the runner's StepNoData
    handler flip it when the run lands — so writing `'invalidated'` over it would
    be undone by the pipeline without anyone being told. There is also nothing to
    withdraw yet: the run has not produced contigs.

    A `'no_data'` row is skipped for a different reason: that run assembled no
    contig at all, so there is no output to withdraw and none to restore. This is
    where the assembly gate diverges from `mask_sample`, which has no such state.
    Because both are skipped, `'completed'` is the only state an `'invalidated'`
    row can have come from, which is what makes restoring to `'completed'` exact
    rather than a guess.

    Runs inside the caller's transaction and takes each row's `FOR UPDATE` lock
    before deciding. Without it the classification is read on one pooled
    connection and applied on another, so a concurrent finalize could flip a row
    out of `'pending'` between the two and the response would describe a state
    that never held.
    """
    require_transaction(conn)
    present = {
        r["prep_sample_idx"]: r["state"]
        for r in await conn.fetch(
            "SELECT prep_sample_idx, state FROM qiita.assembly_sample"
            " WHERE processing_idx = $1 AND prep_sample_idx = ANY($2::bigint[])"
            " ORDER BY prep_sample_idx"
            " FOR UPDATE",
            processing_idx,
            prep_sample_idxs,
        )
    }
    skipped = (_STATE_PENDING, _STATE_NO_DATA)
    not_found = [idx for idx in prep_sample_idxs if idx not in present]
    skipped_pending = [idx for idx, st in present.items() if st == _STATE_PENDING]
    skipped_no_data = [idx for idx, st in present.items() if st == _STATE_NO_DATA]
    unchanged = [idx for idx, st in present.items() if st == state]
    to_update = [idx for idx, st in present.items() if st != state and st not in skipped]
    if to_update:
        invalidating = state == _STATE_INVALIDATED
        await conn.execute(
            "UPDATE qiita.assembly_sample"
            "    SET state = $3,"
            "        invalidated_at = CASE WHEN $4::boolean THEN now() END,"
            "        invalidated_by_idx = CASE WHEN $4::boolean THEN $5::bigint END,"
            "        invalidation_reason = CASE WHEN $4::boolean THEN $6::text END"
            "  WHERE processing_idx = $1 AND prep_sample_idx = ANY($2::bigint[])",
            processing_idx,
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
        "skipped_no_data": sorted(skipped_no_data),
    }
