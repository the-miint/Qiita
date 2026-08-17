"""Repository functions for the qiita.mask_definition table.

A mask's identity is its read-filtering CONFIG (filter workflow + version +
host references + QC params). The mint path is a thin wrapper around the
qiita.mint_mask_definition plpgsql function, which upserts on params_hash so
the same config always resolves to the same mask_idx fleet-wide (idempotent).

The params_hash is computed control-plane-side via
qiita_common.hashing.canonical_params_hash (SHA-256 of the canonical config
JSON) — no pgcrypto dependency on the database. The function only enforces the
dedup and returns the row; asyncpg.ForeignKeyViolationError (unknown
principal_idx) and asyncpg.InvalidParameterValueError (SQLSTATE 22023, a
non-32-byte hash — unreachable via this helper) propagate to the caller.

A config carrying an adapter set hashes under two derivations while the adapter
identity migration is in its expand phase: the current one (the reference's
sorted qiita.feature.sequence_hash values) and the legacy one (SHA-256 of the
serialized adapter Parquet). The mint looks the current hash up first and falls
back to the legacy hash, re-keying the row it finds onto the current one. Full
rationale and the removal plan: the 20260804000000 migration header.

The read side answers three questions a client otherwise needs a psql shell for:
which masks exist (`list_mask_definitions`), what one mask encodes
(`fetch_mask_definition_by_idx`), and which samples are masked under it
(`fetch_mask_prep_samples`). The two list reads resolve a sample's masking state
the same way and narrow to the samples the caller may see; the shared SQL
fragments that do so are defined once below and interpolated into each query.
"""

import json
from typing import Literal, get_args

import asyncpg
from qiita_common.actions import PER_SAMPLE_MASK_ACTION_IDS
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import (
    MaskDefinitionStatus,
    MaskSampleState,
    Tier,
    WorkTicketState,
)

from . import require_transaction

# Column projection backing every MaskDefinition response. Defined once because
# three readers (the mint, the by-idx fetch, and the list) return the same shape,
# and a lifecycle column added to one of them and not the others is a silent
# `status` default in the API rather than an error.
MASK_DEFINITION_RETURNING = (
    "mask_idx, params_hash, filter_workflow, filter_version, params, created_by_idx,"
    " created_at, adapter_hash_scheme, status, deprecated_at, deprecated_by_idx,"
    " deprecation_reason, superseded_by"
)


# The same projection as MASK_DEFINITION_RETURNING, qualified for the list query's
# join against the tally CTE. Derived from it rather than retyped so a lifecycle
# column cannot reach one reader and miss the other.
_MASK_DEFINITION_LIST_COLUMNS = ", ".join(
    f"md.{col.strip()}" for col in MASK_DEFINITION_RETURNING.split(",")
)


class MaskDefinitionDeprecated(Exception):
    """Raised when a mint resolves to a mask whose config has been deprecated.

    Carries the SQLSTATE 23514 the plpgsql mint raises, translated at this layer
    so routes and the runner match on a type rather than on a Postgres error
    string. A deprecated config is void: it must not mask new data."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


def _state_literal(value: str) -> str:
    """Assert `value` is a MaskSampleState member and return it.

    A membership check, not a lookup: the string is still written here. What it
    buys is that a renamed or removed member fails at import instead of silently
    matching no rows."""
    members = get_args(MaskSampleState)
    if value not in members:
        raise ValueError(f"{value!r} is not a MaskSampleState; have {members}")
    return value


# The mask_sample states this module writes into SQL, each asserted against the
# wire type so a renamed member fails at import rather than matching no rows.
# Looked up by value, not by position: the Literal has three members and the
# roster CTE synthesizes only two — 'invalidated' can only be read off an
# existing mask_sample row.
_MASK_STATE_PENDING, _MASK_STATE_COMPLETED, _MASK_STATE_INVALIDATED = (
    _state_literal("pending"),
    _state_literal("completed"),
    _state_literal("invalidated"),
)

# Per-(mask, sample) masking state.
#
# The gate table (`qiita.mask_sample`) is the primary source: both masking paths
# write it first-class — see the canonical contract on
# `repositories.block.fetch_mask_sample_state`. So a masked-COMPLETE sample always
# has a gate row, by either path.
#
# What the gate cannot show is a per-sample mask that ran but did not complete.
# The per-sample path has no PENDING phase (`upsert_mask_sample_completed` writes
# 'completed' in one upsert at the terminal step), so a processing or failed
# per-sample ticket leaves no row at all — indistinguishable, in the gate alone,
# from a sample nobody ever tried to mask. The ticket arm supplies those, which is
# why it is an anti-join against the gate rather than a replacement for it: gate
# row wins wherever one exists.
#
# A ticket the runner has not picked up yet is NOT covered: `_persist_mask_idx`
# writes work_ticket.mask_idx inside the run, after the PENDING/QUEUED →
# PROCESSING transition, so a queued ticket carries a NULL mask and cannot be
# attributed to any mask here.
#
# `ticket_state` collapses a sample's several tickets (retries) to one row: a
# `completed` ticket if any exists, else the newest. `work_ticket_state` is NULL on
# the gate arm — the gate is a rollup, so no single ticket state describes it.
#
# The anti-join sits INSIDE `ticket_state`, before the DISTINCT ON, not on the
# UNION arm after it. Equivalent because the anti-join correlates ONLY on
# `(mask_idx, prep_sample_idx)`, which IS the DISTINCT ON key — every ticket in a
# group shares it, so a group is dropped whole or kept whole either way, and the
# rows reaching the ORDER BY tie-break are unchanged. A future correlation on any
# other `wt` column would break that and must stay on the UNION arm.
#
# Placing it before the sort keeps gate-covered tickets out of the sort input.
# Measured on Postgres 17.10, 1M work_ticket rows / 20k samples / 200 masks, both
# shapes returning identical result sets, unfiltered list:
#
#     gate coverage:   0%     10%     20%     30%     70%
#     inside (this):  854ms  809ms   687ms   648ms   446ms
#     on UNION arm:   766ms  751ms   758ms   768ms   765ms
#
# So it is a win above ~15% gate coverage and a ~10% loss below it. The qiita-miint
# deploy measured 91.8% (4157 of 4530 ticket pairs gate-covered) on the same
# Postgres 17.10, which is where the coverage sits when masking is completing:
# both paths write the gate row first-class and the per-sample backfill populated
# every historical completed mask. That deploy also carries ~4.5k per-sample mask
# tickets — an order of magnitude below the smallest point measured above, so this
# placement is headroom rather than a fix for a hot query.
#
# $1 is the per-sample masking action ids, $2 the completed work_ticket state,
# $3/$4 the two mask_sample states.
_MASKED_SAMPLE_CTE = """
WITH ticket_state AS (
    SELECT DISTINCT ON (wt.mask_idx, wt.prep_sample_idx)
           wt.mask_idx, wt.prep_sample_idx, wt.state AS work_ticket_state
      FROM qiita.work_ticket wt
     WHERE wt.action_id = ANY($1::text[])
       AND wt.mask_idx IS NOT NULL
       AND wt.prep_sample_idx IS NOT NULL
       AND NOT EXISTS (
               SELECT 1 FROM qiita.mask_sample ms0
                WHERE ms0.mask_idx = wt.mask_idx
                  AND ms0.prep_sample_idx = wt.prep_sample_idx
           )
     ORDER BY wt.mask_idx, wt.prep_sample_idx,
              (wt.state = $2::qiita.work_ticket_state) DESC, wt.created_at DESC
),
masked_sample AS (
    SELECT ms.mask_idx, ms.prep_sample_idx,
           ms.state AS mask_state,
           'mask_sample'::text AS source,
           NULL::qiita.work_ticket_state AS work_ticket_state
      FROM qiita.mask_sample ms
    UNION ALL
    SELECT ts.mask_idx, ts.prep_sample_idx,
           CASE WHEN ts.work_ticket_state = $2::qiita.work_ticket_state
                THEN $3 ELSE $4 END,
           'work_ticket'::text,
           ts.work_ticket_state
      FROM ticket_state ts
)
"""

# The CTE's leading binds, in $1..$4 order. Every closed value set is bound from
# its declared twin rather than typed as a SQL literal, so a rename lights up the
# importer instead of silently matching nothing.
_MASKED_SAMPLE_ARGS: tuple = (
    list(PER_SAMPLE_MASK_ACTION_IDS),
    WorkTicketState.COMPLETED.value,
    _MASK_STATE_COMPLETED,
    _MASK_STATE_PENDING,
)

# The per-study narrowing predicate for a plain user: a correlated NOT EXISTS over
# `msk.prep_sample_idx`, admitting a sample only when no linked study denies it.
#
# SQL restatement of the policy the submission gate applies in Python
# (`auth.guards.require_caller_has_admin_on_all_studies`, as reached from
# routes/work_ticket.py). Arm for arm: the caller must hold Tier.ADMIN — or own the
# study — on EVERY non-retired link; a study row that does not exist is skipped
# (inner JOIN); a sample whose links are all retired is admitted (vacuous NOT
# EXISTS). Nothing pins the two together, so a change to either belongs in a PR
# that changes both — the guard carries the reciprocal note.
#
# Composition note: as a GATE the orphan case means "you already named the sample".
# As a FILTER it means every study-less prep_sample is visible to every caller.
#
# `{caller}` is the positional parameter holding the caller's principal_idx;
# `{tier}` the one holding the required tier.
_CALLER_MAY_SEE_SAMPLE = """
    NOT EXISTS (
        SELECT 1
          FROM qiita.prep_sample_to_study pts
          JOIN qiita.study s ON s.idx = pts.study_idx
          LEFT JOIN qiita.study_access sa
            ON sa.study_idx = pts.study_idx AND sa.principal_idx = {caller}
         WHERE pts.prep_sample_idx = msk.prep_sample_idx
           AND pts.retired = false
           AND s.owner_idx IS DISTINCT FROM {caller}
           AND (sa.access_tier IS NULL OR sa.access_tier <> {tier}::qiita.tier)
    )
"""

# Both reads exclude an entity-retired prep_sample, so the list's tally and the
# roster it invites the caller to fetch count the same set. Applied to the shared
# `masked_sample` CTE, not to either query alone. Distinct from the
# `prep_sample_to_study.retired` LINK flag inside the predicate above.
_SAMPLE_NOT_RETIRED = """
    EXISTS (
        SELECT 1 FROM qiita.prep_sample ps_live
         WHERE ps_live.idx = msk.prep_sample_idx AND ps_live.retired = false
    )
"""

# Value stamped on qiita.mask_definition.adapter_hash_scheme for a row whose
# params.resolved_qc.adapter_set_hash came from the reference's sorted
# qiita.feature.sequence_hash values (reference_membership.reference_sequence_set_hash).
# A NULL column is the legacy serialized-Parquet-bytes derivation. Mirrors the
# CHECK constraint in the 20260804000000 migration; adding a value changes both.
ADAPTER_HASH_SCHEME_SEQUENCE_HASH = "sequence_hash_v1"

# The two `params` keys addressing the adapter identity inside the blob
# `runner._mask._build_mask_params` produces. They live here, not next to that
# builder, because this module and the re-key backfill both address the path
# without going through it — the scheme stamp below, the backfill's SQL, and its
# params rewrite — and the repository is what every one of them already imports.
# A rename in the builder has to reach these.
RESOLVED_QC_KEY = "resolved_qc"
ADAPTER_SET_HASH_KEY = "adapter_set_hash"
# The same path as a Postgres jsonb text accessor:
# `params->'resolved_qc'->>'adapter_set_hash'`.
ADAPTER_SET_HASH_JSON_PATH = f"'{RESOLVED_QC_KEY}'->>'{ADAPTER_SET_HASH_KEY}'"


async def mint_mask_definition(
    conn: asyncpg.Connection,
    *,
    filter_workflow: str,
    filter_version: str,
    params: dict,
    principal_idx: int,
    legacy_params: dict | None = None,
    adapter_hash_scheme: str | None = None,
) -> asyncpg.Record:
    """Mint (or return the existing) mask_definition row for a config.

    Deduplicates on the canonical-JSON SHA-256 of `params` alone — the
    dedup key is the config blob, so the same config resolves to the same
    `mask_idx` fleet-wide. `filter_workflow` / `filter_version` are stored as
    descriptive columns; they are expected to also appear inside `params`
    so the hash covers them, but the hash is over `params` so two callers
    that pass the same `params` collapse to one row regardless.

    `legacy_params` is the same config carrying the legacy
    `resolved_qc.adapter_set_hash` (see `runner._mask._adapter_set_hash_legacy`).
    When `params` misses and `legacy_params` hits, that row is this config and is
    re-keyed onto the current hash in place, keeping its mask_idx. Pass None for a
    config that carries no adapter set: the two derivations agree on None, and a
    None legacy hash makes the SQL skip the lookup.

    `adapter_hash_scheme` is the caller stating which derivation produced the
    `resolved_qc.adapter_set_hash` inside `params`. It is NOT inferred from the
    blob: only a caller that derived the hash knows that, and the public
    `POST /mask-definition` route mints whatever `params` a client sends. Left
    None there, such a row stays unstamped and shows up in `qiita-admin backfill
    mask-adapter-hash`'s report — which is where a hash nobody can attribute
    belongs. Inferring it from "params carries a hash" would instead mark it
    converted and let the contract phase's gate read green over an unknown
    derivation. Pass None for a config with no adapter set: there is no
    derivation to record.

    Returns the qiita.mask_definition row as an asyncpg.Record. Raises
    asyncpg.ForeignKeyViolationError when principal_idx does not exist.

    No `require_transaction(conn)` guard: the qiita.mint_mask_definition
    plpgsql body (the SELECT/UPDATE/INSERT upsert loop) executes as a single SQL
    statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    legacy_params_hash = canonical_params_hash(legacy_params) if legacy_params is not None else None
    # asyncpg encodes a dict bound to a jsonb parameter via the JSON codec; pass
    # the serialized string explicitly so the behaviour is independent of
    # whether a JSON codec is registered on the connection.
    try:
        return await conn.fetchrow(
            f"SELECT {MASK_DEFINITION_RETURNING}"
            "  FROM qiita.mint_mask_definition($1, $2, $3, $4::jsonb, $5, $6, $7)",
            params_hash,
            filter_workflow,
            filter_version,
            json.dumps(params),
            principal_idx,
            legacy_params_hash,
            adapter_hash_scheme,
        )
    except asyncpg.CheckViolationError as exc:
        # The mint raises SQLSTATE 23514 for a deprecated config. asyncpg maps that
        # class to CheckViolationError, which a genuine table CHECK would also
        # raise — so match on the message the function sets, and re-raise anything
        # else untouched rather than reporting an unrelated constraint as a
        # deprecation.
        if "is deprecated" not in str(exc):
            raise
        raise MaskDefinitionDeprecated(str(exc)) from exc


async def fetch_mask_definition_by_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    mask_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.mask_definition row for mask_idx, or None.

    Accepts either a pool or a connection so the helper composes inside an
    open transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        f"SELECT {MASK_DEFINITION_RETURNING}  FROM qiita.mask_definition WHERE mask_idx = $1",
        mask_idx,
    )


def _sample_scope_sql(
    *,
    args: list,
    sequenced_pool_idx: int | None,
    prep_sample_idx: int | None,
    visible_to_principal_idx: int | None,
) -> tuple[str, bool]:
    """Build the `masked_sample` narrowing clauses, appending each bound value to
    `args`. Returns (sql, narrowed), where `narrowed` is True iff a caller-supplied
    narrowing was applied.

    The SQL is ANDed onto a query whose FROM carries the `masked_sample` CTE
    aliased `msk`. The retirement exclusion is unconditional and does not count as
    a narrowing — it bounds both reads identically rather than reflecting anything
    the caller asked for. The three that do: `sequenced_pool_idx` joins through
    qiita.sequenced_sample, `prep_sample_idx` matches directly, and
    `visible_to_principal_idx` applies the per-study predicate. Pass None for
    `visible_to_principal_idx` only for a caller holding the bypass role — it
    means "see every sample".
    """
    clauses = f" AND {_SAMPLE_NOT_RETIRED}"
    narrowed = False
    if sequenced_pool_idx is not None:
        narrowed = True
        args.append(sequenced_pool_idx)
        clauses += (
            f" AND EXISTS (SELECT 1 FROM qiita.sequenced_sample ss"
            f"              WHERE ss.prep_sample_idx = msk.prep_sample_idx"
            f"                AND ss.sequenced_pool_idx = ${len(args)})"
        )
    if prep_sample_idx is not None:
        narrowed = True
        args.append(prep_sample_idx)
        clauses += f" AND msk.prep_sample_idx = ${len(args)}"
    if visible_to_principal_idx is not None:
        narrowed = True
        args.append(visible_to_principal_idx)
        caller_param = f"${len(args)}"
        args.append(Tier.ADMIN.value)
        clauses += " AND " + _CALLER_MAY_SEE_SAMPLE.format(
            caller=caller_param, tier=f"${len(args)}"
        )
    return clauses, narrowed


async def list_mask_definitions(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    visible_to_principal_idx: int | None,
    sequenced_pool_idx: int | None = None,
    prep_sample_idx: int | None = None,
    status: MaskDefinitionStatus | None = None,
    limit: int,
) -> list[asyncpg.Record]:
    """Return up to `limit` mask_definition rows, newest first, each with its
    per-mask sample tally under the same narrowings.

    `visible_to_principal_idx` has no default: None means "see every sample", so a
    caller that omitted it by accident would get fleet-wide visibility silently.

    The tally counts `masked_sample` rows (see `_MASKED_SAMPLE_CTE`) by state.
    `samples_completed` is the set a masked-read pull or an assembly submission can
    act on; `samples_pending` is the rest — a covering block still running on the
    block path, a ticket not yet completed on the per-sample one. Entity-retired
    prep_samples are excluded, so the tally counts exactly what
    `fetch_mask_prep_samples` will return.

    Which masks come back:

      * A bypass-role caller with no filters gets every mask, including one that
        has no samples yet (tally 0).
      * Any narrowing — a filter, or a `visible_to_principal_idx` — restricts the
        result to masks with at least one matching sample. A plain user therefore
        sees the masks that touched their own samples, and a zero-tally row never
        reveals a mask the narrowing excluded.

    `status` narrows to one config lifecycle. Omitted, BOTH are returned: a
    deprecated mask stays listed so "what filter produced this published
    submission?" keeps an answer.

    Callers that need to detect truncation pass `limit = cap + 1`; a returned
    length > cap means the set exceeded the cap.
    """
    args: list = [*_MASKED_SAMPLE_ARGS]
    args.append(_MASK_STATE_INVALIDATED)
    invalidated_param = f"${len(args)}"
    scope, narrowed = _sample_scope_sql(
        args=args,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=prep_sample_idx,
        visible_to_principal_idx=visible_to_principal_idx,
    )
    status_predicate = ""
    if status is not None:
        args.append(status.value)
        status_predicate = f"md.status = ${len(args)}"
    args.append(limit)
    limit_param = f"${len(args)}"
    # `narrowed` and `status` are independent WHERE sources, so the connective is
    # computed rather than baked into either fragment.
    where_parts = [p for p in ("t.mask_idx IS NOT NULL" if narrowed else "", status_predicate) if p]
    where_sql = ("  WHERE " + " AND ".join(where_parts)) if where_parts else ""
    query = (
        _MASKED_SAMPLE_CTE + ", tally AS ("
        "    SELECT msk.mask_idx,"
        "           count(*) FILTER (WHERE msk.mask_state = $3) AS samples_completed,"
        "           count(*) FILTER (WHERE msk.mask_state = $4) AS samples_pending,"
        f"           count(*) FILTER (WHERE msk.mask_state = {invalidated_param})"
        "               AS samples_invalidated"
        "      FROM masked_sample msk"
        f"     WHERE true{scope}"
        "     GROUP BY msk.mask_idx"
        ")"
        f" SELECT {_MASK_DEFINITION_LIST_COLUMNS},"
        "        COALESCE(t.samples_completed, 0) AS samples_completed,"
        "        COALESCE(t.samples_pending, 0) AS samples_pending,"
        "        COALESCE(t.samples_invalidated, 0) AS samples_invalidated"
        "   FROM qiita.mask_definition md"
        "   LEFT JOIN tally t ON t.mask_idx = md.mask_idx"
        + where_sql
        + "  ORDER BY md.mask_idx DESC"
        f"  LIMIT {limit_param}"
    )
    return list(await pool_or_conn.fetch(query, *args))


async def fetch_mask_prep_samples(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    mask_idx: int,
    *,
    visible_to_principal_idx: int | None,
    sequenced_pool_idx: int | None = None,
    limit: int,
) -> list[asyncpg.Record]:
    """Return up to `limit` samples masked under `mask_idx`, ascending by
    prep_sample_idx, each with its masking state and which path resolved it.

    `visible_to_principal_idx` has no default, for the reason given on
    `list_mask_definitions`.

    Joins qiita.biosample for the accession so a caller can name the sample
    without a second read; the accession is NULL until the biosample is
    submitted to NCBI. Retired prep_samples are excluded, matching the admin
    export roster and this mask's tally in the list read.

    An empty list for a mask that exists means no sample the caller may see is
    masked under it — not that the mask is absent. The route checks existence
    separately so a typo'd mask_idx 404s rather than reading as "no samples".

    Callers that need to detect truncation pass `limit = cap + 1`.
    """
    args: list = [*_MASKED_SAMPLE_ARGS, mask_idx]
    mask_param = f"${len(args)}"
    scope, _narrowed = _sample_scope_sql(
        args=args,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=None,
        visible_to_principal_idx=visible_to_principal_idx,
    )
    args.append(limit)
    query = (
        _MASKED_SAMPLE_CTE
        + " SELECT msk.prep_sample_idx, msk.mask_state, msk.source, msk.work_ticket_state,"
        "        bs.biosample_accession"
        "   FROM masked_sample msk"
        "   JOIN qiita.prep_sample ps ON ps.idx = msk.prep_sample_idx"
        "   JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
        f"  WHERE msk.mask_idx = {mask_param}"
        f"{scope}"
        "  ORDER BY msk.prep_sample_idx"
        f"  LIMIT ${len(args)}"
    )
    return list(await pool_or_conn.fetch(query, *args))


class MaskDefinitionNotFound(Exception):
    """Raised when the mask_idx does not exist."""

    def __init__(self, mask_idx: int) -> None:
        self.mask_idx = mask_idx
        super().__init__(f"no mask_definition with mask_idx={mask_idx}")


async def transition_mask_definition_status(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    mask_idx: int,
    status: MaskDefinitionStatus,
    reason: str | None,
    superseded_by: int | None,
    principal_idx: int,
) -> asyncpg.Record:
    """Set a mask CONFIG's lifecycle status and return the updated row.

    The three provenance columns move with `status` in one UPDATE, so the
    biconditional CHECK on the table can never see a half-applied transition.
    Idempotent: re-deprecating an already-deprecated mask restamps who/when/why
    rather than refusing, which is what a corrected reason wants.

    Raises MaskDefinitionNotFound when the row does not exist, and
    asyncpg.ForeignKeyViolationError when `superseded_by` names no mask.
    """
    deprecating = status is MaskDefinitionStatus.DEPRECATED
    row = await conn.fetchrow(
        f"UPDATE qiita.mask_definition"
        f"    SET status = $2,"
        f"        deprecated_at = CASE WHEN $3::boolean THEN now() END,"
        f"        deprecated_by_idx = CASE WHEN $3::boolean THEN $4::bigint END,"
        f"        deprecation_reason = CASE WHEN $3::boolean THEN $5::text END,"
        f"        superseded_by = CASE WHEN $3::boolean THEN $6::bigint END"
        f"  WHERE mask_idx = $1"
        f" RETURNING {MASK_DEFINITION_RETURNING}",
        mask_idx,
        status.value,
        deprecating,
        principal_idx,
        reason,
        superseded_by,
    )
    if row is None:
        raise MaskDefinitionNotFound(mask_idx)
    return row


async def set_mask_sample_states(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idxs: list[int],
    state: Literal["completed", "invalidated"],
    reason: str | None,
    principal_idx: int,
) -> dict[str, list[int]]:
    """Withdraw (or restore) specific RUNS of one mask.

    Returns `{"updated", "unchanged", "not_found", "skipped_pending"}` over the
    requested prep_samples: rows whose state changed, rows that already held the
    requested state, prep_samples with no gate row under this mask, and rows still
    `'pending'`.

    A `'pending'` row is skipped rather than written. `'pending'` is the masking
    pipeline's value — reconcile and finalize-mask-sample flip it to `'completed'`
    when the run lands — so writing `'invalidated'` over it would be undone by the
    pipeline without anyone being told. There is also nothing to withdraw yet: the
    run has not produced a pass-set.

    Runs inside the caller's transaction and takes each row's `FOR UPDATE` lock
    before deciding, the same serialization every other mask_sample writer uses
    (`lock_mask_sample`). Without it the classification is read on one pooled
    connection and applied on another, so a concurrent reconcile could flip a row
    out of `'pending'` between the two and the response would describe a state that
    never held.
    """
    require_transaction(conn)
    present = {
        r["prep_sample_idx"]: r["state"]
        for r in await conn.fetch(
            "SELECT prep_sample_idx, state FROM qiita.mask_sample"
            " WHERE mask_idx = $1 AND prep_sample_idx = ANY($2::bigint[])"
            " ORDER BY prep_sample_idx"
            " FOR UPDATE",
            mask_idx,
            prep_sample_idxs,
        )
    }
    not_found = [idx for idx in prep_sample_idxs if idx not in present]
    skipped_pending = [idx for idx, st in present.items() if st == _MASK_STATE_PENDING]
    unchanged = [idx for idx, st in present.items() if st == state]
    to_update = [idx for idx, st in present.items() if st not in (state, _MASK_STATE_PENDING)]
    if to_update:
        invalidating = state == _MASK_STATE_INVALIDATED
        await conn.execute(
            "UPDATE qiita.mask_sample"
            "    SET state = $3,"
            "        invalidated_at = CASE WHEN $4::boolean THEN now() END,"
            "        invalidated_by_idx = CASE WHEN $4::boolean THEN $5::bigint END,"
            "        invalidation_reason = CASE WHEN $4::boolean THEN $6::text END"
            "  WHERE mask_idx = $1 AND prep_sample_idx = ANY($2::bigint[])",
            mask_idx,
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
