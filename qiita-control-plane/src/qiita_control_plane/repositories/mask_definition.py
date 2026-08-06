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
from typing import get_args

import asyncpg
from qiita_common.actions import PER_SAMPLE_MASK_ACTION_IDS
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import MaskSampleState, Tier, WorkTicketState

# The two mask_sample states, taken from the wire type rather than retyped, so the
# SQL and the model cannot drift. Ordered 'pending', 'completed' by the Literal.
_MASK_STATE_PENDING, _MASK_STATE_COMPLETED = get_args(MaskSampleState)

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
    return await conn.fetchrow(
        "SELECT mask_idx, params_hash, filter_workflow, filter_version,"
        "       params, created_by_idx, created_at, adapter_hash_scheme"
        "  FROM qiita.mint_mask_definition($1, $2, $3, $4::jsonb, $5, $6, $7)",
        params_hash,
        filter_workflow,
        filter_version,
        json.dumps(params),
        principal_idx,
        legacy_params_hash,
        adapter_hash_scheme,
    )


async def fetch_mask_definition_by_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    mask_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.mask_definition row for mask_idx, or None.

    Accepts either a pool or a connection so the helper composes inside an
    open transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        "SELECT mask_idx, params_hash, filter_workflow, filter_version,"
        "       params, created_by_idx, created_at, adapter_hash_scheme"
        "  FROM qiita.mask_definition"
        " WHERE mask_idx = $1",
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

    Callers that need to detect truncation pass `limit = cap + 1`; a returned
    length > cap means the set exceeded the cap.
    """
    args: list = [*_MASKED_SAMPLE_ARGS]
    scope, narrowed = _sample_scope_sql(
        args=args,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=prep_sample_idx,
        visible_to_principal_idx=visible_to_principal_idx,
    )
    args.append(limit)
    limit_param = f"${len(args)}"
    query = (
        _MASKED_SAMPLE_CTE + ", tally AS ("
        "    SELECT msk.mask_idx,"
        "           count(*) FILTER (WHERE msk.mask_state = $3) AS samples_completed,"
        "           count(*) FILTER (WHERE msk.mask_state = $4) AS samples_pending"
        "      FROM masked_sample msk"
        f"     WHERE true{scope}"
        "     GROUP BY msk.mask_idx"
        ")"
        " SELECT md.mask_idx, md.filter_workflow, md.filter_version, md.params, md.created_at,"
        "        COALESCE(t.samples_completed, 0) AS samples_completed,"
        "        COALESCE(t.samples_pending, 0) AS samples_pending"
        "   FROM qiita.mask_definition md"
        "   LEFT JOIN tally t ON t.mask_idx = md.mask_idx"
        + ("  WHERE t.mask_idx IS NOT NULL" if narrowed else "")
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
