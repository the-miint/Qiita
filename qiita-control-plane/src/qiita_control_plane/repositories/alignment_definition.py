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
principal_idx) and asyncpg.InvalidParameterValueError (SQLSTATE 22023, a
non-32-byte hash — unreachable via this helper) propagate to the caller.
"""

import json
from collections.abc import Sequence

import asyncpg
from qiita_common.hashing import canonical_params_hash


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
    asyncpg.ForeignKeyViolationError when principal_idx does not exist.

    No `require_transaction(conn)` guard: the qiita.mint_alignment_definition
    plpgsql body (the SELECT/INSERT upsert loop) executes as a single SQL
    statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    # asyncpg encodes a dict bound to a jsonb parameter via the JSON codec; pass
    # the serialized string explicitly so the behaviour is independent of
    # whether a JSON codec is registered on the connection.
    return await conn.fetchrow(
        "SELECT alignment_idx, params_hash, params, created_by_idx, created_at"
        "  FROM qiita.mint_alignment_definition($1, $2::jsonb, $3)",
        params_hash,
        json.dumps(params),
        principal_idx,
    )


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
        "SELECT alignment_idx, params_hash, params, created_by_idx, created_at"
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
    """The pool's non-retired sequenced samples, as prep_sample_idx.

    Same sample set as the other pool rollups (`ps.retired IS NOT TRUE`), so a
    pool's alignments are counted over the same population its completion and
    QC reports describe.
    """
    rows = await pool_or_conn.fetch(
        "SELECT ss.prep_sample_idx"
        "  FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1 AND ps.retired IS NOT TRUE"
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
        "        c.samples_completed, c.samples_total"
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
        "   AND state = 'completed'"
        " ORDER BY prep_sample_idx",
        alignment_idx,
        list(prep_sample_idxs),
    )
    return [r["prep_sample_idx"] for r in rows]
