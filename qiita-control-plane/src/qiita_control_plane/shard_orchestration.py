"""Sharded-index fan-out orchestration.

Sits above the `plan_shards` assignment core (`actions.library`) and turns its
output into N build tickets — the sharded analogue of
`block_planner.plan_and_submit_blocks`, minus the block stack's cover-map /
completion-gate machinery. A shard is a clean partition of ONE reference (the
reference IS the accounting unit), so this stays deliberately lighter:

  * every build ticket is `scope_target_kind='reference'` (no new scope kind),
    discriminated only by `work_ticket.shard_id`;
  * the cover-map is `reference_membership.shard_id` (written by `plan_shards`),
    not a separate `block_member` table;
  * completion is count-based (`finalize_shard`), not a gate table.

`plan_and_submit_shards` is NOT a LIBRARY primitive: it needs a `dispatch_cb`
(the runner threads `lambda idx: schedule_dispatch(app, idx)` down to it) which
a static LIBRARY callable can't receive. The runner's `plan-shards` arm calls it
directly; the reusable assignment core (`plan_shards`) and completion primitive
(`finalize_shard`) stay in `actions.library` where the LIBRARY dict can register
them without an import cycle back through this module.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.models import (
    INDEX_TYPE_BOWTIE2,
    INDEX_TYPE_MINIMAP2,
    ReferenceStatus,
)

from .actions.library import plan_shards
from .actions.reference import IllegalStatusTransition, transition_reference_status
from .fanout_dispatch import DEFAULT_FANOUT_MAX_INFLIGHT, shard_cohort, top_up_dispatch
from .shard_planner import _SHARD_COUNT

# The build-shard-index workflow each fan-out ticket runs. Pinned here (the YAML
# ships at this version); a version bump updates this pair.
BUILD_SHARD_INDEX_ACTION_ID = "build-shard-index"
BUILD_SHARD_INDEX_ACTION_VERSION = "1.0.0"

# action_context build-gate flag -> the reference_index.index_type it produces.
# finalize_shard counts registered shards per expected type; the fan-out copies
# these flags (+ knobs) into each shard ticket's context. Ordered so the gate
# subset is stable. Per-shard rype is no longer built: routing is served by
# the ONE whole-reference `rype_router` the parent reference-add builds, so the
# per-shard analysis indexes are minimap2 + bowtie2 only.
SHARD_BUILD_INDEX_TYPES: dict[str, str] = {
    "build_minimap2": INDEX_TYPE_MINIMAP2,
    "build_bowtie2": INDEX_TYPE_BOWTIE2,
}

# The parent reference-add action_context keys the fan-out copies verbatim into
# each shard build ticket (the build gates + their scalar knobs). The child
# build-shard-index workflow's context_schema is exactly these.
SHARD_BUILD_CONTEXT_KEYS: tuple[str, ...] = (
    "build_minimap2",
    "build_bowtie2",
    "minimap2_preset",
)


def expected_shard_index_types(action_context: dict[str, Any]) -> list[str]:
    """The reference_index.index_type values a sharded build is expected to
    register, derived from the build-gate flags in `action_context` (an absent
    flag counts as ON, matching the build-shard-index workflow defaults, so a
    context that sets none still expects the full set). finalize_shard checks
    each against N."""
    return [
        index_type
        for flag, index_type in SHARD_BUILD_INDEX_TYPES.items()
        if action_context.get(flag, True)
    ]


async def plan_and_submit_shards(
    pool: asyncpg.Pool,
    reference_idx: int,
    *,
    signing_key: bytes,
    data_plane_url: str,
    workspace: Path,
    originator_principal_idx: int,
    build_action_id: str,
    build_action_version: str,
    action_context: dict[str, Any],
    dispatch_cb: Callable[[int], Any],
    max_inflight: int = DEFAULT_FANOUT_MAX_INFLIGHT,
    num_shards: int = _SHARD_COUNT,
) -> dict[str, Any]:
    """Assign this reference's features to shards, then fan out one build ticket
    per shard — THROTTLED.

    Runs `plan_shards` (assignment onto `reference_membership.shard_id`) → N. If
    N == 0 (a reference explicitly asked to shard but with no genomes / no genome
    map — nothing to shard) it returns a zero-shard summary WITHOUT transitioning
    or fanning out; the plan-shards runner arm treats a zero-shard result as an
    error and fails the ticket, since an unroutable `active` reference must never
    be finalized. If N > 0 it transitions the reference `loading -> indexing`
    and, in one transaction, INSERTs one build `work_ticket` per shard (scope
    `reference`, carrying `shard_id=k` and the index-selection `action_context`
    copied from the parent), each `dispatch_held` — then hands off to the pump.

    Throttle: every shard ticket is inserted HELD (`dispatch_held = true`, NOT
    dispatched). `top_up_dispatch` then releases only up to `max_inflight` of
    them; each child's terminal transition re-pumps the cohort and refills the
    freed slots (`dispatch._run_and_log`). So a 1000-shard reference opens at
    most `max_inflight` concurrent data-plane streams instead of ~1000 (the WOL3
    incident: fd exhaustion + ticket-expiry from the backlog). A single shard
    failure fail-stops the cohort (see `fanout_dispatch`).

    Idempotent on redrive: the per-shard INSERT is `ON CONFLICT DO NOTHING`
    against `work_ticket_one_in_flight_per_shard`, so a re-run over
    still-in-flight shard tickets creates no duplicates. The pump's release is
    likewise idempotent (it counts running-and-not-held, so already-in-flight
    shards occupy their slots). Crash between commit and the pump is covered:
    the tickets are left held + PENDING and startup `reconcile_inflight_tickets`
    re-pumps every cohort with held tickets.

    Returns a JSON-able summary (shard count, the fresh held ticket idxs).
    """
    n = await plan_shards(
        pool,
        reference_idx,
        signing_key=signing_key,
        data_plane_url=data_plane_url,
        workspace=workspace,
        num_shards=num_shards,
    )
    if n == 0:
        return {"reference_idx": reference_idx, "shards": 0, "tickets": []}

    action_context_json = json.dumps(action_context)
    fresh_tickets: list[int] = []
    async with pool.acquire() as conn, conn.transaction():
        # loading -> indexing. Idempotent on redrive: a re-run finds the
        # reference already `indexing` (a prior fan-out set it), which the
        # guarded UPDATE rejects with IllegalStatusTransition — tolerate exactly
        # that case, but fail loud if the reference moved anywhere else (e.g.
        # already `active`, where re-fanning-out would be wrong).
        try:
            await transition_reference_status(conn, reference_idx, ReferenceStatus.INDEXING)
        except IllegalStatusTransition:
            current = await conn.fetchval(
                "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
            )
            if current != ReferenceStatus.INDEXING.value:
                raise
        # One set-based INSERT over generate_series(0, n-1) instead of n
        # per-shard round-trips. ON CONFLICT DO NOTHING RETURNING emits a row
        # only for tuples actually inserted, so `fresh_tickets` stays exactly the
        # freshly-minted set (a full redrive returns nothing → dispatches
        # nothing) — the same idempotency the loop had. The ON CONFLICT arbiter
        # clause is byte-identical (the per-shard partial unique index).
        rows = await conn.fetch(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, reference_idx, shard_id, action_context, dispatch_held"
            ") SELECT $1, $2, $3, 'reference', $4, s.shard_id, $6::jsonb, true"
            "    FROM generate_series(0, $5 - 1) AS s(shard_id)"
            " ON CONFLICT (action_id, action_version, reference_idx, shard_id)"
            "   WHERE shard_id IS NOT NULL"
            "     AND state IN ('pending', 'queued', 'processing')"
            " DO NOTHING"
            " RETURNING work_ticket_idx",
            build_action_id,
            build_action_version,
            originator_principal_idx,
            reference_idx,
            n,
            action_context_json,
        )
        fresh_tickets.extend(r["work_ticket_idx"] for r in rows)

    # Every fresh ticket is HELD. Release the first `max_inflight` now; each
    # child's terminal transition re-pumps this cohort and refills the freed
    # slots. On a redrive the pump counts already-in-flight shards and releases
    # only the difference (or nothing under fail-stop).
    await top_up_dispatch(
        pool,
        shard_cohort(reference_idx),
        max_inflight=max_inflight,
        dispatch_cb=dispatch_cb,
    )

    return {"reference_idx": reference_idx, "shards": n, "tickets": fresh_tickets}
