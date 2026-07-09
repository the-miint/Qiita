"""qiita-admin CLI — mask delete subcommand and purge-failed selectors/leaf calls.

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse
import json
import sys
import time

import asyncpg
from qiita_common.api_paths import (
    PATH_MASK_DEFINITION_PREFIX,
    PATH_WORK_TICKET_PREFIX,
    PATH_WORK_TICKET_ROOT,
)

from .. import _common

# ---------------------------------------------------------------------------
# mask delete / mask purge-failed
# ---------------------------------------------------------------------------

# The two affected workflows. The move-then-read ordering bug lived in both
# read-mask/1.0.0 and fastq-to-parquet/1.3.0 (same register→persist shape), so
# the recovery covers both. The selector keys on failure_reason, not workflow,
# but we still scope the candidate set to these action_ids so an unrelated
# action that happens to log the same string is never swept up.
_PURGE_FAILED_ACTION_IDS = ("read-mask", "fastq-to-parquet")

# The failure_reason substring the move-then-read bug leaves behind: host_filter
# and register-files both succeeded (the mask IS registered in DuckLake), only
# persist-read-metrics failed re-opening the moved-away staging path.
_READ_MASK_PARQUET_NOT_FOUND = "read_mask parquet not found"

# Resubmit is faithful ONLY for prep_sample-scoped tickets — both affected
# actions are prep_sample-scoped, so this is the only kind we expect. A
# candidate of any other kind is reported (defensive) rather than guessed at.
_RESUBMITTABLE_SCOPE_KIND = "prep_sample"


def _mask_delete_via_route(base_url: str, token: str, mask_idx: int) -> dict:
    """DELETE /api/v1/mask-definition/{mask_idx} as the PAT.

    Going through the route (not a direct data-plane DoAction) exercises the
    mask_definition:delete scope check AND the route's lake-first ordering
    (DuckLake read_mask rows → Postgres mask_definition row), which is exactly
    what the bulk tool wants per delete."""
    # _common.call prepends API_PREFIX, so pass the post-prefix segment.
    return _common.call(
        "DELETE",
        base_url,
        token,
        f"{PATH_MASK_DEFINITION_PREFIX}/{mask_idx}",
    )


async def _select_purge_failed_candidates(
    pool: asyncpg.Pool, *, action_ids: tuple[str, ...], limit: int | None
) -> list[asyncpg.Record]:
    """Failed tickets for the chosen action(s) carrying the move-then-read
    failure signature, with everything needed to resubmit. Ordered by
    work_ticket_idx so a --limit slice is stable across runs."""
    query = (
        "SELECT work_ticket_idx, action_id, action_version, scope_target_kind,"
        "       prep_sample_idx, action_context, originator_principal_idx, mask_idx"
        "  FROM qiita.work_ticket"
        " WHERE state = 'failed'"
        "   AND action_id = ANY($1::text[])"
        "   AND failure_reason LIKE '%' || $2 || '%'"
        " ORDER BY work_ticket_idx"
    )
    args: list = [list(action_ids), _READ_MASK_PARQUET_NOT_FOUND]
    if limit is not None:
        query += " LIMIT $3"
        args.append(limit)
    return await pool.fetch(query, *args)


async def _count_non_failed_missing_mask_idx(
    pool: asyncpg.Pool, *, action_ids: tuple[str, ...]
) -> int:
    """Count non-failed tickets for these action(s) that have a NULL mask_idx.

    This is the backfill-completeness gate. The shared-mask guard
    (_mask_shared_with_non_failed) keys on `mask_idx = $1 AND state <> 'failed'`,
    so a non-failed ticket that genuinely shares a mask but whose mask_idx was
    never backfilled (still NULL) is INVISIBLE to the guard — we could then
    delete a mask a COMPLETED result depends on, silently dropping its read_mask
    rows. While ANY such ticket exists, the guard is unsound, so --execute must
    refuse. (Tickets in a *failed* state with NULL mask_idx are fine here: they
    are not the ones the guard protects — they land in skipped_no_mask_idx.)"""
    return await pool.fetchval(
        "SELECT COUNT(*) FROM qiita.work_ticket"
        " WHERE action_id = ANY($1::text[])"
        "   AND state <> 'failed'"
        "   AND mask_idx IS NULL",
        list(action_ids),
    )


async def _mask_shared_with_non_failed(pool: asyncpg.Pool, mask_idx: int) -> list[int]:
    """work_ticket_idxs in ANY non-failed state that reference this mask.

    The shared-mask guard: the config-hash dedup means one mask_idx can back
    many tickets across runs, including COMPLETED ones. Deleting a mask a
    non-failed ticket depends on would silently drop a live result's read_mask
    rows — so if this returns a non-empty list, we SKIP the mask entirely.
    Relies on mask_idx being populated on non-failed tickets (tickets predating
    mask_idx tracking read NULL and the guard misses them)."""
    rows = await pool.fetch(
        "SELECT work_ticket_idx FROM qiita.work_ticket"
        " WHERE mask_idx = $1 AND state <> 'failed'"
        " ORDER BY work_ticket_idx",
        mask_idx,
    )
    return [r["work_ticket_idx"] for r in rows]


def _build_resubmit_body(row: asyncpg.Record) -> dict:
    """Reconstruct the WorkTicketCreateRequest body from a stored ticket row.

    Both affected actions are prep_sample-scoped, so the only scope_target we
    rebuild is the prep_sample form. action_context is stored as JSON text on the
    row; decode it back to the object the submit route validates against the
    action's context_schema. originator is NOT carried — the resubmit route sets
    originator_principal_idx server-side from the authenticated caller (the
    operator running this command), which is the intended provenance for a
    recovery re-run."""
    raw_context = row["action_context"]
    if isinstance(raw_context, str):
        action_context = json.loads(raw_context) if raw_context else {}
    elif raw_context is None:
        action_context = {}
    else:
        action_context = dict(raw_context)
    return {
        "action_id": row["action_id"],
        "action_version": row["action_version"],
        "scope_target": {
            "kind": _RESUBMITTABLE_SCOPE_KIND,
            "prep_sample_idx": row["prep_sample_idx"],
        },
        "action_context": action_context,
    }


def _resubmit_work_ticket(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/work-ticket as the PAT, returning {work_ticket_idx, state}.

    Reuses the normal submission path so the resubmit goes through the same
    validation, disallow-without-delete gate (a FAILED original never blocks),
    and dispatch a fresh `qiita` submit would. With the stale mask purged first,
    the re-run mints a fresh mask_idx and runs clean on the reordered workflow."""
    return _common.call(
        "POST", base_url, token, f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_ROOT}", json=body
    )


# Terminal work-ticket states the --wait poll stops on.
_TERMINAL_TICKET_STATES = frozenset({"completed", "no_data", "failed"})

# Default poll cadence + ceiling for --wait. Generous ceiling because a real
# read-mask run is a SLURM job; the operator can Ctrl-C and re-check by hand.
_WAIT_POLL_INTERVAL_SECONDS = 10

_WAIT_TIMEOUT_SECONDS = 3600


def _poll_ticket_to_terminal(base_url: str, token: str, work_ticket_idx: int) -> str:
    """Poll GET /work-ticket/{idx} until a terminal state or the wait ceiling.

    Returns the final observed state (which may still be non-terminal if the
    ceiling is hit — the caller reports it as 'still running' rather than
    blocking the whole batch forever)."""
    deadline = time.monotonic() + _WAIT_TIMEOUT_SECONDS
    state = "unknown"
    while time.monotonic() < deadline:
        body = _common.call("GET", base_url, token, f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}")
        state = body.get("state", "unknown")
        if state in _TERMINAL_TICKET_STATES:
            return state
        time.sleep(_WAIT_POLL_INTERVAL_SECONDS)
    return state


def _handle_mask_delete(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.mask_idx <= 0:
        print("error: mask_idx must be a positive integer", file=sys.stderr)
        return 2

    def _render(body: dict | list) -> None:
        # body is the MaskDefinitionDeleteResponse dict.
        print(json.dumps(body, indent=2))
        if isinstance(body, dict) and "rows_deleted" in body:
            print(
                f"deleted mask_idx={body.get('mask_idx')}:"
                f" {body['rows_deleted']} read_mask row(s) removed.",
                file=sys.stderr,
            )

    return _common.run_http_subcommand(
        lambda t: _mask_delete_via_route(args.base_url, t, args.mask_idx),
        render=_render,
    )
