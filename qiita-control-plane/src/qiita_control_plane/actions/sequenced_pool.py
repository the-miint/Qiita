"""Sequenced-pool hard-delete: gating precheck + FK-ordered cascade.

A sequenced_pool is the bcl-convert "preparation" — one sample sheet's worth
of samples. Removing it tears down the pool row plus every sequenced_sample /
prep_sample under it, their metadata, study links, and any pool-/sample-scoped
work tickets, in one transaction. Mirrors actions/reference.py (the established
hard-delete-cascade pattern), down to the in-flight-vs-terminal work-ticket
gating and the re-gate-inside-the-teardown-transaction contract.

Two things this delete deliberately does NOT touch:
  * biosample rows — a biosample is a physical sample shared across studies and
    not owned by any single prep, so the cascade stops at prep_sample (unlike
    reference delete's orphan-feature GC, where features are reference-owned).
  * the parent sequencing_run — a run may hold other pools.
"""

from __future__ import annotations

import logging
from pathlib import Path

import asyncpg
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    TERMINAL_WORK_TICKET_STATES,
    WorkTicketState,
)

logger = logging.getLogger(__name__)

# Work-ticket states that block a pool delete. In-flight states block
# unconditionally (a running job is reading/writing the pool's data); terminal
# states block only without `force` (a completed test run is exactly what an
# admin purging a mis-pooled preparation wants gone).
#
# Between them these cover EVERY state, which is the point: a state in neither
# arm is invisible to the gate, and since the cascade below is state-blind, the
# delete would proceed unforced and purge its tickets anyway.
#
# NO_DATA belongs on the terminal side even though it mints no result. What the
# force gate protects is the ticket row — the record that this pool WAS processed
# and came back empty — not an artifact. FAILED mints no result either and has
# always blocked, so "has a result to lose" was never the criterion; "is terminal"
# is. The consequence is deliberate: an all-blank plate (no_data is the expected
# outcome for an empty well) needs an explicit `force` to delete.
#
# Not to be confused with _PREFLIGHT_EDIT_BLOCKING_STATES below, where excluding
# no_data IS deliberate.
_WORK_TICKET_IN_FLIGHT_STATES = NON_TERMINAL_WORK_TICKET_STATES
_WORK_TICKET_TERMINAL_STATES = TERMINAL_WORK_TICKET_STATES

# Work-ticket states that block a run-preflight EDIT (distinct from the delete
# gating above). The preflight — notably its lane assignment — feeds bcl-convert
# demultiplexing, so editing it once a job has consumed or is consuming it would
# silently diverge the stored preflight from what was actually processed.
# In-flight states (a job is actively reading the pool / its samples) and
# 'completed' (a result already exists) both block; 'failed', 'no_data', and
# not-yet-submitted deliberately do NOT — a failed run is exactly the recovery
# case the edit exists to serve (a stale preflight may be why it failed), so
# edit-then-retry must be allowed. Note this differs from delete gating, where
# 'failed' blocks unless forced.
#
# Derived (in-flight + COMPLETED) rather than spelled out, so it tracks the enum;
# the deliberate exclusions are the other two terminal states, no_data and failed.
_PREFLIGHT_EDIT_BLOCKING_STATES = (
    *NON_TERMINAL_WORK_TICKET_STATES,
    WorkTicketState.COMPLETED.value,
)


class SequencedPoolNotFound(Exception):
    """Raised when the sequenced_pool_idx doesn't exist."""


class PreflightNotEditable(Exception):
    """Raised when a sequenced_pool's run preflight cannot be edited because the
    run has already been processed — an in-flight or completed work ticket
    references the pool or one of its samples. `blocking` is the count of such
    tickets, surfaced in the route's 409 detail."""

    def __init__(self, *, sequenced_pool_idx: int, blocking: int) -> None:
        self.sequenced_pool_idx = sequenced_pool_idx
        self.blocking = blocking
        super().__init__(
            f"Sequenced pool {sequenced_pool_idx} preflight cannot be edited: "
            f"{blocking} in-flight/completed work ticket(s) "
            f"({'/'.join(_PREFLIGHT_EDIT_BLOCKING_STATES)}) reference it or its "
            "samples; the run has been processed. A failed or unsubmitted run is "
            "still editable — delete any completed result first if a correction "
            "to an already-processed run is truly required."
        )


class SequencedPoolDeleteBlocked(Exception):
    """Raised when a sequenced_pool cannot be deleted.

    `in_flight` work tickets always block. `terminal` work tickets, `published`
    prep_samples (linked into a study with is_published=true), and `ena`
    sequenced_samples (carrying an ENA experiment/run accession) each block only
    when the caller did not pass force=True — they are destructive overrides an
    admin must opt into explicitly."""

    def __init__(
        self,
        *,
        sequenced_pool_idx: int,
        in_flight: int,
        terminal: int,
        published: int,
        ena: int,
    ) -> None:
        self.sequenced_pool_idx = sequenced_pool_idx
        self.in_flight = in_flight
        self.terminal = terminal
        self.published = published
        self.ena = ena
        if in_flight:
            reason = (
                f"{in_flight} in-flight work ticket(s) "
                f"({'/'.join(_WORK_TICKET_IN_FLIGHT_STATES)}) reference it; "
                "wait for them to finish or cancel them"
            )
        else:
            parts: list[str] = []
            if terminal:
                parts.append(
                    f"{terminal} terminal work ticket(s) "
                    f"({'/'.join(_WORK_TICKET_TERMINAL_STATES)}) reference it"
                )
            if published:
                parts.append(f"{published} prep_sample(s) are published into a study")
            if ena:
                parts.append(f"{ena} sample(s) carry an ENA experiment/run accession")
            reason = "; ".join(parts) + "; re-issue with force=true to delete anyway"
        super().__init__(f"Sequenced pool {sequenced_pool_idx} cannot be deleted: {reason}")


def _rowcount(status: str) -> int:
    """Parse the affected-row count out of an asyncpg command tag
    (e.g. 'DELETE 5' → 5). Returns 0 for an unparseable tag rather than
    raising — the count is informational, not a control signal."""
    try:
        return int(status.rsplit(" ", 1)[1])
    except IndexError, ValueError:
        return 0


async def _pool_prep_sample_idxs(
    conn: asyncpg.Pool | asyncpg.Connection, sequenced_pool_idx: int
) -> list[int]:
    """The prep_sample idxs reachable from a pool, via its sequenced_samples.

    sequenced_sample ↔ prep_sample is 1:1 and each sequenced_sample belongs to
    exactly one pool, so this set is exclusive to the pool — no prep_sample
    here is shared with another pool."""
    return [
        r["prep_sample_idx"]
        for r in await conn.fetch(
            "SELECT prep_sample_idx FROM qiita.sequenced_sample WHERE sequenced_pool_idx = $1",
            sequenced_pool_idx,
        )
    ]


async def _pool_work_ticket_state_counts(
    conn: asyncpg.Pool | asyncpg.Connection, sequenced_pool_idx: int
) -> tuple[list[int], dict[str, int]]:
    """Existence check + per-state work_ticket tally for a sequenced_pool.

    Returns ``(prep_sample_idxs, counts)``: the pool's sequenced_samples'
    prep_sample idxs, and a {state: row_count} map over every work_ticket scoped to
    the pool OR any of those prep_samples. Raises SequencedPoolNotFound if the pool
    doesn't exist. asyncpg returns the PG enum `state` as a plain str, so the keys
    compare directly against the state-name tuples the callers sum over.

    Shared by `assert_sequenced_pool_deletable` and `assert_pool_preflight_editable`
    so the pool→prep_sample→work_ticket existence-and-tally lives in one place; each
    gate keeps only its own threshold (grouped counts summed in Python — no
    enum-array binding)."""
    exists = await conn.fetchval(
        "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", sequenced_pool_idx
    )
    if exists is None:
        raise SequencedPoolNotFound(sequenced_pool_idx)

    prep_sample_idxs = await _pool_prep_sample_idxs(conn, sequenced_pool_idx)
    rows = await conn.fetch(
        "SELECT state, count(*) AS n FROM qiita.work_ticket"
        " WHERE sequenced_pool_idx = $1 OR prep_sample_idx = ANY($2::bigint[])"
        " GROUP BY state",
        sequenced_pool_idx,
        prep_sample_idxs,
    )
    return prep_sample_idxs, {r["state"]: r["n"] for r in rows}


async def assert_sequenced_pool_deletable(
    conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
    *,
    force: bool,
) -> list[int]:
    """Existence + gating precheck for a sequenced_pool delete.

    Returns the pool's prep_sample idxs on success (the same set the cascade
    tears down). Raises SequencedPoolNotFound if the pool doesn't exist, or
    SequencedPoolDeleteBlocked if work tickets / publication / ENA state block
    it (in-flight tickets always; terminal tickets, published prep_samples, and
    ENA-submitted samples unless force). Run this *before* any destructive step
    so a blocked delete touches nothing."""
    prep_sample_idxs, counts = await _pool_work_ticket_state_counts(conn, sequenced_pool_idx)
    in_flight = sum(counts.get(s, 0) for s in _WORK_TICKET_IN_FLIGHT_STATES)
    terminal = sum(counts.get(s, 0) for s in _WORK_TICKET_TERMINAL_STATES)

    published = await conn.fetchval(
        "SELECT count(*) FROM qiita.prep_sample_to_study"
        " WHERE prep_sample_idx = ANY($1::bigint[]) AND is_published = true",
        prep_sample_idxs,
    )
    # ENA-submitted samples: a non-null experiment or run accession means the
    # data is in the public archive. ENA-submitted is independent of published
    # (separate columns by design), so the published check above does not cover
    # this — gate it on its own. Keyed on the sequenced_sample subtype (where the
    # accession columns live) rather than the prep_sample set above; the two are
    # the same set here (sequenced_sample↔prep_sample is 1:1 and pool-exclusive).
    ena = await conn.fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample"
        " WHERE sequenced_pool_idx = $1"
        "   AND (ena_experiment_accession IS NOT NULL OR ena_run_accession IS NOT NULL)",
        sequenced_pool_idx,
    )

    if in_flight or (not force and (terminal or published or ena)):
        raise SequencedPoolDeleteBlocked(
            sequenced_pool_idx=sequenced_pool_idx,
            in_flight=in_flight,
            terminal=terminal,
            published=published,
            ena=ena,
        )
    return prep_sample_idxs


async def assert_pool_preflight_editable(
    conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> None:
    """Gate a run-preflight edit on the pool not having been processed.

    Raises SequencedPoolNotFound if the pool doesn't exist, or PreflightNotEditable
    if any work ticket in an in-flight or completed state references the pool or one
    of its sequenced_samples' prep_samples. failed / no_data / not-submitted samples
    do not block — a failed run is the recovery case the edit exists to serve.

    Shares the existence + state-tally query with `assert_sequenced_pool_deletable`
    via `_pool_work_ticket_state_counts`; only the blocking-state threshold differs.
    Run it inside the mutating transaction so a stale precheck cannot let a job that
    has since gone in-flight slip under the edit. Note this re-check re-validates
    against committed state but, under READ COMMITTED, does not by itself serialize
    against a submission committing concurrently mid-edit (see the route docstring)."""
    _prep_sample_idxs, counts = await _pool_work_ticket_state_counts(conn, sequenced_pool_idx)
    blocking = sum(counts.get(s, 0) for s in _PREFLIGHT_EDIT_BLOCKING_STATES)
    if blocking:
        raise PreflightNotEditable(sequenced_pool_idx=sequenced_pool_idx, blocking=blocking)


async def invalidate_completed_steps_for_sequenced_pool(
    conn: asyncpg.Connection,
    *,
    sequenced_pool_idx: int,
) -> int:
    """Drop the COMPLETED work_ticket_step rows of the pool's tickets after a
    run-preflight edit, returning the number deleted.

    The complement of `assert_pool_preflight_editable`: once the gate has let an
    edit through and the blob is rewritten, any samplesheet a prior
    `bcl_convert_prep` already produced is stale (it was built from the pre-edit
    lanes). Its COMPLETED progress row must go — otherwise a
    `POST /work-ticket/{idx}/run` redrive would fast-forward prep, rebuilding its
    output from the persisted workspace manifest, and re-feed the wrong lanes to
    bcl-convert. Dropping the completed rows forces the redrive to re-run from
    prep against the corrected blob.

    Scoped to pool-scoped tickets: `bcl_convert_prep` is the sole consumer of the
    preflight blob, so per-prep_sample tickets for other workflows are correctly
    left untouched. Safe to run unconditionally — the edit gate guarantees no pool
    ticket is in-flight or completed here, so every completed step row belongs to a
    failed/no_data ticket with no live job. Non-completed rows are left for the
    /run redrive's own reset. Must run inside the caller's transaction, alongside
    the blob write, so edit + invalidation commit atomically."""
    return _rowcount(
        await conn.execute(
            "DELETE FROM qiita.work_ticket_step"
            " WHERE state = 'completed'"
            "   AND work_ticket_idx IN ("
            "       SELECT work_ticket_idx FROM qiita.work_ticket"
            "        WHERE sequenced_pool_idx = $1)",
            sequenced_pool_idx,
        )
    )


async def delete_sequenced_pool_cascade(
    conn: asyncpg.Connection,
    sequenced_pool_idx: int,
) -> dict[str, int]:
    """Tear down every Postgres row owned by a sequenced_pool, in FK-dependency
    order, ending with the `qiita.sequenced_pool` row itself. Must run inside
    the caller's transaction; the caller must have already gated via
    `assert_sequenced_pool_deletable`.

    The subtree is ON DELETE RESTRICT throughout (with two exceptions that
    CASCADE: work_ticket_step off work_ticket, and sequence_range off
    prep_sample), so order is explicit:
      work_ticket (pool- and sample-scoped; → work_ticket_step CASCADEs) →
      prep_sample_metadata → prep_sample_field_exception → prep_sample_to_study →
      sequenced_sample (breaks the composite FK to prep_sample) →
      prep_sample (→ sequence_range CASCADEs) → sequenced_pool.

    Returns the per-table delete counts for the caller's response."""
    prep_sample_idxs = await _pool_prep_sample_idxs(conn, sequenced_pool_idx)

    work_ticket_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.work_ticket"
            " WHERE sequenced_pool_idx = $1 OR prep_sample_idx = ANY($2::bigint[])",
            sequenced_pool_idx,
            prep_sample_idxs,
        )
    )
    metadata_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.prep_sample_metadata WHERE prep_sample_idx = ANY($1::bigint[])",
            prep_sample_idxs,
        )
    )
    field_exception_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.prep_sample_field_exception"
            " WHERE prep_sample_idx = ANY($1::bigint[])",
            prep_sample_idxs,
        )
    )
    study_link_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = ANY($1::bigint[])",
            prep_sample_idxs,
        )
    )
    sequenced_sample_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.sequenced_sample WHERE sequenced_pool_idx = $1",
            sequenced_pool_idx,
        )
    )
    prep_sample_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])",
            prep_sample_idxs,
        )
    )
    await conn.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", sequenced_pool_idx)

    return {
        "sequenced_sample_deleted": sequenced_sample_deleted,
        "prep_sample_deleted": prep_sample_deleted,
        "metadata_deleted": metadata_deleted,
        "field_exception_deleted": field_exception_deleted,
        "study_link_deleted": study_link_deleted,
        "work_ticket_deleted": work_ticket_deleted,
    }


def reap_staged_reads(staging_root: Path | None, prep_sample_idxs: list[int]) -> int:
    """Best-effort removal of the durable per-sample staged read copies a deleted
    pool's prep_samples produced, returning the number of `read.parquet` files
    removed.

    The bcl-convert `ingest_reads` step writes each sample's reads once to
    `{staging_root}/reads/{prep_sample_idx}/read.parquet` (the stable,
    prep_sample-addressable input the repeatable read-mask workflow binds). When
    the pool is purged those copies are orphaned alongside the DuckLake rows, so
    we unlink them here and drop the now-empty per-sample directory.

    Best-effort by design — a missing file is success (idempotent: a retry, or a
    pool whose reads never landed, removes nothing) and a per-sample filesystem
    error is logged and skipped rather than raised: the Postgres + DuckLake
    teardown is the authoritative delete; leaking a stale Parquet must never fail
    it. No-op when `staging_root` is None (CP-only/dev, no shared scratch)."""
    if staging_root is None:
        return 0
    reaped = 0
    for prep_sample_idx in prep_sample_idxs:
        path = compute_reads_staging_path(staging_root, prep_sample_idx)
        try:
            try:
                path.unlink()
                reaped += 1
            except FileNotFoundError:
                pass  # already gone — idempotent
            # Drop the now-empty `{prep_sample_idx}/` dir; ignore if it still
            # holds other artifacts or was never created.
            parent = path.parent
            if parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()
        except OSError as exc:
            logger.warning(
                "reap_staged_reads: could not remove %s for prep_sample %d: %s",
                path,
                prep_sample_idx,
                exc,
            )
    return reaped
