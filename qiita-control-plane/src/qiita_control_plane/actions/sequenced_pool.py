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

import asyncpg

# Work-ticket states that block a pool delete. In-flight states block
# unconditionally (a running job is reading/writing the pool's data); terminal
# states block only without `force` (a completed test run is exactly what an
# admin purging a mis-pooled preparation wants gone).
_WORK_TICKET_IN_FLIGHT_STATES = ("pending", "queued", "processing")
_WORK_TICKET_TERMINAL_STATES = ("completed", "failed")


class SequencedPoolNotFound(Exception):
    """Raised when the sequenced_pool_idx doesn't exist."""


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
                parts.append(f"{terminal} completed/failed work ticket(s) reference it")
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
    counts = {r["state"]: r["n"] for r in rows}
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
