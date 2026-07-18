"""Tests for the fan-out dispatch throttle (the "pump").

`qiita_control_plane.fanout_dispatch.top_up_dispatch` releases fan-out child
work_tickets that were INSERTed `dispatch_held` a capped number at a time:

  * it releases at most `max_inflight - running` per call (slot cap), where
    "running" is a cohort ticket that is non-terminal AND not held;
  * one failed ticket in the cohort fail-stops it (releases nothing);
  * `cohort_for_ticket_row` routes a ticket to its cohort by its columns;
  * `held_cohorts` enumerates cohorts that still have held tickets (for the
    startup reconcile re-pump).

The pure-column-routing tests need no DB; the release-semantics tests do.
"""

import secrets

import pytest

from qiita_control_plane.fanout_dispatch import (
    align_block_cohort,
    cohort_for_ticket_row,
    held_cohorts,
    read_mask_block_cohort,
    shard_cohort,
    top_up_dispatch,
)

# ---------------------------------------------------------------------------
# cohort_for_ticket_row — pure column routing (no DB)
# ---------------------------------------------------------------------------


def _row(**overrides):
    base = {
        "reference_idx": None,
        "shard_id": None,
        "block_idx": None,
        "mask_idx": None,
        "alignment_idx": None,
    }
    base.update(overrides)
    return base


def test_cohort_for_ticket_row_shard():
    cohort = cohort_for_ticket_row(_row(reference_idx=7, shard_id=3))
    assert cohort is not None
    assert cohort.label == shard_cohort(7).label


def test_cohort_for_ticket_row_align_block():
    # A block ticket with an alignment_idx is an align cohort (keyed by alignment).
    cohort = cohort_for_ticket_row(_row(block_idx=5, mask_idx=9, alignment_idx=4))
    assert cohort is not None
    assert cohort.label == align_block_cohort(4).label


def test_cohort_for_ticket_row_read_mask_block():
    # A block ticket with a mask but no alignment is a read-mask cohort (by mask).
    cohort = cohort_for_ticket_row(_row(block_idx=5, mask_idx=9))
    assert cohort is not None
    assert cohort.label == read_mask_block_cohort(9).label


def test_cohort_for_ticket_row_non_fanout_is_none():
    # A plain reference-scoped ticket (no shard_id) is not a fan-out child.
    assert cohort_for_ticket_row(_row(reference_idx=7)) is None
    # An entirely unscoped row is not either.
    assert cohort_for_ticket_row(_row()) is None


# ---------------------------------------------------------------------------
# top_up_dispatch — release semantics (DB)
# ---------------------------------------------------------------------------


async def _scaffold(pool):
    suffix = secrets.token_hex(4)
    principal_idx = await pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    reference_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', 'indexing', $2) RETURNING reference_idx",
        f"fanout-pump-{suffix}",
        principal_idx,
    )
    action_id, version = "build-shard-index", "1.0.0"
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'reference', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', NULL, 'failed')"
        " ON CONFLICT (action_id, version) DO NOTHING",
        action_id,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
    )
    return {
        "principal_idx": principal_idx,
        "reference_idx": reference_idx,
        "action_id": action_id,
        "version": version,
    }


async def _insert_held_shard_tickets(pool, sc, n):
    """INSERT n held (dispatch_held=true, pending) shard build tickets for shards
    0..n-1. Returns their work_ticket_idxs in shard order (ascending idx)."""
    idxs = []
    for shard_id in range(n):
        idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, reference_idx, shard_id, dispatch_held"
            ") VALUES ($1, $2, $3, 'reference', $4, $5, true) RETURNING work_ticket_idx",
            sc["action_id"],
            sc["version"],
            sc["principal_idx"],
            sc["reference_idx"],
            shard_id,
        )
        idxs.append(idx)
    return idxs


async def _cleanup(pool, reference_idx):
    await pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx)
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


async def _held_count(pool, reference_idx):
    return await pool.fetchval(
        "SELECT count(*) FROM qiita.work_ticket"
        " WHERE reference_idx = $1 AND shard_id IS NOT NULL AND dispatch_held",
        reference_idx,
    )


@pytest.mark.db
async def test_top_up_releases_only_up_to_cap(postgres_pool):
    sc = await _scaffold(postgres_pool)
    ref = sc["reference_idx"]
    try:
        idxs = await _insert_held_shard_tickets(postgres_pool, sc, 5)
        dispatched: list[int] = []
        released = await top_up_dispatch(
            postgres_pool, shard_cohort(ref), max_inflight=2, dispatch_cb=dispatched.append
        )
        # Exactly the cap, lowest work_ticket_idx first; each dispatched once.
        assert released == idxs[:2]
        assert dispatched == released
        # The 2 released are no longer held; the other 3 stay held.
        assert await _held_count(postgres_pool, ref) == 3
    finally:
        await _cleanup(postgres_pool, ref)


@pytest.mark.db
async def test_top_up_refills_as_running_drains(postgres_pool):
    sc = await _scaffold(postgres_pool)
    ref = sc["reference_idx"]
    try:
        idxs = await _insert_held_shard_tickets(postgres_pool, sc, 5)
        # First pump: cap 2 → release 2 (running becomes 2, no slots left).
        first = await top_up_dispatch(
            postgres_pool, shard_cohort(ref), max_inflight=2, dispatch_cb=lambda _idx: None
        )
        assert first == idxs[:2]
        assert (
            await top_up_dispatch(
                postgres_pool, shard_cohort(ref), max_inflight=2, dispatch_cb=lambda _idx: None
            )
            == []
        )  # still 2 running, 0 free slots

        # One released ticket completes → a slot frees → next pump releases 1.
        await postgres_pool.execute(
            "UPDATE qiita.work_ticket SET state='completed' WHERE work_ticket_idx=$1", first[0]
        )
        refilled = await top_up_dispatch(
            postgres_pool, shard_cohort(ref), max_inflight=2, dispatch_cb=lambda _idx: None
        )
        assert refilled == [idxs[2]]
        assert await _held_count(postgres_pool, ref) == 2  # idxs[3], idxs[4] still held
    finally:
        await _cleanup(postgres_pool, ref)


@pytest.mark.db
async def test_top_up_fail_stops_on_a_failed_ticket(postgres_pool):
    sc = await _scaffold(postgres_pool)
    ref = sc["reference_idx"]
    try:
        idxs = await _insert_held_shard_tickets(postgres_pool, sc, 5)
        # One child failed (released, ran, failed). failure_* set to satisfy the
        # work_ticket_failure_consistent CHECK; submission stage needs no step name.
        await postgres_pool.execute(
            "UPDATE qiita.work_ticket SET state='failed', dispatch_held=false,"
            " failure_type='permanent', failure_stage='submission', failure_reason='boom'"
            " WHERE work_ticket_idx=$1",
            idxs[0],
        )
        dispatched: list[int] = []
        released = await top_up_dispatch(
            postgres_pool, shard_cohort(ref), max_inflight=8, dispatch_cb=dispatched.append
        )
        # Fail-stop: nothing released despite free slots; the 4 stay held.
        assert released == []
        assert dispatched == []
        assert await _held_count(postgres_pool, ref) == 4
    finally:
        await _cleanup(postgres_pool, ref)


@pytest.mark.db
async def test_held_cohorts_includes_a_shard_cohort_with_held_tickets(postgres_pool):
    sc = await _scaffold(postgres_pool)
    ref = sc["reference_idx"]
    try:
        await _insert_held_shard_tickets(postgres_pool, sc, 3)
        cohorts = await held_cohorts(postgres_pool)
        assert shard_cohort(ref).label in {c.label for c in cohorts}
        # After releasing all of them, the cohort drops out of held_cohorts.
        await top_up_dispatch(
            postgres_pool, shard_cohort(ref), max_inflight=100, dispatch_cb=lambda _idx: None
        )
        cohorts_after = await held_cohorts(postgres_pool)
        assert shard_cohort(ref).label not in {c.label for c in cohorts_after}
    finally:
        await _cleanup(postgres_pool, ref)
