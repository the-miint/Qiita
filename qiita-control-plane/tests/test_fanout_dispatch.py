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
from qiita_common.actions import ALIGN_ACTION_ID, BLOCK_MASK_ACTION_ID, READ_MASK_ACTION_ID

from qiita_control_plane.align_planner import ALIGN_ACTION_VERSION
from qiita_control_plane.block_planner import BLOCK_MASK_ACTION_VERSION
from qiita_control_plane.fanout_dispatch import (
    align_block_cohort,
    cohort_for_ticket_row,
    held_cohorts,
    read_mask_block_cohort,
    shard_cohort,
    top_up_dispatch,
)
from qiita_control_plane.repositories.block import create_block
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
)

# The (action_id -> version) pairs these fixtures seed. Taken from the planners
# that actually mint these tickets, so a submitter version bump reaches the tests
# instead of leaving them exercising a version production no longer submits.
_BLOCK_ACTION_VERSIONS = {
    BLOCK_MASK_ACTION_ID: BLOCK_MASK_ACTION_VERSION,
    ALIGN_ACTION_ID: ALIGN_ACTION_VERSION,
}

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
        "action_id": None,
    }
    base.update(overrides)
    return base


def test_cohort_for_ticket_row_shard():
    cohort = cohort_for_ticket_row(_row(reference_idx=7, shard_id=3))
    assert cohort is not None
    assert cohort.label == shard_cohort(7).label


def test_cohort_for_ticket_row_align_block():
    # A block ticket running the align action is an align cohort (keyed by alignment).
    cohort = cohort_for_ticket_row(
        _row(block_idx=5, mask_idx=9, alignment_idx=4, action_id=ALIGN_ACTION_ID)
    )
    assert cohort is not None
    assert cohort.label == align_block_cohort(4).label


def test_cohort_for_ticket_row_read_mask_block():
    # A block ticket running the bulk-masking action is a read-mask cohort (by mask).
    cohort = cohort_for_ticket_row(_row(block_idx=5, mask_idx=9, action_id=BLOCK_MASK_ACTION_ID))
    assert cohort is not None
    assert cohort.label == read_mask_block_cohort(9).label


def test_cohort_for_ticket_row_purged_align_block_is_not_a_read_mask_block():
    """A purged align block ticket must NOT route into the read-mask cohort.

    `DELETE /alignment-definition` NULLs `work_ticket.alignment_idx` but leaves the
    ticket's `mask_idx` and `block_idx`, so on the column shape alone it is
    indistinguishable from a read-mask block. Routing it there puts it in the wrong
    fan-out: a `failed` one fail-stops the read-mask cohort for that mask_idx, and
    the whole cohort then releases nothing. It routes by action instead, so a purged
    align ticket belongs to no cohort at all.
    """
    purged = _row(block_idx=5, mask_idx=9, alignment_idx=None, action_id=ALIGN_ACTION_ID)
    assert cohort_for_ticket_row(purged) is None


def test_cohort_for_ticket_row_non_fanout_is_none():
    # A plain reference-scoped ticket (no shard_id) is not a fan-out child.
    assert cohort_for_ticket_row(_row(reference_idx=7)) is None
    # An entirely unscoped row is not either.
    assert cohort_for_ticket_row(_row()) is None
    # Nor is a block ticket running some other action — the per-sample read-mask
    # here, which is a real action but not one that fans out into blocks.
    assert (
        cohort_for_ticket_row(_row(block_idx=5, mask_idx=9, action_id=READ_MASK_ACTION_ID)) is None
    )


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


# ---------------------------------------------------------------------------
# Purged align tickets must not contaminate the read-mask cohort (DB)
# ---------------------------------------------------------------------------


async def _seed_block_cohort_scaffold(postgres_pool):
    """A mask_definition plus the two real block action rows, for tickets that
    exercise the read-mask-vs-align block discriminator."""
    suffix = secrets.token_hex(4)
    principal_idx = await postgres_pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    async with postgres_pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"workflow": "host_filter", "version": "1.0.0", "s": suffix},
            principal_idx=principal_idx,
        )
    created_actions = {
        action_id: await seed_action_if_absent(postgres_pool, action_id=action_id, version=version)
        for action_id, version in _BLOCK_ACTION_VERSIONS.items()
    }
    return {
        "principal_idx": principal_idx,
        "mask_idx": mask["mask_idx"],
        "created_actions": created_actions,
    }


async def _teardown_block_cohort_scaffold(postgres_pool, sc):
    """FK-reverse: tickets (blocks cascade), the mask, then any action row this
    scaffold created."""
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE mask_idx = $1", sc["mask_idx"])
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = $1", sc["mask_idx"]
    )
    for action_id, created in sc["created_actions"].items():
        await delete_action_if_created(
            postgres_pool,
            action_id=action_id,
            version=_BLOCK_ACTION_VERSIONS[action_id],
            created=created,
        )


async def _insert_block_ticket(
    postgres_pool, sc, *, action_id, state, dispatch_held, alignment_idx=None
):
    """One block + its ticket, in the given lifecycle state. Returns the ticket idx.

    The failure columns travel together with `state = 'failed'`
    (`work_ticket_failure_consistent`), so a failed ticket is given a plausible set
    rather than NULLs the CHECK would reject.
    """
    async with postgres_pool.acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
    failed = state == "failed"
    return await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  block_idx, mask_idx, alignment_idx, state, dispatch_held,"
        "  failure_type, failure_stage, failure_step_name, failure_reason"
        ") VALUES ($1, $12, $2, 'block', $3, $4, $5, $6::qiita.work_ticket_state, $7,"
        "          $8::qiita.failure_type, $9::qiita.work_ticket_failure_stage, $10, $11)"
        " RETURNING work_ticket_idx",
        action_id,
        sc["principal_idx"],
        block_idx,
        sc["mask_idx"],
        alignment_idx,
        state,
        dispatch_held,
        "permanent" if failed else None,
        # step_run is paired with a non-NULL failure_step_name
        # (work_ticket_failure_step_name_consistent).
        "step_run" if failed else None,
        "align_sharded" if failed else None,
        "step timed out at the action walltime ceiling" if failed else None,
        _BLOCK_ACTION_VERSIONS[action_id],
    )


@pytest.mark.db
async def test_purged_align_ticket_does_not_fail_stop_the_read_mask_cohort(postgres_pool):
    """A failed, purged align block must not halt read-mask fan-out for its mask.

    Deleting an alignment NULLs `work_ticket.alignment_idx` but leaves mask_idx and
    block_idx, so a discriminator of `alignment_idx IS NULL` sweeps the leftover into
    `read_mask_block_cohort(mask_idx)`. Since the pump fail-stops a cohort containing
    any `failed` child, a timed-out align plan would then permanently stop every
    future block-mask fan-out under that mask — a fleet-wide config hash, so any pool
    masked the same way. Discriminating by action_id keeps the cohorts disjoint.
    """
    sc = await _seed_block_cohort_scaffold(postgres_pool)
    mask_idx = sc["mask_idx"]
    try:
        # The wreckage of a purged, timed-out align plan.
        await _insert_block_ticket(
            postgres_pool, sc, action_id=ALIGN_ACTION_ID, state="failed", dispatch_held=False
        )
        # A fresh block-mask fan-out under the same mask_idx.
        held = await _insert_block_ticket(
            postgres_pool,
            sc,
            action_id=BLOCK_MASK_ACTION_ID,
            state="pending",
            dispatch_held=True,
        )
        dispatched: list[int] = []
        released = await top_up_dispatch(
            postgres_pool,
            read_mask_block_cohort(mask_idx),
            max_inflight=8,
            dispatch_cb=dispatched.append,
        )
        assert released == [held]
        assert dispatched == [held]
    finally:
        await _teardown_block_cohort_scaffold(postgres_pool, sc)


@pytest.mark.db
async def test_held_cohorts_does_not_report_a_purged_align_ticket_as_a_read_mask_cohort(
    postgres_pool,
):
    """The startup re-pump must not resurrect purged align tickets as mask blocks.

    `held_cohorts` feeds `reconcile_inflight_tickets`, so a held+purged align ticket
    misread as a read-mask block would be released and dispatched on the next CP
    restart — re-running the align action on a block whose plan was abandoned.
    """
    sc = await _seed_block_cohort_scaffold(postgres_pool)
    mask_idx = sc["mask_idx"]
    try:
        await _insert_block_ticket(
            postgres_pool, sc, action_id=ALIGN_ACTION_ID, state="pending", dispatch_held=True
        )
        labels = {c.label for c in await held_cohorts(postgres_pool)}
        assert read_mask_block_cohort(mask_idx).label not in labels
    finally:
        await _teardown_block_cohort_scaffold(postgres_pool, sc)
