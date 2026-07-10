"""DB tests for the sharded-index fan-out + count-based completion.

`plan_and_submit_shards` (qiita_control_plane.shard_orchestration) turns a
`plan_shards` assignment into N build tickets: it transitions the reference
`loading -> indexing`, INSERTs one PENDING work_ticket per shard (each carrying
its `shard_id`), and dispatches each via an injected `dispatch_cb`. N = 0 is a
no-op (nothing to shard). It is idempotent on redrive (ON CONFLICT DO NOTHING
against work_ticket_one_in_flight_per_shard).

`finalize_shard` (qiita_control_plane.actions.library) is the terminal step of
each build ticket: it counts registered shards per expected `index_type`
against the planner's N (derived from reference_membership) and, when every
expected type is complete AND the whole-reference `rype_router` row is present,
does the guarded `indexing -> active`. A single still-missing shard, or a
missing router, leaves the reference honestly in `indexing`.
"""

import secrets

import pytest

from qiita_control_plane.actions.library import finalize_shard

pytestmark = pytest.mark.db


async def _scaffold(pool, *, status="loading"):
    suffix = secrets.token_hex(4)
    principal_idx = await pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    reference_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', $2, $3) RETURNING reference_idx",
        f"shard-fanout-{suffix}",
        status,
        principal_idx,
    )
    action_id = "build-shard-index"
    version = "1.0.0"
    # The build action FK target (idempotent across tests — shared action id).
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
        "pool": pool,
        "principal_idx": principal_idx,
        "reference_idx": reference_idx,
        "action_id": action_id,
        "version": version,
    }


async def _cleanup(pool, reference_idx):
    await pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx)
    await pool.execute("DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx)
    await pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


async def _seed_shard_membership(pool, reference_idx, n):
    """Give the reference N shards by stamping N membership rows shard_id 0..N-1
    (one distinct feature per shard). finalize_shard derives N from these."""
    for shard_id in range(n):
        feat = await pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )
        await pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, shard_id)"
            " VALUES ($1, $2, $3)",
            reference_idx,
            feat,
            shard_id,
        )


async def _register_index_shard(pool, reference_idx, index_type, shard_id):
    await pool.execute(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params, shard_id)"
        " VALUES ($1, $2, $3, '{}'::jsonb, $4)",
        reference_idx,
        index_type,
        f"/derived/{reference_idx}/shards/{shard_id}/{index_type}",
        shard_id,
    )


async def _register_router(pool, reference_idx):
    """Register the whole-reference rype_router row (shard_id NULL) — required
    for finalize_shard to flip `active` (a sharded reference isn't routable, so
    not alignable, without it)."""
    await pool.execute(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params, shard_id)"
        " VALUES ($1, 'rype_router', $2, '{}'::jsonb, NULL)",
        reference_idx,
        f"/derived/{reference_idx}/rype-router.ryxdi",
    )


async def _status(pool, reference_idx):
    return await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )


# ---------------------------------------------------------------------------
# plan_and_submit_shards
# ---------------------------------------------------------------------------


async def test_plan_and_submit_shards_fans_out_n_tickets(postgres_pool, monkeypatch):
    from qiita_control_plane import shard_orchestration

    sc = await _scaffold(postgres_pool, status="loading")
    ref = sc["reference_idx"]
    dispatched: list[int] = []
    try:

        async def fake_plan_shards(pool, reference_idx, **kwargs):
            await _seed_shard_membership(pool, reference_idx, 3)
            return 3

        monkeypatch.setattr(shard_orchestration, "plan_shards", fake_plan_shards)

        result = await shard_orchestration.plan_and_submit_shards(
            postgres_pool,
            ref,
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://unused:1",
            workspace=None,
            originator_principal_idx=sc["principal_idx"],
            build_action_id=sc["action_id"],
            build_action_version=sc["version"],
            action_context={"build_minimap2": True},
            dispatch_cb=dispatched.append,
        )

        assert result["shards"] == 3
        rows = await postgres_pool.fetch(
            "SELECT shard_id, state FROM qiita.work_ticket WHERE reference_idx = $1"
            " ORDER BY shard_id",
            ref,
        )
        assert [r["shard_id"] for r in rows] == [0, 1, 2]
        assert all(r["state"] == "pending" for r in rows)
        # Reference moved loading -> indexing; every fresh ticket dispatched.
        assert await _status(postgres_pool, ref) == "indexing"
        assert sorted(dispatched) == sorted(
            r["work_ticket_idx"]
            for r in await postgres_pool.fetch(
                "SELECT work_ticket_idx FROM qiita.work_ticket WHERE reference_idx = $1", ref
            )
        )
    finally:
        await _cleanup(postgres_pool, ref)


async def test_plan_and_submit_shards_zero_is_noop(postgres_pool, monkeypatch):
    from qiita_control_plane import shard_orchestration

    sc = await _scaffold(postgres_pool, status="loading")
    ref = sc["reference_idx"]
    dispatched: list[int] = []
    try:

        async def fake_plan_shards(pool, reference_idx, **kwargs):
            return 0

        monkeypatch.setattr(shard_orchestration, "plan_shards", fake_plan_shards)

        result = await shard_orchestration.plan_and_submit_shards(
            postgres_pool,
            ref,
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://unused:1",
            workspace=None,
            originator_principal_idx=sc["principal_idx"],
            build_action_id=sc["action_id"],
            build_action_version=sc["version"],
            action_context={"build_minimap2": True},
            dispatch_cb=dispatched.append,
        )

        assert result["shards"] == 0
        assert dispatched == []
        # No fan-out, no transition — the parent finalize applies `active`.
        assert await _status(postgres_pool, ref) == "loading"
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.work_ticket WHERE reference_idx = $1", ref
            )
            == 0
        )
    finally:
        await _cleanup(postgres_pool, ref)


async def test_plan_and_submit_shards_idempotent_redrive(postgres_pool, monkeypatch):
    """A redrive (in-flight shard tickets already present) re-INSERTs with
    ON CONFLICT DO NOTHING — no duplicate tickets, only fresh ones dispatched."""
    from qiita_control_plane import shard_orchestration

    sc = await _scaffold(postgres_pool, status="loading")
    ref = sc["reference_idx"]
    dispatched: list[int] = []
    try:

        async def fake_plan_shards(pool, reference_idx, **kwargs):
            # Idempotent assignment (clear-first in the real one); seed once.
            if not await pool.fetchval(
                "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1", ref
            ):
                await _seed_shard_membership(pool, reference_idx, 3)
            return 3

        monkeypatch.setattr(shard_orchestration, "plan_shards", fake_plan_shards)

        kwargs = dict(
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://unused:1",
            workspace=None,
            originator_principal_idx=sc["principal_idx"],
            build_action_id=sc["action_id"],
            build_action_version=sc["version"],
            action_context={"build_minimap2": True},
        )
        await shard_orchestration.plan_and_submit_shards(
            postgres_pool, ref, dispatch_cb=dispatched.append, **kwargs
        )
        first_count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.work_ticket WHERE reference_idx = $1", ref
        )
        second: list[int] = []
        await shard_orchestration.plan_and_submit_shards(
            postgres_pool, ref, dispatch_cb=second.append, **kwargs
        )
        second_count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.work_ticket WHERE reference_idx = $1", ref
        )
        assert first_count == 3
        assert second_count == 3  # no duplicates
        assert second == []  # nothing fresh to dispatch
    finally:
        await _cleanup(postgres_pool, ref)


# ---------------------------------------------------------------------------
# finalize_shard
# ---------------------------------------------------------------------------


async def test_finalize_shard_activates_when_all_types_and_router_complete(postgres_pool):
    sc = await _scaffold(postgres_pool, status="indexing")
    ref = sc["reference_idx"]
    try:
        await _seed_shard_membership(postgres_pool, ref, 3)
        for shard_id in range(3):
            await _register_index_shard(postgres_pool, ref, "minimap2", shard_id)
            await _register_index_shard(postgres_pool, ref, "bowtie2", shard_id)
        await _register_router(postgres_pool, ref)
        result = await finalize_shard(postgres_pool, ref, ["minimap2", "bowtie2"])
        assert result["router_present"] is True
        assert result["activated"] is True
        assert await _status(postgres_pool, ref) == "active"
    finally:
        await _cleanup(postgres_pool, ref)


async def test_finalize_shard_leaves_indexing_without_router(postgres_pool):
    """All per-shard indexes present but the whole-reference rype_router is not
    yet registered — fail-closed: stays `indexing` (a router-less sharded
    reference can't route reads, so it isn't alignable)."""
    sc = await _scaffold(postgres_pool, status="indexing")
    ref = sc["reference_idx"]
    try:
        await _seed_shard_membership(postgres_pool, ref, 3)
        for shard_id in range(3):
            await _register_index_shard(postgres_pool, ref, "minimap2", shard_id)
            await _register_index_shard(postgres_pool, ref, "bowtie2", shard_id)
        # No router row yet.
        result = await finalize_shard(postgres_pool, ref, ["minimap2", "bowtie2"])
        assert result["router_present"] is False
        assert result["activated"] is False
        assert await _status(postgres_pool, ref) == "indexing"
        # Registering the router now flips it active (the parent's finalize).
        await _register_router(postgres_pool, ref)
        result = await finalize_shard(postgres_pool, ref, ["minimap2", "bowtie2"])
        assert result["router_present"] is True
        assert result["activated"] is True
        assert await _status(postgres_pool, ref) == "active"
    finally:
        await _cleanup(postgres_pool, ref)


async def test_finalize_shard_leaves_indexing_when_shard_missing(postgres_pool):
    sc = await _scaffold(postgres_pool, status="indexing")
    ref = sc["reference_idx"]
    try:
        await _seed_shard_membership(postgres_pool, ref, 3)
        for shard_id in range(3):
            await _register_index_shard(postgres_pool, ref, "minimap2", shard_id)
        # bowtie2 missing shard 2; router present.
        await _register_index_shard(postgres_pool, ref, "bowtie2", 0)
        await _register_index_shard(postgres_pool, ref, "bowtie2", 1)
        await _register_router(postgres_pool, ref)
        result = await finalize_shard(postgres_pool, ref, ["minimap2", "bowtie2"])
        assert result["activated"] is False
        assert await _status(postgres_pool, ref) == "indexing"
    finally:
        await _cleanup(postgres_pool, ref)


async def test_finalize_shard_empty_expected_types_never_activates(postgres_pool):
    """Fail-closed guard: an empty expected-type set must NOT vacuously flip
    `active` (all([]) is True) — the reference stays `indexing`, even with a
    router present."""
    sc = await _scaffold(postgres_pool, status="indexing")
    ref = sc["reference_idx"]
    try:
        await _seed_shard_membership(postgres_pool, ref, 3)
        await _register_router(postgres_pool, ref)
        result = await finalize_shard(postgres_pool, ref, [])
        assert result["activated"] is False
        assert await _status(postgres_pool, ref) == "indexing"
    finally:
        await _cleanup(postgres_pool, ref)


async def test_finalize_shard_idempotent_when_already_active(postgres_pool):
    """A finalize that runs after a sibling already flipped `active` treats the
    IllegalStatusTransition as idempotent success (the last-observer race)."""
    sc = await _scaffold(postgres_pool, status="indexing")
    ref = sc["reference_idx"]
    try:
        await _seed_shard_membership(postgres_pool, ref, 2)
        for shard_id in range(2):
            await _register_index_shard(postgres_pool, ref, "minimap2", shard_id)
        await _register_router(postgres_pool, ref)
        first = await finalize_shard(postgres_pool, ref, ["minimap2"])
        assert first["activated"] is True
        assert await _status(postgres_pool, ref) == "active"
        # Second observer: all still complete, but the reference is already
        # active — no error, reported as an idempotent success.
        second = await finalize_shard(postgres_pool, ref, ["minimap2"])
        assert second["activated"] is True
        assert await _status(postgres_pool, ref) == "active"
    finally:
        await _cleanup(postgres_pool, ref)
