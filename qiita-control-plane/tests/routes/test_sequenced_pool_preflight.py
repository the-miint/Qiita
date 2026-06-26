"""Integration tests for the sequenced-pool run-preflight routes.

GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/preflight (SA-only read):
the compute-worker SA (Scope.SEQUENCED_POOL_PREFLIGHT_READ) happy path,
the auth matrix (HumanUser 403, SA-without-scope 403, anonymous 401),
the membership and presence 404/422 paths (unknown run, unknown pool,
pool-in-wrong-run, pool with no preflight populated), and a base64
round-trip that pins byte-equality between the on-disk blob and the
deserialised response body.

POST .../preflight/update-lane (wet_lab_admin+ server-side lane edit): the
wet_lab_admin happy path that round-trips a real run_preflight SQLite through
run_preflight.update_lane (lanes moved + change_log written + bytes rewritten),
the not-processed gate (409 on a completed/in-flight pool ticket, allowed on a
failed one), the auth matrix (regular-user 403, anonymous 401), the no-preflight
404, and the update_lane ValueError → 422 collision path.
"""

import base64
import secrets
import sqlite3
import tempfile
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_SEQUENCED_POOL_PREFLIGHT,
    URL_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.main import app

from .conftest import delete_idxs, unique_instrument_id

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    # Work tickets FK to sequenced_pool / action ON DELETE RESTRICT, so purge
    # them (by pool) before the pools, then the actions they referenced last.
    for pool_idx in created["sequenced_pool"]:
        await pool.execute("DELETE FROM qiita.work_ticket WHERE sequenced_pool_idx = $1", pool_idx)
    await delete_idxs(pool, "sequenced_pool", created["sequenced_pool"])
    await delete_idxs(pool, "sequencing_run", created["sequencing_run"])
    for action_id, version in created["action"]:
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
            action_id,
            version,
        )


@pytest_asyncio.fixture
async def ctx(
    postgres_pool,
    compute_worker_service_account,
    wet_lab_admin_session,
    regular_user_session,
):
    """Yield a route-test context with the SA client (preflight:read scope),
    a wet_lab_admin client (used to seed runs/pools as a non-SA principal),
    a regular-user client (used for the human-403 path), and the FK-reverse
    `created` tracker."""
    app.state.pool = postgres_pool
    transport = ASGITransport(app=app)
    created: dict = {"sequencing_run": [], "sequenced_pool": [], "action": []}
    async with (
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
        ) as sa,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
        ) as wet,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {regular_user_session['token']}"},
        ) as user,
    ):
        yield {
            "pool": postgres_pool,
            "sa": sa,
            "wet": wet,
            "user": user,
            "sa_session": compute_worker_service_account,
            "wet_session": wet_lab_admin_session,
            "user_session": regular_user_session,
            "created": created,
        }
    await _cleanup_tracked(postgres_pool, created)


@pytest_asyncio.fixture
async def sa_no_preflight_read_scope_client(postgres_pool, compute_worker_service_account):
    """A bearer-auth client whose SA token carries a worker scope OTHER than
    sequenced_pool:preflight:read, so the require_service_with_scope guard's
    403 path is exercised."""
    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=compute_worker_service_account["principal_idx"],
        label=f"sa-no-preflight-{secrets.token_hex(4)}",
        scopes=[Scope.SEQUENCE_RANGE_MINT],
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# DB seeding helpers
# ---------------------------------------------------------------------------


async def _seed_run(ctx, suffix: str) -> int:
    """Insert a minimal sequencing_run, track for cleanup, return its idx."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        unique_instrument_id(suffix),
        ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["sequencing_run"].append(idx)
    return idx


async def _seed_pool(ctx, *, run_idx: int, blob: bytes | None, filename: str | None) -> int:
    """Insert a sequenced_pool against `run_idx` with the given preflight
    pair (both may be None for the no-preflight case), track for cleanup,
    return its idx."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequenced_pool ("
        "    sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        "    created_by_idx"
        ") VALUES ($1, $2, $3, $4) RETURNING idx",
        run_idx,
        blob,
        filename,
        ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["sequenced_pool"].append(idx)
    return idx


def _url(run_idx: int, pool_idx: int) -> str:
    return URL_SEQUENCED_POOL_PREFLIGHT.format(
        sequencing_run_idx=run_idx,
        sequenced_pool_idx=pool_idx,
    )


# ===========================================================================
# Happy path
# ===========================================================================


async def test_get_preflight_sa_happy_path_round_trips_bytes(ctx):
    # SA with Scope.SEQUENCED_POOL_PREFLIGHT_READ reads the (blob, filename)
    # pair. Blob round-trips byte-identical via base64 → BYTEA → base64.
    run_idx = await _seed_run(ctx, "ok")
    blob = b"\x00SQLite header\x01\x02\xff and trailing magic\xfe\xfd"
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="preflight.db")

    resp = await ctx["sa"].get(_url(run_idx, pool_idx))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    expected = {
        "run_preflight_blob": base64.b64encode(blob).decode("ascii"),
        "run_preflight_filename": "preflight.db",
    }
    assert body == expected
    # Decode the base64 surface and compare to the seeded blob byte-for-byte.
    assert base64.b64decode(body["run_preflight_blob"]) == blob


# ===========================================================================
# Auth matrix
# ===========================================================================


async def test_get_preflight_anonymous_401(ctx):
    run_idx = await _seed_run(ctx, "anon")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=b"X", filename="f.db")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_url(run_idx, pool_idx))
    assert resp.status_code == 401


async def test_get_preflight_human_user_403(ctx):
    # A HumanUser (no SA kind) gets 403 from require_service_with_scope
    # regardless of scope set. The route is service-only.
    run_idx = await _seed_run(ctx, "human")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=b"X", filename="f.db")
    resp = await ctx["user"].get(_url(run_idx, pool_idx))
    assert resp.status_code == 403


async def test_get_preflight_sa_without_scope_403(ctx, sa_no_preflight_read_scope_client):
    run_idx = await _seed_run(ctx, "noscope")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=b"X", filename="f.db")
    resp = await sa_no_preflight_read_scope_client.get(_url(run_idx, pool_idx))
    assert resp.status_code == 403
    assert "sequenced_pool:preflight:read" in resp.json()["detail"]


# ===========================================================================
# Membership and presence
# ===========================================================================


async def test_get_preflight_unknown_run_404(ctx):
    # require_sequencing_run_exists fires the 404 before the pool lookup.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.sequencing_run")
    resp = await ctx["sa"].get(_url(max_idx + 100_000, 1))
    assert resp.status_code == 404
    assert "sequencing_run" in resp.json()["detail"]


async def test_get_preflight_unknown_pool_404(ctx):
    # The run exists; require_sequenced_pool_in_run resolves no pool row
    # and surfaces 404 naming the pool idx.
    run_idx = await _seed_run(ctx, "nopool")
    max_pool_idx = await ctx["pool"].fetchval(
        "SELECT COALESCE(MAX(idx), 0) FROM qiita.sequenced_pool"
    )
    resp = await ctx["sa"].get(_url(run_idx, max_pool_idx + 100_000))
    assert resp.status_code == 404
    assert "sequenced_pool" in resp.json()["detail"]


async def test_get_preflight_pool_in_wrong_run_422(ctx):
    # The pool exists but belongs to a different sequencing_run.
    # require_sequenced_pool_in_run maps that to 422 (existing convention
    # for parent-child consistency mismatches; not 404).
    run_a = await _seed_run(ctx, "a")
    run_b = await _seed_run(ctx, "b")
    pool_in_a = await _seed_pool(ctx, run_idx=run_a, blob=b"X", filename="f.db")

    resp = await ctx["sa"].get(_url(run_b, pool_in_a))
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert f"sequenced_pool {pool_in_a}" in detail
    assert f"sequencing_run {run_b}" in detail


async def test_get_preflight_pool_has_no_preflight_404(ctx):
    # The row exists, membership is correct, but the pool was created
    # without a preflight pair (both blob and filename NULL). Distinct
    # 404 with a message naming the pool.
    run_idx = await _seed_run(ctx, "nopre")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=None, filename=None)
    resp = await ctx["sa"].get(_url(run_idx, pool_idx))
    assert resp.status_code == 404
    assert f"sequenced_pool {pool_idx}" in resp.json()["detail"]
    assert "no preflight" in resp.json()["detail"]


# ===========================================================================
# POST .../preflight/update-lane — server-side lane reassignment
# ===========================================================================


def _make_preflight_blob(lanes, *, platform: str = "illumina", same_prepped: bool = False) -> bytes:
    """Build a schema-valid run_preflight SQLite blob with one platform-sample
    row per entry in `lanes` (illumina_sample or tellseq_sample).

    prepped_sample_idx is distinct per row so the unique (prepped_sample, lane)
    index never trips, unless `same_prepped` forces all rows onto idx 1 (used to
    provoke an update_lane collision). FK enforcement is disabled during seeding
    so the rows need no project→…→prepped_sample chain: update_lane only UPDATEs
    the platform table and INSERTs change_log, neither of which revalidates an
    existing row's FKs."""
    from run_preflight import create_db  # local import; run_preflight is a CP dep

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "pf.db"
        conn = create_db(str(db_path))
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            for i, lane in enumerate(lanes, start=1):
                prepped = 1 if same_prepped else i
                if platform == "tellseq":
                    conn.execute(
                        "INSERT INTO tellseq_sample"
                        " (tellseq_sample_idx, prepped_sample_idx, barcode_id, lane)"
                        " VALUES (?, ?, ?, ?)",
                        (i, prepped, f"BC{i:04d}", lane),
                    )
                else:
                    conn.execute(
                        "INSERT INTO illumina_sample"
                        " (illumina_sample_idx, prepped_sample_idx, i7_index_id,"
                        "  i7_sequence, i5_index_id, i5_sequence, lane)"
                        " VALUES (?, ?, 'i7', 'ACGTACGT', 'i5', 'TGCATGCA', ?)",
                        (i, prepped, lane),
                    )
            conn.commit()
        finally:
            conn.close()
        return db_path.read_bytes()


def _read_preflight(blob: bytes, *, table: str = "illumina_sample"):
    """Return (lane list in insertion order, change_log row count) from a stored
    preflight blob. `table` is a fixed test-internal literal (illumina_sample /
    tellseq_sample), so the f-string interpolation is safe."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "check.db"
        db_path.write_bytes(blob)
        conn = sqlite3.connect(str(db_path))
        try:
            lanes = [
                r[0] for r in conn.execute(f"SELECT lane FROM {table} ORDER BY rowid").fetchall()
            ]
            n_changes = conn.execute("SELECT count(*) FROM change_log").fetchone()[0]
        finally:
            conn.close()
    return lanes, n_changes


async def _seed_pool_work_ticket(ctx, pool_idx: int, state: str) -> int:
    """Insert a minimal action + sequenced_pool-scoped work_ticket in `state`,
    tracking the action for FK-reverse cleanup, and return its work_ticket_idx.
    A 'failed' ticket also carries the failure_* columns the
    work_ticket_failure_consistent CHECK requires."""
    action_id = "preflight-edit-test-action"
    version = f"v-{uuid.uuid4()}"
    await ctx["pool"].execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'sequenced_pool', $3::text[], $4::jsonb, $5::jsonb,"
        "          1, 1, '1 minute')",
        action_id,
        version,
        ["prep_sample:write"],
        '{"service": false, "human_roles": ["system_admin"]}',
        "[]",
    )
    ctx["created"]["action"].append((action_id, version))
    # 'submission' stage (not 'step_run') so failure_step_name may stay NULL —
    # the work_ticket_failure_step_name_consistent CHECK pairs a step name only
    # with the step_run stage.
    failure = (
        ("permanent", "submission", "seeded failed ticket for preflight-edit test")
        if state == "failed"
        else (None, None, None)
    )
    return await ctx["pool"].fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_pool_idx, state,"
        "  failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'sequenced_pool', $3, $4::qiita.work_ticket_state,"
        "         $5::qiita.failure_type, $6::qiita.work_ticket_failure_stage, $7)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        pool_idx,
        state,
        *failure,
    )


async def _seed_work_ticket_step(
    ctx,
    work_ticket_idx: int,
    *,
    step_index: int,
    state: str,
    step_name: str = "bcl_convert_prep",
    attempt: int = 0,
    compute_target: str = "local",
    failure_kind: str | None = None,
    failure_reason: str | None = None,
) -> None:
    """Insert one work_ticket_step progress row. Defaults model the bcl-convert
    prep step (local compute target). A 'failed' row needs failure_kind +
    failure_reason (the work_ticket_step_failure_consistent CHECK)."""
    await ctx["pool"].execute(
        "INSERT INTO qiita.work_ticket_step"
        " (work_ticket_idx, step_index, attempt, step_name, compute_target,"
        "  state, failure_kind, failure_reason)"
        " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        work_ticket_idx,
        step_index,
        attempt,
        step_name,
        compute_target,
        state,
        failure_kind,
        failure_reason,
    )


async def _step_rows(ctx, work_ticket_idx: int):
    """(step_index, attempt, state) tuples for a ticket, ordered."""
    rows = await ctx["pool"].fetch(
        "SELECT step_index, attempt, state FROM qiita.work_ticket_step"
        " WHERE work_ticket_idx = $1 ORDER BY step_index, attempt",
        work_ticket_idx,
    )
    return [(r["step_index"], r["attempt"], r["state"]) for r in rows]


def _lane_url(run_idx: int, pool_idx: int) -> str:
    return URL_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE.format(
        sequencing_run_idx=run_idx,
        sequenced_pool_idx=pool_idx,
    )


def _lane_body(*, platform="illumina", from_lane=1, to_lane=2, reason="fix stale lane"):
    return {
        "platform": platform,
        "from_lane": from_lane,
        "to_lane": to_lane,
        "reason": reason,
    }


async def _stored_blob(ctx, pool_idx: int) -> bytes:
    return bytes(
        await ctx["pool"].fetchval(
            "SELECT run_preflight_blob FROM qiita.sequenced_pool WHERE idx = $1",
            pool_idx,
        )
    )


async def test_update_lane_wet_lab_admin_happy_path(ctx):
    # wet_lab_admin moves two lane-1 samples to lane 2 on an unprocessed pool.
    run_idx = await _seed_run(ctx, "lane-ok")
    blob = _make_preflight_blob([1, 1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="preflight.db")

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx), json=_lane_body(from_lane=1, to_lane=2)
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"sequenced_pool_idx": pool_idx, "rows_updated": 2}

    lanes, n_changes = _read_preflight(await _stored_blob(ctx, pool_idx))
    assert lanes == [2, 2]
    assert n_changes == 2  # one change_log row per reassigned sample
    # filename is the co-populated partner and must be untouched by the edit.
    fn = await ctx["pool"].fetchval(
        "SELECT run_preflight_filename FROM qiita.sequenced_pool WHERE idx = $1", pool_idx
    )
    assert fn == "preflight.db"


async def test_update_lane_to_null_clears_lanes(ctx):
    # to_lane=None is a real value (clear the lane). Exercises update_lane's
    # COALESCE(lane,-1) NULL-sentinel + uniformity branch through the route.
    run_idx = await _seed_run(ctx, "lane-null")
    blob = _make_preflight_blob([1, 1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx), json=_lane_body(from_lane=1, to_lane=None)
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows_updated"] == 2
    lanes, _ = _read_preflight(await _stored_blob(ctx, pool_idx))
    assert lanes == [None, None]


async def test_update_lane_tellseq_platform(ctx):
    # The tellseq platform dispatches to tellseq_sample, not illumina_sample.
    run_idx = await _seed_run(ctx, "lane-tellseq")
    blob = _make_preflight_blob([1, 1], platform="tellseq")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx),
        json=_lane_body(platform="tellseq", from_lane=1, to_lane=2),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows_updated"] == 2
    lanes, _ = _read_preflight(await _stored_blob(ctx, pool_idx), table="tellseq_sample")
    assert lanes == [2, 2]


async def test_update_lane_blocked_when_completed_409(ctx):
    run_idx = await _seed_run(ctx, "lane-done")
    blob = _make_preflight_blob([1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    await _seed_pool_work_ticket(ctx, pool_idx, "completed")

    resp = await ctx["wet"].post(_lane_url(run_idx, pool_idx), json=_lane_body())
    assert resp.status_code == 409, resp.text
    assert "processed" in resp.json()["detail"]
    # Blocked before any mutation — blob is byte-identical to the seed.
    assert await _stored_blob(ctx, pool_idx) == blob


async def test_update_lane_blocked_when_in_flight_409(ctx):
    run_idx = await _seed_run(ctx, "lane-busy")
    blob = _make_preflight_blob([1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    await _seed_pool_work_ticket(ctx, pool_idx, "processing")

    resp = await ctx["wet"].post(_lane_url(run_idx, pool_idx), json=_lane_body())
    assert resp.status_code == 409, resp.text
    assert await _stored_blob(ctx, pool_idx) == blob


async def test_update_lane_allowed_when_failed(ctx):
    # A failed run is the recovery case the edit exists to serve — a stale lane
    # may be why it failed — so a failed ticket must NOT block the edit.
    run_idx = await _seed_run(ctx, "lane-failed")
    blob = _make_preflight_blob([1, 1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    await _seed_pool_work_ticket(ctx, pool_idx, "failed")

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx), json=_lane_body(from_lane=1, to_lane=3)
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows_updated"] == 2
    lanes, _ = _read_preflight(await _stored_blob(ctx, pool_idx))
    assert lanes == [3, 3]


async def test_update_lane_no_preflight_404(ctx):
    run_idx = await _seed_run(ctx, "lane-nopre")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=None, filename=None)
    resp = await ctx["wet"].post(_lane_url(run_idx, pool_idx), json=_lane_body())
    assert resp.status_code == 404
    assert "no preflight" in resp.json()["detail"]


async def test_update_lane_collision_422(ctx):
    # Two rows for the SAME prepped_sample at lanes 1 and 2; moving lane 1 -> 2
    # collides on the unique (prepped_sample, lane) index. update_lane raises
    # ValueError, which the route surfaces as 422.
    run_idx = await _seed_run(ctx, "lane-collide")
    blob = _make_preflight_blob([1, 2], same_prepped=True)
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx), json=_lane_body(from_lane=1, to_lane=2)
    )
    assert resp.status_code == 422, resp.text
    # Unchanged: a rejected edit writes nothing back.
    assert await _stored_blob(ctx, pool_idx) == blob


async def test_update_lane_identical_lanes_422(ctx):
    # from_lane == to_lane is a no-op the request model rejects (so the SQLite
    # change_log never gains spurious entries). FastAPI validation -> 422.
    run_idx = await _seed_run(ctx, "lane-same")
    blob = _make_preflight_blob([1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx), json=_lane_body(from_lane=2, to_lane=2)
    )
    assert resp.status_code == 422


async def test_update_lane_regular_user_403(ctx):
    # wet_lab_admin+ only: a plain USER (even one holding prep_sample:write) is
    # rejected by the role gate.
    run_idx = await _seed_run(ctx, "lane-user")
    blob = _make_preflight_blob([1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    resp = await ctx["user"].post(_lane_url(run_idx, pool_idx), json=_lane_body())
    assert resp.status_code == 403
    assert await _stored_blob(ctx, pool_idx) == blob


async def test_update_lane_anonymous_401(ctx):
    run_idx = await _seed_run(ctx, "lane-anon")
    blob = _make_preflight_blob([1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(_lane_url(run_idx, pool_idx), json=_lane_body())
    assert resp.status_code == 401


async def test_update_lane_pool_in_wrong_run_422(ctx):
    run_a = await _seed_run(ctx, "lane-a")
    run_b = await _seed_run(ctx, "lane-b")
    blob = _make_preflight_blob([1])
    pool_in_a = await _seed_pool(ctx, run_idx=run_a, blob=blob, filename="p.db")
    resp = await ctx["wet"].post(_lane_url(run_b, pool_in_a), json=_lane_body())
    assert resp.status_code == 422
    assert await _stored_blob(ctx, pool_in_a) == blob


# ===========================================================================
# POST .../preflight/update-lane — invalidation of preflight-derived steps
# ===========================================================================


async def test_update_lane_invalidates_completed_derived_steps(ctx):
    # Correcting the preflight makes any samplesheet a prior bcl_convert_prep
    # produced stale, so its COMPLETED step row must be dropped — otherwise a
    # `ticket run` redrive would fast-forward prep and reuse the wrong lanes.
    # The non-completed downstream row is left as-is for /run to reset.
    run_idx = await _seed_run(ctx, "lane-invalidate")
    blob = _make_preflight_blob([1, 1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    wt_idx = await _seed_pool_work_ticket(ctx, pool_idx, "failed")
    await _seed_work_ticket_step(
        ctx, wt_idx, step_index=0, step_name="bcl_convert_prep", state="completed"
    )
    await _seed_work_ticket_step(
        ctx,
        wt_idx,
        step_index=1,
        step_name="bcl_convert",
        state="failed",
        failure_kind="contract_violation",
        failure_reason="stale lane",
    )

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx), json=_lane_body(from_lane=1, to_lane=2)
    )
    assert resp.status_code == 200, resp.text

    # The completed prep row is gone; the failed downstream row survives.
    assert await _step_rows(ctx, wt_idx) == [(1, 0, "failed")]


async def test_update_lane_leaves_other_pools_steps_untouched(ctx):
    # Invalidation is scoped to the edited pool's tickets — a completed step on
    # an unrelated pool's ticket must survive.
    run_idx = await _seed_run(ctx, "lane-scope")
    blob_a = _make_preflight_blob([1, 1])
    pool_a = await _seed_pool(ctx, run_idx=run_idx, blob=blob_a, filename="a.db")
    wt_a = await _seed_pool_work_ticket(ctx, pool_a, "failed")
    await _seed_work_ticket_step(ctx, wt_a, step_index=0, state="completed")

    pool_b = await _seed_pool(ctx, run_idx=run_idx, blob=_make_preflight_blob([1]), filename="b.db")
    wt_b = await _seed_pool_work_ticket(ctx, pool_b, "failed")
    await _seed_work_ticket_step(ctx, wt_b, step_index=0, state="completed")

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_a), json=_lane_body(from_lane=1, to_lane=2)
    )
    assert resp.status_code == 200, resp.text

    assert await _step_rows(ctx, wt_a) == []  # pool A's completed step invalidated
    assert await _step_rows(ctx, wt_b) == [(0, 0, "completed")]  # pool B untouched


async def test_update_lane_no_derived_steps_noop(ctx):
    # A pool with no work_ticket_step rows (failed ticket that never recorded a
    # step, plus a pool with no tickets at all) still edits cleanly — the
    # invalidation DELETE is a harmless no-op.
    run_idx = await _seed_run(ctx, "lane-noop")
    blob = _make_preflight_blob([1, 1])
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="p.db")
    await _seed_pool_work_ticket(ctx, pool_idx, "failed")  # no step rows recorded

    resp = await ctx["wet"].post(
        _lane_url(run_idx, pool_idx), json=_lane_body(from_lane=1, to_lane=2)
    )
    assert resp.status_code == 200, resp.text
    lanes, _ = _read_preflight(await _stored_blob(ctx, pool_idx))
    assert lanes == [2, 2]
