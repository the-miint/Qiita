"""DB tests for DELETE /sequencing-run/{run}/sequenced-pool/{pool} — full
sequenced_pool purge.

Exercises the Postgres-teardown + gating half of the delete flow against a
real Postgres. The delete also issues a `delete_pool_reads` DoAction to the data
plane (DuckLake `read`/`read_mask` purge) and reaps the durable on-disk staged
read copies; no data plane runs in this DB-only tier, so the data-plane call is
stubbed by the autouse `stub_data_plane_purge` fixture. These tests cover what
the control plane owns: authorization, FK-ordered cascade, biosample/run
retention, work-ticket / publication / ENA gating, that the DuckLake purge is
issued for the pool's prep_sample set before the Postgres teardown (502 on a
data-plane failure leaves Postgres intact), and that the staged read copies are
reaped. The DuckLake delete itself is covered by the Rust tests.
"""

import secrets
import uuid

import pyarrow.flight as _flight
import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SEQUENCED_POOL_BY_IDX, compute_reads_staging_path

from qiita_control_plane.routes import sequencing_run as _sr_routes
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def stub_data_plane_purge(monkeypatch):
    """Stub the data-plane `delete_pool_reads` call (no data plane in this tier).

    Returns the list of recorded `prep_sample_idxs` arg lists so a test can
    assert the route handed the data plane the pool's prep_sample set. Default
    return is zero counts; a test wanting a specific count or a FlightError
    re-patches `_sr_routes.delete_pool_reads_data` itself."""
    calls: list[list[int]] = []

    async def _fake(*, prep_sample_idxs, hmac_secret, data_plane_url):
        calls.append(list(prep_sample_idxs))
        return {
            "read_rows_deleted": 0,
            "read_mask_rows_deleted": 0,
            "prep_sample_count": len(prep_sample_idxs),
        }

    monkeypatch.setattr(_sr_routes, "delete_pool_reads_data", _fake)
    return calls


def _install_settings(app):
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
    )


async def _seed_study(pool, owner_idx):
    return await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"pool-del-{secrets.token_hex(4)}",
    )


async def _seed_pool_with_sample(pool, owner_idx):
    """Seed a full study → biosample → prep_sample → run → pool →
    sequenced_sample chain plus the two study links the triggers need.

    Returns a dict of every idx so the cascade's per-table effects can be
    asserted and a blocked delete can be cleaned up FK-reverse."""
    study_idx = await _seed_study(pool, owner_idx)
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=owner_idx
    )
    # biosample_to_study is required for the prep_sample_to_study
    # reject_without_biosample_link trigger to pass.
    await seed_biosample_to_study_link(
        pool, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner_idx
    )
    run_idx, pool_idx, sequenced_sample_idx = await seed_sequenced_sample_subtype(
        pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=owner_idx,
        sequenced_pool_item_id="1",
    )
    await pool.execute(
        "INSERT INTO qiita.prep_sample_to_study"
        " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
        prep_sample_idx,
        study_idx,
        owner_idx,
    )
    return {
        "study_idx": study_idx,
        "biosample_idx": biosample_idx,
        "prep_sample_idx": prep_sample_idx,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "sequenced_sample_idx": sequenced_sample_idx,
    }


async def _cleanup(pool, ids):
    """FK-reverse teardown, tolerant of rows a successful delete already
    removed."""
    ps = ids["prep_sample_idx"]
    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE sequenced_pool_idx = $1", ids["pool_idx"]
    )
    await pool.execute("DELETE FROM qiita.work_ticket WHERE prep_sample_idx = $1", ps)
    await pool.execute("DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1", ps)
    await pool.execute("DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = $1", ps)
    await pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"])
    await pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", ids["run_idx"])
    await pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps)
    await pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1", ids["biosample_idx"]
    )
    await pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", ids["biosample_idx"])
    await pool.execute("DELETE FROM qiita.study WHERE idx = $1", ids["study_idx"])


async def _seed_pool_work_ticket(pool, pool_idx, state):
    """Insert a minimal action + sequenced_pool-scoped work_ticket in `state`."""
    action_id = "pool-delete-test-action"
    version = f"v-{uuid.uuid4()}"
    await pool.execute(
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
    await pool.execute(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_pool_idx, state)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'sequenced_pool', $3, $4::qiita.work_ticket_state)",
        action_id,
        version,
        pool_idx,
        state,
    )


@pytest.fixture
async def admin_client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


def _url(ids):
    return URL_SEQUENCED_POOL_BY_IDX.format(
        sequencing_run_idx=ids["run_idx"], sequenced_pool_idx=ids["pool_idx"]
    )


async def test_delete_pool_happy_path(admin_client, postgres_pool, human_admin_session):
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    try:
        resp = await admin_client.delete(_url(ids))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["sequenced_pool_idx"] == ids["pool_idx"]
        assert body["sequenced_sample_deleted"] == 1
        assert body["prep_sample_deleted"] == 1
        assert body["study_link_deleted"] == 1
        assert body["work_ticket_deleted"] == 0

        # Pool + samples gone.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            is None
        )
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.prep_sample WHERE idx = $1", ids["prep_sample_idx"]
            )
            is None
        )
        # Parent run survives.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequencing_run WHERE idx = $1", ids["run_idx"]
            )
            == 1
        )
        # Biosample survives — not pool-owned, never GC'd.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.biosample WHERE idx = $1", ids["biosample_idx"]
            )
            == 1
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_not_found(admin_client):
    resp = await admin_client.delete(
        URL_SEQUENCED_POOL_BY_IDX.format(sequencing_run_idx=1, sequenced_pool_idx=99_999_999)
    )
    assert resp.status_code == 404


async def test_delete_pool_wrong_run_422(admin_client, postgres_pool, human_admin_session):
    """The require_sequenced_pool_in_run guard 422s when the pool exists but
    belongs to a different run."""
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    # A second, real sequencing_run the pool does NOT belong to — so the
    # require_sequencing_run_exists guard passes and require_sequenced_pool_in_run
    # is the one that fires (422), not a 404 on a missing run.
    other_run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"other-run-{secrets.token_hex(4)}",
        human_admin_session["principal_idx"],
    )
    try:
        resp = await admin_client.delete(
            URL_SEQUENCED_POOL_BY_IDX.format(
                sequencing_run_idx=other_run_idx, sequenced_pool_idx=ids["pool_idx"]
            )
        )
        assert resp.status_code == 422
        # Untouched.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            == 1
        )
    finally:
        await _cleanup(postgres_pool, ids)
        await postgres_pool.execute(
            "DELETE FROM qiita.sequencing_run WHERE idx = $1", other_run_idx
        )


async def test_delete_pool_requires_delete_scope(
    postgres_pool, wet_lab_admin_session, human_admin_session
):
    """wet_lab_admin holds prep_sample:write but NOT sequenced_pool:delete → 403."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
        ) as ac:
            resp = await ac.delete(_url(ids))
        assert resp.status_code == 403
        assert "sequenced_pool:delete" in resp.json()["detail"]
        # 403 fired before any teardown.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            == 1
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_blocked_by_inflight_ticket(
    admin_client, postgres_pool, human_admin_session
):
    """An in-flight work ticket blocks the delete even with force=true."""
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    await _seed_pool_work_ticket(postgres_pool, ids["pool_idx"], "processing")
    try:
        resp = await admin_client.delete(_url(ids))
        assert resp.status_code == 409
        assert "in-flight" in resp.json()["detail"]

        forced = await admin_client.delete(_url(ids), params={"force": "true"})
        assert forced.status_code == 409, "in-flight blocks even when forced"
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            == 1
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_assert_deletable_force_does_not_override_inflight(
    postgres_pool, human_admin_session
):
    """Action-level invariant behind the in-tx re-gate: even with force=True,
    an in-flight ticket still raises SequencedPoolDeleteBlocked. This is what
    makes the route's re-gate (which passes force=True) abort a teardown when a
    ticket goes in-flight between precheck and cascade, rather than silently
    deleting it."""
    from qiita_control_plane.actions.sequenced_pool import (
        SequencedPoolDeleteBlocked,
        assert_sequenced_pool_deletable,
    )

    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    await _seed_pool_work_ticket(postgres_pool, ids["pool_idx"], "queued")
    try:
        with pytest.raises(SequencedPoolDeleteBlocked):
            await assert_sequenced_pool_deletable(postgres_pool, ids["pool_idx"], force=True)
        # Nothing was torn down — the precheck raised before any DELETE.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            == 1
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_terminal_ticket_requires_force(
    admin_client, postgres_pool, human_admin_session
):
    """A completed work ticket blocks delete without force, allows it with."""
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    await _seed_pool_work_ticket(postgres_pool, ids["pool_idx"], "completed")
    try:
        blocked = await admin_client.delete(_url(ids))
        assert blocked.status_code == 409
        assert "force=true" in blocked.json()["detail"]

        forced = await admin_client.delete(_url(ids), params={"force": "true"})
        assert forced.status_code == 200, forced.text
        assert forced.json()["work_ticket_deleted"] == 1
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            is None
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_published_requires_force(
    admin_client, postgres_pool, human_admin_session
):
    """A prep_sample published into a study blocks delete without force."""
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    await postgres_pool.execute(
        "UPDATE qiita.prep_sample_to_study SET is_published = true WHERE prep_sample_idx = $1",
        ids["prep_sample_idx"],
    )
    try:
        blocked = await admin_client.delete(_url(ids))
        assert blocked.status_code == 409
        assert "published" in blocked.json()["detail"]

        forced = await admin_client.delete(_url(ids), params={"force": "true"})
        assert forced.status_code == 200, forced.text
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            is None
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_ena_submitted_requires_force(
    admin_client, postgres_pool, human_admin_session
):
    """A sample carrying an ENA accession blocks delete without force."""
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    await postgres_pool.execute(
        "UPDATE qiita.sequenced_sample SET ena_run_accession = 'ERR0001' WHERE idx = $1",
        ids["sequenced_sample_idx"],
    )
    try:
        blocked = await admin_client.delete(_url(ids))
        assert blocked.status_code == 409
        assert "ENA" in blocked.json()["detail"]

        forced = await admin_client.delete(_url(ids), params={"force": "true"})
        assert forced.status_code == 200, forced.text
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            is None
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_issues_ducklake_purge(
    admin_client, postgres_pool, human_admin_session, stub_data_plane_purge, monkeypatch
):
    """The delete hands the data plane exactly the pool's prep_sample set and
    surfaces the returned DuckLake counts in the response."""
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])

    async def _fake(*, prep_sample_idxs, hmac_secret, data_plane_url):
        stub_data_plane_purge.append(list(prep_sample_idxs))
        return {"read_rows_deleted": 7, "read_mask_rows_deleted": 3, "prep_sample_count": 1}

    monkeypatch.setattr(_sr_routes, "delete_pool_reads_data", _fake)
    try:
        resp = await admin_client.delete(_url(ids))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["read_rows_deleted"] == 7
        assert body["read_mask_rows_deleted"] == 3
        # The data plane was handed the pool's (single) prep_sample.
        assert stub_data_plane_purge == [[ids["prep_sample_idx"]]]
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_dataplane_failure_502_leaves_postgres_intact(
    admin_client, postgres_pool, human_admin_session, monkeypatch
):
    """A data-plane FlightError 502s with nothing removed — the purge precedes
    the Postgres teardown, so the pool and its rows survive for a retry."""
    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])

    async def _boom(*, prep_sample_idxs, hmac_secret, data_plane_url):
        raise _flight.FlightError("data plane unreachable")

    monkeypatch.setattr(_sr_routes, "delete_pool_reads_data", _boom)
    try:
        resp = await admin_client.delete(_url(ids))
        assert resp.status_code == 502, resp.text
        assert "nothing removed yet" in resp.json()["detail"]
        # Postgres teardown never ran.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", ids["pool_idx"]
            )
            == 1
        )
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.prep_sample WHERE idx = $1", ids["prep_sample_idx"]
            )
            == 1
        )
    finally:
        await _cleanup(postgres_pool, ids)


async def test_delete_pool_reaps_staged_reads(
    admin_client, postgres_pool, human_admin_session, tmp_path
):
    """The delete reaps the durable per-sample staged read copy on disk and
    reports the count."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    ids = await _seed_pool_with_sample(postgres_pool, human_admin_session["principal_idx"])
    # Point the staging root at a tmp dir and drop a fake durable read copy.
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
        path_scratch_staging=tmp_path,
    )
    staged = compute_reads_staging_path(tmp_path, ids["prep_sample_idx"])
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"PAR1-fake")
    try:
        resp = await admin_client.delete(_url(ids))
        assert resp.status_code == 200, resp.text
        assert resp.json()["staged_reads_reaped"] == 1
        assert not staged.exists()
        assert not staged.parent.exists()  # empty per-sample dir removed too
    finally:
        _install_settings(app)  # restore the no-staging default for other tests
        await _cleanup(postgres_pool, ids)
