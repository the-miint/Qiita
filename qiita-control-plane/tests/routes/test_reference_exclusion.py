"""Route tests for the reference-exclusion curation surface.

POST/DELETE /reference/exclusion (system_admin, `reference:exclusion:write`) and
GET /reference/{idx}/exclusion (`reference:read`). The data-plane sync is stubbed
(the DB tier has no Flight server), so these pin the route logic, the scope gates,
the exactly-one-target validation, and the query endpoint's external-id shape.
"""

import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_REFERENCE_EXCLUSION, URL_REFERENCE_EXCLUSION_BY_IDX

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def _stub_sync(monkeypatch):
    """Neutralize the data-plane exclusion sync (no Flight server in the DB tier)
    and record each call so a test can assert the route actually fired it."""
    calls: list[Path] = []

    async def _fake(*, pool, dest, signing_key, data_plane_url):
        calls.append(dest)
        return 7  # arbitrary synced feature_count the route echoes back

    monkeypatch.setattr("qiita_control_plane.routes.reference.sync_reference_exclusion_data", _fake)
    return calls


def _install_settings(app):
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
        # Non-None so _require_exclusion_sync_dest resolves; the sync is stubbed,
        # so nothing is ever written under this path.
        path_scratch_staging=Path("/tmp/qiita-exclusion-test-staging"),
    )


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


@pytest.fixture
async def wet_lab_client(postgres_pool, wet_lab_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
    ) as ac:
        yield ac


async def _seed_feature(pool) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid()) RETURNING feature_idx"
    )


async def _seed_genome(pool) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1) RETURNING genome_idx",
        f"GCF_{uuid.uuid4().hex[:12]}",
    )


async def _cleanup(pool, *, feature_idxs=(), genome_idxs=()):
    # reference_exclusion FK-cascades on both target deletes, so dropping the
    # seeded features/genomes also removes any block rows the test created.
    if feature_idxs:
        await pool.execute(
            "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
            list(feature_idxs),
        )
        await pool.execute(
            "DELETE FROM qiita.reference_membership WHERE feature_idx = ANY($1::bigint[])",
            list(feature_idxs),
        )
        await pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
            list(feature_idxs),
        )
    if genome_idxs:
        await pool.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])",
            list(genome_idxs),
        )


async def test_add_exclusion_by_feature_syncs_and_reports(client, postgres_pool, _stub_sync):
    feat = await _seed_feature(postgres_pool)
    try:
        resp = await client.post(
            URL_REFERENCE_EXCLUSION, json={"feature_idx": feat, "reason": "bad"}
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["target_kind"] == "feature"
        assert body["feature_idx"] == feat
        assert body["reason"] == "bad"
        assert body["changed"] is True
        assert body["synced_feature_count"] == 7
        # The route re-materialized the lake mirror exactly once.
        assert len(_stub_sync) == 1
        # The Postgres blocklist row exists.
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.reference_exclusion WHERE feature_idx = $1", feat
            )
            == 1
        )
    finally:
        await _cleanup(postgres_pool, feature_idxs=[feat])


async def test_add_exclusion_by_genome(client, postgres_pool):
    genome = await _seed_genome(postgres_pool)
    try:
        resp = await client.post(
            URL_REFERENCE_EXCLUSION, json={"genome_idx": genome, "reason": "contaminant"}
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["target_kind"] == "genome"
        assert body["genome_idx"] == genome
        assert body["changed"] is True
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome])


async def test_add_exclusion_is_idempotent(client, postgres_pool):
    feat = await _seed_feature(postgres_pool)
    try:
        first = await client.post(
            URL_REFERENCE_EXCLUSION, json={"feature_idx": feat, "reason": "x"}
        )
        assert first.json()["changed"] is True
        # Re-blocking is a no-op on the row (keeps the original reason) but still
        # re-syncs so a retry after a prior partial converges.
        second = await client.post(
            URL_REFERENCE_EXCLUSION, json={"feature_idx": feat, "reason": "y"}
        )
        assert second.status_code == 201, second.text
        assert second.json()["changed"] is False
        assert (
            await postgres_pool.fetchval(
                "SELECT reason FROM qiita.reference_exclusion WHERE feature_idx = $1", feat
            )
            == "x"
        )
    finally:
        await _cleanup(postgres_pool, feature_idxs=[feat])


async def test_add_exclusion_requires_exactly_one_target(client):
    both = await client.post(
        URL_REFERENCE_EXCLUSION, json={"genome_idx": 1, "feature_idx": 2, "reason": "r"}
    )
    assert both.status_code == 422
    neither = await client.post(URL_REFERENCE_EXCLUSION, json={"reason": "r"})
    assert neither.status_code == 422


async def test_add_exclusion_unknown_target_is_404(client):
    resp = await client.post(
        URL_REFERENCE_EXCLUSION, json={"feature_idx": 999_999_999, "reason": "r"}
    )
    assert resp.status_code == 404, resp.text


async def test_add_exclusion_requires_write_scope(wet_lab_client, postgres_pool):
    feat = await _seed_feature(postgres_pool)
    try:
        resp = await wet_lab_client.post(
            URL_REFERENCE_EXCLUSION, json={"feature_idx": feat, "reason": "r"}
        )
        assert resp.status_code == 403, resp.text
    finally:
        await _cleanup(postgres_pool, feature_idxs=[feat])


async def test_remove_exclusion_reaches_handler_and_reports(client, postgres_pool, _stub_sync):
    # A valid single-target DELETE against /reference/exclusion must reach THIS
    # handler (200), not be shadowed by DELETE /reference/{reference_idx} (which
    # would 422 trying to parse "exclusion" as an int).
    feat = await _seed_feature(postgres_pool)
    try:
        await client.post(URL_REFERENCE_EXCLUSION, json={"feature_idx": feat, "reason": "r"})
        _stub_sync.clear()
        resp = await client.delete(URL_REFERENCE_EXCLUSION, params={"feature_idx": feat})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["target_kind"] == "feature"
        assert body["changed"] is True
        assert len(_stub_sync) == 1
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.reference_exclusion WHERE feature_idx = $1", feat
            )
            == 0
        )
        # Idempotent: removing again reports changed=false.
        again = await client.delete(URL_REFERENCE_EXCLUSION, params={"feature_idx": feat})
        assert again.status_code == 200
        assert again.json()["changed"] is False
    finally:
        await _cleanup(postgres_pool, feature_idxs=[feat])


async def test_remove_exclusion_requires_exactly_one_target(client):
    both = await client.delete(URL_REFERENCE_EXCLUSION, params={"genome_idx": 1, "feature_idx": 2})
    assert both.status_code == 422
    neither = await client.delete(URL_REFERENCE_EXCLUSION)
    assert neither.status_code == 422


async def test_remove_exclusion_requires_write_scope(wet_lab_client):
    resp = await wet_lab_client.delete(URL_REFERENCE_EXCLUSION, params={"feature_idx": 5})
    assert resp.status_code == 403, resp.text


async def test_list_exclusions_reports_external_ids(client, postgres_pool):
    """GET /reference/{idx}/exclusion intersects the global blocklist with the
    reference's membership and returns provenance: genome (source, source_id) and
    the reference's own accession, plus whether the block is direct or via-genome."""
    ref = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 1) RETURNING reference_idx",
        f"excl-list-{uuid.uuid4()}",
    )
    feat = await _seed_feature(postgres_pool)
    genome = await _seed_genome(postgres_pool)
    try:
        await postgres_pool.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feat,
            genome,
        )
        await postgres_pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, accession)"
            " VALUES ($1, $2, $3)",
            ref,
            feat,
            "NZ_CP012345.1",
        )
        # Block the GENOME → the feature is filtered from the reference via_genome.
        add = await client.post(
            URL_REFERENCE_EXCLUSION, json={"genome_idx": genome, "reason": "contaminant"}
        )
        assert add.status_code == 201, add.text

        resp = await client.get(URL_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        items = resp.json()
        assert len(items) == 1
        item = items[0]
        assert item["feature_idx"] == feat
        assert item["genome_idx"] == genome
        assert item["reason"] == "contaminant"
        assert item["source_id"] is not None
        assert item["accession"] == "NZ_CP012345.1"
        assert item["via_genome"] is True
        assert item["direct_block"] is False
    finally:
        await _cleanup(postgres_pool, feature_idxs=[feat], genome_idxs=[genome])
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref)


async def test_list_exclusions_unknown_reference_is_404(client):
    # A typo'd reference_idx must be distinguishable from a genuinely clean one
    # (fail-loud), matching get_reference_index / get_reference_shard_index_status.
    resp = await client.get(URL_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=99_999_999))
    assert resp.status_code == 404, resp.text


async def test_list_exclusions_existing_reference_no_blocks_is_empty(client, postgres_pool):
    # An existing reference with nothing blocked yields [] (200), distinct from the
    # unknown-reference 404 above.
    ref = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 1) RETURNING reference_idx",
        f"excl-clean-{uuid.uuid4()}",
    )
    try:
        resp = await client.get(URL_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=ref))
        assert resp.status_code == 200
        assert resp.json() == []
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref)


def _install_settings_no_scratch(app):
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
        path_scratch_staging=None,  # no shared data-plane scratch configured
    )


@pytest.fixture
async def client_no_scratch(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings_no_scratch(app)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


async def test_add_exclusion_without_scratch_is_503_and_writes_nothing(
    client_no_scratch, postgres_pool
):
    # Fail-fast: with no shared scratch the block can't reach the enforcement
    # surface, so the route 503s BEFORE any Postgres write (checked here).
    feat = await _seed_feature(postgres_pool)
    try:
        resp = await client_no_scratch.post(
            URL_REFERENCE_EXCLUSION, json={"feature_idx": feat, "reason": "r"}
        )
        assert resp.status_code == 503, resp.text
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.reference_exclusion WHERE feature_idx = $1", feat
            )
            == 0
        ), "no Postgres blocklist row must be written when the sync dest is unavailable"
    finally:
        await _cleanup(postgres_pool, feature_idxs=[feat])


async def test_add_exclusion_sync_failure_is_502_but_postgres_row_persists(
    client, postgres_pool, monkeypatch
):
    # Postgres-first-then-sync: a data-plane FlightError is a 502, and the block
    # row IS present afterward (degraded-but-consistent-on-retry) — so re-POSTing
    # re-runs the wholesale sync. This is the branch the happy-path stub can't cover.
    import pyarrow.flight as _flight

    async def _boom(*, pool, dest, signing_key, data_plane_url):
        raise _flight.FlightError("data plane unreachable")

    monkeypatch.setattr("qiita_control_plane.routes.reference.sync_reference_exclusion_data", _boom)
    feat = await _seed_feature(postgres_pool)
    try:
        resp = await client.post(URL_REFERENCE_EXCLUSION, json={"feature_idx": feat, "reason": "r"})
        assert resp.status_code == 502, resp.text
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.reference_exclusion WHERE feature_idx = $1", feat
            )
            == 1
        ), "the block row must persist after a sync failure so a retry converges"
    finally:
        await _cleanup(postgres_pool, feature_idxs=[feat])
