"""Integration tests for the prep-protocol discovery route — GET /prep-protocol
against real Postgres (uses the seeded protocol plus a temporary retired one)."""

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_PREP_PROTOCOL_PREFIX

pytestmark = pytest.mark.db

_RETIRED_NAME = "zz_test_retired_protocol"


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    """AsyncClient wired to the app with the integration pool. GET /prep-protocol
    is anonymous-OK, but we send the admin PAT to match the other route suites."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


@pytest.fixture
async def retired_protocol(postgres_pool):
    """Insert a retired prep_protocol (owned by an existing principal) and clean
    it up, so the include/exclude filtering has a deterministic row to assert on."""
    owner = await postgres_pool.fetchval(
        "SELECT created_by_idx FROM qiita.prep_protocol ORDER BY idx LIMIT 1"
    )
    # `prep_protocol_retirement_consistent` CHECK requires retired_at +
    # retired_by_idx to be set when retired=true.
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.prep_protocol"
        "  (name, created_by_idx, retired, retired_by_idx, retired_at)"
        " VALUES ($1, $2, true, $2, now()) RETURNING idx",
        _RETIRED_NAME,
        owner,
    )
    yield idx
    await postgres_pool.execute("DELETE FROM qiita.prep_protocol WHERE idx = $1", idx)


async def test_list_prep_protocols_returns_seeded_rows(client):
    """The seeded `short_read_metagenomics` protocol is listed with the expected
    wire shape (PK surfaced as `prep_protocol_idx`)."""
    resp = await client.get(URL_PREP_PROTOCOL_PREFIX)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list) and body
    by_name = {row["name"]: row for row in body}
    assert "short_read_metagenomics" in by_name
    row = by_name["short_read_metagenomics"]
    assert set(row) == {
        "prep_protocol_idx",
        "name",
        "description",
        "retired",
        "created_by_idx",
        "created_at",
    }
    assert row["prep_protocol_idx"] > 0
    assert row["retired"] is False


async def test_list_excludes_retired_by_default(client, retired_protocol):
    """A retired protocol is omitted from the default listing."""
    resp = await client.get(URL_PREP_PROTOCOL_PREFIX)
    assert resp.status_code == 200
    idxs = {row["prep_protocol_idx"] for row in resp.json()}
    assert retired_protocol not in idxs


async def test_list_respects_limit(client):
    """The anonymous listing is bounded — ?limit=1 returns at most one row, and
    an out-of-range limit is rejected by the query-param validator (422)."""
    resp = await client.get(f"{URL_PREP_PROTOCOL_PREFIX}?limit=1")
    assert resp.status_code == 200
    assert len(resp.json()) <= 1

    too_big = await client.get(f"{URL_PREP_PROTOCOL_PREFIX}?limit=1000000")
    assert too_big.status_code == 422


async def test_list_includes_retired_when_requested(client, retired_protocol):
    """`?include_retired=true` surfaces the retired protocol."""
    resp = await client.get(URL_PREP_PROTOCOL_PREFIX, params={"include_retired": "true"})
    assert resp.status_code == 200
    rows = {row["prep_protocol_idx"]: row for row in resp.json()}
    assert retired_protocol in rows
    assert rows[retired_protocol]["retired"] is True
