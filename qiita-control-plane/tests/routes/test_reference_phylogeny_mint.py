"""POST /reference/{idx}/phylogeny/mint-edge-id — the operator mint that gives a
reference tree the edge numbering a placement joins back on.

The data-plane half (the `mint_phylogeny_edge_id` DoAction and its DuckLake UPDATE)
is stubbed here: there is no Flight server in the DB tier, so these tests pin the
route's contract — the reference gate, the scope gate, what it echoes, and which of
the two 502s a given data-plane failure earns. They are not interchangeable: a
transport failure may be re-issued, a reply whose counts cannot describe the tree may
not, and each test pins the detail text that says so.
"""

import pytest
from qiita_common.api_paths import URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def _stub_mint(monkeypatch):
    """Neutralize the DoAction and record each call, so a test can assert the route
    fired it with the reference it was asked for."""
    calls: list[int] = []

    async def _fake(*, reference_idx, signing_key, data_plane_url):
        calls.append(reference_idx)
        return {
            "reference_idx": reference_idx,
            "phylogeny_rows": 392123,
            "already_numbered_rows": 0,
            "minted_rows": 392123,
        }

    monkeypatch.setattr("qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _fake)
    return calls


# The three role clients come from `routes/conftest.py`'s `role_keyed_clients`, and
# Settings from its autouse `_route_settings` — which installs the
# `flight_signing_key` and `data_plane_url` this route's dependencies read, and
# restores the prior value so it does not leak across the xdist worker.


@pytest.fixture
def client(role_keyed_clients):
    return role_keyed_clients["admin"]


@pytest.fixture
def wet_lab_client(role_keyed_clients):
    return role_keyed_clients["wet"]


@pytest.fixture
def regular_user_client(role_keyed_clients):
    return role_keyed_clients["user"]


async def _seed_reference(pool, name) -> int:
    principal_idx = await pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    return await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', $2) RETURNING reference_idx",
        name,
        principal_idx,
    )


async def _drop_reference(pool, reference_idx) -> None:
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


async def test_mint_reports_what_the_data_plane_changed(client, postgres_pool, _stub_mint):
    idx = await _seed_reference(postgres_pool, "phylo-mint-reports")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 200, resp.text
        assert resp.json() == {
            "reference_idx": idx,
            "phylogeny_rows": 392123,
            "already_numbered_rows": 0,
            "minted_rows": 392123,
        }
        assert _stub_mint == [idx]
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_makes_no_postgres_change(client, postgres_pool):
    """The mint writes only in DuckLake — the reference row is untouched, so an
    operator can re-run it without wondering what else moved."""
    idx = await _seed_reference(postgres_pool, "phylo-mint-no-pg-change")
    try:
        before = await postgres_pool.fetchrow(
            "SELECT name, version, status, created_at FROM qiita.reference"
            " WHERE reference_idx = $1",
            idx,
        )
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 200, resp.text
        after = await postgres_pool.fetchrow(
            "SELECT name, version, status, created_at FROM qiita.reference"
            " WHERE reference_idx = $1",
            idx,
        )
        assert dict(after) == dict(before)
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_404s_an_unknown_reference(client, postgres_pool, _stub_mint):
    """A typo'd idx is distinguishable from a tree that needed no mint, and the
    DoAction never fires for a reference that does not exist."""
    missing = (await postgres_pool.fetchval("SELECT MAX(reference_idx) FROM qiita.reference")) or 0
    resp = await client.post(
        URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=missing + 1000)
    )
    assert resp.status_code == 404, resp.text
    assert _stub_mint == []


async def test_a_wet_lab_admin_may_mint(wet_lab_client, postgres_pool):
    """wet_lab_admin holds `reference:write`, so the role that loads a reference can
    also repair one loaded before the loader numbered trees."""
    idx = await _seed_reference(postgres_pool, "phylo-mint-wet-lab")
    try:
        resp = await wet_lab_client.post(
            URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx)
        )
        assert resp.status_code == 200, resp.text
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_refuses_a_principal_without_the_write_scope(
    regular_user_client, postgres_pool, _stub_mint
):
    idx = await _seed_reference(postgres_pool, "phylo-mint-scope")
    try:
        resp = await regular_user_client.post(
            URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx)
        )
        assert resp.status_code == 403, resp.text
        assert _stub_mint == [], "a refused caller must not reach the data plane"
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_data_plane_failure_is_502(client, postgres_pool, monkeypatch):
    """A FlightError is retriable, not a 500. Re-issuing is safe whether or not the
    mint committed before the failure, because the second call re-reads the counts."""
    import pyarrow.flight as _flight

    async def _boom(*, reference_idx, signing_key, data_plane_url):
        raise _flight.FlightError("data plane unreachable")

    monkeypatch.setattr("qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _boom)
    idx = await _seed_reference(postgres_pool, "phylo-mint-502")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 502, resp.text
        assert "can be re-issued" in resp.json()["detail"]
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_409s_a_partially_numbered_tree(client, postgres_pool, monkeypatch):
    """The state the loader's all-or-nothing rule exists to prevent. Filling the rest
    would leave two numberings in one column with nothing to tell them apart, so the
    route refuses instead of reporting a partial write as success."""

    async def _partial(*, reference_idx, signing_key, data_plane_url):
        return {
            "reference_idx": reference_idx,
            "phylogeny_rows": 9,
            "already_numbered_rows": 8,
            "minted_rows": 0,
        }

    monkeypatch.setattr(
        "qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _partial
    )
    idx = await _seed_reference(postgres_pool, "phylo-mint-partial")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 409, resp.text
        assert "8 of 9" in resp.json()["detail"]
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_409s_a_reference_with_no_phylogeny(client, postgres_pool, monkeypatch):
    """Refused rather than reported, so a `minted_rows: 0` that reaches the caller
    always means "already numbered"."""

    async def _empty(*, reference_idx, signing_key, data_plane_url):
        return {
            "reference_idx": reference_idx,
            "phylogeny_rows": 0,
            "already_numbered_rows": 0,
            "minted_rows": 0,
        }

    monkeypatch.setattr("qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _empty)
    idx = await _seed_reference(postgres_pool, "phylo-mint-no-tree")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 409, resp.text
        assert "no phylogeny rows" in resp.json()["detail"]
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_is_a_no_op_on_an_already_numbered_tree(client, postgres_pool, monkeypatch):
    """A replay, or a tree loaded from a decorated Newick: 200 with nothing minted,
    distinguishable from the wrong-reference case by `phylogeny_rows`."""

    async def _already(*, reference_idx, signing_key, data_plane_url):
        return {
            "reference_idx": reference_idx,
            "phylogeny_rows": 9,
            "already_numbered_rows": 9,
            "minted_rows": 0,
        }

    monkeypatch.setattr(
        "qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _already
    )
    idx = await _seed_reference(postgres_pool, "phylo-mint-replay")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 200, resp.text
        assert resp.json()["minted_rows"] == 0
        assert resp.json()["phylogeny_rows"] == 9
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_malformed_data_plane_body_is_502(client, postgres_pool, monkeypatch):
    """A body missing a count is a data-plane failure, not a 500 — and not a
    retriable one: it cannot be validated, so nothing establishes whether the write
    happened, and the detail withholds the re-issue advice the transport arm gives."""

    async def _garbage(*, reference_idx, signing_key, data_plane_url):
        return {"reference_idx": reference_idx}

    monkeypatch.setattr(
        "qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _garbage
    )
    idx = await _seed_reference(postgres_pool, "phylo-mint-garbage")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 502, resp.text
        detail = resp.json()["detail"]
        # Arm-unique: without this, folding ValidationError back into the transport
        # arm would leave this test green while the advice silently inverted.
        assert "do NOT re-issue" in detail
        assert "can be re-issued" not in detail
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_incomplete_numbering_is_502(client, postgres_pool, monkeypatch):
    """A tree that was entirely NULL and came back still partly NULL. Reporting that
    as a 200 would let a write which touched fewer rows than it should read as a
    completed numbering — and the advice has to differ from the nothing-written case,
    because re-issuing after a partial write hits the all-or-nothing 409 above and
    cannot clear it."""

    async def _short(*, reference_idx, signing_key, data_plane_url):
        return {
            "reference_idx": reference_idx,
            "phylogeny_rows": 9,
            "already_numbered_rows": 0,
            "minted_rows": 4,
        }

    monkeypatch.setattr("qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _short)
    idx = await _seed_reference(postgres_pool, "phylo-mint-short")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 502, resp.text
        detail = resp.json()["detail"]
        assert "4 of reference" in detail and "9 phylogeny rows" in detail
        assert "do NOT re-issue" in detail, "re-issuing a partial write dead-ends on the 409"
    finally:
        await _drop_reference(postgres_pool, idx)


async def test_mint_nothing_numbered_is_a_retriable_502(client, postgres_pool, monkeypatch):
    """The other half of the same guard: an unnumbered tree that came back with
    nothing written. Here re-issuing IS correct, so the detail must say so rather
    than repeating the partial-write warning."""

    async def _none(*, reference_idx, signing_key, data_plane_url):
        return {
            "reference_idx": reference_idx,
            "phylogeny_rows": 9,
            "already_numbered_rows": 0,
            "minted_rows": 0,
        }

    monkeypatch.setattr("qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _none)
    idx = await _seed_reference(postgres_pool, "phylo-mint-none")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 502, resp.text
        detail = resp.json()["detail"]
        # Arm-unique: the transport 502's detail also says "can be re-issued", so
        # asserting only that would stay green if this reply stopped reaching the guard.
        assert "numbered none of" in detail
        assert "can be re-issued" in detail
        assert "do NOT re-issue" not in detail
    finally:
        await _drop_reference(postgres_pool, idx)


@pytest.mark.parametrize(
    ("label", "counts"),
    [
        # Already numbered in full, yet rows were also changed: the two 200 outcomes
        # the response model documents are "minted everything" and "minted nothing",
        # and this is neither.
        (
            "minted-on-top-of-numbered",
            {"phylogeny_rows": 9, "already_numbered_rows": 9, "minted_rows": 9},
        ),
        # More numbered rows than the tree has.
        (
            "numbered-exceeds-tree",
            {"phylogeny_rows": 9, "already_numbered_rows": 10, "minted_rows": 0},
        ),
    ],
)
async def test_mint_502s_counts_that_do_not_add_up(
    client, postgres_pool, monkeypatch, label, counts
):
    """Arithmetically impossible replies are refused, not echoed.

    Both combinations reach the route with `already_numbered_rows` neither 0 nor a
    strict fraction of the tree, so neither of the route's own two guards fires; the
    response model's validator is what keeps them out of a 200 whose body contradicts
    itself.
    """

    async def _bad(*, reference_idx, signing_key, data_plane_url):
        return {"reference_idx": reference_idx, **counts}

    monkeypatch.setattr("qiita_control_plane.routes.reference.mint_phylogeny_edge_id_data", _bad)
    idx = await _seed_reference(postgres_pool, f"phylo-mint-{label}")
    try:
        resp = await client.post(URL_REFERENCE_PHYLOGENY_MINT_EDGE_ID.format(reference_idx=idx))
        assert resp.status_code == 502, resp.text
        detail = resp.json()["detail"]
        # Arm-unique. The transport 502 says "can be re-issued"; this arm must not,
        # because counts like these can come from a reply that already wrote rows.
        assert "do NOT re-issue" in detail
        assert "counts that do not describe" in detail
    finally:
        await _drop_reference(postgres_pool, idx)
