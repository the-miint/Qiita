"""Route tests for the mask kind of the public handles:

  * POST /exported-processing/mask — the handle for a mask.
  * POST /exported-identifier/mask — the handle for each prep_sample under a mask.

Both are gated as the masked-read export is (system_admin plus
admin:masked_read_export), are idempotent, and refuse a deprecated mask. The
identifier mint also refuses a cohort with a member not masked-complete under the
mask. The schema guarantees are in tests/test_exported_mask_kind_schema.py.
"""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_EXPORTED_IDENTIFIER_MASK, URL_EXPORTED_PROCESSING_MASK
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.main import app
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import seed_biosample_with_sequenced_prep_sample
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def seeded(role_keyed_clients):
    """A mask, and two prep_samples: ps_done is masked-complete under it, ps_pending
    has a pending gate row."""
    pool = role_keyed_clients["pool"]
    owner = role_keyed_clients["admin_session"]["principal_idx"]
    async with pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"probe": f"exported-mask-route-{secrets.token_hex(4)}"},
            principal_idx=owner,
        )
    mask_idx = mask["mask_idx"]
    bs_done, ps_done = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner)
    bs_pending, ps_pending = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner)
    await pool.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed'), ($1, $3, 'pending')",
        mask_idx,
        ps_done,
        ps_pending,
    )
    yield {
        "pool": pool,
        "owner": owner,
        "mask_idx": mask_idx,
        "ps_done": ps_done,
        "ps_pending": ps_pending,
    }
    await pool.execute("DELETE FROM qiita.exported_identifier WHERE mask_idx = $1", mask_idx)
    await pool.execute("DELETE FROM qiita.exported_processing WHERE mask_idx = $1", mask_idx)
    await teardown_entity_graph(
        pool,
        study_idxs=[],
        biosample_idxs=[bs_done, bs_pending],
        prep_sample_idxs=[ps_done, ps_pending],
    )
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


async def _deprecate(pool, mask_idx, principal_idx):
    await pool.execute(
        "UPDATE qiita.mask_definition"
        "   SET status = 'deprecated', deprecated_at = now(), deprecated_by_idx = $2,"
        "       deprecation_reason = 'probe: config void'"
        " WHERE mask_idx = $1",
        mask_idx,
        principal_idx,
    )


def _processing_body(seeded):
    return {"mask_idx": seeded["mask_idx"]}


def _identifier_body(seeded):
    return {"mask_idx": seeded["mask_idx"], "prep_sample_idx": [seeded["ps_done"]]}


_ROUTES = [
    pytest.param(URL_EXPORTED_PROCESSING_MASK, _processing_body, id="processing"),
    pytest.param(URL_EXPORTED_IDENTIFIER_MASK, _identifier_body, id="identifier"),
]


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("url", "body"), _ROUTES)
async def test_anonymous_is_refused(role_keyed_clients, seeded, url, body):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(url, json=body(seeded))
    assert resp.status_code == 401, resp.text


@pytest.mark.parametrize("client", ["user", "wet"])
@pytest.mark.parametrize(("url", "body"), _ROUTES)
async def test_a_non_system_admin_is_refused(role_keyed_clients, seeded, url, body, client):
    resp = await role_keyed_clients[client].post(url, json=body(seeded))
    assert resp.status_code == 403, resp.text


@pytest.mark.parametrize(("url", "body"), _ROUTES)
async def test_a_system_admin_without_the_export_scope_is_refused(
    role_keyed_clients, seeded, url, body
):
    token, _ = await mint_api_token(
        seeded["pool"],
        principal_idx=seeded["owner"],
        label="admin-without-masked-export-scope",
        scopes=[Scope.SELF_PROFILE],
    )
    resp = await role_keyed_clients["admin"].post(
        url, json=body(seeded), headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# /exported-processing/mask
# ---------------------------------------------------------------------------


async def test_a_mask_is_named_with_a_minted_handle_idempotently(role_keyed_clients, seeded):
    admin = role_keyed_clients["admin"]
    first = await admin.post(URL_EXPORTED_PROCESSING_MASK, json=_processing_body(seeded))
    assert first.status_code == 201, first.text
    body = first.json()
    assert body["mask_idx"] == seeded["mask_idx"]
    assert body["export_processing_id"].startswith("QP")
    assert body["export_processing_id"][2:].isdigit()

    second = await admin.post(URL_EXPORTED_PROCESSING_MASK, json=_processing_body(seeded))
    assert second.status_code == 201, second.text
    assert second.json() == body
    live = await seeded["pool"].fetchval(
        "SELECT count(*) FROM qiita.exported_processing WHERE mask_idx = $1 AND NOT retired",
        seeded["mask_idx"],
    )
    assert live == 1


# ---------------------------------------------------------------------------
# /exported-identifier/mask
# ---------------------------------------------------------------------------


async def test_each_masked_sample_is_named_idempotently(role_keyed_clients, seeded):
    admin = role_keyed_clients["admin"]
    first = await admin.post(URL_EXPORTED_IDENTIFIER_MASK, json=_identifier_body(seeded))
    assert first.status_code == 201, first.text
    body = first.json()
    assert body["mask_idx"] == seeded["mask_idx"]
    assert body["count"] == 1
    [identifier] = body["identifiers"]
    assert identifier["prep_sample_idx"] == seeded["ps_done"]
    assert identifier["export_id"].startswith("QM")
    assert identifier["export_id"][2:].isdigit()

    second = await admin.post(URL_EXPORTED_IDENTIFIER_MASK, json=_identifier_body(seeded))
    assert second.status_code == 201, second.text
    assert second.json() == body


async def test_a_sample_not_masked_complete_is_refused_and_nothing_is_minted(
    role_keyed_clients, seeded
):
    resp = await role_keyed_clients["admin"].post(
        URL_EXPORTED_IDENTIFIER_MASK,
        json={
            "mask_idx": seeded["mask_idx"],
            "prep_sample_idx": [seeded["ps_done"], seeded["ps_pending"]],
        },
    )
    assert resp.status_code == 409, resp.text
    assert str(seeded["ps_pending"]) in resp.json()["detail"]
    minted = await seeded["pool"].fetchval(
        "SELECT count(*) FROM qiita.exported_identifier WHERE mask_idx = $1",
        seeded["mask_idx"],
    )
    assert minted == 0


async def test_an_empty_cohort_is_refused(role_keyed_clients, seeded):
    resp = await role_keyed_clients["admin"].post(
        URL_EXPORTED_IDENTIFIER_MASK, json={"mask_idx": seeded["mask_idx"], "prep_sample_idx": []}
    )
    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# Both routes: unknown and deprecated masks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("url", "body"), _ROUTES)
async def test_an_unknown_mask_is_refused(role_keyed_clients, seeded, url, body):
    payload = body(seeded) | {"mask_idx": 999_999_999}
    resp = await role_keyed_clients["admin"].post(url, json=payload)
    assert resp.status_code == 404, resp.text


@pytest.mark.parametrize(("url", "body"), _ROUTES)
async def test_a_deprecated_mask_is_refused(role_keyed_clients, seeded, url, body):
    await _deprecate(seeded["pool"], seeded["mask_idx"], seeded["owner"])
    resp = await role_keyed_clients["admin"].post(url, json=body(seeded))
    assert resp.status_code == 409, resp.text
    assert "deprecated" in resp.json()["detail"]
    minted = await seeded["pool"].fetchval(
        "SELECT (SELECT count(*) FROM qiita.exported_processing WHERE mask_idx = $1)"
        "     + (SELECT count(*) FROM qiita.exported_identifier WHERE mask_idx = $1)",
        seeded["mask_idx"],
    )
    assert minted == 0
