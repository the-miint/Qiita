"""Route tests for POST /exported-processing — the public name for a processing.

The schema tests (`tests/test_exported_processing_schema.py`) cover what the
database guarantees. These cover the route's promises:

* the handle is minted and returned, and no internal identifier rides along
  except the `alignment_idx` the caller themselves sent;
* the mint is idempotent, which is the contract for anything published;
* an alignment that does not exist is a 404, before anything else happens;
* an unauthenticated caller gets nothing.

The seeded shape is the shared `pool_alignment_seed` fixture, the same one the
discovery and cohort-mint tests use — a manifest names the processing a table was
built from, so this route and those have to agree about which alignments exist.
"""

import pytest
from qiita_common.api_paths import URL_EXPORTED_PROCESSING

pytestmark = pytest.mark.db


@pytest.fixture
def seeded(pool_alignment_seed):
    return pool_alignment_seed


async def _drop(pool, alignment_idx):
    await pool.execute(
        "DELETE FROM qiita.exported_processing WHERE alignment_idx = $1", alignment_idx
    )


async def test_a_processing_is_named_with_a_minted_handle(role_keyed_clients, seeded):
    """`QP<n>`, not `alignment_idx`. A manifest is published beside the table, so the
    thing it names the processing with cannot be ours."""
    db = role_keyed_clients["pool"]
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_PROCESSING, json={"alignment_idx": seeded["align_1"]}
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["alignment_idx"] == seeded["align_1"]
        assert body["export_processing_id"].startswith("QP")
        assert body["export_processing_id"][2:].isdigit()
    finally:
        await _drop(db, seeded["align_1"])


async def test_the_handle_is_stable_across_requests(role_keyed_clients, seeded):
    """Idempotent, and that is the contract rather than an optimization: two tables
    built from one processing must cite it identically, or nobody can tell they share
    it."""
    db = role_keyed_clients["pool"]
    try:
        body = {"alignment_idx": seeded["align_1"]}
        first = await role_keyed_clients["user"].post(URL_EXPORTED_PROCESSING, json=body)
        second = await role_keyed_clients["user"].post(URL_EXPORTED_PROCESSING, json=body)
        assert first.status_code == second.status_code == 201
        assert first.json()["export_processing_id"] == second.json()["export_processing_id"]
        assert (
            await db.fetchval(
                "SELECT count(*) FROM qiita.exported_processing WHERE alignment_idx = $1",
                seeded["align_1"],
            )
            == 1
        )
    finally:
        await _drop(db, seeded["align_1"])


async def test_two_processings_get_two_handles(role_keyed_clients, seeded):
    db = role_keyed_clients["pool"]
    try:
        first = await role_keyed_clients["user"].post(
            URL_EXPORTED_PROCESSING, json={"alignment_idx": seeded["align_1"]}
        )
        second = await role_keyed_clients["user"].post(
            URL_EXPORTED_PROCESSING, json={"alignment_idx": seeded["align_2"]}
        )
        assert first.status_code == second.status_code == 201
        assert first.json()["export_processing_id"] != second.json()["export_processing_id"]
    finally:
        await _drop(db, seeded["align_1"])
        await _drop(db, seeded["align_2"])


async def test_an_unknown_alignment_is_refused(role_keyed_clients):
    db = role_keyed_clients["pool"]
    absent = await db.fetchval(
        "SELECT coalesce(max(alignment_idx), 0) + 1000 FROM qiita.alignment_definition"
    )
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING, json={"alignment_idx": absent}
    )
    assert resp.status_code == 404, resp.text


async def test_an_unauthenticated_caller_is_refused(role_keyed_clients, seeded):
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING,
        json={"alignment_idx": seeded["align_1"]},
        headers={"Authorization": "Bearer nope"},
    )
    assert resp.status_code == 401, resp.text
