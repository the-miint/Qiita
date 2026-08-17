"""Route tests for POST /exported-processing — the public name for a processing.

The schema tests (`tests/test_exported_processing_schema.py`) cover what the
database guarantees. These cover the route's promises:

* the handle is minted and returned, and no internal identifier rides along
  except the `alignment_idx` the caller themselves sent;
* the mint is idempotent, which is the contract for anything published;
* **a caller who could not build the table gets no manifest handle for it** —
  minting is a write, so the cohort gate is not decoration; and
* the three refusals arrive in `/exported-identifier`'s order, because the two
  routes take the same pair and must never disagree about it.

The seeded shape is the shared `pool_alignment_seed` fixture, the same one the
discovery, cohort-mint and sample-label tests use. `regular_user` holds
`Tier.VIEWER` on study_1 only, so it may read ps_a and ps_c; `align_2` is gated
solely to ps_b in study_2, which makes it the alignment this caller must be
refused for.
"""

import pytest
from qiita_common.api_paths import URL_EXPORTED_PROCESSING

pytestmark = pytest.mark.db


@pytest.fixture
def seeded(pool_alignment_seed):
    """A local abbreviation, nothing more — every test here names it several times
    inside a request body, and the full name pushes those past the line limit."""
    return pool_alignment_seed


async def _drop(pool, *alignment_idxs):
    await pool.execute(
        "DELETE FROM qiita.exported_processing WHERE alignment_idx = ANY($1::bigint[])",
        list(alignment_idxs),
    )


async def test_a_processing_is_named_with_a_minted_handle(role_keyed_clients, seeded):
    """`QP<n>`, not `alignment_idx`. A manifest is published beside the table, so the
    thing it names the processing with cannot be ours."""
    db = role_keyed_clients["pool"]
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_PROCESSING,
            json={"alignment_idx": seeded["align_1"], "prep_sample_idx": [seeded["ps_a"]]},
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
        body = {"alignment_idx": seeded["align_1"], "prep_sample_idx": [seeded["ps_a"]]}
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


async def test_a_different_cohort_of_one_processing_cites_the_same_handle(
    role_keyed_clients, seeded
):
    """The handle names the PROCESSING, not the cohort — which is what lets a reader
    of two differently-scoped tables see that they share one processing. The cohort is
    in the request to authorize it, and for no other reason."""
    db = role_keyed_clients["pool"]
    try:
        one = await role_keyed_clients["wet"].post(
            URL_EXPORTED_PROCESSING,
            json={"alignment_idx": seeded["align_1"], "prep_sample_idx": [seeded["ps_a"]]},
        )
        other = await role_keyed_clients["wet"].post(
            URL_EXPORTED_PROCESSING,
            json={
                "alignment_idx": seeded["align_1"],
                "prep_sample_idx": [seeded["ps_a"], seeded["ps_b"]],
            },
        )
        assert one.status_code == other.status_code == 201
        assert one.json()["export_processing_id"] == other.json()["export_processing_id"]
    finally:
        await _drop(db, seeded["align_1"])


async def test_two_processings_get_two_handles(role_keyed_clients, seeded):
    """Via wet_lab_admin, which is the only caller here that may read both alignments'
    data — align_2 is gated solely to ps_b in the study `regular_user` cannot see."""
    db = role_keyed_clients["pool"]
    try:
        first = await role_keyed_clients["wet"].post(
            URL_EXPORTED_PROCESSING,
            json={"alignment_idx": seeded["align_1"], "prep_sample_idx": [seeded["ps_a"]]},
        )
        second = await role_keyed_clients["wet"].post(
            URL_EXPORTED_PROCESSING,
            json={"alignment_idx": seeded["align_2"], "prep_sample_idx": [seeded["ps_b"]]},
        )
        assert first.status_code == second.status_code == 201
        assert first.json()["export_processing_id"] != second.json()["export_processing_id"]
    finally:
        await _drop(db, seeded["align_1"], seeded["align_2"])


async def test_a_caller_who_cannot_read_the_data_gets_no_handle_for_it(role_keyed_clients, seeded):
    """The reason the cohort is in the body at all.

    Minting is a write. Without this gate any human holding `prep_sample:read` — which
    every role holds — could walk the `alignment_idx` range and collect a durable
    public handle for every processing in the system, including ones over data they
    have no access to. The pool listing refuses to so much as mention such an
    alignment; a route that minted for it would be advertising what that listing
    hides.
    """
    db = role_keyed_clients["pool"]
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING,
        json={"alignment_idx": seeded["align_2"], "prep_sample_idx": [seeded["ps_b"]]},
    )
    assert resp.status_code == 403, resp.text
    assert (
        await db.fetchval(
            "SELECT count(*) FROM qiita.exported_processing WHERE alignment_idx = $1",
            seeded["align_2"],
        )
        == 0
    ), "a refused request must not have written a row"


async def test_naming_a_readable_cohort_of_someone_elses_alignment_is_refused(
    role_keyed_clients, seeded
):
    """The gap the cohort gate would leave on its own: a caller could pass samples they
    genuinely may read while naming an alignment those samples were never part of, and
    so still probe the `alignment_idx` range. The completion check closes it — ps_a is
    readable but has no completed row for align_2."""
    db = role_keyed_clients["pool"]
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING,
        json={"alignment_idx": seeded["align_2"], "prep_sample_idx": [seeded["ps_a"]]},
    )
    assert resp.status_code == 422, resp.text
    assert (
        await db.fetchval(
            "SELECT count(*) FROM qiita.exported_processing WHERE alignment_idx = $1",
            seeded["align_2"],
        )
        == 0
    )


async def test_an_incomplete_sample_is_refused(role_keyed_clients, seeded):
    """ps_c is readable and part of align_1, but pending. A manifest describes
    processed data, so there is nothing yet to describe."""
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING,
        json={"alignment_idx": seeded["align_1"], "prep_sample_idx": [seeded["ps_c"]]},
    )
    assert resp.status_code == 422, resp.text
    assert "not completed" in resp.json()["detail"]


async def test_an_unknown_alignment_is_refused(role_keyed_clients, seeded):
    """404 before the cohort is looked at, so a typo'd alignment is distinguishable
    from one whose data the caller may not read."""
    db = role_keyed_clients["pool"]
    absent = await db.fetchval(
        "SELECT coalesce(max(alignment_idx), 0) + 1000 FROM qiita.alignment_definition"
    )
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING,
        json={"alignment_idx": absent, "prep_sample_idx": [seeded["ps_a"]]},
    )
    assert resp.status_code == 404, resp.text


async def test_an_empty_cohort_is_refused(role_keyed_clients, seeded):
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING,
        json={"alignment_idx": seeded["align_1"], "prep_sample_idx": []},
    )
    assert resp.status_code == 422, resp.text


async def test_an_unauthenticated_caller_is_refused(role_keyed_clients, seeded):
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_PROCESSING,
        json={"alignment_idx": seeded["align_1"], "prep_sample_idx": [seeded["ps_a"]]},
        headers={"Authorization": "Bearer nope"},
    )
    assert resp.status_code == 401, resp.text
