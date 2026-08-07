"""Route tests for POST /sample-label — the public sample label map.

Resolves a prep_sample cohort to the labels a published feature table carries in
place of our identifiers. Shares its access gate, its refusal wording, and its
cohort cap with the human alignment mint, so the tests here concentrate on what
is NEW: which label form is chosen for which sample, and the three refusals
(access → existence → labellability) in that order.

`pool_alignment_seed` seeds four samples across two studies with a reader holding
VIEWER on one — the whole access matrix — and seeds no accessions at all, which
is why every happy-path test stamps the ones it needs.
"""

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SAMPLE_LABEL

pytestmark = pytest.mark.db


async def _stamp(pool, prep_sample_idx, *, biosample_accession=None, ena_run_accession=None):
    """Give a seeded sample the accessions a label needs. The seed helpers leave
    both NULL (they seed the minimum the FKs require), so a test says which of
    the three label forms it is exercising by which of these it sets."""
    if biosample_accession is not None:
        await pool.execute(
            "UPDATE qiita.biosample SET biosample_accession = $2"
            " WHERE idx = (SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $1)",
            prep_sample_idx,
            biosample_accession,
        )
    if ena_run_accession is not None:
        await pool.execute(
            "UPDATE qiita.sequenced_sample SET ena_run_accession = $2 WHERE prep_sample_idx = $1",
            prep_sample_idx,
            ena_run_accession,
        )


async def _unpool(pool, prep_sample_idx):
    """Detach a sample from its pool. `sequenced_pool_idx` is nullable, and the
    CHECK requires it and `sequenced_pool_item_id` to be set together."""
    await pool.execute(
        "UPDATE qiita.sequenced_sample"
        "   SET sequenced_pool_idx = NULL, sequenced_pool_item_id = NULL"
        " WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )


async def test_label_map_returns_a_label_per_sample(role_keyed_clients, pool_alignment_seed):
    """One entry per requested sample, ascending by prep_sample_idx, each carrying
    the parts it was composed from so nothing has to parse the label."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")
    await _stamp(db, seed["ps_c"], biosample_accession="SAMN00000003")

    cohort = sorted([seed["ps_a"], seed["ps_c"]])
    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_c"], seed["ps_a"]]}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == 2
    assert [entry["prep_sample_idx"] for entry in body["labels"]] == cohort
    first = body["labels"][0]
    assert first["biosample_accession"] is not None
    assert first["sequencing_run_idx"] == seed["run_idx"]
    assert first["sequenced_pool_idx"] == seed["pool_idx"]


async def test_label_map_prefers_the_ena_run_accession(role_keyed_clients, pool_alignment_seed):
    """A submitted sample labels as its run accession, not the composite — public
    and unique per sequenced sample. The composite's parts still ship."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(
        db, seed["ps_a"], biosample_accession="SAMN00000001", ena_run_accession="ERR1234567"
    )

    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_a"]]}
    )
    assert resp.status_code == 200, resp.text
    entry = resp.json()["labels"][0]
    assert entry["label"] == "ERR1234567"
    assert entry["ena_run_accession"] == "ERR1234567"
    assert entry["biosample_accession"] == "SAMN00000001"


async def test_label_map_falls_back_to_the_pooled_composite(
    role_keyed_clients, pool_alignment_seed
):
    """No run accession (unsubmitted): `<accession>.<run>.<pool>.<prep_sample>`,
    byte-identical to what the masked-read export names its files."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")

    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_a"]]}
    )
    assert resp.status_code == 200, resp.text
    entry = resp.json()["labels"][0]
    assert entry["label"] == f"SAMN00000001.{seed['run_idx']}.{seed['pool_idx']}.{seed['ps_a']}"
    assert entry["ena_run_accession"] is None


async def test_label_map_falls_back_to_the_short_form_when_unpooled(
    role_keyed_clients, pool_alignment_seed
):
    """`sequenced_pool_idx` is nullable, so the four-part composite is not always
    constructible — the roadmap's scheme has a gap this fills rather than 422s.
    The pool and run columns come back null so a consumer can tell which form it
    is looking at without parsing."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")
    await _unpool(db, seed["ps_a"])

    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_a"]]}
    )
    assert resp.status_code == 200, resp.text
    entry = resp.json()["labels"][0]
    assert entry["label"] == f"SAMN00000001.{seed['ps_a']}"
    assert entry["sequenced_pool_idx"] is None
    assert entry["sequencing_run_idx"] is None


async def test_label_map_422_names_samples_with_no_biosample_accession(
    role_keyed_clients, pool_alignment_seed
):
    """Fail loud rather than ship a column with a hole in it. Truncated like every
    other caller-supplied identifier list."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")
    # ps_c keeps its seeded NULL accession.

    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_a"], seed["ps_c"]]}
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert str(seed["ps_c"]) in detail
    assert "biosample_accession" in detail


async def test_label_map_refuses_a_partially_readable_cohort_before_labelling(
    role_keyed_clients, pool_alignment_seed
):
    """ps_b is unreadable AND unlabellable, which pins two rules at once.

    All-or-nothing: the readable ps_a is not quietly returned on its own — a label
    map covering fewer samples than the table it ships with is worse than no map.
    And access wins over labellability: a 422 naming ps_b would tell the caller it
    exists and lacks an accession, for a sample they may not read at all.
    """
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_a"], seed["ps_b"]]}
    )
    assert resp.status_code == 403, resp.text
    detail = resp.json()["detail"]
    assert "study access" in detail
    assert "biosample_accession" not in detail


async def test_label_map_403_does_not_enumerate_the_cohort(role_keyed_clients, pool_alignment_seed):
    """The M3 disclosure lesson, re-pinned on this route: the refusal must not
    name every blocked sample alongside the study that blocked it — that is an
    enumeration oracle over a body the caller controls."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL,
        json={"prep_sample_idx": [seed["ps_a"], seed["ps_b"], seed["ps_d"]]},
    )
    assert resp.status_code == 403, resp.text
    detail = resp.json()["detail"]
    # ps_b is blocked by study_2 and ps_d is an orphan; neither prep_sample_idx
    # may appear next to the study that denied it.
    assert str(seed["ps_b"]) not in detail
    assert str(seed["study_2"]) in detail  # the study IS named — it is what to go ask for
    assert "no active study link" in detail


async def test_label_map_404s_an_unknown_prep_sample(role_keyed_clients, pool_alignment_seed):
    """Only a role-bypassed caller reaches this (for anyone else an unknown idx has
    no study link and 403s first), but without it a typo'd identifier would vanish
    silently from an answer that claims to cover the whole cohort."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")

    resp = await role_keyed_clients["wet"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_a"], 99_999_999]}
    )
    assert resp.status_code == 404, resp.text
    assert "99999999" in resp.json()["detail"]


async def test_label_map_bypasses_at_wet_lab_admin(role_keyed_clients, pool_alignment_seed):
    """wet_lab_admin sees the whole cohort including the orphan (ps_d), matching
    filter_prep_samples_caller_can_read's bypass — an admin must be able to see
    the anomaly in order to act on it."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    for key, accession in (("ps_b", "SAMN00000002"), ("ps_d", "SAMN00000004")):
        await _stamp(db, seed[key], biosample_accession=accession)

    resp = await role_keyed_clients["wet"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_b"], seed["ps_d"]]}
    )
    assert resp.status_code == 200, resp.text
    assert {e["prep_sample_idx"] for e in resp.json()["labels"]} == {seed["ps_b"], seed["ps_d"]}


async def test_label_map_dedups_the_cohort(role_keyed_clients, pool_alignment_seed):
    """A repeated identifier is one entry: the cohort is a set, and a duplicated
    label row would double-count a sample in whatever the map is joined to."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")

    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL, json={"prep_sample_idx": [seed["ps_a"], seed["ps_a"]]}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["count"] == 1


async def test_label_map_requires_a_cohort(role_keyed_clients):
    """An empty cohort has no answer to give — a caller meaning "all of them" has
    to say which."""
    resp = await role_keyed_clients["user"].post(URL_SAMPLE_LABEL, json={"prep_sample_idx": []})
    assert resp.status_code == 422, resp.text


async def test_label_map_caps_the_cohort(role_keyed_clients):
    """Bounded by the same constant as the alignment mint — both bound a cohort a
    scientist assembles, for the same payload-size and disclosure-width reasons."""
    from qiita_common.models._base import MAX_COHORT_PREP_SAMPLE_IDX

    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL,
        json={"prep_sample_idx": list(range(1, MAX_COHORT_PREP_SAMPLE_IDX + 2))},
    )
    assert resp.status_code == 422, resp.text


async def test_label_map_rejects_unknown_body_fields(role_keyed_clients, pool_alignment_seed):
    """`extra="forbid"`: a misspelled field must not be silently ignored on a
    request whose whole point is naming an exact cohort."""
    resp = await role_keyed_clients["user"].post(
        URL_SAMPLE_LABEL,
        json={"prep_sample_idx": [pool_alignment_seed["ps_a"]], "prep_sample_idxs": [1]},
    )
    assert resp.status_code == 422, resp.text


async def test_label_map_below_scope_is_403(make_pat_client):
    """A token without prep-sample:read is refused before the handler runs."""
    from qiita_common.auth_constants import Scope

    client = await make_pat_client(label="label-no-ps-read", scopes=[Scope.SELF_PROFILE])
    resp = await client.post(URL_SAMPLE_LABEL, json={"prep_sample_idx": [1]})
    assert resp.status_code == 403, resp.text


async def test_label_map_requires_auth(postgres_pool):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(URL_SAMPLE_LABEL, json={"prep_sample_idx": [1]})
    assert resp.status_code == 401, resp.text
