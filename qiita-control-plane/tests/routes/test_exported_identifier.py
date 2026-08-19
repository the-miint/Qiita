"""Route tests for POST /exported-identifier — the public handle map.

Mints an `export_id` for every processed sample in a cohort, so a published table
can name its samples without carrying ours. Shares its access gate, its refusal
wording, and its cohort cap with the human alignment mint, so the tests here
concentrate on what is NEW: that the identifier is minted and stable, that it is
Postgres-authored, and the three refusals (alignment → access → completeness) in
that order.

`pool_alignment_seed` is exactly the shape this needs: ps_a is readable and
completed for align_1, ps_c is readable but PENDING, ps_b is unreadable, ps_d is
an orphan. It seeds no accessions, which is why the tests that care stamp them.
"""

import re

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_EXPORTED_IDENTIFIER

pytestmark = pytest.mark.db


async def _stamp(pool, prep_sample_idx, *, biosample_accession=None, ena_run_accession=None):
    """Give a seeded sample the accessions that ride alongside an export_id. The
    seed helpers leave both NULL, which is itself a case worth testing: an
    unaccessioned sample still gets an identifier."""
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


def _body(seed, *prep_sample_idx, alignment_idx=None):
    return {
        "alignment_idx": alignment_idx if alignment_idx is not None else seed["align_1"],
        "prep_sample_idx": list(prep_sample_idx),
    }


async def test_mints_a_qm_identifier_per_sample(role_keyed_clients, pool_alignment_seed):
    """One entry per requested sample, ascending by prep_sample_idx, each carrying a
    `QM<idx>` handle and the accessions it does not replace."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")

    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"])
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["count"] == 1
    assert body["alignment_idx"] == seed["align_1"]
    entry = body["identifiers"][0]
    assert entry["prep_sample_idx"] == seed["ps_a"]
    assert entry["export_id"].startswith("QM")
    assert entry["export_id"][2:].isdigit()
    assert entry["biosample_accession"] == "SAMN00000001"


async def test_identifier_is_stable_across_requests(role_keyed_clients, pool_alignment_seed):
    """The mint is idempotent, and that is the contract: an export_id is published,
    so the same processed sample must resolve the same way forever. A second
    request adds no row and returns the same handle."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]

    first = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"])
    )
    second = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"])
    )
    assert first.status_code == second.status_code == 201, second.text
    assert first.json()["identifiers"] == second.json()["identifiers"]

    rows = await db.fetchval(
        "SELECT count(*) FROM qiita.exported_identifier"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        seed["align_1"],
        seed["ps_a"],
    )
    assert rows == 1


async def test_a_different_alignment_gets_a_different_identifier(
    role_keyed_clients, pool_alignment_seed
):
    """An export_id names a PROCESSED sample, not a sample. The same prep_sample
    under a second alignment is different data and gets its own handle."""
    seed = pool_alignment_seed

    one = await role_keyed_clients["wet"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_b"], alignment_idx=seed["align_1"])
    )
    two = await role_keyed_clients["wet"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_b"], alignment_idx=seed["align_2"])
    )
    assert one.status_code == two.status_code == 201, two.text
    assert one.json()["identifiers"][0]["export_id"] != two.json()["identifiers"][0]["export_id"]


async def test_an_unaccessioned_sample_still_gets_an_identifier(
    role_keyed_clients, pool_alignment_seed
):
    """The label this replaced could not be composed without a biosample_accession
    and 422'd without one. An export_id always can, so the refusal is gone."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"])
    )
    assert resp.status_code == 201, resp.text
    entry = resp.json()["identifiers"][0]
    assert entry["biosample_accession"] is None
    assert entry["export_id"].startswith("QM")


async def test_accessions_ride_along_when_present(role_keyed_clients, pool_alignment_seed):
    """Both accessions are informational passengers, not the identifier — they ship
    because they are already public and neither can do export_id's job."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(
        db, seed["ps_a"], biosample_accession="SAMN00000001", ena_run_accession="ERR1234567"
    )

    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"])
    )
    assert resp.status_code == 201, resp.text
    entry = resp.json()["identifiers"][0]
    assert entry["ena_run_accession"] == "ERR1234567"
    assert entry["biosample_accession"] == "SAMN00000001"
    # The accession is NOT the identifier, however public it is.
    assert entry["export_id"] != "ERR1234567"


async def test_export_id_is_not_caller_supplied(role_keyed_clients, pool_alignment_seed):
    """`extra="forbid"` plus a GENERATED ALWAYS column: there is no path by which a
    caller authors a public identifier."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER,
        json={**_body(seed, seed["ps_a"]), "export_id": "QMhack"},
    )
    assert resp.status_code == 422, resp.text


async def test_leaks_no_internal_identifier_beyond_the_join_key(
    role_keyed_clients, pool_alignment_seed
):
    """The response carries prep_sample_idx and alignment_idx — both identifiers the
    caller sent us, and both needed to join the map to their own rows — and nothing
    else of ours. No pool, no run: those shipped with the old label so nothing had
    to parse it, and there is no label now.

    Asserted on the key SETS rather than by searching the body text. A substring
    search for a small idx matches any digit that happens to appear in an accession
    or another identifier, which is a test that fails on seed ordering rather than
    on a leak.
    """
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    await _stamp(db, seed["ps_a"], biosample_accession="SAMN00000001")

    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"])
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert set(body) == {"alignment_idx", "identifiers", "count"}
    assert set(body["identifiers"][0]) == {
        "prep_sample_idx",
        "export_id",
        "biosample_accession",
        "ena_run_accession",
    }


async def test_422_on_a_sample_not_completed_for_the_alignment(
    role_keyed_clients, pool_alignment_seed
):
    """ps_c is readable but PENDING for align_1. An identifier names processed data,
    so there is nothing yet to name."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"], seed["ps_c"])
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert str(seed["ps_c"]) in detail
    assert "not completed" in detail


async def test_422_subsumes_a_sample_outside_the_alignment(role_keyed_clients, pool_alignment_seed):
    """ps_b is completed for align_2 but has no gate row for align_1 at all. It must
    not vanish from an answer that claims to cover the whole cohort."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["wet"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_b"], alignment_idx=seed["align_2"])
    )
    assert resp.status_code == 201, resp.text

    resp = await role_keyed_clients["wet"].post(
        URL_EXPORTED_IDENTIFIER,
        json=_body(seed, seed["ps_a"], seed["ps_b"], alignment_idx=seed["align_2"]),
    )
    assert resp.status_code == 422, resp.text
    assert str(seed["ps_a"]) in resp.json()["detail"]


async def test_404s_an_unknown_alignment(role_keyed_clients, pool_alignment_seed):
    """404 first, before anything discloses cohort state — mirroring the mint."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"], alignment_idx=99_999_999)
    )
    assert resp.status_code == 404, resp.text


async def test_refuses_a_partially_readable_cohort_before_completeness(
    role_keyed_clients, pool_alignment_seed
):
    """All-or-nothing, and access wins over completeness. A partially-minted cohort
    would name some of a caller's samples and silently omit the rest; and a 422
    naming ps_b would tell the caller it exists, for a sample they may not read."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"], seed["ps_b"])
    )
    assert resp.status_code == 403, resp.text
    detail = resp.json()["detail"]
    assert "study access" in detail
    assert "not completed" not in detail


async def test_403_does_not_enumerate_the_cohort(role_keyed_clients, pool_alignment_seed):
    """The refusal must not name every blocked sample alongside the study that
    blocked it — over a caller-controlled body that is an enumeration oracle for
    "which of these exist, and whose are they".

    ps_b is blocked by study_2, ps_d is the orphan. The study must be named (it is
    what to go ask for); ps_b must not be, in any clause.
    """
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER,
        json=_body(seed, seed["ps_a"], seed["ps_b"], seed["ps_d"]),
    )
    assert resp.status_code == 403, resp.text
    detail = resp.json()["detail"]
    # Every integer of every clause, compared as sets, rather than substrings of the
    # whole message. `prep_sample_access_denied_detail` joins its clauses with "; "
    # and renders each as a count plus `first_few` examples, so a clause holds its
    # count and its own identifiers and nothing else numeric — and ps_b named
    # anywhere fails, including appended to the study clause outside its `(e.g. …)`.
    # Substrings cannot do that job in either direction: `qiita.study.idx` and
    # `qiita.prep_sample.idx` are separate identity sequences running to similar
    # magnitudes, so `str(ps_b) in detail` fires on a study idx that equals ps_b,
    # and `str(study_2) in detail` is satisfied by a prep_sample idx that does.
    # The same refusal is asserted by substring in routes/test_alignment_cohort_mint.py,
    # which carries the same collision and is unchanged.
    clauses = [{int(n) for n in re.findall(r"\d+", part)} for part in detail.split("; ")]
    blocked_studies, unlinked_samples = {seed["study_2"]}, {seed["ps_d"]}
    assert clauses == [
        {len(blocked_studies), *blocked_studies},
        {len(unlinked_samples), *unlinked_samples},
    ], detail
    assert "no active study link" in detail


async def test_nothing_is_minted_when_the_cohort_is_refused(
    role_keyed_clients, pool_alignment_seed
):
    """A refusal must not leave a partial mint behind. The readable half of a 403'd
    cohort gets no identifier — otherwise a retry after fixing access would find
    some samples already handled and the map would be assembled from two answers."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"], seed["ps_b"])
    )
    assert resp.status_code == 403, resp.text
    assert (
        await db.fetchval(
            "SELECT count(*) FROM qiita.exported_identifier WHERE prep_sample_idx = $1",
            seed["ps_a"],
        )
        == 0
    )


async def test_bypasses_at_wet_lab_admin(role_keyed_clients, pool_alignment_seed):
    """wet_lab_admin sees the whole cohort including the orphan (ps_d), matching
    filter_prep_samples_caller_can_read's bypass — an admin must be able to see the
    anomaly in order to act on it."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["wet"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_b"], seed["ps_d"])
    )
    assert resp.status_code == 201, resp.text
    assert {e["prep_sample_idx"] for e in resp.json()["identifiers"]} == {
        seed["ps_b"],
        seed["ps_d"],
    }


async def test_dedups_the_cohort(role_keyed_clients, pool_alignment_seed):
    """A repeated identifier is one entry: the cohort is a set, and a duplicated row
    would double-count a sample in whatever the map is joined to."""
    seed = pool_alignment_seed
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(seed, seed["ps_a"], seed["ps_a"])
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["count"] == 1


async def test_requires_a_cohort(role_keyed_clients, pool_alignment_seed):
    """An empty cohort has no answer to give — a caller meaning "all of them" has to
    say which."""
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json=_body(pool_alignment_seed)
    )
    assert resp.status_code == 422, resp.text


async def test_caps_the_cohort(role_keyed_clients, pool_alignment_seed):
    """Bounded by the same constant as the alignment mint — both bound a cohort a
    scientist assembles, for the same payload-size and disclosure-width reasons."""
    from qiita_common.models._base import MAX_COHORT_PREP_SAMPLE_IDX

    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER,
        json=_body(pool_alignment_seed, *range(1, MAX_COHORT_PREP_SAMPLE_IDX + 2)),
    )
    assert resp.status_code == 422, resp.text


async def test_requires_an_alignment_idx(role_keyed_clients, pool_alignment_seed):
    """The processing is not optional: an identifier with no processing behind it
    would name a sample, which is what an accession already fails to do usefully."""
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_IDENTIFIER, json={"prep_sample_idx": [pool_alignment_seed["ps_a"]]}
    )
    assert resp.status_code == 422, resp.text


async def test_below_scope_is_403(make_pat_client):
    """A token without prep_sample:read is refused before the handler runs."""
    from qiita_common.auth_constants import Scope

    client = await make_pat_client(label="expid-no-ps-read", scopes=[Scope.SELF_PROFILE])
    resp = await client.post(
        URL_EXPORTED_IDENTIFIER, json={"alignment_idx": 1, "prep_sample_idx": [1]}
    )
    assert resp.status_code == 403, resp.text


async def test_requires_auth(postgres_pool):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            URL_EXPORTED_IDENTIFIER, json={"alignment_idx": 1, "prep_sample_idx": [1]}
        )
    assert resp.status_code == 401, resp.text
