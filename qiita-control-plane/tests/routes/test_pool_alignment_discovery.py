"""Route tests for the two pool-alignment discovery reads.

    GET /sequencing-run/{run}/sequenced-pool/{pool}/alignment
    GET /sequencing-run/{run}/sequenced-pool/{pool}/alignment/{alignment_idx}/cohort

Both NARROW to the caller's readable slice rather than rejecting a pool that
spans studies they only partly hold — listing is where narrowing is safe,
because no scientific result depends on it. They pair with an all-or-nothing
mint: you discover exactly the cohort you are then allowed to sign, so the
two-step flow never surprises you.

The seeded shape is the shared `pool_alignment_seed` fixture (see its docstring
in conftest.py) — one pool spanning two studies, of which `regular_user` may
read only the first. The mint tests seed from the same fixture, deliberately:
these two routes are halves of one contract, and separate fixtures are how the
cohort discovery hands back drifts from the cohort the mint accepts.
"""

import pytest
from qiita_common.api_paths import (
    URL_SEQUENCED_POOL_ALIGNMENT,
    URL_SEQUENCED_POOL_ALIGNMENT_COHORT,
)

pytestmark = pytest.mark.db


@pytest.fixture
def ctx(role_keyed_clients):
    return dict(role_keyed_clients)


@pytest.fixture
def seeded(pool_alignment_seed):
    """Local alias for the shared fixture — the shape lives in conftest.py."""
    return pool_alignment_seed


def _list_url(s):
    return URL_SEQUENCED_POOL_ALIGNMENT.format(
        sequencing_run_idx=s["run_idx"], sequenced_pool_idx=s["pool_idx"]
    )


def _cohort_url(s, alignment_idx):
    return URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=s["run_idx"],
        sequenced_pool_idx=s["pool_idx"],
        alignment_idx=alignment_idx,
    )


# ---------------------------------------------------------------------------
# Alignments over a pool
# ---------------------------------------------------------------------------


async def test_pool_alignment_list_counts_only_the_callers_readable_samples(ctx, seeded):
    """The reader holds study_1 only, so alignment_1 reports 1 of 2 — not the
    pool's real 3 of 4.

    Caller-scoped counts are not cosmetic: the mint is all-or-nothing, so a
    caller shown "3 completed" would ask for three samples and get a 403. The
    two routes have to agree about what "your cohort" means.
    """
    resp = await ctx["user"].get(_list_url(seeded))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded["pool_idx"]
    assert body["sequencing_run_idx"] == seeded["run_idx"]

    by_idx = {a["alignment_idx"]: a for a in body["alignments"]}
    assert by_idx[seeded["align_1"]]["samples_completed"] == 1
    assert by_idx[seeded["align_1"]]["samples_total"] == 2
    assert by_idx[seeded["align_1"]]["params"]["aligner"] == "minimap2"


async def test_pool_alignment_list_omits_an_alignment_the_caller_can_see_no_sample_of(ctx, seeded):
    """alignment_2 touches only ps_b (study_2), which the reader cannot read, so
    the alignment does not appear at all — not as a zero-count row. A row with
    zero readable samples still discloses that the alignment exists."""
    resp = await ctx["user"].get(_list_url(seeded))
    assert resp.status_code == 200, resp.text
    idxs = [a["alignment_idx"] for a in resp.json()["alignments"]]
    assert seeded["align_1"] in idxs
    assert seeded["align_2"] not in idxs


async def test_pool_alignment_list_bypasses_at_wet_lab_admin(ctx, seeded):
    """A wet_lab_admin skips the per-study check entirely (the standing bypass
    on every resource gate), so it sees both alignments and the pool's real
    counts — including the orphaned ps_d, which is exactly the anomaly an admin
    should be able to see."""
    resp = await ctx["wet"].get(_list_url(seeded))
    assert resp.status_code == 200, resp.text
    by_idx = {a["alignment_idx"]: a for a in resp.json()["alignments"]}
    # 4 of 5: ps_a, ps_b, ps_d, ps_e completed; ps_c pending.
    assert by_idx[seeded["align_1"]]["samples_completed"] == 4
    assert by_idx[seeded["align_1"]]["samples_total"] == 5
    assert seeded["align_2"] in by_idx


async def test_pool_alignment_list_excludes_an_orphaned_prep_sample(ctx, seeded):
    """ps_d's only study link is retired, so a plain reader never counts it.

    An unlinked sample has no study to authorize against, and failing OPEN on a
    data-integrity anomaly is the wrong default for a read. The reader's total
    of 2 (ps_a + ps_c) is what proves ps_d was dropped.
    """
    resp = await ctx["user"].get(_list_url(seeded))
    by_idx = {a["alignment_idx"]: a for a in resp.json()["alignments"]}
    assert by_idx[seeded["align_1"]]["samples_total"] == 2


# ---------------------------------------------------------------------------
# Cohort for (pool, alignment)
# ---------------------------------------------------------------------------


async def test_cohort_returns_only_completed_and_readable_samples(ctx, seeded):
    """Every filter at once: ps_a survives; ps_b is in a study this caller cannot
    see, ps_c is pending, ps_d is orphaned, ps_e is shared into study_2."""
    resp = await ctx["user"].get(_cohort_url(seeded, seeded["align_1"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["alignment_idx"] == seeded["align_1"]
    assert body["prep_sample_idx"] == [seeded["ps_a"]]


async def test_cohort_excludes_a_sample_shared_into_an_unreadable_study(ctx, seeded):
    """ps_e is linked to study_1 (readable) AND study_2 (not), and is excluded.

    This is the all-of rule, and it is the one property the discovery/mint
    contract cannot be wrong about: a signed cohort is the whole authorization
    boundary, with no second check behind it. A gate that granted a sample on ANY
    readable link would leak every sample that had ever been shared into a study
    the caller happens to see. Named separately from the composite test above so a
    regression reads as "all-of broke" rather than "a list changed".
    """
    resp = await ctx["user"].get(_cohort_url(seeded, seeded["align_1"]))
    assert resp.status_code == 200, resp.text
    assert seeded["ps_e"] not in resp.json()["prep_sample_idx"]

    # And the admin bypass still sees it — the sample is real, only unreadable.
    resp = await ctx["wet"].get(_cohort_url(seeded, seeded["align_1"]))
    assert seeded["ps_e"] in resp.json()["prep_sample_idx"]


async def test_cohort_excludes_a_pending_alignment_sample(ctx, seeded):
    """`pending` is not `completed`, and alignment rows are NOT 1:1 with reads,
    so presence of rows must never be read as done. ps_c is readable and still
    excluded — the completion gate is a first-class state."""
    resp = await ctx["wet"].get(_cohort_url(seeded, seeded["align_1"]))
    assert resp.status_code == 200, resp.text
    assert seeded["ps_c"] not in resp.json()["prep_sample_idx"]


async def test_cohort_for_wet_lab_admin_spans_both_studies(ctx, seeded):
    """The bypass caller's cohort is every completed sample in the pool."""
    resp = await ctx["wet"].get(_cohort_url(seeded, seeded["align_1"]))
    assert resp.status_code == 200, resp.text
    got = resp.json()["prep_sample_idx"]
    assert seeded["ps_a"] in got and seeded["ps_b"] in got


async def test_cohort_is_sorted_for_a_stable_mint_body(ctx, seeded):
    """Sorted so the same pool + alignment always yields the same request body —
    the cohort is signed into a ticket, and an unstable order would make two
    identical requests produce different payload bytes."""
    resp = await ctx["wet"].get(_cohort_url(seeded, seeded["align_1"]))
    got = resp.json()["prep_sample_idx"]
    assert got == sorted(got)


async def test_cohort_of_an_alignment_the_caller_cannot_read_is_empty_not_403(ctx, seeded):
    """Discovery narrows; it does not reject. An alignment whose samples the
    caller cannot read comes back with an empty cohort, which is the honest
    answer to "what may I mint here" — and, unlike a 403, does not confirm which
    of the pool's alignments touch data they lack."""
    resp = await ctx["user"].get(_cohort_url(seeded, seeded["align_2"]))
    assert resp.status_code == 200, resp.text
    assert resp.json()["prep_sample_idx"] == []


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


async def test_discovery_is_open_to_a_plain_user(ctx, seeded):
    """Unlike the wet_lab_admin-gated pool-completion route, these are open to
    role `user`: the per-study tier check is the boundary, not the role."""
    assert (await ctx["user"].get(_list_url(seeded))).status_code == 200
    assert (await ctx["user"].get(_cohort_url(seeded, seeded["align_1"]))).status_code == 200


async def test_discovery_401_when_anonymous(ctx, seeded):
    from httpx import ASGITransport, AsyncClient

    from qiita_control_plane.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        assert (await anon.get(_list_url(seeded))).status_code == 401
        assert (await anon.get(_cohort_url(seeded, seeded["align_1"]))).status_code == 401


async def test_discovery_403_without_prep_sample_read(ctx, seeded, no_prep_sample_read_client):
    assert (await no_prep_sample_read_client.get(_list_url(seeded))).status_code == 403


async def test_discovery_404_for_a_missing_pool(ctx, seeded):
    url = URL_SEQUENCED_POOL_ALIGNMENT.format(
        sequencing_run_idx=seeded["run_idx"], sequenced_pool_idx=999_999_999
    )
    assert (await ctx["wet"].get(url)).status_code == 404


async def test_discovery_422_when_pool_is_not_in_the_run(ctx, seeded):
    url = URL_SEQUENCED_POOL_ALIGNMENT.format(
        sequencing_run_idx=999_999_999, sequenced_pool_idx=seeded["pool_idx"]
    )
    assert (await ctx["wet"].get(url)).status_code == 422
