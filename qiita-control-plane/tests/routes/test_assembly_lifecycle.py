"""DB tests for the assembly lifecycle: run-config deprecation, per-run
invalidation, and the /processing read surface both of them stay visible in.

A processing_idx is the canonical-params hash over {workflow, version, mask_idx,
assembler}, so a run built from a pass-set later judged unsound re-resolves to
the SAME identity and assembles more samples from the same defect. Two markers
answer two different questions, and the tests below are organized by which:

  * `processing.status = 'deprecated'` — the run CONFIG is void. Refused at the
    mint, which is the guard that stops new bad data.
  * `assembly_sample.state = 'invalidated'` — a RUN of a sound config is not
    trustworthy. Refused wherever contigs are consumed, which needs no new check:
    every consumer already proceeds only on `'completed'`.
"""

import json
import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_PROCESSING_BY_IDX,
    URL_PROCESSING_PREFIX,
    URL_PROCESSING_PREP_SAMPLE,
    URL_PROCESSING_SAMPLE_STATUS,
    URL_PROCESSING_STATUS,
    LibraryPrimitive,
)
from qiita_common.models import ProcessingStatus

from qiita_control_plane.repositories.processing import ProcessingDeprecated, mint_processing
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
)

pytestmark = pytest.mark.db


def _install_settings(app):
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )


@pytest_asyncio.fixture
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


@pytest_asyncio.fixture
async def lifecycle(postgres_pool, human_admin_session):
    """One assembly run plus three prep_samples gated under it, one per state the
    PATCH route has to treat differently: 'completed' (withdrawable), 'pending'
    (the pipeline's to write) and 'no_data' (nothing to withdraw)."""
    principal_idx = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)
    params = {
        "workflow": "long-read-assembly",
        "version": "1.0.0",
        "mask_idx": 1,
        "assembler": "hifiasm_meta",
        "nonce": suffix,
    }
    async with postgres_pool.acquire() as conn:
        row = await mint_processing(
            conn, workflow="long-read-assembly", version="1.0.0", params=params
        )
    processing_idx = row["processing_idx"]
    seeded = [
        await seed_biosample_with_sequenced_prep_sample(postgres_pool, owner_idx=principal_idx)
        for _ in range(3)
    ]
    (bs_done, ps_done), (bs_pend, ps_pend), (bs_nd, ps_nd) = seeded
    await postgres_pool.executemany(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, $3)",
        [
            (processing_idx, ps_done, "completed"),
            (processing_idx, ps_pend, "pending"),
            (processing_idx, ps_nd, "no_data"),
        ],
    )
    yield {
        "processing_idx": processing_idx,
        "params": params,
        "principal_idx": principal_idx,
        "ps_done": ps_done,
        "ps_pending": ps_pend,
        "ps_no_data": ps_nd,
        # The prep_sample_to_study trigger requires a live biosample_to_study link
        # first, so the narrowing test needs the parent too.
        "bs_done": bs_done,
    }
    await postgres_pool.execute(
        "DELETE FROM qiita.assembly_sample WHERE processing_idx = $1", processing_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])",
        [ps_done, ps_pend, ps_nd],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs_done, bs_pend, bs_nd]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = $1", processing_idx
    )


async def _deprecate(client, processing_idx, reason="assembled from a mask since deprecated"):
    return await client.patch(
        URL_PROCESSING_STATUS.format(processing_idx=processing_idx),
        json={"status": "deprecated", "reason": reason},
    )


async def _invalidate(client, lifecycle, samples, reason="withdrawn"):
    return await client.patch(
        URL_PROCESSING_SAMPLE_STATUS.format(processing_idx=lifecycle["processing_idx"]),
        json={"prep_sample_idx": samples, "state": "invalidated", "reason": reason},
    )


# ---------------------------------------------------------------------------
# Config lifecycle: refuse to PRODUCE
# ---------------------------------------------------------------------------


async def test_minting_against_a_deprecated_config_fails_loudly(postgres_pool, client, lifecycle):
    """The guard that stops NEW bad data. Enforced in qiita.mint_processing, so it
    holds for every caller of the mint rather than per call site."""
    resp = await _deprecate(client, lifecycle["processing_idx"])
    assert resp.status_code == 200, resp.text

    with pytest.raises(ProcessingDeprecated) as ei:
        async with postgres_pool.acquire() as conn:
            await mint_processing(
                conn,
                workflow="long-read-assembly",
                version="1.0.0",
                params=lifecycle["params"],
            )
    assert str(lifecycle["processing_idx"]) in str(ei.value)
    assert "assembled from a mask since deprecated" in str(ei.value)


async def test_minting_against_an_active_config_still_returns_it(postgres_pool, lifecycle):
    """The control for the test above: the same call on an un-deprecated run
    resolves idempotently to the same row, so the refusal is attributable to the
    status and not to the mint being broken."""
    async with postgres_pool.acquire() as conn:
        row = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version="1.0.0",
            params=lifecycle["params"],
        )
    assert row["processing_idx"] == lifecycle["processing_idx"]
    assert row["status"] == ProcessingStatus.ACTIVE.value


async def test_mint_processing_has_exactly_one_overload(postgres_pool):
    """The lifecycle migration re-states the mint function whole (CREATE OR REPLACE
    has no partial form). Restating the wrong SIGNATURE adds an overload instead of
    replacing the body, leaving the guard on a function nothing calls."""
    sigs = [
        r["sig"]
        for r in await postgres_pool.fetch(
            "SELECT p.oid::regprocedure::text AS sig FROM pg_proc p"
            " JOIN pg_namespace n ON n.oid = p.pronamespace"
            " WHERE n.nspname = 'qiita' AND p.proname = 'mint_processing'"
        )
    ]
    assert len(sigs) == 1, sigs


async def test_the_runner_mint_reports_a_deprecated_config_as_bad_input(postgres_pool, client):
    """Left untranslated the plpgsql 23514 reaches run_workflow's catch-all and is
    recorded as UNKNOWN_PERMANENT, which tells the operator nothing. The runner
    turns it into a SUBMISSION/BAD_INPUT failure naming the reason.

    Mints through `_mint_processing_idx` rather than reusing the fixture's row:
    the runner hashes exactly `_build_processing_params`' four keys, so an
    identity minted any other way is a different row and the deprecation would be
    aimed at the wrong one.
    """
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import WorkTicketFailureStage

    from qiita_control_plane.runner._processing import _mint_processing_idx

    # No FK on the params blob, so a random mask_idx is enough to make this test's
    # identity its own. Drawn ONCE: the params are the identity, so re-drawing per
    # call would mint a new run each time and the idempotence control below would
    # be testing nothing.
    bound = {"mask_idx": secrets.randbelow(10**9) + 10**9}

    async def _mint():
        return await _mint_processing_idx(
            postgres_pool,
            action_id="long-read-assembly",
            action_version="1.0.0",
            bound=bound,
            assembler_default="hifiasm_meta",
        )

    bindings = await _mint()
    processing_idx = bindings["processing_idx"]
    try:
        # Control: the same call is idempotent while the config is active, so the
        # refusal below is attributable to the status.
        assert (await _mint())["processing_idx"] == processing_idx

        resp = await _deprecate(client, processing_idx, reason="assembler misconfigured")
        assert resp.status_code == 200, resp.text

        with pytest.raises(BackendFailure) as ei:
            await _mint()
        assert ei.value.kind is FailureKind.BAD_INPUT
        assert ei.value.stage is WorkTicketFailureStage.SUBMISSION
        assert "assembler misconfigured" in ei.value.reason
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.processing WHERE processing_idx = $1", processing_idx
        )


# ---------------------------------------------------------------------------
# Config lifecycle: the PATCH route, and staying visible
# ---------------------------------------------------------------------------


async def test_deprecation_records_who_when_and_why(client, lifecycle):
    resp = await _deprecate(client, lifecycle["processing_idx"])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "deprecated"
    assert body["deprecation_reason"] == "assembled from a mask since deprecated"
    assert body["deprecated_by_idx"] == lifecycle["principal_idx"]
    assert body["deprecated_at"] is not None


async def test_deprecating_requires_a_reason(client, lifecycle):
    """A bare status flip leaves whoever finds a deprecated run behind published
    contigs with no way to tell a wrong assembly from a superseded one."""
    resp = await client.patch(
        URL_PROCESSING_STATUS.format(processing_idx=lifecycle["processing_idx"]),
        json={"status": "deprecated"},
    )
    assert resp.status_code == 422, resp.text


async def test_reactivating_clears_the_deprecation_provenance(client, lifecycle):
    await _deprecate(client, lifecycle["processing_idx"])
    resp = await client.patch(
        URL_PROCESSING_STATUS.format(processing_idx=lifecycle["processing_idx"]),
        json={"status": "active"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "active"
    assert body["deprecated_at"] is None
    assert body["deprecation_reason"] is None
    assert body["superseded_by"] is None


async def test_patch_status_on_an_unknown_run_404s(client):
    resp = await _deprecate(client, 99_999_999)
    assert resp.status_code == 404


async def test_a_deprecated_run_is_still_listed_and_readable(client, lifecycle):
    """Deprecation is not deletion — and here it cannot be, since assembly has no
    delete path at all. "What assembled this published contig set?" has to keep an
    answer."""
    await _deprecate(client, lifecycle["processing_idx"])

    one = await client.get(URL_PROCESSING_BY_IDX.format(processing_idx=lifecycle["processing_idx"]))
    assert one.status_code == 200
    assert one.json()["status"] == "deprecated"

    listed = await client.get(URL_PROCESSING_PREFIX)
    assert listed.status_code == 200
    found = [
        p for p in listed.json()["processing"] if p["processing_idx"] == lifecycle["processing_idx"]
    ]
    assert found and found[0]["status"] == "deprecated"


async def test_status_query_filters_the_list_both_ways(client, lifecycle):
    processing_idx = lifecycle["processing_idx"]
    await _deprecate(client, processing_idx)

    dep = await client.get(URL_PROCESSING_PREFIX, params={"status": "deprecated"})
    assert processing_idx in {p["processing_idx"] for p in dep.json()["processing"]}

    act = await client.get(URL_PROCESSING_PREFIX, params={"status": "active"})
    assert processing_idx not in {p["processing_idx"] for p in act.json()["processing"]}


async def test_status_and_narrowing_filters_compose(client, lifecycle):
    """Both WHERE fragments at once. Each alone is covered above, and the SQL joins
    them with an explicit connective — a form that is wrong only when both are
    present."""
    processing_idx = lifecycle["processing_idx"]
    params = {"prep_sample_idx": lifecycle["ps_done"], "status": "active"}

    def _idxs(resp):
        return {p["processing_idx"] for p in resp.json()["processing"]}

    assert processing_idx in _idxs(await client.get(URL_PROCESSING_PREFIX, params=params))
    await _deprecate(client, processing_idx)
    assert processing_idx not in _idxs(await client.get(URL_PROCESSING_PREFIX, params=params))
    params["status"] = "deprecated"
    assert processing_idx in _idxs(await client.get(URL_PROCESSING_PREFIX, params=params))


async def test_superseded_by_records_the_replacement_and_rejects_self(client, lifecycle):
    """`superseded_by` is accepted only alongside a deprecation, must name a real
    run, and cannot be the run itself — the last of which the wire model cannot
    catch, since it does not see the path parameter."""
    processing_idx = lifecycle["processing_idx"]
    url = URL_PROCESSING_STATUS.format(processing_idx=processing_idx)

    unknown = await client.patch(
        url, json={"status": "deprecated", "reason": "r", "superseded_by": 99_999_999}
    )
    assert unknown.status_code == 422, unknown.text

    itself = await client.patch(
        url, json={"status": "deprecated", "reason": "r", "superseded_by": processing_idx}
    )
    assert itself.status_code == 422, itself.text

    only_when_deprecated = await client.patch(
        url, json={"status": "active", "superseded_by": processing_idx}
    )
    assert only_when_deprecated.status_code == 422, only_when_deprecated.text


# ---------------------------------------------------------------------------
# Run lifecycle: per-(run, sample) invalidation
# ---------------------------------------------------------------------------


async def test_invalidating_a_run_reports_every_bucket(client, postgres_pool, lifecycle):
    """One call covers the five outcomes the response separates, because a bulk
    withdrawal that reported only a count would leave the operator unable to tell a
    typo'd prep_sample from one that had nothing to withdraw."""
    resp = await _invalidate(
        client,
        lifecycle,
        [
            lifecycle["ps_done"],
            lifecycle["ps_pending"],
            lifecycle["ps_no_data"],
            99_999_999,
        ],
        reason="built on mask 9's pass-set",
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["updated"] == [lifecycle["ps_done"]]
    assert body["skipped_pending"] == [lifecycle["ps_pending"]]
    assert body["skipped_no_data"] == [lifecycle["ps_no_data"]]
    assert body["not_found"] == [99_999_999]
    assert body["unchanged"] == []

    row = await postgres_pool.fetchrow(
        "SELECT state, invalidation_reason, invalidated_by_idx, invalidated_at"
        "  FROM qiita.assembly_sample WHERE processing_idx = $1 AND prep_sample_idx = $2",
        lifecycle["processing_idx"],
        lifecycle["ps_done"],
    )
    assert row["state"] == "invalidated"
    assert row["invalidation_reason"] == "built on mask 9's pass-set"
    assert row["invalidated_by_idx"] == lifecycle["principal_idx"]
    assert row["invalidated_at"] is not None

    # The two skipped rows were left entirely alone.
    states = {
        r["prep_sample_idx"]: r["state"]
        for r in await postgres_pool.fetch(
            "SELECT prep_sample_idx, state FROM qiita.assembly_sample"
            " WHERE processing_idx = $1 AND prep_sample_idx = ANY($2::bigint[])",
            lifecycle["processing_idx"],
            [lifecycle["ps_pending"], lifecycle["ps_no_data"]],
        )
    }
    assert states == {lifecycle["ps_pending"]: "pending", lifecycle["ps_no_data"]: "no_data"}


async def test_invalidation_is_idempotent(client, lifecycle):
    first = await _invalidate(client, lifecycle, [lifecycle["ps_done"]], reason="same reason")
    second = await _invalidate(client, lifecycle, [lifecycle["ps_done"]], reason="same reason")
    assert first.json()["updated"] == [lifecycle["ps_done"]]
    assert second.json()["updated"] == []
    assert second.json()["unchanged"] == [lifecycle["ps_done"]]


async def test_restoring_a_run_clears_the_invalidation_provenance(client, postgres_pool, lifecycle):
    await _invalidate(client, lifecycle, [lifecycle["ps_done"]], reason="withdrawn in error")
    resp = await client.patch(
        URL_PROCESSING_SAMPLE_STATUS.format(processing_idx=lifecycle["processing_idx"]),
        json={"prep_sample_idx": [lifecycle["ps_done"]], "state": "completed"},
    )
    assert resp.status_code == 200, resp.text
    row = await postgres_pool.fetchrow(
        "SELECT state, invalidation_reason, invalidated_by_idx, invalidated_at"
        "  FROM qiita.assembly_sample WHERE processing_idx = $1 AND prep_sample_idx = $2",
        lifecycle["processing_idx"],
        lifecycle["ps_done"],
    )
    assert row["state"] == "completed"
    assert row["invalidation_reason"] is None
    assert row["invalidated_by_idx"] is None
    assert row["invalidated_at"] is None


async def test_restoring_never_reaches_a_pending_or_no_data_row(client, postgres_pool, lifecycle):
    """The other half of what makes restoring-to-'completed' exact rather than a
    guess: since neither state can be withdrawn, neither can be restored into
    something it never was."""
    resp = await client.patch(
        URL_PROCESSING_SAMPLE_STATUS.format(processing_idx=lifecycle["processing_idx"]),
        json={
            "prep_sample_idx": [lifecycle["ps_pending"], lifecycle["ps_no_data"]],
            "state": "completed",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["updated"] == []
    assert body["skipped_pending"] == [lifecycle["ps_pending"]]
    assert body["skipped_no_data"] == [lifecycle["ps_no_data"]]
    states = {
        r["prep_sample_idx"]: r["state"]
        for r in await postgres_pool.fetch(
            "SELECT prep_sample_idx, state FROM qiita.assembly_sample WHERE processing_idx = $1",
            lifecycle["processing_idx"],
        )
    }
    assert states[lifecycle["ps_pending"]] == "pending"
    assert states[lifecycle["ps_no_data"]] == "no_data"


async def test_invalidating_requires_a_reason(client, lifecycle):
    resp = await client.patch(
        URL_PROCESSING_SAMPLE_STATUS.format(processing_idx=lifecycle["processing_idx"]),
        json={"prep_sample_idx": [lifecycle["ps_done"]], "state": "invalidated"},
    )
    assert resp.status_code == 422, resp.text


async def test_sample_status_on_an_unknown_run_404s(client, lifecycle):
    resp = await client.patch(
        URL_PROCESSING_SAMPLE_STATUS.format(processing_idx=99_999_999),
        json={
            "prep_sample_idx": [lifecycle["ps_done"]],
            "state": "invalidated",
            "reason": "x",
        },
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# The read surface both markers stay visible in
# ---------------------------------------------------------------------------


async def test_the_list_tally_counts_each_state_separately(client, lifecycle):
    """An invalidated run is neither usable nor still coming, and a no_data one
    finished having produced nothing — folding either into `samples_pending` would
    read as "wait for it"."""
    params = {"prep_sample_idx": lifecycle["ps_done"]}
    row = next(
        p
        for p in (await client.get(URL_PROCESSING_PREFIX, params=params)).json()["processing"]
        if p["processing_idx"] == lifecycle["processing_idx"]
    )
    assert (row["samples_completed"], row["samples_invalidated"]) == (1, 0)

    await _invalidate(client, lifecycle, [lifecycle["ps_done"]])
    row = next(
        p
        for p in (await client.get(URL_PROCESSING_PREFIX, params=params)).json()["processing"]
        if p["processing_idx"] == lifecycle["processing_idx"]
    )
    assert (row["samples_completed"], row["samples_invalidated"]) == (0, 1)


async def test_the_unfiltered_tally_separates_all_four_states(client, lifecycle):
    """The fixture holds one sample in each of three states; the fourth bucket is
    what the withdrawal moves one of them into."""
    await _invalidate(client, lifecycle, [lifecycle["ps_done"]])
    row = next(
        p
        for p in (await client.get(URL_PROCESSING_PREFIX)).json()["processing"]
        if p["processing_idx"] == lifecycle["processing_idx"]
    )
    assert row["samples_completed"] == 0
    assert row["samples_pending"] == 1
    assert row["samples_no_data"] == 1
    assert row["samples_invalidated"] == 1


async def test_the_roster_reports_every_gated_sample_and_its_state(client, lifecycle):
    resp = await client.get(
        URL_PROCESSING_PREP_SAMPLE.format(processing_idx=lifecycle["processing_idx"])
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["processing_idx"] == lifecycle["processing_idx"]
    states = {s["prep_sample_idx"]: s["assembly_state"] for s in body["samples"]}
    assert states == {
        lifecycle["ps_done"]: "completed",
        lifecycle["ps_pending"]: "pending",
        lifecycle["ps_no_data"]: "no_data",
    }

    await _invalidate(client, lifecycle, [lifecycle["ps_done"]])
    after = await client.get(
        URL_PROCESSING_PREP_SAMPLE.format(processing_idx=lifecycle["processing_idx"])
    )
    states = {s["prep_sample_idx"]: s["assembly_state"] for s in after.json()["samples"]}
    assert states[lifecycle["ps_done"]] == "invalidated"


async def test_a_plain_user_sees_only_the_runs_over_samples_they_may_see(
    postgres_pool, client, lifecycle, regular_user_session
):
    """The reads sit at `prep_sample:read`, which every human role holds, so what
    keeps one user out of another's runs is the per-study narrowing rather than
    the role. A `user` with no tier on the sample's study sees neither the run in
    the list nor the sample in the roster — and a zero-tally row does not leak the
    run either, since a narrowed list returns only runs with a matching sample.

    The by-idx read is deliberately NOT narrowed: it is one params blob with no
    sample in it. Asserted here so that stays a decision rather than an oversight.
    """
    from qiita_control_plane.main import app

    # Link the completed sample to a study the plain user has no access to. Without
    # a study link the predicate is vacuous (every study-less prep_sample is
    # visible to everyone), so this link is what makes the test discriminate.
    study_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        lifecycle["principal_idx"],
        f"assembly-lifecycle-{secrets.token_hex(4)}",
    )
    await seed_biosample_to_study_link(
        postgres_pool,
        biosample_idx=lifecycle["bs_done"],
        study_idx=study_idx,
        created_by_idx=lifecycle["principal_idx"],
    )
    await seed_prep_sample_to_study_link(
        postgres_pool,
        prep_sample_idx=lifecycle["ps_done"],
        study_idx=study_idx,
        created_by_idx=lifecycle["principal_idx"],
    )
    app.state.pool = postgres_pool
    _install_settings(app)
    try:
        # Control: the admin, who bypasses the narrowing, still sees both.
        admin_list = await client.get(
            URL_PROCESSING_PREFIX, params={"prep_sample_idx": lifecycle["ps_done"]}
        )
        assert lifecycle["processing_idx"] in {
            p["processing_idx"] for p in admin_list.json()["processing"]
        }

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {regular_user_session['token']}"},
        ) as ac:
            listed = await ac.get(
                URL_PROCESSING_PREFIX, params={"prep_sample_idx": lifecycle["ps_done"]}
            )
            roster = await ac.get(
                URL_PROCESSING_PREP_SAMPLE.format(processing_idx=lifecycle["processing_idx"])
            )
            by_idx = await ac.get(
                URL_PROCESSING_BY_IDX.format(processing_idx=lifecycle["processing_idx"])
            )
        assert listed.status_code == 200, listed.text
        assert lifecycle["processing_idx"] not in {
            p["processing_idx"] for p in listed.json()["processing"]
        }
        assert roster.status_code == 200, roster.text
        assert lifecycle["ps_done"] not in {s["prep_sample_idx"] for s in roster.json()["samples"]}
        assert by_idx.status_code == 200, by_idx.text
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx
        )
        await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


async def test_superseded_by_round_trips_and_a_re_deprecate_replaces_it(
    postgres_pool, client, lifecycle
):
    """The happy path, and what a second PATCH does to it.

    Without this, every assertion about `superseded_by` is a 422 — the column
    could be dropped from the UPDATE entirely and the refusal tests would still
    pass. The second half pins the replace-whole-block behaviour the route
    documents: correcting a reason without re-supplying the replacement clears it,
    which is the shape a caller has to know about.
    """
    replacement = await postgres_pool.fetchval(
        "INSERT INTO qiita.processing (params_hash, workflow, version, params)"
        " VALUES (sha256($1::bytea), 'long-read-assembly', '1.0.0', '{}'::jsonb)"
        " RETURNING processing_idx",
        secrets.token_bytes(16),
    )
    url = URL_PROCESSING_STATUS.format(processing_idx=lifecycle["processing_idx"])
    try:
        first = await client.patch(
            url,
            json={"status": "deprecated", "reason": "re-minted", "superseded_by": replacement},
        )
        assert first.status_code == 200, first.text
        assert first.json()["superseded_by"] == replacement
        assert (
            await postgres_pool.fetchval(
                "SELECT superseded_by FROM qiita.processing WHERE processing_idx = $1",
                lifecycle["processing_idx"],
            )
            == replacement
        )

        second = await client.patch(url, json={"status": "deprecated", "reason": "typo fixed"})
        assert second.status_code == 200, second.text
        assert second.json()["superseded_by"] is None
        assert second.json()["deprecation_reason"] == "typo fixed"
    finally:
        await postgres_pool.execute(
            "UPDATE qiita.processing SET superseded_by = NULL WHERE processing_idx = $1",
            lifecycle["processing_idx"],
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.processing WHERE processing_idx = $1", replacement
        )


async def test_the_sequenced_pool_filter_narrows_both_reads(postgres_pool, client, lifecycle):
    """The one `sample_scope_sql` arm carrying its own subquery bind, and the only
    one no test reached. Its `$N` differs per caller because the list and the
    roster seed `args` to different depths, so a numbering slip shows up here and
    nowhere else.

    Both directions are asserted: the pool the sample is on returns it, a pool it
    is not on excludes it. Without the negative half a filter the SQL ignored
    entirely would pass.
    """
    from qiita_control_plane.testing.db_seeds import seed_sequenced_sample_subtype

    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=lifecycle["ps_done"],
        owner_idx=lifecycle["principal_idx"],
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    other_run, other_pool, other_ss = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=lifecycle["ps_pending"],
        owner_idx=lifecycle["principal_idx"],
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    try:
        listed = await client.get(URL_PROCESSING_PREFIX, params={"sequenced_pool_idx": pool_idx})
        assert listed.status_code == 200, listed.text
        row = next(
            p
            for p in listed.json()["processing"]
            if p["processing_idx"] == lifecycle["processing_idx"]
        )
        # The tally is scoped to the filter, so only the pool's one sample counts.
        assert (row["samples_completed"], row["samples_pending"]) == (1, 0)

        roster = await client.get(
            URL_PROCESSING_PREP_SAMPLE.format(processing_idx=lifecycle["processing_idx"]),
            params={"sequenced_pool_idx": pool_idx},
        )
        assert roster.status_code == 200, roster.text
        assert {s["prep_sample_idx"] for s in roster.json()["samples"]} == {lifecycle["ps_done"]}

        # The other pool holds the 'pending' sample, so the same run comes back
        # with a different tally and a different roster — the filter is read, not
        # ignored.
        other_roster = await client.get(
            URL_PROCESSING_PREP_SAMPLE.format(processing_idx=lifecycle["processing_idx"]),
            params={"sequenced_pool_idx": other_pool},
        )
        assert {s["prep_sample_idx"] for s in other_roster.json()["samples"]} == {
            lifecycle["ps_pending"]
        }
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.sequenced_sample WHERE idx = ANY($1::bigint[])",
            [ss_idx, other_ss],
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.sequenced_pool WHERE idx = ANY($1::bigint[])",
            [pool_idx, other_pool],
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.sequencing_run WHERE idx = ANY($1::bigint[])",
            [run_idx, other_run],
        )


async def test_the_roster_on_an_unknown_run_404s(client):
    resp = await client.get(URL_PROCESSING_PREP_SAMPLE.format(processing_idx=99_999_999))
    assert resp.status_code == 404


async def test_get_by_idx_on_an_unknown_run_404s(client):
    resp = await client.get(URL_PROCESSING_BY_IDX.format(processing_idx=99_999_999))
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Run lifecycle: refuse to CONSUME
# ---------------------------------------------------------------------------


async def test_an_invalidated_run_is_refused_as_an_alignment_subject(
    postgres_pool, client, lifecycle
):
    """The de novo align resolver is the gate's only consumer today. It admits
    'completed' alone, so the withdrawal is refused by construction — this pins
    that the message names the invalidation rather than falling into the
    "not complete yet" catch-all, which would send the operator looking for a
    stalled run."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    from qiita_control_plane.runner._alignment import _require_assembly_subject

    kwargs = dict(processing_idx=lifecycle["processing_idx"], prep_sample_idx=lifecycle["ps_done"])
    # Control: while the run reads 'completed' the resolver returns, so the
    # refusal below is attributable to the state.
    assert await _require_assembly_subject(postgres_pool, **kwargs) is None

    await _invalidate(client, lifecycle, [lifecycle["ps_done"]])
    with pytest.raises(BackendFailure) as ei:
        await _require_assembly_subject(postgres_pool, **kwargs)
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert "was invalidated" in ei.value.reason


async def test_a_run_completing_over_a_withdrawal_fails_as_bad_input(
    postgres_pool, client, lifecycle, tmp_path
):
    """A redrive that lands while the pair is withdrawn does not overturn it.

    The gate write refuses, and the dispatch arm types the refusal: without that
    it reaches run_workflow's catch-all as UNKNOWN_PERMANENT, which tells the
    operator to look for a bug when what happened is a decision someone made.
    """
    from qiita_common.actions import PROCESSING_IDX_BINDING, WorkflowAction
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import ScopeTargetKind, WorkTicketFailureStage

    from qiita_control_plane.runner._reconstruct import _run_action_primitive

    entry = WorkflowAction(
        kind="action",
        name=LibraryPrimitive.FINALIZE_ASSEMBLY_SAMPLE,
        inputs=[],
        outputs=[],
    )

    async def _finalize():
        return await _run_action_primitive(
            postgres_pool,
            entry,
            {PROCESSING_IDX_BINDING: lifecycle["processing_idx"]},
            tmp_path,
            {
                "kind": ScopeTargetKind.PREP_SAMPLE.value,
                "prep_sample_idx": lifecycle["ps_done"],
            },
            work_ticket_idx=1,  # this arm ignores it; required by the signature
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://unused:50051",
        )

    # Control: while the pair reads 'completed' the terminal step re-affirms it,
    # so the failure below is attributable to the withdrawal.
    assert await _finalize() == {}

    await _invalidate(client, lifecycle, [lifecycle["ps_done"]])
    with pytest.raises(BackendFailure) as ei:
        await _finalize()
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert ei.value.stage is WorkTicketFailureStage.STEP_RUN
    assert "was invalidated" in ei.value.reason
    # The withdrawal stands; the redrive did not re-complete it.
    assert (
        await postgres_pool.fetchval(
            "SELECT state FROM qiita.assembly_sample"
            " WHERE processing_idx = $1 AND prep_sample_idx = $2",
            lifecycle["processing_idx"],
            lifecycle["ps_done"],
        )
        == "invalidated"
    )


async def test_a_deprecated_run_still_signs_an_assembly_doget_ticket(
    postgres_pool, client, lifecycle, compute_worker_service_account
):
    """The DoGet route answers "did this run assemble at all",
    and a deprecated run stays answerable so provenance for published data
    survives. Deprecation stops the MINT, not the read."""
    from qiita_common.api_paths import URL_ASSEMBLY_DOGET
    from qiita_common.assembly_constants import ASSEMBLED_SEQUENCE_TABLE

    from qiita_control_plane.main import app
    from qiita_control_plane.testing.db_seeds import seed_bare_feature

    feature_idx = await seed_bare_feature(postgres_pool)
    await postgres_pool.execute(
        "INSERT INTO qiita.assembly_membership"
        " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)"
        " VALUES ($1, $2, 'MAG', 'bin.1', $3)",
        lifecycle["ps_done"],
        lifecycle["processing_idx"],
        feature_idx,
    )
    app.state.pool = postgres_pool
    _install_settings(app)
    body = {
        "prep_sample_idx": lifecycle["ps_done"],
        "processing_idx": lifecycle["processing_idx"],
        "table": ASSEMBLED_SEQUENCE_TABLE,
    }
    try:
        # The route is service-account-only (`ticket:doget`), so it is driven with
        # the worker token rather than the admin client the PATCH uses.
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
        ) as ac:
            # Control: the ticket signs while the run is active, so a 201 after the
            # deprecation is attributable to the status being ignored here.
            before = await ac.post(URL_ASSEMBLY_DOGET, json=body)
            assert before.status_code == 201, before.text

            await _deprecate(client, lifecycle["processing_idx"])
            after = await ac.post(URL_ASSEMBLY_DOGET, json=body)
        assert after.status_code == 201, after.text
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.assembly_membership WHERE processing_idx = $1",
            lifecycle["processing_idx"],
        )
        await postgres_pool.execute("DELETE FROM qiita.feature WHERE feature_idx = $1", feature_idx)


async def test_deprecation_does_not_touch_its_runs(client, postgres_pool, lifecycle):
    """The two markers are independent: voiding a config says nothing about the
    runs already made under it, which is the whole reason both exist."""
    await _deprecate(client, lifecycle["processing_idx"])
    states = {
        r["prep_sample_idx"]: r["state"]
        for r in await postgres_pool.fetch(
            "SELECT prep_sample_idx, state FROM qiita.assembly_sample WHERE processing_idx = $1",
            lifecycle["processing_idx"],
        )
    }
    assert states == {
        lifecycle["ps_done"]: "completed",
        lifecycle["ps_pending"]: "pending",
        lifecycle["ps_no_data"]: "no_data",
    }


async def test_lifecycle_patches_require_the_lifecycle_scope(
    postgres_pool, wet_lab_admin_session, lifecycle
):
    """wet_lab_admin holds prep_sample:write but not processing:lifecycle —
    deciding that published contigs came from a run we no longer stand behind is a
    system_admin judgement."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
    ) as ac:
        status_resp = await ac.patch(
            URL_PROCESSING_STATUS.format(processing_idx=lifecycle["processing_idx"]),
            json={"status": "deprecated", "reason": "x"},
        )
        sample_resp = await ac.patch(
            URL_PROCESSING_SAMPLE_STATUS.format(processing_idx=lifecycle["processing_idx"]),
            json={
                "prep_sample_idx": [lifecycle["ps_done"]],
                "state": "invalidated",
                "reason": "x",
            },
        )
        # The reads are open to every human role, so the 403s above are about the
        # lifecycle scope and not about wet_lab_admin being shut out of /processing.
        read_resp = await ac.get(
            URL_PROCESSING_BY_IDX.format(processing_idx=lifecycle["processing_idx"])
        )
    assert status_resp.status_code == 403
    assert sample_resp.status_code == 403
    assert read_resp.status_code == 200
    assert json.loads(status_resp.text)["detail"]
