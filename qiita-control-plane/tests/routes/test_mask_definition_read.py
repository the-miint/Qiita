"""DB tests for the mask-definition read surface.

Three GETs: the mask list with its per-mask sample tally, one mask's config, and
the per-mask sample roster. Two behaviours carry most of the coverage here:

* **State resolution.** Both masking paths write the `qiita.mask_sample` gate row,
  so a masked-complete sample always resolves from it. What the gate cannot show
  is a per-sample mask that has NOT completed — that path writes its row
  'completed' in one upsert at the terminal step, so a queued / processing /
  failed ticket leaves no row. The ticket arm supplies those, for both per-sample
  masking actions.
* **Per-study narrowing.** Below wet_lab_admin a caller sees only the samples it
  could submit against — Tier.ADMIN on every non-retired linked study — and the
  narrowing also decides which masks the list returns.
"""

import secrets
import uuid

import pytest
from qiita_common.actions import PER_SAMPLE_MASK_ACTION_IDS, READ_MASK_ACTION_ID
from qiita_common.api_paths import (
    URL_MASK_DEFINITION_BY_IDX,
    URL_MASK_DEFINITION_PREFIX,
    URL_MASK_DEFINITION_PREP_SAMPLE,
)
from qiita_common.models import ScopeTargetKind

from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

from .conftest import (  # noqa: F401
    _grant_study_access,
    _seed_study,
    delete_idxs,
    role_keyed_clients,
)

pytestmark = pytest.mark.db

_MASK_ACTION_VERSION = "1.0.0"


async def _seed_mask(ctx, *, workflow: str = "read-mask", params: str = "{}") -> int:
    """Insert a qiita.mask_definition row with a random params_hash; track it."""
    mask_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, $2, '1.0.0', $3::jsonb, $4)"
        " RETURNING mask_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,  # 32-byte params_hash
        workflow,
        params,
        ctx["admin_session"]["principal_idx"],
    )
    ctx["created"]["mask"].append(mask_idx)
    return mask_idx


async def _seed_sample_on_pool(ctx, *, owner_idx: int, study_idx: int | None = None):
    """Seed biosample -> prep_sample -> run/pool/sequenced_sample, optionally
    linking the prep_sample to a study. Returns (prep_sample_idx, pool_idx)."""
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        ctx["pool"], owner_idx=owner_idx
    )
    ctx["created"]["biosample"].append(biosample_idx)
    ctx["created"]["prep_sample"].append(prep_sample_idx)
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        ctx["pool"],
        prep_sample_idx=prep_sample_idx,
        owner_idx=owner_idx,
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    ctx["created"]["sequenced_sample"].append(ss_idx)
    ctx["created"]["sequenced_pool"].append(pool_idx)
    ctx["created"]["sequencing_run"].append(run_idx)
    if study_idx is not None:
        # prep_sample_to_study_reject_without_biosample_link requires the parent
        # biosample to be linked to the same study first.
        await seed_biosample_to_study_link(
            ctx["pool"],
            biosample_idx=biosample_idx,
            study_idx=study_idx,
            created_by_idx=owner_idx,
        )
        ctx["created"]["biosample_to_study"].append((biosample_idx, study_idx))
        await ctx["pool"].execute(
            "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
            " VALUES ($1, $2, $3)",
            prep_sample_idx,
            study_idx,
            owner_idx,
        )
        ctx["created"]["prep_sample_to_study"].append((prep_sample_idx, study_idx))
    return prep_sample_idx, pool_idx


async def _seed_gate_row(ctx, *, mask_idx: int, prep_sample_idx: int, state: str) -> None:
    """Write the block path's qiita.mask_sample gate row."""
    await ctx["pool"].execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state) VALUES ($1, $2, $3)",
        mask_idx,
        prep_sample_idx,
        state,
    )
    ctx["created"]["mask_sample"].append((mask_idx, prep_sample_idx))


async def _seed_mask_ticket(
    ctx,
    *,
    mask_idx: int,
    prep_sample_idx: int,
    state: str,
    action_id: str = READ_MASK_ACTION_ID,
) -> int:
    """Write a per-sample masking work_ticket carrying the mask. `action_id`
    selects which of the two per-sample masking actions produced it.

    A 'failed' ticket carries the whole failure surface; work_ticket_failure_consistent
    rejects a FAILED row with NULL failure_* columns. Stage 'submission' rather than
    'step_run' — the latter also requires a failure_step_name, which nothing here
    reads."""
    failed = state == "failed"
    ticket_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  prep_sample_idx, mask_idx, state, failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, $4::qiita.scope_target_kind, $5, $6,"
        "         $7::qiita.work_ticket_state, $8::qiita.failure_type,"
        "         $9::qiita.work_ticket_failure_stage, $10)"
        " RETURNING work_ticket_idx",
        action_id,
        _MASK_ACTION_VERSION,
        ctx["admin_session"]["principal_idx"],
        ScopeTargetKind.PREP_SAMPLE.value,
        prep_sample_idx,
        mask_idx,
        state,
        "permanent" if failed else None,
        "submission" if failed else None,
        "seeded failure" if failed else None,
    )
    ctx["created"]["work_ticket"].append(ticket_idx)
    return ticket_idx


@pytest.fixture
async def ctx(role_keyed_clients):  # noqa: F811
    """role_keyed_clients plus the per-table teardown ledger these tests fill.

    Seeds both real per-sample masking action rows (insert-if-absent) because a
    work_ticket FKs `(action_id, action_version)` and the roster's ticket arm keys
    on that id set."""
    pool = role_keyed_clients["pool"]
    actions_created = {
        action_id: await seed_action_if_absent(
            pool,
            action_id=action_id,
            version=_MASK_ACTION_VERSION,
            target_kind=ScopeTargetKind.PREP_SAMPLE.value,
        )
        for action_id in PER_SAMPLE_MASK_ACTION_IDS
    }
    role_keyed_clients["created"] = {
        "work_ticket": [],
        "mask_sample": [],
        "mask": [],
        "sequenced_sample": [],
        "sequenced_pool": [],
        "sequencing_run": [],
        "prep_sample_to_study": [],
        "biosample_to_study": [],
        "prep_sample": [],
        "biosample": [],
        "study_access": [],
        "study": [],
    }
    yield role_keyed_clients

    created = role_keyed_clients["created"]
    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
        created["work_ticket"],
    )
    for mask_idx, prep_sample_idx in created["mask_sample"]:
        await pool.execute(
            "DELETE FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
            mask_idx,
            prep_sample_idx,
        )
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])", created["mask"]
    )
    await delete_idxs(pool, "sequenced_sample", created["sequenced_sample"])
    await delete_idxs(pool, "sequenced_pool", created["sequenced_pool"])
    await delete_idxs(pool, "sequencing_run", created["sequencing_run"])
    for prep_sample_idx, study_idx in created["prep_sample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1 AND study_idx = $2",
            prep_sample_idx,
            study_idx,
        )
    await delete_idxs(pool, "prep_sample", created["prep_sample"])
    for biosample_idx, study_idx in created["biosample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
            biosample_idx,
            study_idx,
        )
    await delete_idxs(pool, "biosample", created["biosample"])
    for study_idx, principal_idx in created["study_access"]:
        await pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            study_idx,
            principal_idx,
        )
    await delete_idxs(pool, "study", created["study"])
    for action_id, was_created in actions_created.items():
        await delete_action_if_created(
            pool, action_id=action_id, version=_MASK_ACTION_VERSION, created=was_created
        )


def _mask_row(body: dict, mask_idx: int) -> dict | None:
    return next((m for m in body["masks"] if m["mask_idx"] == mask_idx), None)


# --------------------------------------------------------------------------- show


async def test_show_returns_the_config_blob(ctx):
    """GET /{mask_idx} returns `params` — the thing that says what the filter ran
    with, and what separates a human-filtered mask from a QC-only one."""
    mask_idx = await _seed_mask(ctx, params='{"host_rype_reference_idx": 42}')

    resp = await ctx["wet"].get(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mask_idx"] == mask_idx
    assert body["params"] == {"host_rype_reference_idx": 42}
    assert body["filter_workflow"] == "read-mask"


async def test_show_404s_on_absent_mask(ctx):
    resp = await ctx["wet"].get(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=2**40))
    assert resp.status_code == 404, resp.text


async def test_show_is_reachable_by_a_plain_user(ctx):
    """A plain `user` is in long-read-assembly's audience and needs a mask_idx to
    submit, so the read must not be admin-only."""
    mask_idx = await _seed_mask(ctx)
    resp = await ctx["user"].get(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx))
    assert resp.status_code == 200, resp.text


async def test_reads_require_prep_sample_read_scope(ctx, no_prep_sample_read_client):
    mask_idx = await _seed_mask(ctx)
    for url in (
        URL_MASK_DEFINITION_PREFIX,
        URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx),
        URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx),
    ):
        resp = await no_prep_sample_read_client.get(url)
        assert resp.status_code == 403, f"{url}: {resp.text}"


# --------------------------------------------------------------------------- roster


async def test_roster_reads_the_gate_row_for_a_masked_sample(ctx):
    """Both masking paths write the gate row, so a masked-complete sample always
    resolves from it."""
    mask_idx = await _seed_mask(ctx)
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"]
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed")

    resp = await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == 1
    row = body["samples"][0]
    assert row["prep_sample_idx"] == prep_sample_idx
    assert row["mask_state"] == "completed"
    assert row["source"] == "mask_sample"
    # The gate is a rollup, so no single ticket state describes it.
    assert row["work_ticket_state"] is None


@pytest.mark.parametrize("action_id", PER_SAMPLE_MASK_ACTION_IDS)
async def test_roster_names_an_in_flight_per_sample_ticket(ctx, action_id):
    """The state the gate table cannot represent: the per-sample path writes its
    gate row 'completed' in one upsert at the terminal step, so a ticket still
    running leaves no row — indistinguishable there from a sample nobody masked.
    Parameterized over both per-sample masking actions."""
    mask_idx = await _seed_mask(ctx)
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"]
    )
    await _seed_mask_ticket(
        ctx,
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        state="processing",
        action_id=action_id,
    )

    resp = await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))

    assert resp.status_code == 200, resp.text
    row = resp.json()["samples"][0]
    assert row["prep_sample_idx"] == prep_sample_idx
    assert row["mask_state"] == "pending"
    assert row["source"] == "work_ticket"
    assert row["work_ticket_state"] == "processing"


async def test_roster_reports_a_failed_ticket_as_pending_and_names_the_state(ctx):
    """A FAILED per-sample ticket wrote no gate row, so `mask_state` is 'pending';
    `work_ticket_state` is what separates it from a running one."""
    mask_idx = await _seed_mask(ctx)
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"]
    )
    await _seed_mask_ticket(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="failed")

    resp = await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))

    row = resp.json()["samples"][0]
    assert row["mask_state"] == "pending"
    assert row["work_ticket_state"] == "failed"


async def test_roster_collapses_retries_to_one_row(ctx):
    """A resubmitted sample has several tickets under one mask; the newest wins,
    so the sample reports once rather than once per attempt."""
    mask_idx = await _seed_mask(ctx)
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"]
    )
    await _seed_mask_ticket(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="failed")
    await _seed_mask_ticket(
        ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="processing"
    )

    body = (await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))).json()

    assert body["count"] == 1
    assert body["samples"][0]["work_ticket_state"] == "processing"


async def test_roster_prefers_the_gate_row_over_the_ticket(ctx):
    """A block-path gate row co-exists with per-sample tickets for the same pair;
    the gate is the reconciled answer, so the ticket arm's anti-join drops them.

    TWO tickets of differing state, not one: the anti-join runs inside
    `ticket_state` ahead of the DISTINCT ON, which is only equivalent to running
    it after because it correlates solely on the DISTINCT ON key. With a single
    ticket per group both placements agree trivially and pin nothing. With two, a
    correlation that varied within the group would strip the winning row and
    surface here.
    """
    mask_idx = await _seed_mask(ctx)
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"]
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="pending")
    await _seed_mask_ticket(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="failed")
    await _seed_mask_ticket(
        ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="processing"
    )

    body = (await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))).json()

    assert body["count"] == 1
    assert body["samples"][0]["source"] == "mask_sample"
    assert body["samples"][0]["mask_state"] == "pending"
    assert body["samples"][0]["work_ticket_state"] is None


async def test_a_sample_with_no_live_study_link_is_visible_to_any_caller(ctx):
    """The orphan case, pinned because it is a visibility decision rather than an
    accident: the per-study predicate admits a sample with no non-retired study
    link, matching the submission gate, which lets an orphaned sample through.

    As a gate that means "you already named the sample"; as a filter it means a
    study-less sample is visible to every caller. Changing it changes who sees
    what, so it should fail here rather than drift.
    """
    mask_idx = await _seed_mask(ctx)
    # _seed_sample_on_pool with study_idx=None links the sample to no study.
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"]
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed")
    assert (
        await ctx["pool"].fetchval(
            "SELECT count(*) FROM qiita.prep_sample_to_study"
            " WHERE prep_sample_idx = $1 AND retired = false",
            prep_sample_idx,
        )
        == 0
    )

    roster = (
        await ctx["user"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))
    ).json()

    assert [r["prep_sample_idx"] for r in roster["samples"]] == [prep_sample_idx]


async def test_retired_sample_drops_from_both_the_roster_and_the_tally(ctx):
    """The list invites the caller to fetch the roster, so the two must count the
    same set — a tally the roster can't produce is unexplainable."""
    mask_idx = await _seed_mask(ctx)
    owner_idx = ctx["admin_session"]["principal_idx"]
    kept_idx, pool_idx = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    retired_idx, _ = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    for prep_sample_idx in (kept_idx, retired_idx):
        await _seed_gate_row(
            ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed"
        )
    await ctx["pool"].execute(
        "UPDATE qiita.prep_sample SET retired = true, retired_at = now(), retired_by_idx = $2"
        " WHERE idx = $1",
        retired_idx,
        owner_idx,
    )

    roster = (
        await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))
    ).json()
    listed = (await ctx["wet"].get(URL_MASK_DEFINITION_PREFIX)).json()

    assert [r["prep_sample_idx"] for r in roster["samples"]] == [kept_idx]
    assert _mask_row(listed, mask_idx)["samples_completed"] == 1
    # The pool filter narrows both the same way.
    scoped = (
        await ctx["wet"].get(URL_MASK_DEFINITION_PREFIX, params={"sequenced_pool_idx": pool_idx})
    ).json()
    assert _mask_row(scoped, mask_idx)["samples_completed"] == 1


async def test_roster_404s_on_absent_mask_rather_than_returning_empty(ctx):
    """A typo'd mask_idx fails loudly instead of reading as 'no samples'."""
    resp = await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=2**40))
    assert resp.status_code == 404, resp.text


async def test_roster_filters_by_sequenced_pool(ctx):
    mask_idx = await _seed_mask(ctx)
    owner_idx = ctx["admin_session"]["principal_idx"]
    kept_idx, kept_pool = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    other_idx, _other_pool = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    for prep_sample_idx in (kept_idx, other_idx):
        await _seed_gate_row(
            ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed"
        )

    body = (
        await ctx["wet"].get(
            URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx),
            params={"sequenced_pool_idx": kept_pool},
        )
    ).json()

    assert [r["prep_sample_idx"] for r in body["samples"]] == [kept_idx]
    assert body["sequenced_pool_idx"] == kept_pool


# --------------------------------------------------------------------------- list


async def test_list_tallies_samples_by_state(ctx):
    """The tally is what tells an analyst which of a pool's masks is usable."""
    mask_idx = await _seed_mask(ctx)
    owner_idx = ctx["admin_session"]["principal_idx"]
    done_idx, pool_idx = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=done_idx, state="completed")
    # A second sample on the same pool, still mid-flight, via the other path.
    waiting_idx, _ = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    await _seed_mask_ticket(ctx, mask_idx=mask_idx, prep_sample_idx=waiting_idx, state="processing")

    body = (await ctx["wet"].get(URL_MASK_DEFINITION_PREFIX)).json()

    row = _mask_row(body, mask_idx)
    assert row is not None
    assert row["samples_completed"] == 1
    assert row["samples_pending"] == 1
    # The tally follows the filter it was called with.
    scoped = _mask_row(
        (
            await ctx["wet"].get(
                URL_MASK_DEFINITION_PREFIX, params={"sequenced_pool_idx": pool_idx}
            )
        ).json(),
        mask_idx,
    )
    assert scoped["samples_completed"] == 1
    assert scoped["samples_pending"] == 0


async def test_list_pool_filter_separates_the_masks_a_pool_carries(ctx):
    """A pool with a QC-only mask and a human-filtered one comes back with both,
    each tallied, and a mask on another pool does not."""
    owner_idx = ctx["admin_session"]["principal_idx"]
    qc_only = await _seed_mask(ctx, params="{}")
    human_filtered = await _seed_mask(ctx, params='{"host_rype_reference_idx": 42}')
    elsewhere = await _seed_mask(ctx)
    prep_sample_idx, pool_idx = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    for mask_idx in (qc_only, human_filtered):
        await _seed_gate_row(
            ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed"
        )
    off_pool_idx, _ = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    await _seed_gate_row(ctx, mask_idx=elsewhere, prep_sample_idx=off_pool_idx, state="completed")

    body = (
        await ctx["wet"].get(URL_MASK_DEFINITION_PREFIX, params={"sequenced_pool_idx": pool_idx})
    ).json()

    returned = {m["mask_idx"] for m in body["masks"]}
    assert {qc_only, human_filtered} <= returned
    assert elsewhere not in returned
    assert _mask_row(body, human_filtered)["params"] == {"host_rype_reference_idx": 42}


async def test_list_filters_by_prep_sample(ctx):
    owner_idx = ctx["admin_session"]["principal_idx"]
    mine = await _seed_mask(ctx)
    other = await _seed_mask(ctx)
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    other_idx, _ = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
    await _seed_gate_row(ctx, mask_idx=mine, prep_sample_idx=prep_sample_idx, state="completed")
    await _seed_gate_row(ctx, mask_idx=other, prep_sample_idx=other_idx, state="completed")

    body = (
        await ctx["wet"].get(
            URL_MASK_DEFINITION_PREFIX, params={"prep_sample_idx": prep_sample_idx}
        )
    ).json()

    returned = {m["mask_idx"] for m in body["masks"]}
    assert mine in returned
    assert other not in returned
    assert body["prep_sample_idx"] == prep_sample_idx


# --------------------------------------------------------------------------- narrowing


async def test_user_sees_a_mask_over_a_study_they_admin(ctx):
    """Study-admin on every non-retired link is the same policy the submission
    gate applies, so discovery matches what the user could submit."""
    mask_idx = await _seed_mask(ctx)
    user_idx = ctx["user_session"]["principal_idx"]
    study_idx = await _seed_study(ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="ok")
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=user_idx,
        tier="admin",
        granted_by_idx=ctx["admin_session"]["principal_idx"],
    )
    prep_sample_idx, pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], study_idx=study_idx
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed")

    listed = (
        await ctx["user"].get(URL_MASK_DEFINITION_PREFIX, params={"sequenced_pool_idx": pool_idx})
    ).json()
    roster = (
        await ctx["user"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))
    ).json()

    assert _mask_row(listed, mask_idx)["samples_completed"] == 1
    assert [r["prep_sample_idx"] for r in roster["samples"]] == [prep_sample_idx]


async def test_user_without_study_admin_sees_neither_the_sample_nor_the_mask(ctx):
    """Below Tier.ADMIN the sample drops out of the roster, and the mask drops out
    of the list rather than appearing with a zero tally."""
    mask_idx = await _seed_mask(ctx)
    admin_idx = ctx["admin_session"]["principal_idx"]
    study_idx = await _seed_study(ctx, owner_idx=admin_idx, suffix="noaccess")
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="member",
        granted_by_idx=admin_idx,
    )
    prep_sample_idx, pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=admin_idx, study_idx=study_idx
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed")

    listed = (
        await ctx["user"].get(URL_MASK_DEFINITION_PREFIX, params={"sequenced_pool_idx": pool_idx})
    ).json()
    roster = (
        await ctx["user"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))
    ).json()

    assert _mask_row(listed, mask_idx) is None
    assert roster["samples"] == []
    # wet_lab_admin bypasses the narrowing and still sees it.
    wet = (
        await ctx["wet"].get(URL_MASK_DEFINITION_PREFIX, params={"sequenced_pool_idx": pool_idx})
    ).json()
    assert _mask_row(wet, mask_idx) is not None


async def test_wet_lab_admin_sees_a_mask_with_no_samples_yet(ctx):
    """An unfiltered bypass-role list includes a minted-but-unused mask; the
    narrowed list does not, so a zero tally never leaks a mask the narrowing
    excluded."""
    mask_idx = await _seed_mask(ctx)

    wet = (await ctx["wet"].get(URL_MASK_DEFINITION_PREFIX)).json()
    user = (await ctx["user"].get(URL_MASK_DEFINITION_PREFIX)).json()

    assert _mask_row(wet, mask_idx) is not None
    assert _mask_row(user, mask_idx) is None


async def test_user_needs_admin_on_every_linked_study(ctx):
    """ "EVERY" is the load-bearing word: a sample linked to two studies is visible
    only when the caller admins both, matching the submission gate — otherwise
    discovery would offer a sample the submit would then 403."""
    mask_idx = await _seed_mask(ctx)
    admin_idx = ctx["admin_session"]["principal_idx"]
    user_idx = ctx["user_session"]["principal_idx"]
    granted = await _seed_study(ctx, owner_idx=admin_idx, suffix="granted")
    ungranted = await _seed_study(ctx, owner_idx=admin_idx, suffix="ungranted")
    await _grant_study_access(
        ctx,
        study_idx=granted,
        principal_idx=user_idx,
        tier="admin",
        granted_by_idx=admin_idx,
    )
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=admin_idx, study_idx=granted
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed")

    url = URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx)
    assert [r["prep_sample_idx"] for r in (await ctx["user"].get(url)).json()["samples"]] == [
        prep_sample_idx
    ]

    # Add a second link the caller has no access to; the sample drops out.
    await seed_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=ctx["created"]["biosample"][-1],
        study_idx=ungranted,
        created_by_idx=admin_idx,
    )
    ctx["created"]["biosample_to_study"].append((ctx["created"]["biosample"][-1], ungranted))
    await ctx["pool"].execute(
        "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        prep_sample_idx,
        ungranted,
        admin_idx,
    )
    ctx["created"]["prep_sample_to_study"].append((prep_sample_idx, ungranted))

    assert (await ctx["user"].get(url)).json()["samples"] == []


async def test_study_owner_sees_the_sample_without_a_study_access_row(ctx):
    """Owner bypass: the study's owner holds no study_access row, so a predicate
    that only consulted access_tier would hide their own sample."""
    mask_idx = await _seed_mask(ctx)
    user_idx = ctx["user_session"]["principal_idx"]
    study_idx = await _seed_study(ctx, owner_idx=user_idx, suffix="owned")
    prep_sample_idx, _pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=user_idx, study_idx=study_idx
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed")

    assert (
        await ctx["pool"].fetchval(
            "SELECT count(*) FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            study_idx,
            user_idx,
        )
        == 0
    )
    roster = (
        await ctx["user"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))
    ).json()

    assert [r["prep_sample_idx"] for r in roster["samples"]] == [prep_sample_idx]


async def test_roster_reports_truncation_at_the_cap(ctx, monkeypatch):
    """The reads over-fetch by one so a full page is distinguishable from a cut
    one. Cap of 1 keeps the seeding to two samples."""
    monkeypatch.setattr("qiita_control_plane.routes.read_masked._MASK_PREP_SAMPLE_HARD_CAP", 1)
    mask_idx = await _seed_mask(ctx)
    owner_idx = ctx["admin_session"]["principal_idx"]
    for _ in range(2):
        prep_sample_idx, _pool_idx = await _seed_sample_on_pool(ctx, owner_idx=owner_idx)
        await _seed_gate_row(
            ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed"
        )

    body = (await ctx["wet"].get(URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=mask_idx))).json()

    assert body["truncated"] is True
    assert body["count"] == 1


async def test_list_reports_truncation_at_the_cap(ctx, monkeypatch):
    monkeypatch.setattr("qiita_control_plane.routes.read_masked._MASK_LIST_HARD_CAP", 1)
    await _seed_mask(ctx)
    await _seed_mask(ctx)

    body = (await ctx["wet"].get(URL_MASK_DEFINITION_PREFIX)).json()

    assert body["truncated"] is True
    assert body["count"] == 1
