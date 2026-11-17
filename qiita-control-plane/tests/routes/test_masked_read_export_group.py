"""DB tests for the admin masked-read export across a mask group:

  * the roster resolves each pool sample across the named mask's group
    (`definition_resolver`): processed under which member, incomplete, or not
    processed, with the reason;
  * a deprecated mask named directly is refused, by the roster and by the ticket
    route.

Auth, the roster's base shape and the ticket's signed contents are
test_admin_masked_read_export.py.
"""

import json
import secrets

import pytest
import pytest_asyncio
from qiita_common.api_paths import (
    URL_ADMIN_MASKED_READ_EXPORT_TICKET,
    URL_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
)
from qiita_common.models import (
    JOINING_VERDICTS,
    DefinitionGroupRefusal,
    DefinitionKind,
    MaskDefinitionStatus,
    SampleResolutionReason,
    SampleResolutionState,
    WorkTicketState,
)

from qiita_control_plane.definition_similarity import decide_equivalence, open_equivalence
from qiita_control_plane.main import app
from qiita_control_plane.repositories.block import MASK_SAMPLE_COMPLETED
from qiita_control_plane.repositories.mask_definition import transition_mask_definition_status
from qiita_control_plane.testing.db_seeds import seed_biosample_with_sequenced_prep_sample
from qiita_control_plane.testing.db_teardown import delete_output_equivalences

from . import test_mask_definition_read as mask_read
from .conftest import role_keyed_clients  # noqa: F401
from .test_mask_definition_read import (
    _seed_gate_row,
    _seed_mask,
    _seed_mask_ticket,
    _seed_sample_on_pool,
)

pytestmark = pytest.mark.db

ctx = mask_read.ctx


@pytest.fixture(autouse=True)
def _signing_key(ctx):
    """A known signing key on app.state, so the ticket route's
    get_flight_signing_key resolves."""
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )


def _roster_url(sequenced_pool_idx: int) -> str:
    return URL_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT.format(sequenced_pool_idx=sequenced_pool_idx)


async def _add_to_pool(ctx, *, pool_idx: int) -> int:
    """A prep_sample with a biosample accession, on `pool_idx`."""
    pool = ctx["pool"]
    owner = ctx["admin_session"]["principal_idx"]
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=owner
    )
    ctx["created"]["biosample"].append(biosample_idx)
    ctx["created"]["prep_sample"].append(prep_sample_idx)
    await pool.execute(
        "UPDATE qiita.biosample SET biosample_accession = $1 WHERE idx = $2",
        f"SAMN{secrets.token_hex(4)}",
        biosample_idx,
    )
    await pool.execute(
        "INSERT INTO qiita.sequenced_sample"
        "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
        " VALUES ($1, $2, $3, $4)",
        prep_sample_idx,
        pool_idx,
        f"item-{secrets.token_hex(4)}",
        owner,
    )
    return prep_sample_idx


@pytest_asyncio.fixture(params=sorted(JOINING_VERDICTS), ids=str)
async def group(ctx, request):
    """Masks `low` < `high`, decided with a joining verdict (each test runs once per
    verdict in `JOINING_VERDICTS`, which behave identically), and one pool of four
    samples:

    - `on_low`: completed under `low` only → processed under `low`;
    - `on_both`: completed under both → processed under `high` (the newest);
    - `running`: completed under `low`, a per-sample ticket running under `high`
      → incomplete (in_progress) under `high`;
    - `none`: no run under either → not processed (no_run).
    """
    pool = ctx["pool"]
    owner = ctx["admin_session"]["principal_idx"]
    low = await _seed_mask(ctx, params=json.dumps({"t": f"low-{secrets.token_hex(4)}"}))
    high = await _seed_mask(ctx, params=json.dumps({"t": f"high-{secrets.token_hex(4)}"}))
    assert low < high
    async with pool.acquire() as conn, conn.transaction():
        equivalence, _ = await open_equivalence(
            conn, kind=DefinitionKind.MASK, definition_idxs=[low, high], principal_idx=owner
        )
        await decide_equivalence(
            conn,
            output_equivalence_idx=equivalence["output_equivalence_idx"],
            verdict=request.param,
            reason="same filter, checked against a mock community",
            principal_idx=owner,
        )

    first, pool_idx = await _seed_sample_on_pool(ctx, owner_idx=owner)
    await pool.execute(
        "UPDATE qiita.biosample SET biosample_accession = $1"
        " WHERE idx = (SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $2)",
        f"SAMN{secrets.token_hex(4)}",
        first,
    )
    on_both = await _add_to_pool(ctx, pool_idx=pool_idx)
    running = await _add_to_pool(ctx, pool_idx=pool_idx)
    none = await _add_to_pool(ctx, pool_idx=pool_idx)
    for ps in (first, on_both, running):
        await _seed_gate_row(ctx, mask_idx=low, prep_sample_idx=ps, state=MASK_SAMPLE_COMPLETED)
    await _seed_gate_row(ctx, mask_idx=high, prep_sample_idx=on_both, state=MASK_SAMPLE_COMPLETED)
    ticket = await _seed_mask_ticket(
        ctx, mask_idx=high, prep_sample_idx=running, state=WorkTicketState.PROCESSING
    )

    yield {
        "low": low,
        "high": high,
        "pool_idx": pool_idx,
        "on_low": first,
        "on_both": on_both,
        "running": running,
        "none": none,
        "ticket": ticket,
    }

    await delete_output_equivalences(pool, [equivalence["output_equivalence_idx"]])


@pytest.mark.parametrize("anchor", ["low", "high"])
async def test_roster_resolves_each_sample_across_the_group(ctx, group, anchor):
    """Either member names the whole group, and gives the same resolution."""
    mask_idx = group[anchor]
    resp = await ctx["admin"].get(_roster_url(group["pool_idx"]), params={"mask_idx": mask_idx})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mask_idx"] == mask_idx
    assert body["group"] == [group["low"], group["high"]]
    by_prep = {s["prep_sample_idx"]: s for s in body["samples"]}
    assert set(by_prep) == {group["on_low"], group["on_both"], group["running"], group["none"]}

    def resolved(ps):
        r = by_prep[group[ps]]["resolution"]
        return (r["state"], r["reason"], r["definition_idx"], r["work_ticket_idx"])

    state, reason = SampleResolutionState, SampleResolutionReason
    assert resolved("on_low") == (state.PROCESSED, None, group["low"], None)
    assert resolved("on_both") == (state.PROCESSED, None, group["high"], None)
    assert resolved("running") == (
        state.INCOMPLETE,
        reason.IN_PROGRESS,
        group["high"],
        group["ticket"],
    )
    assert resolved("none") == (state.NOT_PROCESSED, reason.NO_RUN, None, None)
    # mask_state stays the gate under the mask the request named.
    expected_gate = {
        "low": dict.fromkeys(("on_low", "on_both", "running"), MASK_SAMPLE_COMPLETED),
        "high": {"on_both": MASK_SAMPLE_COMPLETED},
    }[anchor]
    for name in ("on_low", "on_both", "running", "none"):
        assert by_prep[group[name]]["mask_state"] == expected_gate.get(name), name


async def test_roster_and_ticket_record_and_take_the_decision_set(ctx, group):
    """The roster and the ticket each record the decisions they resolved under, and
    both refuse a token that names no decision set."""
    roster = await ctx["admin"].get(
        _roster_url(group["pool_idx"]), params={"mask_idx": group["low"]}
    )
    assert roster.status_code == 200, roster.text
    decisions = roster.json()["decisions"]
    assert (decisions["as_of"], decisions["equivalences_used"]) == (False, 1)

    ticket = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET,
        json={
            "prep_sample_idx": group["on_both"],
            "mask_idx": group["high"],
            "as_of_event": decisions["decision_set"],
        },
    )
    assert ticket.status_code == 201, ticket.text
    assert ticket.json()["decisions"] == {**decisions, "as_of": True}

    unknown = "sha256:" + "f" * 64
    assert (
        await ctx["admin"].get(
            _roster_url(group["pool_idx"]),
            params={"mask_idx": group["low"], "as_of_event": unknown},
        )
    ).status_code == 422
    assert (
        await ctx["admin"].post(
            URL_ADMIN_MASKED_READ_EXPORT_TICKET,
            json={
                "prep_sample_idx": group["on_both"],
                "mask_idx": group["high"],
                "as_of_event": unknown,
            },
        )
    ).status_code == 422


async def _deprecated_mask(ctx) -> int:
    mask_idx = await _seed_mask(ctx, params=json.dumps({"t": f"dep-{secrets.token_hex(4)}"}))
    await transition_mask_definition_status(
        ctx["pool"],
        mask_idx=mask_idx,
        status=MaskDefinitionStatus.DEPRECATED,
        reason="filter found to keep host reads",
        superseded_by=None,
        principal_idx=ctx["admin_session"]["principal_idx"],
    )
    return mask_idx


def _refused_as_deprecated(resp) -> None:
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert (detail["reason"], detail["deprecation_reason"]) == (
        "deprecated",
        "filter found to keep host reads",
    )
    assert "--include-deprecated" in detail["detail"]


async def _withdrawn_gate(ctx, *, mask_idx: int, prep_sample_idx: int) -> None:
    await ctx["pool"].execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state, invalidated_at,"
        " invalidated_by_idx, invalidation_reason)"
        " VALUES ($1, $2, 'invalidated', now(), $3, 'withdrawn by a test')",
        mask_idx,
        prep_sample_idx,
        ctx["admin_session"]["principal_idx"],
    )


async def test_roster_refuses_a_deprecated_mask_without_the_flag(ctx, group):
    mask_idx = await _deprecated_mask(ctx)
    _refused_as_deprecated(
        await ctx["admin"].get(_roster_url(group["pool_idx"]), params={"mask_idx": mask_idx})
    )


async def test_roster_reads_a_deprecated_mask_exactly_under_the_flag(ctx, group):
    """Each prep_sample is resolved to the deprecated mask alone: completed there is
    processed, withdrawn there stays not processed, no run is no_run."""
    mask_idx = await _deprecated_mask(ctx)
    await _seed_gate_row(
        ctx, mask_idx=mask_idx, prep_sample_idx=group["on_low"], state=MASK_SAMPLE_COMPLETED
    )
    await _withdrawn_gate(ctx, mask_idx=mask_idx, prep_sample_idx=group["on_both"])
    resp = await ctx["admin"].get(
        _roster_url(group["pool_idx"]),
        params={"mask_idx": mask_idx, "include_deprecated": "true"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["deprecated"], body["deprecation_reason"], body["group"]) == (
        True,
        "filter found to keep host reads",
        [mask_idx],
    )
    by_sample = {s["prep_sample_idx"]: s["resolution"] for s in body["samples"]}
    assert (by_sample[group["on_low"]]["state"], by_sample[group["on_low"]]["definition_idx"]) == (
        "processed",
        mask_idx,
    )
    assert by_sample[group["on_both"]]["reason"] == "withdrawn"
    assert by_sample[group["none"]]["reason"] == "no_run"


async def test_roster_of_an_active_mask_with_the_flag_is_as_without(ctx, group):
    url = _roster_url(group["pool_idx"])
    plain = await ctx["admin"].get(url, params={"mask_idx": group["low"]})
    flagged = await ctx["admin"].get(
        url, params={"mask_idx": group["low"], "include_deprecated": "true"}
    )
    assert (plain.status_code, flagged.status_code) == (200, 200)
    assert plain.json() == flagged.json()
    assert flagged.json()["deprecated"] is False


async def test_ticket_refuses_a_deprecated_mask_without_the_flag(ctx, group):
    """A completed gate does not make a deprecated mask exportable without the flag."""
    mask_idx = await _deprecated_mask(ctx)
    ps = group["on_low"]
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=ps, state=MASK_SAMPLE_COMPLETED)
    _refused_as_deprecated(
        await ctx["admin"].post(
            URL_ADMIN_MASKED_READ_EXPORT_TICKET, json={"prep_sample_idx": ps, "mask_idx": mask_idx}
        )
    )


async def test_ticket_reads_a_deprecated_mask_exactly_under_the_flag(ctx, group):
    mask_idx = await _deprecated_mask(ctx)
    ps = group["on_low"]
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=ps, state=MASK_SAMPLE_COMPLETED)
    await _withdrawn_gate(ctx, mask_idx=mask_idx, prep_sample_idx=group["on_both"])
    ok = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET,
        json={"prep_sample_idx": ps, "mask_idx": mask_idx, "include_deprecated": True},
    )
    assert ok.status_code == 201, ok.text
    withdrawn = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET,
        json={
            "prep_sample_idx": group["on_both"],
            "mask_idx": mask_idx,
            "include_deprecated": True,
        },
    )
    assert withdrawn.status_code == 409, withdrawn.text


async def test_ticket_of_an_active_mask_with_the_flag_is_as_without(ctx, group):
    """`on_low` resolves to `low` and `on_both` to `high`: the flag changes neither."""
    for ps, mask, expected in (
        (group["on_low"], group["low"], 201),
        (group["on_both"], group["low"], 409),
    ):
        for flag in (False, True):
            resp = await ctx["admin"].post(
                URL_ADMIN_MASKED_READ_EXPORT_TICKET,
                json={"prep_sample_idx": ps, "mask_idx": mask, "include_deprecated": flag},
            )
            assert resp.status_code == expected, (ps, flag, resp.text)


async def test_ticket_under_each_resolved_member(ctx, group):
    """The ticket route keeps its exact-id completion gate: each processed sample
    gets a ticket under the member it resolved to, and not under one where it has
    no completed gate."""
    ok_high = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET,
        json={"prep_sample_idx": group["on_both"], "mask_idx": group["high"]},
    )
    refused = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET,
        json={"prep_sample_idx": group["on_low"], "mask_idx": group["high"]},
    )
    assert ok_high.status_code == 201, ok_high.text
    assert refused.status_code == 409, refused.text


@pytest.mark.parametrize(
    ("sample", "mask", "refusal"),
    [
        # Completed under `low`, but resolves to `high`, the newer member.
        ("on_both", "low", DefinitionGroupRefusal.RESOLVED_ELSEWHERE),
        # Completed under `low`, with a ticket still running under `high`.
        ("running", "low", DefinitionGroupRefusal.INCOMPLETE),
    ],
)
async def test_ticket_refuses_a_completed_gate_that_is_not_the_resolution(
    ctx, group, sample, mask, refusal
):
    """A completed gate under the named mask is not enough: the prep_sample must
    resolve to that mask in its group, as the cohort DoGet requires."""
    resp = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET,
        json={"prep_sample_idx": group[sample], "mask_idx": group[mask]},
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == refusal
    assert detail["resolution"]["prep_sample_idx"] == group[sample]
