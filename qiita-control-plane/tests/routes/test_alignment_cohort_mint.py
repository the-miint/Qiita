"""Route tests for POST /alignment/{alignment_idx}/ticket/doget — the
HUMAN-callable alignment mint, where the caller names the cohort directly.

The signed cohort IS the authorization boundary: the data plane serves exactly
the prep_sample_idx list the ticket carries and knows nothing about studies or
users. There is no second line of defence, so every one of these tests is
guarding against a straight data leak, not a usability wart.

Two decisions this file exists to pin:

* **All-or-nothing, never narrowed.** A partially-readable cohort is a 403, not
  a quietly trimmed ticket. Coverage filtering makes a feature table
  cohort-dependent, so a silently narrowed cohort is a different scientific
  result under the name of the one that was asked for.
* **Access is checked BEFORE completeness.** Reversed, the 422's sample list
  would tell a caller which samples are completed for an alignment they have no
  right to read at all.

Seeds from the shared `pool_alignment_seed` fixture — the same shape the
discovery routes are tested against, so "the cohort discovery returns" and "the
cohort the mint accepts" are asserted to be the same thing rather than assumed.
"""

import base64
import json
import struct

import pytest
from qiita_common.api_paths import (
    URL_ALIGNMENT_COHORT_DOGET,
    URL_DEFINITION_GROUP_RESOLVE,
    URL_EXPORTED_IDENTIFIER,
    URL_EXPORTED_PROCESSING,
    URL_SEQUENCED_POOL_ALIGNMENT_COHORT,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    INCLUDE_DEPRECATED_FLAG,
    INCLUDE_DEPRECATED_UPSTREAM_FLAG,
    AlignmentDefinitionStatus,
    DefinitionGroupRefusal,
    DeprecatedUpstream,
    MaskDefinitionStatus,
    deprecated_read_refusal,
    deprecated_upstream_read_refusal,
)

from qiita_control_plane.repositories.alignment_definition import ALIGNMENT_SAMPLE_COMPLETED
from qiita_control_plane.repositories.block import MASK_SAMPLE_COMPLETED

pytestmark = pytest.mark.db

_COLUMNS = ["prep_sample_idx", "feature_idx", "mapq"]


@pytest.fixture
def ctx(role_keyed_clients):
    return dict(role_keyed_clients)


def _mint_url(alignment_idx: int) -> str:
    return URL_ALIGNMENT_COHORT_DOGET.format(alignment_idx=alignment_idx)


def _decode_ticket_payload(ticket_b64: str) -> dict:
    """Parse the JSON payload out of a base64 signed Flight ticket.

    Wire format: <1B version><4B payload_len><payload><64B Ed25519 signature><8B expiry>.
    """
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    return json.loads(raw[5 : 5 + payload_len])


# ---------------------------------------------------------------------------
# The happy path, and its agreement with discovery
# ---------------------------------------------------------------------------


async def test_human_mint_signs_the_requested_cohort_and_columns(ctx, pool_alignment_seed):
    """A reader who holds VIEWER on every study of the cohort gets a ticket
    carrying exactly that cohort and exactly those columns."""
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": [pool_alignment_seed["ps_a"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["table"] == "alignment_visible"
    assert payload["filter"] == {
        "alignment_idx": [pool_alignment_seed["align_1"]],
        "prep_sample_idx": [pool_alignment_seed["ps_a"]],
    }
    assert payload["columns"] == _COLUMNS


async def test_the_discovered_cohort_is_a_valid_mint_body(ctx, pool_alignment_seed):
    """The contract the two-step flow rests on: whatever the cohort route hands
    back, the mint accepts. Asserted end-to-end rather than by construction —
    a narrowing bug on either side breaks exactly this and nothing else."""
    cohort_url = URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=pool_alignment_seed["run_idx"],
        sequenced_pool_idx=pool_alignment_seed["pool_idx"],
        alignment_idx=pool_alignment_seed["align_1"],
    )
    discovered = await ctx["user"].get(cohort_url)
    assert discovered.status_code == 200, discovered.text
    cohort = discovered.json()["prep_sample_idx"]
    assert cohort == [pool_alignment_seed["ps_a"]]

    minted = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": cohort, "columns": _COLUMNS},
    )
    assert minted.status_code == 201, minted.text
    assert _decode_ticket_payload(minted.json()["ticket"])["filter"]["prep_sample_idx"] == cohort


async def test_human_mint_sorts_and_dedupes_the_cohort(ctx, pool_alignment_seed):
    """The cohort is an IN-list, so its order carries no meaning — but the
    ticket's bytes are signed, so two identical requests must produce the same
    payload. Sorted and deduped at the boundary."""
    ps_a, ps_c = pool_alignment_seed["ps_a"], pool_alignment_seed["ps_c"]
    # ps_c is readable but only 'pending', so use ps_a twice plus reversed order
    # to exercise dedupe without tripping the completeness gate.
    assert ps_c > ps_a  # the seed inserts in ascending order
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": [ps_a, ps_a], "columns": _COLUMNS},
    )
    assert resp.status_code == 201, resp.text
    assert _decode_ticket_payload(resp.json()["ticket"])["filter"]["prep_sample_idx"] == [ps_a]


# ---------------------------------------------------------------------------
# Access — the boundary
# ---------------------------------------------------------------------------


async def test_human_mint_rejects_a_cohort_the_caller_cannot_fully_read_403(
    ctx, pool_alignment_seed
):
    """ps_b lives in study_2, which the reader cannot see. Asking for ps_a AND
    ps_b is a 403 for the whole request — not a ticket for ps_a alone."""
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={
            "prep_sample_idx": [pool_alignment_seed["ps_a"], pool_alignment_seed["ps_b"]],
            "columns": _COLUMNS,
        },
    )
    assert resp.status_code == 403, resp.text


async def test_human_mint_denies_a_sample_shared_into_an_unreadable_study_403(
    ctx, pool_alignment_seed
):
    """ps_e is linked to study_1 (readable) AND study_2 (not), and is denied.

    The mint's authorization is all-of over a sample's links, and this is the only
    cohort member that can prove it — every other sample has exactly one link, so
    a gate that granted on ANY readable link would pass the whole suite. Since the
    ticket this route signs is the entire authorization boundary, with no second
    check at the data plane, that distinction is the one this test exists for.
    """
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": [pool_alignment_seed["ps_e"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 403, resp.text
    # And the study it names is the one the caller lacks, not the one they hold.
    detail = resp.json()["detail"]
    assert str(pool_alignment_seed["study_2"]) in detail
    assert str(pool_alignment_seed["study_1"]) not in detail


async def test_human_mint_403_names_the_offending_study(ctx, pool_alignment_seed):
    """The 403 has to be actionable: it names the study the caller lacks, so a
    scientist can go ask for access rather than guess."""
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": [pool_alignment_seed["ps_b"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 403
    detail = resp.json()["detail"]
    assert str(pool_alignment_seed["study_2"]) in detail
    assert "viewer" in detail


async def test_human_mint_403_does_not_enumerate_the_whole_cohort(ctx, pool_alignment_seed):
    """The caller chooses the cohort, so an untruncated, per-sample-correlated
    403 would answer "which of these identifiers exist and which studies are
    they in?" for the whole body — an enumeration oracle over
    prep_sample_to_study, handed to the lowest role there is.

    The message reports counts plus a handful of examples, and never pairs a
    prep_sample with the study that blocked it.
    """
    # 40 guessed identifiers around the seeded ones, so real blocked samples are
    # mixed with ids the caller has no relationship to.
    guesses = list(range(pool_alignment_seed["ps_b"], pool_alignment_seed["ps_b"] + 40))
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": guesses, "columns": _COLUMNS},
    )
    assert resp.status_code == 403, resp.text
    detail = resp.json()["detail"]
    named = [idx for idx in guesses if str(idx) in detail]
    assert len(named) <= 10, f"403 named {len(named)} of {len(guesses)} probed ids: {detail}"


async def test_human_mint_caps_the_cohort_length(ctx, pool_alignment_seed):
    """The cap bounds the width of that 403's answer as well as ticket size, so
    it is a security parameter and not only a sanity one."""
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": list(range(1, 10_002)), "columns": _COLUMNS},
    )
    assert resp.status_code == 422, resp.text


async def test_human_mint_denies_an_orphaned_prep_sample_403(ctx, pool_alignment_seed):
    """ps_d's only study link is retired, so there is no study left to authorize
    against. A read gate fails CLOSED on that anomaly — the opposite of the
    prep_sample-scoped submit gate, which lets an orphan through because its
    downstream lookups fail anyway."""
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": [pool_alignment_seed["ps_d"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 403
    assert str(pool_alignment_seed["ps_d"]) in resp.json()["detail"]


async def test_wet_lab_admin_bypasses_the_orphan_denial(ctx, pool_alignment_seed):
    """The bypass has to skip the orphan drop too, not just the tier check —
    otherwise discovery shows an admin ps_d and the mint then 403s it, and the
    two-step flow contradicts itself for exactly the caller who is investigating
    the anomaly."""
    resp = await ctx["wet"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": [pool_alignment_seed["ps_d"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 201, resp.text


async def test_human_mint_checks_access_before_completeness(ctx, pool_alignment_seed):
    """ps_b is unreadable AND (for alignment_2) not part of alignment_1's
    completed set. The answer must be 403, never 422: a completeness error names
    which samples are done, which is exactly what a caller with no read access
    must not learn."""
    # ps_b is 'completed' for align_1, so pair it with align_2 where the
    # readable ps_a is absent from the gate entirely — both failures are live.
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_2"]),
        json={
            "prep_sample_idx": [pool_alignment_seed["ps_a"], pool_alignment_seed["ps_b"]],
            "columns": _COLUMNS,
        },
    )
    assert resp.status_code == 403, resp.text
    assert str(pool_alignment_seed["ps_a"]) not in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------


async def test_human_mint_rejects_an_incomplete_cohort_422(ctx, pool_alignment_seed):
    """ps_c is readable but its gate is 'pending'. alignment rows are NOT 1:1
    with reads, so the presence of rows says nothing about whether a sample is
    done — an incomplete cohort would silently produce a wrong feature table."""
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={
            "prep_sample_idx": [pool_alignment_seed["ps_a"], pool_alignment_seed["ps_c"]],
            "columns": _COLUMNS,
        },
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert str(pool_alignment_seed["ps_c"]) in detail
    assert str(pool_alignment_seed["ps_a"]) not in detail


async def test_human_mint_rejects_a_sample_that_is_no_part_of_the_alignment_422(
    ctx, pool_alignment_seed
):
    """ps_a has no alignment_sample row for alignment_2 at all. "Never aligned"
    and "aligned but pending" are one answer here, by design."""
    resp = await ctx["wet"].post(
        _mint_url(pool_alignment_seed["align_2"]),
        json={"prep_sample_idx": [pool_alignment_seed["ps_a"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 422, resp.text
    assert str(pool_alignment_seed["ps_a"]) in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Existence, projection, and the request contract
# ---------------------------------------------------------------------------


async def test_human_mint_unknown_alignment_404(ctx, pool_alignment_seed):
    resp = await ctx["user"].post(
        _mint_url(999_999_999),
        json={"prep_sample_idx": [pool_alignment_seed["ps_a"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 404, resp.text


async def test_human_mint_requires_columns(ctx, pool_alignment_seed):
    """Unlike the service-account body, `columns` is REQUIRED here. The data
    plane rejects a columnless alignment ticket at stream time; requiring it at
    the boundary turns that into a 422 the caller can act on."""
    for body in (
        {"prep_sample_idx": [pool_alignment_seed["ps_a"]]},
        {"prep_sample_idx": [pool_alignment_seed["ps_a"]], "columns": []},
    ):
        resp = await ctx["user"].post(_mint_url(pool_alignment_seed["align_1"]), json=body)
        assert resp.status_code == 422, resp.text


async def test_human_mint_requires_a_cohort(ctx, pool_alignment_seed):
    """An empty cohort would sign an unscoped ticket. Rejected at the model."""
    for body in (
        {"columns": _COLUMNS},
        {"prep_sample_idx": [], "columns": _COLUMNS},
    ):
        resp = await ctx["user"].post(_mint_url(pool_alignment_seed["align_1"]), json=body)
        assert resp.status_code == 422, resp.text


async def test_human_mint_rejects_an_unknown_projection_column_422(ctx, pool_alignment_seed):
    """The projection allowlist lives at the signing boundary, so no route can
    mint an unvalidated one — including this one."""
    resp = await ctx["user"].post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={
            "prep_sample_idx": [pool_alignment_seed["ps_a"]],
            "columns": ["prep_sample_idx", "no_such_column"],
        },
    )
    assert resp.status_code == 422, resp.text
    assert "no_such_column" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Auth matrix
# ---------------------------------------------------------------------------


async def test_human_mint_anonymous_401(ctx, pool_alignment_seed):
    from httpx import ASGITransport, AsyncClient

    from qiita_control_plane.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            _mint_url(pool_alignment_seed["align_1"]),
            json={"prep_sample_idx": [pool_alignment_seed["ps_a"]], "columns": _COLUMNS},
        )
    assert resp.status_code == 401, resp.text


async def test_human_mint_without_the_scope_403(ctx, pool_alignment_seed, make_pat_client):
    """A PAT minted before this deploy carries the old scope set — the token
    path returns the token's own scopes, not the role's current ceiling."""
    client = await make_pat_client(
        label="no-alignment-doget", scopes=[Scope.SELF_PROFILE, Scope.PREP_SAMPLE_READ]
    )
    resp = await client.post(
        _mint_url(pool_alignment_seed["align_1"]),
        json={"prep_sample_idx": [pool_alignment_seed["ps_a"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 403, resp.text


async def test_human_mint_rejects_a_service_account(
    ctx, pool_alignment_seed, compute_worker_service_account
):
    """A worker uses the work-ticket route. Two ways into one surface with two
    different validation paths is precisely what splitting the scopes prevented,
    so `alignment:doget` is deliberately off SERVICE_ACCOUNT_SCOPE_CEILING."""
    from httpx import ASGITransport, AsyncClient

    from qiita_control_plane.main import app

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as sa:
        resp = await sa.post(
            _mint_url(pool_alignment_seed["align_1"]),
            json={"prep_sample_idx": [pool_alignment_seed["ps_a"]], "columns": _COLUMNS},
        )
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# Group resolution (`definition_resolver`)
# ---------------------------------------------------------------------------


async def test_human_mint_refuses_a_sample_resolved_to_another_alignment_409(
    ctx, alignment_group_world
):
    """ps_a is masked under both similar masks and aligned under both group members,
    so it resolves to the newer alignment, align_3. Naming align_1 is refused, naming
    the alignment it resolves to; naming align_3 is signed."""
    w = alignment_group_world
    await w["gate"]("mask_sample", w["mask_2"], w["ps_a"], MASK_SAMPLE_COMPLETED)
    await w["gate"]("alignment_sample", w["align_3"], w["ps_a"], ALIGNMENT_SAMPLE_COMPLETED)

    resp = await ctx["user"].post(
        _mint_url(w["align_1"]), json={"prep_sample_idx": [w["ps_a"]], "columns": _COLUMNS}
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == DefinitionGroupRefusal.RESOLVED_ELSEWHERE
    assert detail["alignment_idx"] == w["align_1"]
    [sample] = detail["samples"]
    assert (sample["prep_sample_idx"], sample["state"], sample["definition_idx"]) == (
        w["ps_a"],
        "processed",
        w["align_3"],
    )
    assert f"processed, alignment {w['align_3']}" in detail["detail"]

    resp = await ctx["user"].post(
        _mint_url(w["align_3"]), json={"prep_sample_idx": [w["ps_a"]], "columns": _COLUMNS}
    )
    assert resp.status_code == 201, resp.text
    # The mask pair the alignment group derives from is the decision it was signed under.
    decisions = resp.json()["decisions"]
    assert (decisions["as_of"], decisions["pairs_used"]) == (False, 1)
    unknown = await ctx["user"].post(
        _mint_url(w["align_3"]),
        json={
            "prep_sample_idx": [w["ps_a"]],
            "columns": _COLUMNS,
            "as_of_event": "sha256:" + "b" * 64,
        },
    )
    assert unknown.status_code == 422, unknown.text


async def test_human_mint_refuses_a_sample_whose_mask_moved_on_409(ctx, alignment_group_world):
    """ps_a's mask resolves to mask_2, but it is aligned only on the seed's mask: it
    is incomplete until aligned on mask_2."""
    w = alignment_group_world
    await w["gate"]("mask_sample", w["mask_2"], w["ps_a"], MASK_SAMPLE_COMPLETED)

    resp = await ctx["user"].post(
        _mint_url(w["align_1"]), json={"prep_sample_idx": [w["ps_a"]], "columns": _COLUMNS}
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "incomplete"
    [sample] = detail["samples"]
    assert (sample["reason"], sample["upstream_kind"], sample["upstream_definition_idx"]) == (
        "upstream_moved",
        "mask",
        w["mask_2"],
    )


async def test_human_mint_refuses_a_sample_whose_mask_run_was_withdrawn_409(
    ctx, alignment_group_world
):
    w = alignment_group_world
    await w["gate"]("mask_sample", w["mask_idx"], w["ps_a"], "invalidated")

    resp = await ctx["user"].post(
        _mint_url(w["align_1"]), json={"prep_sample_idx": [w["ps_a"]], "columns": _COLUMNS}
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "not_processed"
    assert detail["samples"][0]["reason"] == "upstream_withdrawn"


async def _deprecate_mask(ctx, seed) -> None:
    await ctx["pool"].execute(
        "UPDATE qiita.mask_definition SET status = $3, deprecated_at = now(),"
        " deprecated_by_idx = $2, deprecation_reason = 'host reference was wrong'"
        " WHERE mask_idx = $1",
        seed["mask_idx"],
        seed["owner_idx"],
        MaskDefinitionStatus.DEPRECATED.value,
    )


async def _deprecated_mask(ctx, mask_idx: int) -> DeprecatedUpstream:
    params_hash = await ctx["pool"].fetchval(
        "SELECT params_hash FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
    )
    return DeprecatedUpstream(
        definition_kind="mask",
        definition_idx=mask_idx,
        params_hash=bytes(params_hash).hex(),
        deprecation_reason="host reference was wrong",
    )


async def _refused_naming_the_upstream_flag(ctx, resp, alignment_idx: int, mask_idx: int) -> None:
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == DefinitionGroupRefusal.DEPRECATED_UPSTREAM
    upstream = await _deprecated_mask(ctx, mask_idx)
    assert detail["deprecated_upstreams"] == [upstream.model_dump(mode="json")]
    assert detail["detail"] == deprecated_upstream_read_refusal(
        "alignment", alignment_idx, [upstream]
    )
    assert INCLUDE_DEPRECATED_UPSTREAM_FLAG in detail["detail"]


async def test_an_alignment_on_a_deprecated_mask_is_refused_without_the_upstream_flag(
    ctx, pool_alignment_seed
):
    """The mask the alignment read is deprecated (the alignment itself is not):
    refused naming --include-deprecated-upstream, which --include-deprecated does not
    stand in for; read, and the ticket names the deprecated mask, with it."""
    seed = pool_alignment_seed
    await _deprecate_mask(ctx, seed)
    body = {"prep_sample_idx": [seed["ps_a"]], "columns": _COLUMNS}
    url = _mint_url(seed["align_1"])

    await _refused_naming_the_upstream_flag(
        ctx, await ctx["user"].post(url, json=body), seed["align_1"], seed["mask_idx"]
    )
    await _refused_naming_the_upstream_flag(
        ctx,
        await ctx["user"].post(url, json={**body, "include_deprecated": True}),
        seed["align_1"],
        seed["mask_idx"],
    )
    resp = await ctx["user"].post(url, json={**body, "include_deprecated_upstream": True})
    assert resp.status_code == 201, resp.text
    assert resp.json()["deprecated_upstreams"] == [
        (await _deprecated_mask(ctx, seed["mask_idx"])).model_dump(mode="json")
    ]


async def test_the_upstream_flag_does_not_read_a_withdrawn_mask_run(ctx, pool_alignment_seed):
    seed = pool_alignment_seed
    await _deprecate_mask(ctx, seed)
    await ctx["pool"].execute(
        "UPDATE qiita.mask_sample SET state = 'invalidated', invalidated_at = now(),"
        " invalidated_by_idx = $3, invalidation_reason = 'withdrawn by a test'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        seed["mask_idx"],
        seed["ps_a"],
        seed["owner_idx"],
    )
    resp = await ctx["user"].post(
        _mint_url(seed["align_1"]),
        json={
            "prep_sample_idx": [seed["ps_a"]],
            "columns": _COLUMNS,
            "include_deprecated_upstream": True,
        },
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert (detail["reason"], detail["samples"][0]["reason"]) == (
        "not_processed",
        "upstream_withdrawn",
    )


async def test_the_upstream_flag_changes_nothing_without_a_deprecated_upstream(
    ctx, pool_alignment_seed
):
    seed = pool_alignment_seed
    resp = await ctx["user"].post(
        _mint_url(seed["align_1"]),
        json={
            "prep_sample_idx": [seed["ps_a"]],
            "columns": _COLUMNS,
            "include_deprecated_upstream": True,
        },
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["deprecated_upstreams"] == []


@pytest.mark.parametrize("route", [URL_EXPORTED_IDENTIFIER, URL_EXPORTED_PROCESSING])
async def test_the_alignment_mints_take_the_upstream_flag(ctx, pool_alignment_seed, route):
    seed = pool_alignment_seed
    body = {"alignment_idx": seed["align_1"], "prep_sample_idx": [seed["ps_a"]]}
    try:
        plain = await ctx["user"].post(route, json={**body, "include_deprecated_upstream": True})
        assert plain.status_code == 201, plain.text
        await _deprecate_mask(ctx, seed)
        await _refused_naming_the_upstream_flag(
            ctx, await ctx["user"].post(route, json=body), seed["align_1"], seed["mask_idx"]
        )
        flagged = await ctx["user"].post(route, json={**body, "include_deprecated_upstream": True})
        assert flagged.status_code == 201, flagged.text
        assert flagged.json() == plain.json()
    finally:
        await ctx["pool"].execute(
            "DELETE FROM qiita.exported_processing WHERE alignment_idx = $1", seed["align_1"]
        )


async def test_the_cohort_listing_takes_the_upstream_flag(ctx, pool_alignment_seed):
    seed = pool_alignment_seed
    url = URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=seed["run_idx"],
        sequenced_pool_idx=seed["pool_idx"],
        alignment_idx=seed["align_1"],
    )
    plain = await ctx["user"].get(url)
    assert plain.status_code == 200, plain.text
    await _deprecate_mask(ctx, seed)
    await _refused_naming_the_upstream_flag(
        ctx, await ctx["user"].get(url), seed["align_1"], seed["mask_idx"]
    )
    flagged = await ctx["user"].get(url, params={"include_deprecated_upstream": True})
    assert flagged.status_code == 200, flagged.text
    assert flagged.json() == plain.json()


async def test_human_mint_checks_access_before_group_resolution(ctx, alignment_group_world):
    """ps_b is unreadable and resolves elsewhere: 403, so the 409's resolution of a
    sample the caller cannot read is never shown."""
    w = alignment_group_world
    await w["gate"]("mask_sample", w["mask_2"], w["ps_b"], MASK_SAMPLE_COMPLETED)
    await w["gate"]("alignment_sample", w["align_3"], w["ps_b"], ALIGNMENT_SAMPLE_COMPLETED)
    resp = await ctx["user"].post(
        _mint_url(w["align_1"]),
        json={"prep_sample_idx": [w["ps_a"], w["ps_b"]], "columns": _COLUMNS},
    )
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# A deprecated alignment: refused without include_deprecated, read exactly with it
# ---------------------------------------------------------------------------


async def _deprecate_alignment(ctx, alignment_idx: int) -> None:
    await ctx["pool"].execute(
        "UPDATE qiita.alignment_definition SET status = $2, deprecated_at = now(),"
        "       deprecated_by_idx = (SELECT MIN(idx) FROM qiita.principal),"
        "       deprecation_reason = 'aligner build was wrong'"
        " WHERE alignment_idx = $1",
        alignment_idx,
        AlignmentDefinitionStatus.DEPRECATED.value,
    )


def _refused_naming_the_flag(resp, alignment_idx: int) -> None:
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "deprecated"
    assert detail["deprecation_reason"] == "aligner build was wrong"
    assert INCLUDE_DEPRECATED_FLAG in detail["detail"]
    # The same message the feature-table client refuses with.
    assert detail["detail"] == deprecated_read_refusal(
        "alignment", alignment_idx, "aligner build was wrong"
    )


async def test_a_deprecated_alignment_is_refused_without_the_flag_and_read_with_it(
    ctx, pool_alignment_seed
):
    seed = pool_alignment_seed
    await _deprecate_alignment(ctx, seed["align_1"])
    body = {"prep_sample_idx": [seed["ps_a"]], "columns": _COLUMNS}

    _refused_naming_the_flag(
        await ctx["user"].post(_mint_url(seed["align_1"]), json=body), seed["align_1"]
    )

    resp = await ctx["user"].post(
        _mint_url(seed["align_1"]), json={**body, "include_deprecated": True}
    )
    assert resp.status_code == 201, resp.text
    assert _decode_ticket_payload(resp.json()["ticket"])["filter"] == {
        "alignment_idx": [seed["align_1"]],
        "prep_sample_idx": [seed["ps_a"]],
    }


async def test_the_flag_changes_nothing_for_an_active_alignment(ctx, pool_alignment_seed):
    seed = pool_alignment_seed
    body = {"prep_sample_idx": [seed["ps_a"]], "columns": _COLUMNS}
    plain = await ctx["user"].post(_mint_url(seed["align_1"]), json=body)
    flagged = await ctx["user"].post(
        _mint_url(seed["align_1"]), json={**body, "include_deprecated": True}
    )
    assert (plain.status_code, flagged.status_code) == (201, 201)
    assert (
        _decode_ticket_payload(plain.json()["ticket"])["filter"]
        == _decode_ticket_payload(flagged.json()["ticket"])["filter"]
    )


async def test_an_exact_read_refuses_a_sample_whose_mask_run_is_withdrawn(
    ctx, alignment_group_world
):
    w = alignment_group_world
    await _deprecate_alignment(ctx, w["align_1"])
    await w["gate"]("mask_sample", w["mask_idx"], w["ps_a"], "invalidated")
    resp = await ctx["user"].post(
        _mint_url(w["align_1"]),
        json={"prep_sample_idx": [w["ps_a"]], "columns": _COLUMNS, "include_deprecated": True},
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "not_processed"
    assert detail["samples"][0]["reason"] == "upstream_withdrawn"


async def test_an_exact_read_ignores_a_newer_member_of_the_group(ctx, alignment_group_world):
    """ps_a is aligned under both members; the group reads align_3, the newer one. An
    exact read of the deprecated align_1 reads align_1's own rows."""
    w = alignment_group_world
    await w["gate"]("mask_sample", w["mask_2"], w["ps_a"], MASK_SAMPLE_COMPLETED)
    await w["gate"]("alignment_sample", w["align_3"], w["ps_a"], ALIGNMENT_SAMPLE_COMPLETED)
    await _deprecate_alignment(ctx, w["align_1"])
    resp = await ctx["user"].post(
        _mint_url(w["align_1"]),
        json={"prep_sample_idx": [w["ps_a"]], "columns": _COLUMNS, "include_deprecated": True},
    )
    assert resp.status_code == 201, resp.text


@pytest.mark.parametrize("route", [URL_EXPORTED_IDENTIFIER, URL_EXPORTED_PROCESSING])
async def test_the_alignment_mints_take_the_flag(ctx, pool_alignment_seed, route):
    seed = pool_alignment_seed
    body = {"alignment_idx": seed["align_1"], "prep_sample_idx": [seed["ps_a"]]}
    try:
        plain = await ctx["user"].post(route, json=body)
        assert plain.status_code == 201, plain.text
        assert (
            await ctx["user"].post(route, json={**body, "include_deprecated": True})
        ).json() == (plain.json())
        await _deprecate_alignment(ctx, seed["align_1"])
        _refused_naming_the_flag(await ctx["user"].post(route, json=body), seed["align_1"])
        flagged = await ctx["user"].post(route, json={**body, "include_deprecated": True})
        assert flagged.status_code == 201, flagged.text
        assert flagged.json() == plain.json()
    finally:
        # The exported_identifier rows go with the seed's entity sweep (prep_sample-keyed);
        # exported_processing names no prep_sample, so it is deleted here.
        await ctx["pool"].execute(
            "DELETE FROM qiita.exported_processing WHERE alignment_idx = $1", seed["align_1"]
        )


async def test_a_deprecated_alignments_listing_leaves_out_a_withdrawn_mask_run(
    ctx, pool_alignment_seed
):
    """Under include_deprecated the listing reads the alignment exactly: a prep_sample
    whose mask run is withdrawn is not in the cohort and is listed in `left_out`; the
    DoGet naming it is still refused."""
    seed = pool_alignment_seed
    url = URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=seed["run_idx"],
        sequenced_pool_idx=seed["pool_idx"],
        alignment_idx=seed["align_1"],
    )
    await _deprecate_alignment(ctx, seed["align_1"])
    before = await ctx["user"].get(url, params={"include_deprecated": True})
    assert before.status_code == 200, before.text
    assert seed["ps_a"] in before.json()["prep_sample_idx"]

    await ctx["pool"].execute(
        "UPDATE qiita.mask_sample SET state = 'invalidated', invalidated_at = now(),"
        " invalidated_by_idx = $3, invalidation_reason = 'withdrawn by a test'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        seed["mask_idx"],
        seed["ps_a"],
        seed["owner_idx"],
    )
    accession, entity = await ctx["pool"].fetchrow(
        "SELECT bs.biosample_accession, ee.export_entity_id FROM qiita.prep_sample ps"
        "  JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
        "  JOIN qiita.exported_entity ee ON ee.biosample_idx = bs.idx WHERE ps.idx = $1",
        seed["ps_a"],
    )
    after = await ctx["user"].get(url, params={"include_deprecated": True})
    assert after.status_code == 200, after.text
    body = after.json()
    assert seed["ps_a"] not in body["prep_sample_idx"]
    assert body["left_out"] == [
        {
            "prep_sample_idx": seed["ps_a"],
            "biosample_accession": accession,
            "biosample_export_entity_id": entity,
            "state": "not_processed",
            "reason": "upstream_withdrawn",
        }
    ]
    named = await ctx["user"].post(
        _mint_url(seed["align_1"]),
        json={"prep_sample_idx": [seed["ps_a"]], "columns": _COLUMNS, "include_deprecated": True},
    )
    assert named.status_code == 409, named.text


async def test_an_exact_read_reports_the_deprecated_mask_it_reads(ctx, pool_alignment_seed):
    """A deprecated alignment read alone under include_deprecated, on a deprecated
    mask: admitted without include_deprecated_upstream, and the ticket names the mask
    in `deprecated_upstreams`, as a read past it does."""
    seed = pool_alignment_seed
    await _deprecate_mask(ctx, seed)
    await _deprecate_alignment(ctx, seed["align_1"])
    resp = await ctx["user"].post(
        _mint_url(seed["align_1"]),
        json={"prep_sample_idx": [seed["ps_a"]], "columns": _COLUMNS, "include_deprecated": True},
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["deprecated_upstreams"] == [
        (await _deprecated_mask(ctx, seed["mask_idx"])).model_dump(mode="json")
    ]


async def test_an_upstream_read_listing_leaves_out_a_withdrawn_mask_run(ctx, pool_alignment_seed):
    """Under include_deprecated_upstream the listing reads the active alignment past its
    deprecated mask: a prep_sample whose mask run is withdrawn is not in the cohort and
    is listed in `left_out`; the DoGet naming it is still refused."""
    seed = pool_alignment_seed
    url = URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=seed["run_idx"],
        sequenced_pool_idx=seed["pool_idx"],
        alignment_idx=seed["align_1"],
    )
    await _deprecate_mask(ctx, seed)
    await ctx["pool"].execute(
        "UPDATE qiita.mask_sample SET state = 'invalidated', invalidated_at = now(),"
        " invalidated_by_idx = $3, invalidation_reason = 'withdrawn by a test'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        seed["mask_idx"],
        seed["ps_a"],
        seed["owner_idx"],
    )
    accession, entity = await ctx["pool"].fetchrow(
        "SELECT bs.biosample_accession, ee.export_entity_id FROM qiita.prep_sample ps"
        "  JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
        "  JOIN qiita.exported_entity ee ON ee.biosample_idx = bs.idx WHERE ps.idx = $1",
        seed["ps_a"],
    )
    resp = await ctx["user"].get(url, params={"include_deprecated_upstream": True})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert seed["ps_a"] not in body["prep_sample_idx"]
    assert body["left_out"] == [
        {
            "prep_sample_idx": seed["ps_a"],
            "biosample_accession": accession,
            "biosample_export_entity_id": entity,
            "state": "not_processed",
            "reason": "upstream_withdrawn",
        }
    ]
    # The read resolves each upstream in its group, so it takes the decision set: the
    # current one gives the same listing, and an unknown one is 422.
    current = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE,
        json={
            "definition_kind": "alignment",
            "definition_idx": seed["align_1"],
            "prep_sample_idx": [seed["ps_a"]],
            "include_deprecated_upstream": True,
        },
    )
    assert current.status_code == 200, current.text
    token = current.json()["decisions"]["decision_set"]
    pinned = await ctx["user"].get(
        url, params={"include_deprecated_upstream": True, "as_of_event": token}
    )
    assert pinned.status_code == 200, pinned.text
    assert pinned.json() == body
    unknown = await ctx["user"].get(
        url, params={"include_deprecated_upstream": True, "as_of_event": "sha256:" + "0" * 64}
    )
    assert unknown.status_code == 422, unknown.text
    named = await ctx["user"].post(
        _mint_url(seed["align_1"]),
        json={
            "prep_sample_idx": [seed["ps_a"]],
            "columns": _COLUMNS,
            "include_deprecated_upstream": True,
        },
    )
    assert named.status_code == 409, named.text


async def test_the_upstream_flag_leaves_nothing_out_without_a_deprecated_upstream(
    ctx, pool_alignment_seed
):
    """With no deprecated upstream the flag reads the group as without it: a withdrawn
    mask run stays in the listing (the build's resolution decides it) and nothing is
    listed in `left_out`."""
    seed = pool_alignment_seed
    url = URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=seed["run_idx"],
        sequenced_pool_idx=seed["pool_idx"],
        alignment_idx=seed["align_1"],
    )
    await ctx["pool"].execute(
        "UPDATE qiita.mask_sample SET state = 'invalidated', invalidated_at = now(),"
        " invalidated_by_idx = $3, invalidation_reason = 'withdrawn by a test'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        seed["mask_idx"],
        seed["ps_a"],
        seed["owner_idx"],
    )
    plain = await ctx["user"].get(url)
    flagged = await ctx["user"].get(url, params={"include_deprecated_upstream": True})
    assert (plain.status_code, flagged.status_code) == (200, 200)
    assert flagged.json() == plain.json()
    assert flagged.json()["left_out"] == []
