"""Tests for the single-sample host-filter resolver.

The resolver's whole job is to be fail-closed: anything it cannot determine must
come back UNRESOLVED, never as "no filtering". So the cases below pair each
happy path with the ambiguity next to it — a host with a profile vs. the same
host without one, a deliberate 'not applicable' vs. an uninformative 'not
collected'. The distinction between the last two is the one that matters most:
both are missing-reasons, and reading the second as "no host" is exactly how an
un-depleted human sample would slip through.

DB-bound: the resolver reads the trigger-maintained global_field_idx, and the
trigger is the thing that makes the cross-study read work, so mocking the query
out would test nothing.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.models import MISSING_REASON_CONTROL_SAMPLE, Platform

from qiita_control_plane.host_filter_resolver import (
    HostFilterOutcome,
    is_control_sample,
    resolve_host_filter,
    resolve_host_filter_many,
)
from qiita_control_plane.repositories._sample_helpers import (
    _get_or_create_globally_linked_study_field,
)
from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.host_filter_profile import insert_host_filter_profile
from qiita_control_plane.testing.db_seeds import (
    NCBI_TAXONOMY_HUMAN_TERM_ID,
    fetch_missing_value_reason_idx,
    fetch_ncbi_taxonomy_term,
    seed_host_reference,
    seed_user_principal,
)

pytestmark = pytest.mark.db

_HOST_TAXON_FIELD = "host_taxon_id"


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """Seed a principal, a study, a host reference pair, and a study field bound
    to the seeded `host_taxon_id` global field.

    The study field must be GLOBALLY LINKED, not local: it is that link the
    biosample_metadata trigger follows to populate `global_field_idx`, which is
    the column the resolver reads. A purely-local field with the same name would
    leave global_field_idx NULL and the resolver would (correctly) see nothing.
    """
    pool = postgres_pool
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(pool, prefix="hfr", suffix=suffix)
    study_idx = await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        principal_idx,
        f"hfr-{suffix}",
    )

    host_gf_idx = await pool.fetchval(
        "SELECT idx FROM qiita.biosample_global_field WHERE internal_name = $1",
        _HOST_TAXON_FIELD,
    )
    assert host_gf_idx is not None, f"{_HOST_TAXON_FIELD} global field should be seeded"

    async with pool.acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            global_field_idx=host_gf_idx,
            display_name="host taxon id",
            created_by_idx=principal_idx,
        )

    rype_idx = await seed_host_reference(
        pool, name=f"hfr-rype-{suffix}", created_by_idx=principal_idx
    )
    minimap2_idx = await seed_host_reference(
        pool, name=f"hfr-mm2-{suffix}", created_by_idx=principal_idx
    )

    human_term = await fetch_ncbi_taxonomy_term(pool, NCBI_TAXONOMY_HUMAN_TERM_ID)
    human_term_idx = human_term["idx"] if human_term else None
    assert human_term_idx is not None, "NCBI 9606 should be seeded by migration"

    # Two profiles for the same host: one carrying both stages, one that stops
    # after rype. Between them they pin that the SECOND STAGE IS OPTIONAL and
    # that the resolver reports whichever the platform's profile declares. They
    # do NOT pin any platform<->stage coupling — the schema deliberately leaves
    # which stages a (host, platform) runs to config rather than a CHECK, so the
    # resolver must report what the row says, not what the platform implies.
    async with pool.acquire() as conn:
        await insert_host_filter_profile(
            conn,
            host_term_idx=human_term_idx,
            platform=Platform.ILLUMINA,
            rype_reference_idx=rype_idx,
            minimap2_reference_idx=minimap2_idx,
            principal_idx=principal_idx,
        )
        await insert_host_filter_profile(
            conn,
            host_term_idx=human_term_idx,
            platform=Platform.PACBIO_SMRT,
            rype_reference_idx=rype_idx,
            principal_idx=principal_idx,
        )

    # `studies` / `biosample_study_field` hold rows seeded by tests that need a
    # SECOND study (the cross-study uniqueness test); the auto-seeded pair above
    # is torn down separately at the end.
    created: dict[str, list[int]] = {
        "biosample_metadata": [],
        "biosample": [],
        "biosample_study_field": [],
        "studies": [],
    }
    state = {
        "pool": pool,
        "principal_idx": principal_idx,
        "study_idx": study_idx,
        "host_gf_idx": host_gf_idx,
        "field_idx": field_idx,
        "rype_idx": rype_idx,
        "minimap2_idx": minimap2_idx,
        "human_term_idx": human_term_idx,
        "created": created,
    }
    yield state

    # FK-reverse teardown.
    await pool.execute(
        "DELETE FROM qiita.biosample_metadata WHERE idx = ANY($1::bigint[])",
        created["biosample_metadata"],
    )
    await pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = ANY($1::bigint[])",
        created["biosample"],
    )
    await pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", created["biosample"]
    )
    await pool.execute(
        "DELETE FROM qiita.biosample_study_field WHERE idx = ANY($1::bigint[])",
        created["biosample_study_field"],
    )
    await pool.execute("DELETE FROM qiita.biosample_study_field WHERE idx = $1", field_idx)
    await pool.execute(
        "DELETE FROM qiita.host_filter_profile WHERE created_by_idx = $1", principal_idx
    )
    await pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
        [rype_idx, minimap2_idx],
    )
    await pool.execute("DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", created["studies"])
    await pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)
    await pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _make_biosample(ctx):
    """Create a biosample linked to the fixture's study, tracked for cleanup."""
    from qiita_control_plane.repositories._sample_helpers import insert_entity_to_study

    async with ctx["pool"].acquire() as conn, conn.transaction():
        bs_idx = await insert_biosample(
            conn, owner_idx=ctx["principal_idx"], created_by_idx=ctx["principal_idx"]
        )
        await insert_entity_to_study(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=bs_idx,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample"].append(bs_idx)
    return bs_idx


async def _set_host_term(ctx, biosample_idx, term_idx):
    """Write host_taxon_id as a terminology term on the biosample."""
    meta_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_metadata"
        " (biosample_idx, biosample_study_field_idx, value_terminology_term_idx,"
        "  created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        biosample_idx,
        ctx["field_idx"],
        term_idx,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_metadata"].append(meta_idx)


async def _set_host_missing_reason(ctx, biosample_idx, reason_name):
    """Write host_taxon_id as a missing-reason on the biosample."""
    reason_idx = await fetch_missing_value_reason_idx(ctx["pool"], reason_name)
    assert reason_idx is not None, f"missing_value_reason {reason_name!r} should be seeded"
    meta_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_metadata"
        " (biosample_idx, biosample_study_field_idx, value_missing_reason_idx,"
        "  created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        biosample_idx,
        ctx["field_idx"],
        reason_idx,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_metadata"].append(meta_idx)


# ---------------------------------------------------------------------------
# FILTER — the host has a profile on this platform
# ---------------------------------------------------------------------------


async def test_host_with_short_read_profile_filters_against_both_stages(ctx):
    bs_idx = await _make_biosample(ctx)
    await _set_host_term(ctx, bs_idx, ctx["human_term_idx"])

    res = await resolve_host_filter(ctx["pool"], biosample_idx=bs_idx, platform=Platform.ILLUMINA)

    assert res.outcome is HostFilterOutcome.FILTER
    assert res.host_term_idx == ctx["human_term_idx"]
    assert res.rype_reference_idx == ctx["rype_idx"]
    assert res.minimap2_reference_idx == ctx["minimap2_idx"]


async def test_host_with_long_read_profile_has_no_minimap2_stage(ctx):
    """The same host on a long-read platform resolves to a rype-only profile.
    minimap2_reference_idx is None, not the illumina profile's value — the
    platform, not the organism, decides the aligner tier."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_term(ctx, bs_idx, ctx["human_term_idx"])

    res = await resolve_host_filter(
        ctx["pool"], biosample_idx=bs_idx, platform=Platform.PACBIO_SMRT
    )

    assert res.outcome is HostFilterOutcome.FILTER
    assert res.rype_reference_idx == ctx["rype_idx"]
    assert res.minimap2_reference_idx is None


# ---------------------------------------------------------------------------
# UNRESOLVED — fail-closed
# ---------------------------------------------------------------------------


async def test_host_with_no_profile_on_this_platform_is_unresolved(ctx):
    """A known host on a platform we have no build for. This must NOT silently
    become pass-through: the sample HAS a host, we just cannot deplete it yet."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_term(ctx, bs_idx, ctx["human_term_idx"])

    res = await resolve_host_filter(
        ctx["pool"], biosample_idx=bs_idx, platform=Platform.OXFORD_NANOPORE
    )

    assert res.outcome is HostFilterOutcome.UNRESOLVED
    # The term rides along even on the failure, so the caller can say WHICH host
    # lacks a build and offer a fix.
    assert res.host_term_idx == ctx["human_term_idx"]
    assert res.rype_reference_idx is None
    assert "no host_filter_profile" in res.reason


async def test_absent_host_taxon_id_is_unresolved(ctx):
    """No host_taxon_id row at all. 'Nobody told us' is not 'there is no host'."""
    bs_idx = await _make_biosample(ctx)

    res = await resolve_host_filter(ctx["pool"], biosample_idx=bs_idx, platform=Platform.ILLUMINA)

    assert res.outcome is HostFilterOutcome.UNRESOLVED
    assert res.host_term_idx is None
    assert "not set" in res.reason


async def test_uninformative_missing_reason_is_unresolved(ctx):
    """'not collected' is a missing-reason like 'not applicable' is, but it says
    nothing about whether a host exists — so it fails closed. This is the case
    that separates a deliberate no-host from an unknown one; getting it wrong
    passes an un-depleted host sample straight through."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, bs_idx, "not collected")

    res = await resolve_host_filter(ctx["pool"], biosample_idx=bs_idx, platform=Platform.ILLUMINA)

    assert res.outcome is HostFilterOutcome.UNRESOLVED
    assert "not collected" in res.reason


async def test_unresolved_reasons_are_distinct_per_cause(ctx):
    """An absent field and a host-without-a-build are both UNRESOLVED but are
    different problems with different fixes, so their reasons must not collapse
    into one indistinguishable string."""
    absent_bs = await _make_biosample(ctx)
    no_profile_bs = await _make_biosample(ctx)
    await _set_host_term(ctx, no_profile_bs, ctx["human_term_idx"])

    absent = await resolve_host_filter(
        ctx["pool"], biosample_idx=absent_bs, platform=Platform.ILLUMINA
    )
    no_profile = await resolve_host_filter(
        ctx["pool"], biosample_idx=no_profile_bs, platform=Platform.OXFORD_NANOPORE
    )

    assert absent.outcome is no_profile.outcome is HostFilterOutcome.UNRESOLVED
    assert absent.reason != no_profile.reason


# ---------------------------------------------------------------------------
# PASS_THROUGH / CONTROL
# ---------------------------------------------------------------------------


async def test_not_applicable_is_pass_through(ctx):
    """'not applicable' is a decision, not a gap — a water or soil sample that
    deliberately has no host. Nothing to deplete."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, bs_idx, "not applicable")

    res = await resolve_host_filter(ctx["pool"], biosample_idx=bs_idx, platform=Platform.ILLUMINA)

    assert res.outcome is HostFilterOutcome.PASS_THROUGH
    assert res.host_term_idx is None
    assert res.rype_reference_idx is None


async def test_control_sample_is_a_marker_not_a_decision(ctx):
    """A blank resolves to CONTROL and stops. The resolver deliberately does NOT
    pick references for it: what a control gets filtered against is the union of
    its POOL's hosts, which is a pool-level fact this single-sample function
    cannot see."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, bs_idx, "missing: control sample")

    res = await resolve_host_filter(ctx["pool"], biosample_idx=bs_idx, platform=Platform.ILLUMINA)

    assert res.outcome is HostFilterOutcome.CONTROL
    assert res.rype_reference_idx is None
    assert res.minimap2_reference_idx is None


# ---------------------------------------------------------------------------
# is_control_sample — the zero-read read-mask classifier (#177)
# ---------------------------------------------------------------------------
# Shares _RECOGNISED_MISSING_REASON + the metadata read with resolve_host_filter,
# so the "is this a control" answer here can never drift from the CONTROL outcome
# above. These pin that the read-mask reads binder can tell a blank apart from an
# unexpected-empty data well when a sample yields zero stored reads.


async def test_is_control_sample_true_for_control_marker(ctx):
    """The control missing-reason marks an expected-empty control — True."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, bs_idx, MISSING_REASON_CONTROL_SAMPLE)
    assert await is_control_sample(ctx["pool"], biosample_idx=bs_idx) is True


async def test_is_control_sample_false_for_non_control_states(ctx):
    """Everything that is NOT a control reads False: a named host, a deliberate
    'not applicable' (a real hostless sample, not a control), an uninformative
    'not collected', and a biosample that never set host_taxon_id at all. A
    zero-read data well in any of these states is a genuine failure, not a benign
    no_data."""
    host = await _make_biosample(ctx)
    await _set_host_term(ctx, host, ctx["human_term_idx"])
    assert await is_control_sample(ctx["pool"], biosample_idx=host) is False

    not_applicable = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, not_applicable, "not applicable")
    assert await is_control_sample(ctx["pool"], biosample_idx=not_applicable) is False

    not_collected = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, not_collected, "not collected")
    assert await is_control_sample(ctx["pool"], biosample_idx=not_collected) is False

    absent = await _make_biosample(ctx)
    assert await is_control_sample(ctx["pool"], biosample_idx=absent) is False


async def test_prep_sample_expected_empty_control_end_to_end(ctx):
    """The read-mask seam's full prep_sample → biosample → control lookup: a
    prep_sample prepped from a control biosample is an expected-empty control; one
    from a data biosample is not; a non-existent prep_sample is fail-safe False
    (disposed as a data well, never silently benign)."""
    from qiita_control_plane.runner._read_ingest import (
        _prep_sample_is_expected_empty_control,
    )
    from qiita_control_plane.testing.db_seeds import seed_sequenced_prep_sample

    pool = ctx["pool"]
    control_bs = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, control_bs, MISSING_REASON_CONTROL_SAMPLE)
    data_bs = await _make_biosample(ctx)
    await _set_host_term(ctx, data_bs, ctx["human_term_idx"])

    control_ps = await seed_sequenced_prep_sample(
        pool, biosample_idx=control_bs, owner_idx=ctx["principal_idx"]
    )
    data_ps = await seed_sequenced_prep_sample(
        pool, biosample_idx=data_bs, owner_idx=ctx["principal_idx"]
    )
    try:
        assert await _prep_sample_is_expected_empty_control(pool, control_ps) is True
        assert await _prep_sample_is_expected_empty_control(pool, data_ps) is False
        # A prep_sample_idx that doesn't exist → fail-safe False.
        assert await _prep_sample_is_expected_empty_control(pool, 9_999_999_999) is False
    finally:
        # Delete the prep_samples so the fixture's biosample teardown (ON DELETE
        # RESTRICT) isn't blocked.
        await pool.execute(
            "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])",
            [control_ps, data_ps],
        )


# ---------------------------------------------------------------------------
# The invariant the resolver's single-row read depends on
# ---------------------------------------------------------------------------


async def test_a_biosample_cannot_carry_two_host_taxon_id_values(ctx):
    """The resolver reads host_taxon_id with fetchrow, i.e. it assumes a biosample
    has AT MOST ONE value for the field. That is not obvious: a biosample can be
    linked to several studies, and each study has its own study_field row, so the
    per-study UNIQUE (biosample_idx, biosample_study_field_idx) does NOT prevent
    two host values landing on one biosample from two different studies.

    What prevents it is the PARTIAL unique index
    biosample_metadata_one_value_per_global_field on (biosample_idx,
    global_field_idx). This test pins that index specifically — it writes the
    second value through a DIFFERENT study's field, so the per-study constraint
    cannot be what rejects it. If the index were ever relaxed, fetchrow would
    start silently returning an arbitrary one of two conflicting hosts, which is
    exactly the silent-wrong-answer this module exists to prevent.
    """
    from qiita_control_plane.repositories._sample_helpers import insert_entity_to_study

    bs_idx = await _make_biosample(ctx)
    await _set_host_term(ctx, bs_idx, ctx["human_term_idx"])

    # A second study, its own study_field bound to the SAME host_taxon_id global
    # field, and the biosample linked into it.
    other_study_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        ctx["principal_idx"],
        f"hfr-second-{secrets.token_hex(4)}",
    )
    ctx["created"]["studies"].append(other_study_idx)

    async with ctx["pool"].acquire() as conn, conn.transaction():
        other_field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=other_study_idx,
            global_field_idx=ctx["host_gf_idx"],
            display_name="host taxon id",
            created_by_idx=ctx["principal_idx"],
        )
        await insert_entity_to_study(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=bs_idx,
            study_idx=other_study_idx,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_study_field"].append(other_field_idx)

    # Same biosample, same global field, DIFFERENT study field — rejected.
    with pytest.raises(asyncpg.UniqueViolationError):
        await ctx["pool"].execute(
            "INSERT INTO qiita.biosample_metadata"
            " (biosample_idx, biosample_study_field_idx, value_terminology_term_idx,"
            "  created_by_idx)"
            " VALUES ($1, $2, $3, $4)",
            bs_idx,
            other_field_idx,
            ctx["human_term_idx"],
            ctx["principal_idx"],
        )

    # And the resolver still gets its single unambiguous answer.
    res = await resolve_host_filter(ctx["pool"], biosample_idx=bs_idx, platform=Platform.ILLUMINA)
    assert res.outcome is HostFilterOutcome.FILTER
    assert res.host_term_idx == ctx["human_term_idx"]


# ---------------------------------------------------------------------------
# The batch path must agree with the single-sample path, exactly
# ---------------------------------------------------------------------------


async def test_batch_agrees_with_single_sample_for_every_outcome(ctx):
    """resolve_host_filter_many is an OPTIMIZATION, not a second implementation:
    the roster resolves a whole pool in two queries while a submit resolves one
    sample at a time, and if the two ever disagree an operator is shown one plan
    and a different one runs.

    They share `_classify`, but they fetch its inputs by different routes (one
    profile lookup vs. a platform-scoped list indexed by host term), so "cannot
    drift" is only true if something checks it. Compare the FULL resolution —
    including `reason`, which is the operator-facing text and the field most
    likely to diverge silently.
    """
    biosamples = {}

    # One biosample per outcome, so the comparison spans every branch.
    biosamples["filter"] = await _make_biosample(ctx)
    await _set_host_term(ctx, biosamples["filter"], ctx["human_term_idx"])

    biosamples["pass_through"] = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, biosamples["pass_through"], "not applicable")

    biosamples["control"] = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, biosamples["control"], "missing: control sample")

    biosamples["unrecognised_reason"] = await _make_biosample(ctx)
    await _set_host_missing_reason(ctx, biosamples["unrecognised_reason"], "not collected")

    # No host_taxon_id at all.
    biosamples["absent"] = await _make_biosample(ctx)

    idxs = list(biosamples.values())
    batch = await resolve_host_filter_many(
        ctx["pool"], biosample_idxs=idxs, platform=Platform.ILLUMINA
    )
    for idx in idxs:
        single = await resolve_host_filter(
            ctx["pool"], biosample_idx=idx, platform=Platform.ILLUMINA
        )
        assert batch[idx] == single, f"batch/single disagree for biosample {idx}"

    # And the fan actually covered every branch — otherwise the equality above
    # could pass by comparing five identical UNRESOLVEDs.
    assert {batch[i].outcome for i in idxs} == {
        HostFilterOutcome.FILTER,
        HostFilterOutcome.PASS_THROUGH,
        HostFilterOutcome.CONTROL,
        HostFilterOutcome.UNRESOLVED,
    }


async def test_batch_agrees_with_single_sample_when_the_host_has_no_profile(ctx):
    """The no-profile-for-this-platform branch, which is where the two paths'
    inputs are fetched most differently (a lookup that misses, vs. a term absent
    from the platform's profile map) — and where the reason string names the
    platform, so an enum-vs-str slip would show up here first."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_term(ctx, bs_idx, ctx["human_term_idx"])

    # oxford_nanopore has no profile seeded for this host.
    batch = await resolve_host_filter_many(
        ctx["pool"], biosample_idxs=[bs_idx], platform=Platform.OXFORD_NANOPORE
    )
    single = await resolve_host_filter(
        ctx["pool"], biosample_idx=bs_idx, platform=Platform.OXFORD_NANOPORE
    )

    assert batch[bs_idx] == single
    assert batch[bs_idx].outcome is HostFilterOutcome.UNRESOLVED
    # The platform renders as its value, not as a StrEnum repr — this reason is
    # shown to an operator.
    assert "on platform oxford_nanopore" in batch[bs_idx].reason


async def test_batch_accepts_a_raw_platform_string(ctx):
    """The roster reads `platform` straight off a sequencing_run row, and asyncpg
    hands a qiita.platform column back as a plain str. Coercing at the boundary is
    what keeps that caller from silently producing different reason text than an
    enum-passing caller would."""
    bs_idx = await _make_biosample(ctx)
    await _set_host_term(ctx, bs_idx, ctx["human_term_idx"])

    from_str = await resolve_host_filter_many(
        ctx["pool"], biosample_idxs=[bs_idx], platform="illumina"
    )
    from_enum = await resolve_host_filter_many(
        ctx["pool"], biosample_idxs=[bs_idx], platform=Platform.ILLUMINA
    )
    assert from_str[bs_idx] == from_enum[bs_idx]
    assert from_str[bs_idx].outcome is HostFilterOutcome.FILTER


async def test_batch_edge_cases(ctx):
    """Empty input is an empty dict (not a query); a repeated idx collapses rather
    than duplicating; and an idx with no metadata row comes back UNRESOLVED rather
    than being silently dropped from the result — a missing key would KeyError the
    roster, which is the fail-loud direction but a worse one than reporting it."""
    assert (
        await resolve_host_filter_many(ctx["pool"], biosample_idxs=[], platform=Platform.ILLUMINA)
        == {}
    )

    bs_idx = await _make_biosample(ctx)
    repeated = await resolve_host_filter_many(
        ctx["pool"], biosample_idxs=[bs_idx, bs_idx], platform=Platform.ILLUMINA
    )
    assert list(repeated) == [bs_idx]
    assert repeated[bs_idx].outcome is HostFilterOutcome.UNRESOLVED
