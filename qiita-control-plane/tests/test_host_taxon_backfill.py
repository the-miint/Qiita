"""Tests for the host_taxon_id backfill.

Two things have to hold, and they pull in opposite directions.

It must be AGGRESSIVE enough to be worth running: a sample whose taxon implies a
host, and a blank, both get written — otherwise the submit path stays blocked on
every pool, since every pool contains blanks.

And it must be TIMID enough to be safe: a sample nothing settles is left
unwritten, so it stays UNRESOLVED at submit and aborts, rather than being
guessed at and silently under-depleted. The residue is a worklist, not a
rounding error.

The pure classifier is tested without a DB; the plan/apply pair is db-marked
because the whole point of `plan_backfill` is the SQL that finds candidates and
the trigger that binds them to the global field.
"""

import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.backfill.host_taxon import (
    HostTaxonSource,
    apply_backfill,
    classify,
    plan_backfill,
)
from qiita_control_plane.repositories._sample_helpers import (
    _get_or_create_globally_linked_study_field,
    insert_entity_to_study,
)
from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.testing.db_seeds import (
    NCBI_TAXONOMY_HUMAN_TERM_ID,
    fetch_ncbi_taxonomy_term,
    seed_user_principal,
)

# NCBI taxa the live data actually carries, as the sample's OWN taxon_id.
_HUMAN_GUT_METAGENOME = "408170"
_SEAWATER_METAGENOME = "1561972"
_GENERIC_METAGENOME = "256318"  # the bare root — names no environment


# ---------------------------------------------------------------------------
# The pure classifier
# ---------------------------------------------------------------------------


def _classify(**kw):
    base = {
        "biosample_idx": 1,
        "study_idx": 1,
        "is_control": False,
        "sample_taxon_term_id": None,
        "sample_taxon_label": None,
    }
    return classify(**{**base, **kw})


def test_human_gut_metagenome_implies_a_human_host():
    a = _classify(sample_taxon_term_id=_HUMAN_GUT_METAGENOME)
    assert a.source is HostTaxonSource.TAXON
    assert a.host_term_id == NCBI_TAXONOMY_HUMAN_TERM_ID
    assert a.missing_reason is None


def test_seawater_metagenome_implies_no_host():
    """'not applicable' is a DECISION — the sample deliberately has no host — and
    it resolves PASS_THROUGH, i.e. no depletion. Distinct from UNRESOLVED."""
    a = _classify(sample_taxon_term_id=_SEAWATER_METAGENOME)
    assert a.source is HostTaxonSource.NO_HOST
    assert a.missing_reason == "not applicable"
    assert a.host_term_id is None


def test_control_wins_over_its_taxon():
    """A blank has no host of its own whatever taxon it carries — and on the live
    data blanks DO carry one (the generic metagenome root). If taxon were checked
    first, every blank would fall through to UNRESOLVED and abort every pool,
    because every pool contains blanks. This ordering is the whole ballgame."""
    a = _classify(is_control=True, sample_taxon_term_id=_GENERIC_METAGENOME)
    assert a.source is HostTaxonSource.CONTROL
    assert a.missing_reason == "missing: control sample"


def test_a_control_carrying_a_host_taxon_is_still_a_control():
    """Even if a blank somehow carried the human-gut taxon, it is a blank."""
    a = _classify(is_control=True, sample_taxon_term_id=_HUMAN_GUT_METAGENOME)
    assert a.source is HostTaxonSource.CONTROL
    assert a.host_term_id is None


def test_generic_metagenome_is_unresolved_not_hostless():
    """The bare `metagenome` root names no environment, so it implies no host —
    but "we can't tell" is NOT the same claim as "it has no host". Mapping it to
    'not applicable' would silently stop depleting a sample that may well be
    human. It must fail closed."""
    a = _classify(sample_taxon_term_id=_GENERIC_METAGENOME)
    assert a.source is HostTaxonSource.UNRESOLVED
    assert a.host_term_id is None
    assert a.missing_reason is None


def test_absent_taxon_is_unresolved():
    a = _classify(sample_taxon_term_id=None)
    assert a.source is HostTaxonSource.UNRESOLVED


def test_unknown_taxon_is_unresolved():
    """A taxon the curated table has never seen. Absence from the table is not
    consent — it aborts."""
    a = _classify(sample_taxon_term_id="9999999")
    assert a.source is HostTaxonSource.UNRESOLVED


# ---------------------------------------------------------------------------
# plan + apply, against a real DB
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """A study with a globally-linked `taxon_id` field, so biosamples can carry
    the sample-taxon the backfill reads."""
    pool = postgres_pool
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(pool, prefix="hxb", suffix=suffix)
    study_idx = await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        principal_idx,
        f"hxb-{suffix}",
    )
    taxon_gf_idx = await pool.fetchval(
        "SELECT idx FROM qiita.biosample_global_field WHERE internal_name = 'taxon_id'"
    )
    async with pool.acquire() as conn, conn.transaction():
        taxon_field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            global_field_idx=taxon_gf_idx,
            display_name="taxon id",
            created_by_idx=principal_idx,
        )

    created: dict[str, list[int]] = {"biosample": []}
    state = {
        "pool": pool,
        "principal_idx": principal_idx,
        "study_idx": study_idx,
        "taxon_field_idx": taxon_field_idx,
        "created": created,
    }
    yield state

    await pool.execute(
        "DELETE FROM qiita.biosample_metadata WHERE biosample_idx = ANY($1::bigint[])",
        created["biosample"],
    )
    await pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = ANY($1::bigint[])",
        created["biosample"],
    )
    await pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", created["biosample"]
    )
    # Bulk-delete by study rather than by tracked idx: the backfill CREATES the
    # study's host_taxon_id field as a side effect (that is the point), so the
    # test cannot know every field idx up front. The study is test-owned, so no
    # other test can have planted a field on it.
    await pool.execute("DELETE FROM qiita.biosample_study_field WHERE study_idx = $1", study_idx)
    await pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)
    await pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _seed_biosample_with_taxon(ctx, term_id):
    """Create a biosample carrying `term_id` as its own taxon_id."""
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

    term = await fetch_ncbi_taxonomy_term(ctx["pool"], term_id)
    assert term is not None, f"NCBI term {term_id} should be seeded"
    await ctx["pool"].execute(
        "INSERT INTO qiita.biosample_metadata"
        " (biosample_idx, biosample_study_field_idx, value_terminology_term_idx, created_by_idx)"
        " VALUES ($1, $2, $3, $4)",
        bs_idx,
        ctx["taxon_field_idx"],
        term["idx"],
        ctx["principal_idx"],
    )
    return bs_idx


async def _host_taxon_of(ctx, biosample_idx):
    """Read back the host_taxon_id the backfill wrote: (term_id, reason_name)."""
    return await ctx["pool"].fetchrow(
        "SELECT tt.term_id, mvr.name AS reason"
        "  FROM qiita.biosample_metadata bm"
        "  JOIN qiita.biosample_global_field bgf"
        "    ON bgf.idx = bm.global_field_idx AND bgf.internal_name = 'host_taxon_id'"
        "  LEFT JOIN qiita.terminology_term tt ON tt.idx = bm.value_terminology_term_idx"
        "  LEFT JOIN qiita.missing_value_reason mvr ON mvr.idx = bm.value_missing_reason_idx"
        " WHERE bm.biosample_idx = $1",
        biosample_idx,
    )


@pytest.mark.db
async def test_plan_then_apply_writes_the_resolvable_and_skips_the_rest(ctx):
    """End to end: a human-gut sample gets a host, a seawater sample gets
    'not applicable', and a generic-metagenome sample gets NOTHING — it stays
    UNRESOLVED at submit, which is the fail-closed outcome."""
    human = await _seed_biosample_with_taxon(ctx, _HUMAN_GUT_METAGENOME)
    water = await _seed_biosample_with_taxon(ctx, _SEAWATER_METAGENOME)
    generic = await _seed_biosample_with_taxon(ctx, _GENERIC_METAGENOME)

    plan = await plan_backfill(ctx["pool"])
    mine = {
        a.biosample_idx: a for a in plan.assignments if a.biosample_idx in (human, water, generic)
    }
    assert mine[human].source is HostTaxonSource.TAXON
    assert mine[water].source is HostTaxonSource.NO_HOST
    assert mine[generic].source is HostTaxonSource.UNRESOLVED

    written = await apply_backfill(
        ctx["pool"],
        [a for a in plan.assignments if a.biosample_idx in (human, water, generic)],
        principal_idx=ctx["principal_idx"],
    )
    assert written == 2  # the generic one is not written

    assert (await _host_taxon_of(ctx, human))["term_id"] == NCBI_TAXONOMY_HUMAN_TERM_ID
    assert (await _host_taxon_of(ctx, water))["reason"] == "not applicable"
    assert await _host_taxon_of(ctx, generic) is None


@pytest.mark.db
async def test_backfill_is_idempotent(ctx):
    """Re-running must be a no-op, not a duplicate-key crash — the backfill is
    expected to be re-run as curation lands, so a second pass has to be safe.
    The already-written sample drops out of the PLAN, which is what makes the
    apply trivially safe rather than relying on the unique index to save us."""
    human = await _seed_biosample_with_taxon(ctx, _HUMAN_GUT_METAGENOME)

    first = await plan_backfill(ctx["pool"])
    assert any(a.biosample_idx == human for a in first.assignments)
    await apply_backfill(
        ctx["pool"],
        [a for a in first.assignments if a.biosample_idx == human],
        principal_idx=ctx["principal_idx"],
    )

    second = await plan_backfill(ctx["pool"])
    assert not any(a.biosample_idx == human for a in second.assignments), (
        "an already-backfilled biosample must not reappear as a candidate"
    )
    # And applying the (now empty) slice writes nothing rather than raising.
    written = await apply_backfill(
        ctx["pool"],
        [a for a in second.assignments if a.biosample_idx == human],
        principal_idx=ctx["principal_idx"],
    )
    assert written == 0


@pytest.mark.db
async def test_written_value_is_visible_to_the_host_filter_resolver(ctx):
    """The backfill's whole purpose is to make the resolver stop saying
    UNRESOLVED. Writing a row that the resolver cannot see (e.g. bound to a
    study-LOCAL field, leaving global_field_idx NULL) would look like success and
    change nothing — so assert against the resolver itself, not against the row."""
    from qiita_common.models import Platform

    from qiita_control_plane.host_filter_resolver import (
        HostFilterOutcome,
        resolve_host_filter,
    )

    human = await _seed_biosample_with_taxon(ctx, _HUMAN_GUT_METAGENOME)

    before = await resolve_host_filter(ctx["pool"], biosample_idx=human, platform=Platform.ILLUMINA)
    assert before.outcome is HostFilterOutcome.UNRESOLVED

    plan = await plan_backfill(ctx["pool"])
    await apply_backfill(
        ctx["pool"],
        [a for a in plan.assignments if a.biosample_idx == human],
        principal_idx=ctx["principal_idx"],
    )

    after = await resolve_host_filter(ctx["pool"], biosample_idx=human, platform=Platform.ILLUMINA)
    # The host is now KNOWN — that is this backfill's whole claim, and it is what
    # moves the sample off "host_taxon_id is not set".
    #
    # Deliberately NOT asserting on the outcome or the reason string: whether the
    # resolver can go on to FILTER depends on a host_filter_profile existing for
    # (9606, illumina), which is GLOBAL state that sibling tests seed and tear
    # down, and the suite runs -n auto. Coupling to it would be a flake.
    assert before.host_term_idx is None
    assert after.host_term_idx is not None


@pytest.mark.db
async def test_a_biosample_with_no_taxon_is_counted_not_silently_dropped(ctx):
    """The candidate query is driven FROM the taxon_id row, so a biosample without
    one cannot appear in it. That is a legitimate limitation (no taxon row means no
    study to hang the value on), but it must be REPORTED — an invisible sample is
    one nobody knows to curate. Counting it is what makes the residue honest."""
    async with ctx["pool"].acquire() as conn, conn.transaction():
        bare = await insert_biosample(
            conn, owner_idx=ctx["principal_idx"], created_by_idx=ctx["principal_idx"]
        )
        await insert_entity_to_study(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=bare,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample"].append(bare)

    plan = await plan_backfill(ctx["pool"])

    assert not any(a.biosample_idx == bare for a in plan.assignments)
    assert plan.no_taxon >= 1
