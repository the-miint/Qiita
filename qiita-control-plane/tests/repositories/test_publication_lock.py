"""Tests for the publication-lock infrastructure (migration
20260520000000_publication_lock.sql).

Three behaviors under test:

1. **Cascade** — an UPDATE on sequenced_sample setting any ENA accession
   from NULL to non-NULL, or on biosample setting ena_sample_accession
   from NULL to non-NULL, flips `is_published = TRUE` on EVERY
   prep_sample_to_study link of the affected prep(s).

2. **Lock** — once `is_published = TRUE` on any of a prep's links, the
   BEFORE UPDATE trigger family rejects further mutation on the
   prep_sample, its sequenced_sample subtype, its prep_sample_metadata,
   the published link itself, and the underlying biosample / its
   metadata / its study link.

3. **First-publication path** — the first UPDATE that *sets* an ENA
   accession is NOT rejected by the lock (pre-write the row is still
   pre-publication); the cascade then flips the flag and subsequent
   UPDATEs are rejected.

The never-DELETE guardrail on prep_sample / sequenced_sample is
deliberately NOT covered here -- it's deferred from this migration
because it would conflict with the existing test-cleanup helper that
DELETEs prep_samples.
"""

import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Seed helpers (committed; cleanup via the ctx fixture's FK-reverse sweep)
# ---------------------------------------------------------------------------


async def _seed_published_prep(ctx, *, ena_experiment_accession=None, ena_run_accession=None):
    """Seed `biosample -> prep_sample -> sequenced_sample -> link(study)`
    plus a second study link, and optionally trigger the
    sequenced_sample-side ENA cascade by setting one or both accessions
    in the same call. Returns the relevant idxs.

    The two study links exercise the multi-study cascade: both should
    end up `is_published = TRUE` when any ENA accession transitions
    NULL -> non-NULL.
    """
    pool = ctx["pool"]
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=ctx["biosample_owner_idx"]
    )
    ctx["created"]["biosample"].append(biosample_idx)
    ctx["created"]["prep_sample"].append(prep_sample_idx)
    # biosample_to_study is required for the prep_sample_to_study
    # reject_without_biosample_link trigger to pass.
    await pool.execute(
        "INSERT INTO qiita.biosample_to_study"
        " (biosample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
        biosample_idx,
        ctx["study_idx"],
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_to_study"].append((biosample_idx, ctx["study_idx"]))
    # The minimal sequenced_sample subtype row, no pool linkage so the
    # co-populated pool/pool_item_id pair stays NULL/NULL. The subtype has
    # its own GENERATED ALWAYS idx; the back-pointer prep_sample_idx is
    # the cascade/lock join key.
    sequenced_sample_idx = await pool.fetchval(
        "INSERT INTO qiita.sequenced_sample (prep_sample_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        prep_sample_idx,
        ctx["principal_idx"],
    )
    ctx["created"]["sequenced_sample"].append(sequenced_sample_idx)
    await pool.execute(
        "INSERT INTO qiita.prep_sample_to_study"
        " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
        prep_sample_idx,
        ctx["study_idx"],
        ctx["principal_idx"],
    )
    ctx["created"]["prep_sample_to_study"].append((prep_sample_idx, ctx["study_idx"]))

    # Second study so the cascade-across-links case has something to flip.
    second_study_idx = await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx) VALUES ($1, $2, $1)"
        " RETURNING idx",
        ctx["principal_idx"],
        f"second-study-{biosample_idx}",
    )
    ctx["created"]["studies"].append(second_study_idx)
    await pool.execute(
        "INSERT INTO qiita.biosample_to_study"
        " (biosample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
        biosample_idx,
        second_study_idx,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_to_study"].append((biosample_idx, second_study_idx))
    await pool.execute(
        "INSERT INTO qiita.prep_sample_to_study"
        " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
        prep_sample_idx,
        second_study_idx,
        ctx["principal_idx"],
    )
    ctx["created"]["prep_sample_to_study"].append((prep_sample_idx, second_study_idx))

    if ena_experiment_accession is not None or ena_run_accession is not None:
        await pool.execute(
            "UPDATE qiita.sequenced_sample"
            "    SET ena_experiment_accession = $2, ena_run_accession = $3"
            "  WHERE idx = $1",
            sequenced_sample_idx,
            ena_experiment_accession,
            ena_run_accession,
        )

    return {
        "biosample_idx": biosample_idx,
        "prep_sample_idx": prep_sample_idx,
        "sequenced_sample_idx": sequenced_sample_idx,
        "primary_study_idx": ctx["study_idx"],
        "secondary_study_idx": second_study_idx,
    }


async def _is_published(pool, prep_sample_idx, study_idx):
    return await pool.fetchval(
        "SELECT is_published FROM qiita.prep_sample_to_study"
        " WHERE prep_sample_idx = $1 AND study_idx = $2",
        prep_sample_idx,
        study_idx,
    )


# ---------------------------------------------------------------------------
# Column + default
# ---------------------------------------------------------------------------


async def test_is_published_defaults_false_on_new_link(ctx):
    """A freshly-inserted prep_sample_to_study link defaults to
    is_published = FALSE — published is opt-in via cascade or explicit set."""
    seeded = await _seed_published_prep(ctx)
    assert (
        await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["primary_study_idx"])
        is False
    )
    assert (
        await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["secondary_study_idx"])
        is False
    )


# ---------------------------------------------------------------------------
# Cascade
# ---------------------------------------------------------------------------


async def test_cascade_publishes_all_links_on_sequenced_sample_ena_experiment(ctx):
    """Setting ena_experiment_accession on sequenced_sample (NULL -> set)
    flips is_published = TRUE on EVERY prep_sample_to_study link of that
    prep, not just the primary."""
    seeded = await _seed_published_prep(ctx, ena_experiment_accession="ERX1234567")
    assert (
        await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["primary_study_idx"])
        is True
    )
    assert (
        await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["secondary_study_idx"])
        is True
    )


# NB: a parallel test for ena_run_accession isn't included because the
# sequenced_sample_run_accession_requires_run CHECK refuses to let that
# column be set on a pool-less subtype row, and _seed_published_prep
# doesn't attach a sequencing_run + sequenced_pool. The cascade trigger's
# ena_run branch is structurally identical to the ena_experiment branch
# (same UPDATE on prep_sample_to_study, same WHERE clause); coverage
# doesn't depend on exercising both at the integration layer. Adding a
# run+pool seed helper lands with the future "qiita prep-sample publish"
# action commit.


async def test_cascade_publishes_links_on_biosample_ena_sample(ctx):
    """Setting biosample.ena_sample_accession (NULL -> set) flips every
    prep_sample_to_study link of every prep that references this
    biosample. Cross-prep span is intentional: once the biosample is in
    ENA, every downstream prep is observationally public too."""
    seeded = await _seed_published_prep(ctx)
    await ctx["pool"].execute(
        "UPDATE qiita.biosample SET ena_sample_accession = $2 WHERE idx = $1",
        seeded["biosample_idx"],
        "SAMEA1234567",
    )
    assert (
        await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["primary_study_idx"])
        is True
    )
    assert (
        await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["secondary_study_idx"])
        is True
    )


# NB: there's no separate test for "cascade only fires on NULL -> set, not
# on set -> set." Once a sequenced_sample has any ENA accession, the lock
# trigger rejects any further UPDATE on the row, so a set-to-set
# transition is unreachable in practice. The cascade trigger's
# IS NULL guard is defense-in-depth but lives behind a closed door.


# ---------------------------------------------------------------------------
# Lock — first-publication path is permitted
# ---------------------------------------------------------------------------


async def test_first_ena_set_on_sequenced_sample_succeeds(ctx):
    """The lock trigger on sequenced_sample looks at OLD's prep
    publication state. Pre-publication, is_published is FALSE on every
    link, so the lock allows the UPDATE; the cascade then flips the
    flag AFTER the row write. Result: the publishing UPDATE itself is
    never rejected."""
    seeded = await _seed_published_prep(ctx)
    # No exception expected.
    await ctx["pool"].execute(
        "UPDATE qiita.sequenced_sample SET ena_experiment_accession = $2 WHERE idx = $1",
        seeded["prep_sample_idx"],
        "ERX_FIRST_SET",
    )


# ---------------------------------------------------------------------------
# Lock — published prep blocks subsequent UPDATEs
# ---------------------------------------------------------------------------


async def test_lock_blocks_prep_sample_update_after_publication(ctx):
    """Once any of a prep's links is is_published = TRUE, a BEFORE
    UPDATE trigger on prep_sample rejects further mutation, including
    the retire path (which is itself an UPDATE)."""
    import asyncpg

    seeded = await _seed_published_prep(ctx, ena_experiment_accession="ERX1234567")
    with pytest.raises(asyncpg.RaiseError, match="prep_sample .* is published"):
        await ctx["pool"].execute(
            "UPDATE qiita.prep_sample SET retired = TRUE, retired_at = now(),"
            " retired_by_idx = $2, retire_reason = 'test' WHERE idx = $1",
            seeded["prep_sample_idx"],
            SYSTEM_PRINCIPAL_IDX,
        )


async def test_lock_blocks_prep_sample_to_study_link_retire_when_published(ctx):
    """Retiring a published link would silently strand the public-facing
    pointer; the lock on prep_sample_to_study itself rejects the UPDATE."""
    import asyncpg

    seeded = await _seed_published_prep(ctx, ena_experiment_accession="ERX1234567")
    with pytest.raises(asyncpg.RaiseError, match="prep_sample_to_study.* is published"):
        await ctx["pool"].execute(
            "UPDATE qiita.prep_sample_to_study"
            "    SET retired = TRUE, retired_at = now(), retired_by_idx = $3,"
            "        retire_reason = 'test'"
            "  WHERE prep_sample_idx = $1 AND study_idx = $2",
            seeded["prep_sample_idx"],
            seeded["primary_study_idx"],
            SYSTEM_PRINCIPAL_IDX,
        )


async def test_lock_blocks_biosample_update_when_reaching_published_prep(ctx):
    """A biosample referenced by a published prep is itself frozen,
    even though biosample.ena_sample_accession was never set on it
    directly (the publication came via sequenced_sample's ENA accession)."""
    import asyncpg

    seeded = await _seed_published_prep(ctx, ena_experiment_accession="ERX1234567")
    with pytest.raises(asyncpg.RaiseError, match="biosample .* is referenced by a published"):
        await ctx["pool"].execute(
            "UPDATE qiita.biosample SET biosample_accession = $2 WHERE idx = $1",
            seeded["biosample_idx"],
            "SAMN999999",
        )


async def test_lock_blocks_second_ena_change_on_sequenced_sample(ctx):
    """After the first ENA set publishes the prep, a second UPDATE on
    the same sequenced_sample (changing or clearing the accession) is
    rejected by the publication lock."""
    import asyncpg

    seeded = await _seed_published_prep(ctx, ena_experiment_accession="ERX_FIRST")
    with pytest.raises(asyncpg.RaiseError, match="sequenced_sample .* is on a published"):
        await ctx["pool"].execute(
            "UPDATE qiita.sequenced_sample SET ena_experiment_accession = $2 WHERE idx = $1",
            seeded["sequenced_sample_idx"],
            "ERX_SECOND",
        )
