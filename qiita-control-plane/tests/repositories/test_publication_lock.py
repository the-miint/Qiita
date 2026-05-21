"""Tests for the publication-lock infrastructure (migration
20260520000000_publication_lock.sql).

The corrected model:

1. **`is_published` is set only by a deliberate publish action.** ENA
   accessions do NOT publish anything — an accession records a
   submission (which may sit under embargo), not a publication. There is
   no cascade. Tests simulate the future owner-driven publish action
   with a direct `UPDATE prep_sample_to_study SET is_published = TRUE`.

2. **Lock** — once `is_published = TRUE` on any of a prep's links, the
   BEFORE UPDATE trigger family rejects further mutation on the
   prep_sample, its sequenced_sample subtype, its prep_sample_metadata,
   the published link itself, and the underlying biosample / its
   metadata / its study link.

3. **Direction** — publication freezes upward (a published prep freezes
   its biosample) but never downward: a sibling prep on the same,
   now-frozen biosample stays mutable as long as it is not itself
   published.

The publish action's own FALSE -> TRUE UPDATE on the link is permitted:
pre-write the link is still unpublished, so the lock lets it through.

The never-DELETE guardrail on prep_sample / sequenced_sample is
deliberately NOT covered here -- it's deferred from this migration
because it would conflict with the existing test-cleanup helper that
DELETEs prep_samples.
"""

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_prep_sample,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Seed helper (committed; cleanup via the ctx fixture's FK-reverse sweep)
# ---------------------------------------------------------------------------


async def _seed_prep(ctx, *, publish=False):
    """Seed `biosample -> prep_sample -> sequenced_sample -> link(study)`.

    When `publish=True`, flip `is_published = TRUE` on the study link
    after seeding — simulating the future owner-driven publish action.
    Returns the relevant idxs.
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
    # co-populated pool/pool_item_id pair stays NULL/NULL.
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

    if publish:
        # The publish action: FALSE -> TRUE on the link. OLD.is_published
        # is still FALSE here, so the link lock permits this UPDATE.
        await pool.execute(
            "UPDATE qiita.prep_sample_to_study SET is_published = TRUE"
            " WHERE prep_sample_idx = $1 AND study_idx = $2",
            prep_sample_idx,
            ctx["study_idx"],
        )

    return {
        "biosample_idx": biosample_idx,
        "prep_sample_idx": prep_sample_idx,
        "sequenced_sample_idx": sequenced_sample_idx,
        "study_idx": ctx["study_idx"],
    }


async def _is_published(pool, prep_sample_idx, study_idx):
    return await pool.fetchval(
        "SELECT is_published FROM qiita.prep_sample_to_study"
        " WHERE prep_sample_idx = $1 AND study_idx = $2",
        prep_sample_idx,
        study_idx,
    )


# ---------------------------------------------------------------------------
# Column default + the publish path
# ---------------------------------------------------------------------------


async def test_is_published_defaults_false_on_new_link(ctx):
    """A freshly-inserted prep_sample_to_study link defaults to
    is_published = FALSE — publication is opt-in, never implicit."""
    seeded = await _seed_prep(ctx)
    assert await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["study_idx"]) is False


async def test_publish_flip_false_to_true_is_allowed(ctx):
    """The publish action — UPDATE is_published FALSE -> TRUE — is not
    blocked by the link's own lock trigger: pre-write OLD.is_published is
    still FALSE, so the lock lets the publishing UPDATE through."""
    seeded = await _seed_prep(ctx)
    await ctx["pool"].execute(
        "UPDATE qiita.prep_sample_to_study SET is_published = TRUE"
        " WHERE prep_sample_idx = $1 AND study_idx = $2",
        seeded["prep_sample_idx"],
        seeded["study_idx"],
    )
    assert await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["study_idx"]) is True


async def test_setting_ena_accession_does_not_publish(ctx):
    """Setting an ENA accession on a sequenced_sample records a
    submission — it does NOT publish the prep. The link stays
    is_published = FALSE and the sequenced_sample remains mutable."""
    seeded = await _seed_prep(ctx)
    await ctx["pool"].execute(
        "UPDATE qiita.sequenced_sample SET ena_experiment_accession = $2 WHERE idx = $1",
        seeded["sequenced_sample_idx"],
        "ERX1234567",
    )
    assert await _is_published(ctx["pool"], seeded["prep_sample_idx"], seeded["study_idx"]) is False
    # Still mutable — no lock engaged by the accession. A self-UPDATE
    # fires the BEFORE UPDATE lock trigger and must pass.
    await ctx["pool"].execute(
        "UPDATE qiita.sequenced_sample SET created_by_idx = created_by_idx WHERE idx = $1",
        seeded["sequenced_sample_idx"],
    )


# ---------------------------------------------------------------------------
# Lock — a published prep blocks UPDATEs
# ---------------------------------------------------------------------------


async def test_lock_blocks_prep_sample_update_when_published(ctx):
    """Once a prep has a published link, the BEFORE UPDATE trigger on
    prep_sample rejects further mutation, including the retire path
    (which is itself an UPDATE)."""
    seeded = await _seed_prep(ctx, publish=True)
    with pytest.raises(asyncpg.RaiseError, match="prep_sample .* is published"):
        await ctx["pool"].execute(
            "UPDATE qiita.prep_sample SET retired = TRUE, retired_at = now(),"
            " retired_by_idx = $2, retire_reason = 'test' WHERE idx = $1",
            seeded["prep_sample_idx"],
            SYSTEM_PRINCIPAL_IDX,
        )


async def test_lock_blocks_sequenced_sample_update_when_published(ctx):
    """The sequenced_sample subtype of a published prep is frozen — an
    UPDATE on it (here, setting an ENA accession) is rejected."""
    seeded = await _seed_prep(ctx, publish=True)
    with pytest.raises(asyncpg.RaiseError, match="sequenced_sample .* is on a published"):
        await ctx["pool"].execute(
            "UPDATE qiita.sequenced_sample SET ena_experiment_accession = $2 WHERE idx = $1",
            seeded["sequenced_sample_idx"],
            "ERX9999999",
        )


async def test_lock_blocks_published_link_retire(ctx):
    """Retiring a published link would silently strand the public-facing
    pointer; the lock on prep_sample_to_study rejects the UPDATE."""
    seeded = await _seed_prep(ctx, publish=True)
    with pytest.raises(asyncpg.RaiseError, match="prep_sample_to_study.* is published"):
        await ctx["pool"].execute(
            "UPDATE qiita.prep_sample_to_study"
            "    SET retired = TRUE, retired_at = now(), retired_by_idx = $3,"
            "        retire_reason = 'test'"
            "  WHERE prep_sample_idx = $1 AND study_idx = $2",
            seeded["prep_sample_idx"],
            seeded["study_idx"],
            SYSTEM_PRINCIPAL_IDX,
        )


async def test_lock_blocks_biosample_update_when_reaching_published_prep(ctx):
    """Upward freeze: a biosample referenced by a published prep is
    itself frozen — the published prep's record sits on the specimen."""
    seeded = await _seed_prep(ctx, publish=True)
    with pytest.raises(asyncpg.RaiseError, match="biosample .* is referenced by a published"):
        await ctx["pool"].execute(
            "UPDATE qiita.biosample SET biosample_accession = $2 WHERE idx = $1",
            seeded["biosample_idx"],
            "SAMN999999",
        )


# ---------------------------------------------------------------------------
# Direction — publication does not freeze downward
# ---------------------------------------------------------------------------


async def test_published_prep_does_not_freeze_sibling_prep(ctx):
    """A published prep freezes its biosample (upward), but a sibling
    prep on that same, now-frozen biosample stays mutable as long as it
    is not itself published — a specimen can carry unpublished preps."""
    seeded = await _seed_prep(ctx, publish=True)
    biosample_idx = seeded["biosample_idx"]

    # A second, independent prep on the SAME biosample, linked to the
    # same study, never published.
    sibling_prep_idx = await seed_sequenced_prep_sample(
        ctx["pool"],
        biosample_idx=biosample_idx,
        owner_idx=ctx["biosample_owner_idx"],
    )
    ctx["created"]["prep_sample"].append(sibling_prep_idx)
    await ctx["pool"].execute(
        "INSERT INTO qiita.prep_sample_to_study"
        " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
        sibling_prep_idx,
        ctx["study_idx"],
        ctx["principal_idx"],
    )
    ctx["created"]["prep_sample_to_study"].append((sibling_prep_idx, ctx["study_idx"]))

    # The biosample is frozen — it reaches the published prep.
    with pytest.raises(asyncpg.RaiseError, match="biosample .* is referenced by a published"):
        await ctx["pool"].execute(
            "UPDATE qiita.biosample SET biosample_accession = $2 WHERE idx = $1",
            biosample_idx,
            "SAMN_FROZEN",
        )

    # The sibling prep is NOT frozen — it has no published link. A
    # self-UPDATE fires the BEFORE UPDATE lock trigger and must pass.
    await ctx["pool"].execute(
        "UPDATE qiita.prep_sample SET owner_idx = owner_idx WHERE idx = $1",
        sibling_prep_idx,
    )
    assert await _is_published(ctx["pool"], sibling_prep_idx, ctx["study_idx"]) is False
