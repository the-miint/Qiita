"""Align planner / tiler for bulk-block sharded alignment.

The align analog of `block_planner`: decouples the COMPUTE unit (a fixed
~10M-read block) from the ACCOUNTING unit (per-sample completion). Given a pool's
samples + a sharded reference + an aligner, the planner:

  1. LOOKS UP each sample's already-minted host-depletion `mask_idx` (the same
     `_build_mask_params` shape the block-mask planner minted under — a pure
     lookup, NEVER a mint) and requires the sample's `mask_sample` gate to be
     `completed` (align only fully-masked samples);
  2. asserts the reference is ACTIVE + sharded (router + per-aligner shard rows)
     via the resolver, failing 4xx early otherwise;
  3. partitions the to-align samples by resolved `mask_idx` and mints one
     `alignment_idx` per partition over `{reference_idx, aligner, mask_idx,
     shard_ids}` (the mask-style identity, deduped fleet-wide);
  4. tiles each partition into ≤`_BLOCK_TARGET_READS`-read blocks (reusing the
     PURE `block_planner.tile_partition` over `qiita.sequence_range` bounds),
     persists the `block` / `block_member` cover-map + an `alignment_sample`
     PENDING gate per sample, creates one block `work_ticket` per block (carrying
     `mask_idx` AND `alignment_idx` + the align action_context), back-fills
     `block.work_ticket_idx`, and dispatches each.

The block stack (block / block_member / tiling / the block work_ticket + in-flight
gate) is WHY-agnostic and shared verbatim with read masking — only the WHY column
differs (`work_ticket.alignment_idx` instead of `mask_idx`) and the per-sample gate
is `alignment_sample` instead of `mask_sample`. The REST route is the only caller.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import asyncpg

from .actions.reference import ReferenceNotFound
from .block_planner import (
    _BLOCK_TARGET_READS,
    _MASK_FILTER_VERSION,
    _MASK_FILTER_WORKFLOW,
    _enumerate_pool_samples,
    tile_partition,
)
from .dispatch import schedule_dispatch
from .repositories.alignment_definition import mint_alignment_definition
from .repositories.block import (
    add_block_members,
    create_alignment_sample_pending,
    create_block,
    set_block_work_ticket,
)
from .repositories.mask_definition import lookup_mask_idx_by_params
from .repositories.reference_membership import reference_shard_ids
from .runner import (
    ReferenceIndexNotBuilt,
    _build_mask_params,
    _resolve_sharded_align_indexes,
)

if TYPE_CHECKING:
    from fastapi import FastAPI

# The action a block work_ticket is submitted against — the sharded `align`
# workflow (`workflows/align/1.0.0.yaml`, synced out-of-tree via `qiita-admin
# actions sync`).
ALIGN_ACTION_ID = "align"
ALIGN_ACTION_VERSION = "1.0.0"


class AlignReferenceNotFound(RuntimeError):
    """The requested `reference_idx` does not exist. The route maps this to 404."""


class AlignReferenceNotReady(RuntimeError):
    """The reference exists but cannot be aligned against: it is not ACTIVE, or is
    not sharded (no whole-reference rype router, or no per-aligner shard index
    built yet), or the aligner is unknown. The route maps this to 409 — the
    operator must build/shard the reference (or wait for it to go active) first."""


class AlignUnsupportedPlatform(RuntimeError):
    """The pool's sequencing platform has no defined sharded aligner. The route maps
    this to 422 — alignment is only supported for the platforms in
    `_ALIGNER_BY_PLATFORM` (short-read Illumina → bowtie2, long-read PacBio HiFi /
    Nanopore → minimap2); an exotic platform fails loud rather than defaulting."""


# Sharded aligner by sequencing platform: short reads align with bowtie2, long reads
# with minimap2. The CP resolves the aligner from `sequencing_run.platform` at
# align-plan time (it is NOT a caller choice), so the aligner always matches the read
# chemistry. Only the platforms with a defined mapping are alignable via the sharded
# path; anything else raises AlignUnsupportedPlatform rather than guessing.
_ALIGNER_BY_PLATFORM: dict[str, str] = {
    "illumina": "bowtie2",
    "pacbio_smrt": "minimap2",
    "oxford_nanopore": "minimap2",
}


def _aligner_for_platform(platform: str) -> str:
    """Map a `qiita.platform` value to its sharded aligner, or raise
    AlignUnsupportedPlatform for a platform with no defined mapping (fail-loud)."""
    try:
        return _ALIGNER_BY_PLATFORM[platform]
    except KeyError as exc:
        supported = ", ".join(sorted(_ALIGNER_BY_PLATFORM))
        raise AlignUnsupportedPlatform(
            f"no sharded aligner defined for platform {platform!r}; sharded alignment "
            f"supports only: {supported}"
        ) from exc


class AlignResubmitError(RuntimeError):
    """One or more requested samples already carry an `alignment_sample` gate for
    their resolved `alignment_idx`, so a fresh (`only_missing=False`) plan is
    refused — the alignment analog of `BlockMaskResubmitError`:

    - a COMPLETED gate → re-planning re-aligns reads already aligned,
      double-writing `alignment` rows (DuckLake has no uniqueness);
    - a still-PENDING gate → a prior plan's covering block is in-flight or failed,
      and minting a fresh same-footprint covering block would wedge the sample's
      finalize forever (`has_incomplete_covering_alignment_block` keeps seeing the
      stale non-completed block, so the gate never flips).

    An existing gate requires an explicit DELETE before resubmission. The operator
    either DELETEs the alignment first (to genuinely re-align) or passes
    `only_missing=true` to plan only the not-yet-gated samples. The route maps this
    to 409."""

    def __init__(self, conflicting_prep_sample_idxs: list[int]):
        self.conflicting_prep_sample_idxs = conflicting_prep_sample_idxs
        super().__init__(
            f"{len(conflicting_prep_sample_idxs)} sample(s) already have an alignment gate "
            "(pending or completed) for the resolved alignment config; a fresh plan would "
            "double-write a completed alignment or wedge an in-flight one. DELETE the "
            "alignment first to re-align, or pass only_missing=true to plan only the ungated "
            f"samples. prep_sample_idxs: {conflicting_prep_sample_idxs}"
        )


async def plan_and_submit_alignments(
    pool: asyncpg.Pool,
    *,
    app: FastAPI,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
    reference_idx: int,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
    only_missing: bool,
    adapter_set_hash: str | None,
    originator_principal_idx: int,
    align_action_id: str,
    align_action_version: str,
    target_reads: int = _BLOCK_TARGET_READS,
) -> dict[str, Any]:
    """Plan + submit a pool's bulk-block sharded-alignment work.

    Resolves each sample's already-minted `mask_idx` at submit time (a LOOKUP, not
    a mint) and requires its `mask_sample` gate `completed`; partitions the
    to-align samples by `mask_idx`, mints one `alignment_idx` per partition, tiles
    each into ≤`target_reads`-read blocks, then in ONE transaction persists the
    `block` / `block_member` cover-map, a PENDING `alignment_sample` gate per
    sample, and one block `work_ticket` per block (scope `block`, carrying the
    partition's `mask_idx` + `alignment_idx` + the align `action_context`),
    back-filling `block.work_ticket_idx`. After commit each ticket is dispatched.

    `adapter_set_hash` is resolved by the caller (matching the per-sample read-mask
    mint) so the mask LOOKUP finds the same mask the block-mask plan minted.
    `only_missing` drops samples already carrying an `alignment_sample` row for
    their resolved alignment (an interrupted plan re-runs only the gap). On a fresh
    plan any already-gated sample raises `AlignResubmitError` (409).

    Raises `AlignReferenceNotFound` (404) / `AlignReferenceNotReady` (409) if the
    reference can't be aligned against; asyncpg errors on a genuine DB fault (fail
    loud). Samples that can't be planned are reported (not raised) in the
    `samples_skipped_*` counts.
    """
    # The aligner is derived from the run's PLATFORM (short-read Illumina → bowtie2,
    # long-read PacBio HiFi / Nanopore → minimap2), NOT chosen by the caller, so it
    # always matches the read chemistry. instrument_model is part of the mask
    # identity (gates QC polyG); read both in one row. `platform` is NOT NULL in the
    # schema, so a missing row would surface as an AttributeError on `.` below (the
    # run existence is fronted by the route's `require_sequencing_run_exists`).
    run_row = await pool.fetchrow(
        "SELECT platform, instrument_model FROM qiita.sequencing_run WHERE idx = $1",
        sequencing_run_idx,
    )
    aligner = _aligner_for_platform(run_row["platform"])
    instrument_model = run_row["instrument_model"]

    # Assert ACTIVE + sharded (fail-fast; the route maps the typed errors to 4xx).
    # We don't need the resolved paths here — the runner resolves them per block at
    # dispatch — only the readiness guarantee before we mint anything.
    try:
        await _resolve_sharded_align_indexes(pool, reference_idx, aligner)
    except ReferenceNotFound as exc:
        raise AlignReferenceNotFound(str(exc)) from exc
    except ReferenceIndexNotBuilt as exc:
        # A ValueError subclass — caught before the bare ValueError arm. The
        # reference is active but its router / per-shard index isn't built yet.
        raise AlignReferenceNotReady(str(exc)) from exc
    except ValueError as exc:
        # Non-active reference, or an unknown aligner (both ValueError).
        raise AlignReferenceNotReady(str(exc)) from exc

    # The reference's current shard-set, baked into the alignment identity
    # (`mint_alignment_definition`) so a grown reference mints a NEW alignment_idx
    # over only its new shards (the growth foundation). Non-empty by construction
    # here: the caller already asserted a per-shard index is built, which is only
    # registered after the shard assignment stamped these rows.
    shard_ids = await reference_shard_ids(pool, reference_idx)

    all_samples = await _enumerate_pool_samples(pool, sequenced_pool_idx)

    # ACTIVE pool samples whose reads were never ingested (no sequence_range) can't
    # be tiled — report them (mirrors block_planner). Retired samples are already
    # excluded from all_samples, so this count matches the active set.
    skipped_no_reads = await pool.fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        "  LEFT JOIN qiita.sequence_range sr ON sr.prep_sample_idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1 AND ps.retired = false"
        "   AND sr.prep_sample_idx IS NULL",
        sequenced_pool_idx,
    )

    # LOOK UP mask_idx per sample (never mint). Memoize by prep_protocol_idx — the
    # only per-sample input to the mask identity (everything else is pool-constant).
    mask_by_protocol: dict[int | None, int | None] = {}
    for protocol_idx in {s.prep_protocol_idx for s in all_samples}:
        params = _build_mask_params(
            action_id=_MASK_FILTER_WORKFLOW,
            action_version=_MASK_FILTER_VERSION,
            prep_protocol_idx=protocol_idx,
            instrument_model=instrument_model,
            adapter_set_hash=adapter_set_hash,
            host_rype_reference_idx=host_rype_reference_idx,
            host_minimap2_reference_idx=host_minimap2_reference_idx,
            # Align looks up BLOCK masks (it aligns block-masked reads), and the
            # block read-mask workflow is `qc -> host_filter` only — it has no lima
            # chain and no syndna step, so a block mask never carries either. This
            # MUST mirror block_planner's mint call exactly or the reconstructed
            # hash won't match the mask the block path minted.
            resolved_lima=None,
            resolved_syndna=None,
        )
        mask_by_protocol[protocol_idx] = await lookup_mask_idx_by_params(pool, params)

    # A sample with no minted mask for its config was never block-masked under this
    # host config — skip it (align only masked samples).
    samples_with_mask = [
        (s, mask_by_protocol[s.prep_protocol_idx])
        for s in all_samples
        if mask_by_protocol[s.prep_protocol_idx] is not None
    ]
    skipped_no_mask = len(all_samples) - len(samples_with_mask)

    # Require the sample's mask to be COMPLETED (the read-mask-block milestone flips
    # mask_sample once every covering block is done). Align only fully-masked
    # samples — a partially-masked sample would align an incomplete read set. One
    # batched query over the (mask_idx, prep_sample_idx) pairs.
    completed_prep_sample_idxs: set[int] = set()
    if samples_with_mask:
        completed_rows = await pool.fetch(
            "SELECT ms.prep_sample_idx FROM qiita.mask_sample ms"
            "  JOIN unnest($1::bigint[], $2::bigint[]) AS t(mask_idx, prep_sample_idx)"
            "    ON ms.mask_idx = t.mask_idx AND ms.prep_sample_idx = t.prep_sample_idx"
            " WHERE ms.state = 'completed'",
            [m for (_s, m) in samples_with_mask],
            [s.prep_sample_idx for (s, _m) in samples_with_mask],
        )
        completed_prep_sample_idxs = {r["prep_sample_idx"] for r in completed_rows}
    to_consider = [
        (s, m) for (s, m) in samples_with_mask if s.prep_sample_idx in completed_prep_sample_idxs
    ]
    skipped_mask_incomplete = len(samples_with_mask) - len(to_consider)

    # Mint one alignment_idx per DISTINCT resolved mask (idempotent upsert on the
    # config hash — a re-plan of the same config resolves to the same
    # alignment_idx). shard_ids is reference-constant, so different partitions
    # differ only by mask_idx. The real per-mask sample grouping is `plan_partitions`
    # below (built over `to_plan` after only_missing drops gated samples) — here we
    # only need the distinct mask set to mint over.
    alignment_by_mask: dict[int, int] = {}
    async with pool.acquire() as conn:
        for mask_idx in {m for (_s, m) in to_consider}:
            row = await mint_alignment_definition(
                conn,
                params={
                    "reference_idx": reference_idx,
                    "aligner": aligner,
                    "mask_idx": mask_idx,
                    "shard_ids": shard_ids,
                },
                principal_idx=originator_principal_idx,
            )
            alignment_by_mask[mask_idx] = row["alignment_idx"]

    # only_missing: drop samples already gated under their resolved alignment (a
    # prior plan reached them) so an interrupted plan re-runs only the gap. One
    # batched query over the (alignment_idx, prep_sample_idx) pairs.
    skipped_existing = 0
    to_plan = list(to_consider)
    if only_missing and to_consider:
        gated = await pool.fetch(
            "SELECT als.alignment_idx, als.prep_sample_idx FROM qiita.alignment_sample als"
            "  JOIN unnest($1::bigint[], $2::bigint[]) AS t(alignment_idx, prep_sample_idx)"
            "    ON als.alignment_idx = t.alignment_idx"
            "   AND als.prep_sample_idx = t.prep_sample_idx",
            [alignment_by_mask[m] for (_s, m) in to_consider],
            [s.prep_sample_idx for (s, _m) in to_consider],
        )
        gated_pairs = {(r["alignment_idx"], r["prep_sample_idx"]) for r in gated}
        to_plan = [
            (s, m)
            for (s, m) in to_consider
            if (alignment_by_mask[m], s.prep_sample_idx) not in gated_pairs
        ]
        skipped_existing = len(to_consider) - len(to_plan)

    # disallow-without-delete: on a fresh plan (only_missing=False) refuse to
    # re-plan ANY sample already carrying an alignment_sample gate for its resolved
    # alignment — pending (a prior in-flight/failed plan; a fresh same-footprint
    # block would wedge finalize) or completed (re-aligning double-writes rows).
    # `only_missing` already dropped all gated samples above, so this fires only
    # when only_missing is False. One batched query over the pairs.
    if not only_missing and to_plan:
        conflicting = await pool.fetch(
            "SELECT als.prep_sample_idx FROM qiita.alignment_sample als"
            "  JOIN unnest($1::bigint[], $2::bigint[]) AS t(alignment_idx, prep_sample_idx)"
            "    ON als.alignment_idx = t.alignment_idx"
            "   AND als.prep_sample_idx = t.prep_sample_idx",
            [alignment_by_mask[m] for (_s, m) in to_plan],
            [s.prep_sample_idx for (s, _m) in to_plan],
        )
        if conflicting:
            raise AlignResubmitError(sorted(r["prep_sample_idx"] for r in conflicting))

    # Re-partition the to-plan samples by mask_idx (only_missing may have dropped
    # some), preserving the minted alignment_idx per partition.
    plan_partitions: dict[int, list[Any]] = {}
    for s, m in to_plan:
        plan_partitions.setdefault(m, []).append(s)

    # Persist the whole plan in ONE transaction — a partial plan must roll back
    # (the alignment_definitions minted above are idempotent and survive a rollback
    # harmlessly).
    block_summaries: list[dict[str, Any]] = []
    partition_summaries: list[dict[str, Any]] = []
    async with pool.acquire() as conn, conn.transaction():
        for mask_idx, samples in sorted(plan_partitions.items()):
            alignment_idx = alignment_by_mask[mask_idx]
            await create_alignment_sample_pending(
                conn,
                alignment_idx=alignment_idx,
                prep_sample_idxs=[s.prep_sample_idx for s in samples],
            )
            # The align block ticket's action_context: the sharded-index resolver
            # keys on align_reference_idx + aligner; alignment_idx rides through the
            # step's params (+ is stamped on every output row); align_mask_idx is a
            # provenance mirror of the ticket's mask_idx.
            action_context_json = json.dumps(
                {
                    "align_reference_idx": reference_idx,
                    "aligner": aligner,
                    "alignment_idx": alignment_idx,
                    "align_mask_idx": mask_idx,
                }
            )
            blocks = tile_partition([s.sample_range for s in samples], target_reads=target_reads)
            for members in blocks:
                block_idx = await create_block(conn)
                await add_block_members(
                    conn,
                    block_idx=block_idx,
                    members=[
                        (m.prep_sample_idx, m.min_sequence_idx, m.max_sequence_idx) for m in members
                    ],
                )
                work_ticket_idx = await conn.fetchval(
                    "INSERT INTO qiita.work_ticket ("
                    "  action_id, action_version, originator_principal_idx,"
                    "  scope_target_kind, block_idx, mask_idx, alignment_idx, action_context"
                    ") VALUES ($1, $2, $3, 'block', $4, $5, $6, $7::jsonb)"
                    " RETURNING work_ticket_idx",
                    align_action_id,
                    align_action_version,
                    originator_principal_idx,
                    block_idx,
                    mask_idx,
                    alignment_idx,
                    action_context_json,
                )
                await set_block_work_ticket(
                    conn, block_idx=block_idx, work_ticket_idx=work_ticket_idx
                )
                block_summaries.append(
                    {
                        "block_idx": block_idx,
                        "work_ticket_idx": work_ticket_idx,
                        "alignment_idx": alignment_idx,
                        "mask_idx": mask_idx,
                        "member_count": len(members),
                        "read_count": sum(
                            m.max_sequence_idx - m.min_sequence_idx + 1 for m in members
                        ),
                    }
                )
            partition_summaries.append(
                {
                    "alignment_idx": alignment_idx,
                    "mask_idx": mask_idx,
                    "sample_count": len(samples),
                    "block_count": len(blocks),
                }
            )

    # Post-commit, fire-and-forget dispatch of each fresh PENDING block ticket.
    for b in block_summaries:
        schedule_dispatch(app, b["work_ticket_idx"])

    return {
        "sequencing_run_idx": sequencing_run_idx,
        "sequenced_pool_idx": sequenced_pool_idx,
        "reference_idx": reference_idx,
        "aligner": aligner,
        "host_rype_reference_idx": host_rype_reference_idx,
        "host_minimap2_reference_idx": host_minimap2_reference_idx,
        "samples_planned": len(to_plan),
        "samples_skipped_existing": skipped_existing,
        "samples_skipped_no_mask": skipped_no_mask,
        "samples_skipped_mask_incomplete": skipped_mask_incomplete,
        "samples_skipped_no_reads": skipped_no_reads,
        "partitions": partition_summaries,
        "blocks": block_summaries,
        "blocks_created": len(block_summaries),
    }
