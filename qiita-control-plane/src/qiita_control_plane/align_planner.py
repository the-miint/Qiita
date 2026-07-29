"""Align planner / tiler for bulk-block sharded alignment.

The align analog of `block_planner`: decouples the COMPUTE unit (a block, sized per
PLATFORM — ~10M reads short-read, ~1M long-read; see
`_BLOCK_TARGET_READS_BY_PLATFORM`) from the ACCOUNTING unit (per-sample completion).
Given a pool's samples + a sharded reference + a caller-named `mask_idx`, the
planner:

  1. selects the pool's samples whose `mask_sample` gate is `completed` under the
     caller-named `mask_idx`. Alignment does NOT re-derive the mask config — it is
     TOLD which mask its reads were produced under (a nonexistent `mask_idx` is a
     404; a pool with no sample masked-complete under it is a 422);
  2. asserts the reference is ACTIVE + sharded (router + per-aligner shard rows)
     via the resolver, failing 4xx early otherwise;
  3. mints one `alignment_idx` over `{reference_idx, aligner, mask_idx, shard_ids}`
     (the mask-style identity, deduped fleet-wide) for the single mask being aligned;
  4. tiles the to-align samples into blocks of ≤ the platform's read target (reusing
     the PURE `block_planner.tile_partition` over `qiita.sequence_range` bounds),
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
    _enumerate_pool_samples,
    tile_partition,
)
from .dispatch import schedule_dispatch
from .fanout_dispatch import align_block_cohort, top_up_dispatch
from .repositories.alignment_definition import mint_alignment_definition
from .repositories.block import (
    add_block_members,
    create_alignment_sample_pending,
    create_block,
    set_block_work_ticket,
)
from .repositories.reference_membership import reference_shard_ids
from .runner import (
    ReferenceIndexNotBuilt,
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


class AlignMaskNotFound(RuntimeError):
    """The requested `mask_idx` does not exist. The caller names the mask to align
    under (a client-supplied identifier), so a nonexistent one is a 404 — distinct
    from a valid mask under which no pool sample is masked-complete
    (`AlignNoMasksFound`, 422)."""


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

# Reads per block for a LONG-READ platform. A block is one work ticket / SLURM job,
# so this is the per-job input size; the short-read value is `_BLOCK_TARGET_READS`
# (10M), sized on read COUNT because short-read work is count-bound.
#
# Long reads need a smaller block because the sharded aligner's cost is driven by
# BYTES, not reads. Each of the reference's ~1000 shards re-reads the block's reads to
# pull its own routed subset, so the total re-scan is `n_shards x block_bytes` per
# block — and summed over a sample's blocks that is `n_shards x total_bytes`,
# INVARIANT to block size. What block size does control is the per-JOB share of it,
# i.e. whether one ticket fits its walltime. At ~15 kb/read a 10M-read HiFi block is
# ~150 GB, so its re-scan alone is ~4.5 h against the align step's PT4H baseline —
# the job cannot finish. At 1M reads (~15 GB) the same work is ~27 min.
#
# 1M is also what makes the aligner's query relation materializable: ~15-20 GB fits
# the step's resolved ~57 GB DuckDB limit, where a 10M-read block does not and would
# spill the whole block to shared scratch.
#
# This is deliberately NOT a change to `block_planner._BLOCK_TARGET_READS`, which
# read masking also uses: masking has no 1000-shard fan-out, so its cost is linear in
# the block and 10M stays right there. Whether long-read MASK blocks want a different
# size is a separate question, not settled here.
_LONG_READ_BLOCK_TARGET_READS = 1_000_000

# Reads per block by platform. Every platform in `_ALIGNER_BY_PLATFORM` MUST appear
# here — `test_block_target_covers_every_aligned_platform` fails otherwise, so adding
# a platform forces an explicit decision about its block size instead of inheriting
# the short-read default silently.
_BLOCK_TARGET_READS_BY_PLATFORM: dict[str, int] = {
    "illumina": _BLOCK_TARGET_READS,
    "pacbio_smrt": _LONG_READ_BLOCK_TARGET_READS,
    "oxford_nanopore": _LONG_READ_BLOCK_TARGET_READS,
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


def _block_target_for_platform(platform: str) -> int:
    """Reads per align block for `platform` (see `_BLOCK_TARGET_READS_BY_PLATFORM`).

    Callers reach this only after `_aligner_for_platform` has accepted the platform,
    so a KeyError here means the two maps have drifted — raise rather than fall back
    to the short-read default, which would hand a long-read pool 10M-read blocks that
    cannot finish inside the align step's walltime."""
    try:
        return _BLOCK_TARGET_READS_BY_PLATFORM[platform]
    except KeyError as exc:  # pragma: no cover - the parity test prevents this
        raise AlignUnsupportedPlatform(
            f"no align block-read target defined for platform {platform!r}; "
            "_BLOCK_TARGET_READS_BY_PLATFORM has drifted from _ALIGNER_BY_PLATFORM"
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


class AlignNoMasksFound(RuntimeError):
    """NOT ONE of the pool's samples is masked under the caller-named `mask_idx`
    (no `mask_sample` gate row at all for any of them), so there is nothing this
    plan could align.

    A partially-masked pool is normal (its unmasked-under-this-mask samples are
    reported `samples_skipped_no_mask` and the rest align); this fires only when the
    WHOLE pool misses, which is never a benign no-op — the caller named a mask this
    pool was not masked under (a wrong `mask_idx`, or the pool was masked under a
    different config). Surfaced as a 422 rather than a silent 202/0 so the
    names-the-wrong-mask outcome is loud. Distinct from `AlignMaskNotFound` (the
    mask does not exist at all — a 404) and from `samples_skipped_mask_incomplete`
    (a gate row EXISTS but masking hasn't finished — a legitimate in-flight state
    that stays a 202)."""


async def plan_and_submit_alignments(
    pool: asyncpg.Pool,
    *,
    app: FastAPI,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
    reference_idx: int,
    mask_idx: int,
    only_missing: bool,
    originator_principal_idx: int,
    align_action_id: str,
    align_action_version: str,
    target_reads: int | None = None,
) -> dict[str, Any]:
    """Plan + submit a pool's bulk-block sharded-alignment work.

    Aligns the pool's samples whose reads are masked-complete under the
    caller-named `mask_idx`. Alignment does NOT re-derive the mask config — the
    caller names the mask its reads were produced under, so a pool masked any way
    (per-sample or block; any host / adapter / lima / syndna config) aligns by
    pointing at the `mask_idx` it produced. Selects the samples whose `mask_sample`
    gate is `completed` under that `mask_idx`, mints one `alignment_idx` over
    `{reference_idx, aligner, mask_idx, shard_ids}`, tiles them into
    ≤`target_reads`-read blocks, then in ONE transaction persists the `block` /
    `block_member` cover-map, a PENDING `alignment_sample` gate per sample, and one
    block `work_ticket` per block (scope `block`, carrying `mask_idx` +
    `alignment_idx` + the align `action_context`), back-filling
    `block.work_ticket_idx`. After commit each ticket is dispatched.

    `target_reads` is the block size; `None` (the normal case) resolves it FROM THE
    PLATFORM via `_BLOCK_TARGET_READS_BY_PLATFORM` — 10M for short reads, 1M for long
    reads, whose per-shard re-scan is byte-driven and would otherwise blow the align
    step's walltime. An explicit value overrides that (tests tile small pools without
    seeding 10M reads).

    `only_missing` drops samples already carrying an `alignment_sample` row for
    their resolved alignment (an interrupted plan re-runs only the gap). On a fresh
    plan any already-gated sample raises `AlignResubmitError` (409).

    Raises `AlignMaskNotFound` (404) if `mask_idx` does not exist,
    `AlignNoMasksFound` (422) if no pool sample is masked under it,
    `AlignReferenceNotFound` (404) / `AlignReferenceNotReady` (409) if the reference
    can't be aligned against; asyncpg errors on a genuine DB fault (fail loud).
    Samples that can't be planned are reported (not raised) in the
    `samples_skipped_*` counts.
    """
    # The aligner is derived from the run's PLATFORM (short-read Illumina → bowtie2,
    # long-read PacBio HiFi / Nanopore → minimap2), NOT chosen by the caller, so it
    # always matches the read chemistry. `platform` is NOT NULL in the schema, so a
    # missing row would surface as an AttributeError on `.` below (the run existence
    # is fronted by the route's `require_sequencing_run_exists`).
    run_row = await pool.fetchrow(
        "SELECT platform FROM qiita.sequencing_run WHERE idx = $1",
        sequencing_run_idx,
    )
    aligner = _aligner_for_platform(run_row["platform"])
    # Block size is per-platform too, and for the same reason the aligner is: it
    # follows the read chemistry, not the caller. Resolved here (rather than at the
    # tiling call below) so the platform lookup happens once, next to the aligner's.
    if target_reads is None:
        target_reads = _block_target_for_platform(run_row["platform"])

    # The caller names the mask to align under; it must exist. A pool with no sample
    # masked under an EXISTING mask is AlignNoMasksFound (422, below); a nonexistent
    # mask_idx is a client error (404).
    if (
        await pool.fetchval("SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
        is None
    ):
        raise AlignMaskNotFound(f"no mask_definition with mask_idx={mask_idx}")

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

    # Which of the pool's samples are masked under the caller-named mask_idx, and in
    # what gate state? Alignment does NOT re-derive the mask config — the caller
    # names the mask its reads were produced under (per-sample or block, any host /
    # adapter / lima / syndna config), and we select the samples whose mask_sample
    # gate is 'completed' (gate contract: see `fetch_mask_sample_state` — a sample
    # with NO gate row under this mask was never masked under it).
    gate_rows = await pool.fetch(
        "SELECT prep_sample_idx, state FROM qiita.mask_sample"
        " WHERE mask_idx = $1 AND prep_sample_idx = ANY($2::bigint[])",
        mask_idx,
        [s.prep_sample_idx for s in all_samples],
    )
    gate_state_by_prep_sample = {r["prep_sample_idx"]: r["state"] for r in gate_rows}

    # No gate row under this mask ⇒ the sample was never masked under it — skip.
    skipped_no_mask = sum(
        1 for s in all_samples if s.prep_sample_idx not in gate_state_by_prep_sample
    )

    # If the pool has samples but NONE is masked under this mask_idx at all (no gate
    # rows), refuse loudly rather than return a silent 202/0 — the caller named a
    # mask this pool was not masked under. A PARTIAL miss (some samples masked, some
    # not) is normal and stays a 202 with skipped_no_mask counts.
    if all_samples and not gate_state_by_prep_sample:
        raise AlignNoMasksFound(
            f"none of the pool's {len(all_samples)} sample(s) is masked under "
            f"mask_idx={mask_idx}; there is nothing to align. Check the mask_idx names the "
            "read-mask this pool was actually masked under."
        )

    # Require the gate 'completed' (align only fully-masked samples — a non-completed
    # row means a covering block is still masking, so the read set is partial). A
    # sample with a non-completed gate row is reported skipped_mask_incomplete (a
    # legitimate in-flight state, retry later), distinct from skipped_no_mask.
    to_consider = [
        (s, mask_idx)
        for s in all_samples
        if gate_state_by_prep_sample.get(s.prep_sample_idx) == "completed"
    ]
    skipped_mask_incomplete = len(gate_state_by_prep_sample) - len(to_consider)

    # Mint the alignment_idx for {reference_idx, aligner, mask_idx, shard_ids} — the
    # mask-style identity, idempotent on the config hash (a re-plan of the same
    # config resolves to the same alignment_idx). shard_ids is reference-constant.
    # Skipped when nothing is to-align (an all-incomplete pool returns a 202/0).
    alignment_by_mask: dict[int, int] = {}
    if to_consider:
        async with pool.acquire() as conn:
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

    # Persist the whole plan in ONE transaction — a partial plan must roll back
    # (the alignment_definitions minted above are idempotent and survive a rollback
    # harmlessly). By construction there is a single partition: every `to_plan` entry
    # is keyed by the caller's `mask_idx` and its one minted `alignment_idx`, so we
    # persist that partition directly instead of block_planner's per-mask loop. An
    # empty `to_plan` (all samples skipped) persists nothing and returns a 202/0.
    block_summaries: list[dict[str, Any]] = []
    partition_summaries: list[dict[str, Any]] = []
    if to_plan:
        samples = [s for (s, _m) in to_plan]
        async with pool.acquire() as conn, conn.transaction():
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
                    "  scope_target_kind, block_idx, mask_idx, alignment_idx,"
                    "  action_context, dispatch_held"
                    ") VALUES ($1, $2, $3, 'block', $4, $5, $6, $7::jsonb, true)"
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

    # Every block ticket was INSERTed `dispatch_held`; the pump releases up to
    # FANOUT_MAX_INFLIGHT per alignment cohort and refills as each block finishes
    # (dispatch._run_and_log completion hook). Post-commit, so a released ticket
    # is durable before its background task starts.
    max_inflight = app.state.settings.fanout_max_inflight
    for cohort_alignment_idx in {b["alignment_idx"] for b in block_summaries}:
        await top_up_dispatch(
            pool,
            align_block_cohort(cohort_alignment_idx),
            max_inflight=max_inflight,
            dispatch_cb=lambda idx: schedule_dispatch(app, idx),
        )

    return {
        "sequencing_run_idx": sequencing_run_idx,
        "sequenced_pool_idx": sequenced_pool_idx,
        "reference_idx": reference_idx,
        "aligner": aligner,
        "samples_planned": len(to_plan),
        "samples_skipped_existing": skipped_existing,
        "samples_skipped_no_mask": skipped_no_mask,
        "samples_skipped_mask_incomplete": skipped_mask_incomplete,
        "samples_skipped_no_reads": skipped_no_reads,
        "partitions": partition_summaries,
        "blocks": block_summaries,
        "blocks_created": len(block_summaries),
    }
