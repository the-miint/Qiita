"""Block planner / tiler for bulk-block read masking.

Decouples the COMPUTE unit (a fixed ~10M-read block) from the ACCOUNTING unit
(per-sample completion). Given a pool's samples + filtering criteria, the
planner:

  1. resolves each sample's `mask_idx` AT SUBMIT TIME (so it can be the partition
     key), reusing the same `_build_mask_params` shape the per-sample runner mints
     under — with `filter_workflow="read-mask"` so a block-masked sample and a
     per-sample `read-mask` of the identical config collapse to ONE mask_idx;
  2. partitions the samples by resolved `mask_idx` (a pool can span several —
     mixed prep_protocol / instrument);
  3. tiles each partition into ≤`_BLOCK_TARGET_READS`-read blocks (pure metadata
     arithmetic over `qiita.sequence_range` bounds — no read data touched);
  4. persists the `block` / `block_member` cover-map + a `mask_sample` PENDING
     gate per sample, creates one block `work_ticket` per block, back-fills
     `block.work_ticket_idx`, and dispatches each.

This module holds the PURE tiler (`tile_partition`, unit-testable with no DB) and
the server-side orchestration (`plan_and_submit_blocks`, DB + data-plane). The
REST route is the only caller; the CLI reaches it over HTTP.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NamedTuple

import asyncpg

from .dispatch import schedule_dispatch
from .repositories.block import (
    add_block_members,
    create_block,
    create_mask_sample_pending,
    set_block_work_ticket,
)
from .repositories.mask_definition import mint_mask_definition
from .runner import _build_mask_params

if TYPE_CHECKING:
    from fastapi import FastAPI

# Target reads per block. A block is one work ticket / SLURM job, so this sets
# the per-job input size — chosen so QC + host-filter run on a predictable
# ~10M-read envelope regardless of the (bimodal) per-sample sizes. Tunable; the
# tiler is exact for any positive value, and the route may override it for tests.
_BLOCK_TARGET_READS = 10_000_000


class BlockMember(NamedTuple):
    """One sample's contiguous slice within a block: the inclusive
    `[min_sequence_idx, max_sequence_idx]` sub-range of `prep_sample_idx`'s reads
    this block covers. Maps 1:1 onto a qiita.block_member row and onto an
    `export_read_block` member."""

    prep_sample_idx: int
    min_sequence_idx: int
    max_sequence_idx: int


class SampleRange(NamedTuple):
    """A sample's full contiguous read range, from qiita.sequence_range. Exact
    read count is `sequence_idx_stop - sequence_idx_start + 1`."""

    prep_sample_idx: int
    sequence_idx_start: int
    sequence_idx_stop: int

    @property
    def count(self) -> int:
        return self.sequence_idx_stop - self.sequence_idx_start + 1


def tile_partition(
    samples: Sequence[SampleRange],
    target_reads: int = _BLOCK_TARGET_READS,
) -> list[list[BlockMember]]:
    """Tile one mask-partition's samples into blocks of ≤`target_reads` reads.

    The samples (all resolving to one `mask_idx`) are laid end-to-end in
    `sequence_idx` order into a single logical tape of reads, then cut every
    `target_reads` reads. Every block but the last holds exactly `target_reads`
    reads; the last holds the remainder. A sample that straddles a cut is SPLIT
    into disjoint sub-ranges across consecutive blocks (each an exact
    `[min, max]`), and a sample's sub-ranges across its blocks together cover its
    whole `[start, stop]` with no gap or overlap — the invariant the reconcile
    count-assertion and the exact `export_read_block` selector both rely on.

    Pure metadata arithmetic: no read data is touched. Deterministic and
    re-derivable from the same `samples` + `target_reads` (samples are sorted by
    `sequence_idx_start` internally, so input order does not matter).

    Returns a list of blocks, each a non-empty list of `BlockMember`. An empty
    `samples` yields `[]`. Raises ValueError on a non-positive `target_reads` or
    a sample whose range is inverted/empty (a caller bug — sequence_range bounds
    are always start <= stop).
    """
    if target_reads <= 0:
        raise ValueError(f"target_reads must be positive, got {target_reads}")

    blocks: list[list[BlockMember]] = []
    current: list[BlockMember] = []
    current_count = 0

    # Sort by start so the tape is laid out in sequence_idx order regardless of
    # the caller's input ordering (keeps the tiling deterministic).
    for sample in sorted(samples, key=lambda s: s.sequence_idx_start):
        if sample.count <= 0:
            raise ValueError(
                f"sample {sample.prep_sample_idx} has an empty/inverted range "
                f"[{sample.sequence_idx_start}, {sample.sequence_idx_stop}]"
            )
        pos = sample.sequence_idx_start
        remaining = sample.count
        while remaining > 0:
            space = target_reads - current_count
            take = min(remaining, space)
            current.append(BlockMember(sample.prep_sample_idx, pos, pos + take - 1))
            current_count += take
            pos += take
            remaining -= take
            # Block full — emit it and start a fresh one. A sample with reads
            # still remaining continues into the next block (the split).
            if current_count == target_reads:
                blocks.append(current)
                current = []
                current_count = 0

    if current:
        blocks.append(current)
    return blocks


# The filter identity a block mask is minted under. Deliberately "read-mask"
# (NOT the block orchestration action_id "read-mask-block"): the mask identity is
# the FILTER config, not the workflow that ran it, so a block-masked sample and a
# per-sample read-mask of the identical config collapse to ONE mask_idx. Keeps
# the read_mask table + the export gate coherent and makes a single-sample block
# equivalent to a per-sample read-mask at the data level. Must stay in lockstep
# with the per-sample read-mask action_id/version `_mint_read_mask` uses.
_MASK_FILTER_WORKFLOW = "read-mask"
_MASK_FILTER_VERSION = "1.0.0"

# The action a block work_ticket is submitted against — the bulk-block masking
# workflow (synced out-of-tree via `qiita-admin actions sync`; the YAML lands in
# a later phase). Distinct from the mask filter identity above: the ticket runs
# under "read-mask-block", but the mask it produces is minted under the shared
# "read-mask" filter identity so it collapses with the per-sample path.
BLOCK_MASK_ACTION_ID = "read-mask-block"
BLOCK_MASK_ACTION_VERSION = "1.0.0"


class AdapterMaterializationUnavailable(RuntimeError):
    """The block mask identity needs the canonical adapter-set hash (a default
    adapter reference is configured AND the read-mask workflow declares
    adapter_parquet), but no scratch staging root is available to materialize it.
    The route maps this to a 503 — a misconfiguration, not a client error."""


class BlockMaskResubmitError(RuntimeError):
    """One or more requested samples already have a COMPLETED `mask_sample` gate
    for their resolved mask — re-planning would re-mask reads that are already
    masked, double-writing `read_mask` rows (DuckLake has no uniqueness).

    The block-compute analog of the sequenced_pool COMPLETED-resubmit rule: a
    completed result requires an explicit DELETE before resubmission. The operator
    either DELETEs the mask first (to genuinely re-mask) or passes
    `only_missing=true` to plan only the not-yet-gated samples. The route maps this
    to 409."""

    def __init__(self, completed_prep_sample_idxs: list[int]):
        self.completed_prep_sample_idxs = completed_prep_sample_idxs
        super().__init__(
            f"{len(completed_prep_sample_idxs)} sample(s) already have a COMPLETED mask for "
            "the resolved filtering config; re-planning would double-write their read_mask. "
            "DELETE the mask first to re-mask, or pass only_missing=true to plan only the "
            f"ungated samples. prep_sample_idxs: {completed_prep_sample_idxs}"
        )


async def resolve_block_mask_adapter_hash(
    pool: asyncpg.Pool,
    *,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    hmac_secret: bytes,
    staging_root,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
) -> str | None:
    """Return the adapter_set_hash the block mask identity must fold in — computed
    to match the per-sample read-mask mint EXACTLY, so a block-masked sample and a
    per-sample read-mask of the identical config collapse to one mask_idx.

    The per-sample runner folds the hash iff `_workflow_needs_adapters(read-mask
    steps)` (the qc step declares `adapter_parquet`) AND a default adapter
    reference is configured; otherwise adapter_set_hash is None. This mirrors that
    decision by loading the read-mask filter workflow and gating on ITS declared
    inputs (not on the config alone) — so the two mask identities stay identical
    even if the read-mask YAML ever drops adapters while a default reference stays
    configured. When it must materialize but no `staging_root` is available it
    raises AdapterMaterializationUnavailable (the route → 503)."""
    from .runner import (
        _fetch_action,
        _materialize_backfill_adapter_set_hash,
        _workflow_needs_adapters,
    )

    if default_adapter_reference_idx is None:
        return None
    action = await _fetch_action(pool, _MASK_FILTER_WORKFLOW, _MASK_FILTER_VERSION)
    if action is None or not _workflow_needs_adapters(action.steps):
        # The read-mask filter workflow doesn't fold an adapter hash into its
        # mask identity, so neither does the block plan (keeps them identical).
        return None
    if staging_root is None:
        raise AdapterMaterializationUnavailable(
            "a default adapter reference is configured and the read-mask workflow "
            "declares adapter_parquet, but PATH_SCRATCH staging is unset, so the "
            "adapter set cannot be materialized for the block mask identity"
        )
    workspace = staging_root / f"block-plan-adapter-{sequencing_run_idx}-{sequenced_pool_idx}"
    workspace.mkdir(parents=True, exist_ok=True)
    return await _materialize_backfill_adapter_set_hash(
        pool,
        default_adapter_reference_idx=default_adapter_reference_idx,
        data_plane_url=data_plane_url,
        hmac_secret=hmac_secret,
        workspace=workspace,
    )


class _PlanSample(NamedTuple):
    """A pool sample resolved for planning: its prep_sample_idx, the
    prep_protocol_idx feeding the mask identity, and its full read range."""

    prep_sample_idx: int
    prep_protocol_idx: int | None
    sample_range: SampleRange


async def _enumerate_pool_samples(
    conn: asyncpg.Connection | asyncpg.Pool, sequenced_pool_idx: int
) -> list[_PlanSample]:
    """The pool's ACTIVE sequenced samples that have stored reads (a
    sequence_range), with the prep_protocol_idx that feeds the mask identity.
    Samples without a sequence_range (reads never ingested) are excluded — there
    is nothing to tile — and reported separately by the caller. Retired
    prep_samples are excluded too (`ps.retired = false`), matching the per-sample
    roster query fetch_sequenced_pool_samples that submit-host-filter-pool + the
    pool status endpoints use — the block plan must not re-mask a retired sample.
    Ordered by sequence_idx_start so the tiling tape is deterministic."""
    rows = await conn.fetch(
        "SELECT ss.prep_sample_idx, ps.prep_protocol_idx,"
        "       sr.sequence_idx_start, sr.sequence_idx_stop"
        "  FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        "  JOIN qiita.sequence_range sr ON sr.prep_sample_idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1"
        "   AND ps.retired = false"
        " ORDER BY sr.sequence_idx_start",
        sequenced_pool_idx,
    )
    return [
        _PlanSample(
            prep_sample_idx=r["prep_sample_idx"],
            prep_protocol_idx=r["prep_protocol_idx"],
            sample_range=SampleRange(
                r["prep_sample_idx"], r["sequence_idx_start"], r["sequence_idx_stop"]
            ),
        )
        for r in rows
    ]


async def plan_and_submit_blocks(
    pool: asyncpg.Pool,
    *,
    app: FastAPI,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
    only_missing: bool,
    adapter_set_hash: str | None,
    originator_principal_idx: int,
    block_action_id: str,
    block_action_version: str,
    target_reads: int = _BLOCK_TARGET_READS,
) -> dict[str, Any]:
    """Plan + submit a pool's bulk-block read-masking work.

    Resolves each sample's `mask_idx` at submit time (partitioning key), tiles
    each mask-partition into ≤`target_reads`-read blocks, then in ONE transaction
    persists the `block` / `block_member` cover-map, a PENDING `mask_sample` gate
    per sample, and one block `work_ticket` per block (scope `block`, carrying the
    partition's `mask_idx` and the host/instrument `action_context`), back-filling
    `block.work_ticket_idx`. After commit each ticket is dispatched.

    `adapter_set_hash` is resolved by the caller (a data-plane DoGet over the
    canonical adapter set) and threaded into the mask identity so it matches what
    a per-sample read-mask would mint. `only_missing` drops samples already
    carrying a `mask_sample` row for their resolved mask (an interrupted plan is
    re-runnable without duplicating work). Host filtering is pool-wide: a rype
    reference (optional minimap2) depletes every sample, or none is a QC-only
    pass-through — the same `action_context` shape the per-sample path uses.

    Returns a JSON-able summary (partitions, blocks + their tickets, counts).
    Raises asyncpg errors on a genuine DB fault (fail loud); a sample without a
    sequence_range is reported in `samples_skipped_no_reads`, not raised.
    """
    host_filter_enabled = host_rype_reference_idx is not None

    # instrument_model gates QC's polyG and is part of the mask identity; read it
    # once from the run (server is the source of truth; nullable).
    instrument_model = await pool.fetchval(
        "SELECT instrument_model FROM qiita.sequencing_run WHERE idx = $1",
        sequencing_run_idx,
    )

    all_samples = await _enumerate_pool_samples(pool, sequenced_pool_idx)

    # ACTIVE pool samples whose reads were never ingested (no sequence_range)
    # can't be tiled — report them so the operator sees the gap rather than a
    # silent drop. Retired samples are excluded (they are not planned at all), so
    # this count matches the active set _enumerate_pool_samples draws from.
    skipped_no_reads = await pool.fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        "  LEFT JOIN qiita.sequence_range sr ON sr.prep_sample_idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1 AND ps.retired = false"
        "   AND sr.prep_sample_idx IS NULL",
        sequenced_pool_idx,
    )

    # Resolve mask_idx per sample. Everything but prep_protocol_idx is
    # pool-constant, so memoize by prep_protocol_idx — one mint per distinct
    # protocol (the "prep-protocol over-partitioning" v1 choice), not per sample.
    mask_by_protocol: dict[int | None, int] = {}
    async with pool.acquire() as conn:
        for protocol_idx in {s.prep_protocol_idx for s in all_samples}:
            params = _build_mask_params(
                action_id=_MASK_FILTER_WORKFLOW,
                action_version=_MASK_FILTER_VERSION,
                prep_protocol_idx=protocol_idx,
                instrument_model=instrument_model,
                adapter_set_hash=adapter_set_hash,
                host_rype_reference_idx=host_rype_reference_idx,
                host_minimap2_reference_idx=host_minimap2_reference_idx,
            )
            mask_row = await mint_mask_definition(
                conn,
                filter_workflow=_MASK_FILTER_WORKFLOW,
                filter_version=_MASK_FILTER_VERSION,
                params=params,
                principal_idx=originator_principal_idx,
            )
            mask_by_protocol[protocol_idx] = mask_row["mask_idx"]

    # only_missing: drop samples already gated under their resolved mask (a prior
    # plan reached them) so an interrupted plan re-runs only the gap. One batched
    # query over the (mask_idx, prep_sample_idx) pairs — the same unnest pattern
    # the completed-check below uses — not a SELECT per sample.
    skipped_existing = 0
    to_plan: list[_PlanSample] = list(all_samples)
    if only_missing and all_samples:
        gated = await pool.fetch(
            "SELECT ms.prep_sample_idx FROM qiita.mask_sample ms"
            "  JOIN unnest($1::bigint[], $2::bigint[]) AS t(mask_idx, prep_sample_idx)"
            "    ON ms.mask_idx = t.mask_idx AND ms.prep_sample_idx = t.prep_sample_idx",
            [mask_by_protocol[s.prep_protocol_idx] for s in all_samples],
            [s.prep_sample_idx for s in all_samples],
        )
        gated_prep_sample_idxs = {r["prep_sample_idx"] for r in gated}
        to_plan = [s for s in all_samples if s.prep_sample_idx not in gated_prep_sample_idxs]
        skipped_existing = len(all_samples) - len(to_plan)

    # disallow-without-delete: refuse to re-plan a sample already COMPLETED for
    # its resolved mask — re-masking double-writes its read_mask (DuckLake has no
    # uniqueness), and the sample is already exportable. Mirrors the sequenced_pool
    # COMPLETED-resubmit gate. `only_missing` already dropped ALL gated samples
    # above (pending or completed), so this fires only when only_missing is False
    # (a fresh plan over a pool that was already block-masked). One batched query
    # over the (mask_idx, prep_sample_idx) pairs; a genuine re-mask DELETEs first.
    if to_plan:
        completed = await pool.fetch(
            "SELECT ms.prep_sample_idx FROM qiita.mask_sample ms"
            "  JOIN unnest($1::bigint[], $2::bigint[]) AS t(mask_idx, prep_sample_idx)"
            "    ON ms.mask_idx = t.mask_idx AND ms.prep_sample_idx = t.prep_sample_idx"
            " WHERE ms.state = 'completed'",
            [mask_by_protocol[s.prep_protocol_idx] for s in to_plan],
            [s.prep_sample_idx for s in to_plan],
        )
        if completed:
            raise BlockMaskResubmitError(sorted(r["prep_sample_idx"] for r in completed))

    # Partition the to-plan samples by resolved mask_idx.
    partitions: dict[int, list[_PlanSample]] = {}
    for s in to_plan:
        partitions.setdefault(mask_by_protocol[s.prep_protocol_idx], []).append(s)

    action_context = {"host_filter_enabled": host_filter_enabled}
    if host_filter_enabled:
        action_context["host_rype_reference_idx"] = host_rype_reference_idx
        if host_minimap2_reference_idx is not None:
            action_context["host_minimap2_reference_idx"] = host_minimap2_reference_idx
    if instrument_model is not None:
        action_context["instrument_model"] = instrument_model
    action_context_json = json.dumps(action_context)

    # Persist the whole plan in ONE transaction — a partial plan must roll back
    # (the masks minted above are idempotent and survive a rollback harmlessly).
    block_summaries: list[dict[str, Any]] = []
    partition_summaries: list[dict[str, Any]] = []
    async with pool.acquire() as conn, conn.transaction():
        for mask_idx, samples in sorted(partitions.items()):
            await create_mask_sample_pending(
                conn,
                mask_idx=mask_idx,
                prep_sample_idxs=[s.prep_sample_idx for s in samples],
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
                    "  scope_target_kind, block_idx, mask_idx, action_context"
                    ") VALUES ($1, $2, $3, 'block', $4, $5, $6::jsonb)"
                    " RETURNING work_ticket_idx",
                    block_action_id,
                    block_action_version,
                    originator_principal_idx,
                    block_idx,
                    mask_idx,
                    action_context_json,
                )
                await set_block_work_ticket(
                    conn, block_idx=block_idx, work_ticket_idx=work_ticket_idx
                )
                block_summaries.append(
                    {
                        "block_idx": block_idx,
                        "work_ticket_idx": work_ticket_idx,
                        "mask_idx": mask_idx,
                        "member_count": len(members),
                        "read_count": sum(
                            m.max_sequence_idx - m.min_sequence_idx + 1 for m in members
                        ),
                    }
                )
            partition_summaries.append(
                {
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
        "instrument_model": instrument_model,
        "host_filter_enabled": host_filter_enabled,
        "host_rype_reference_idx": host_rype_reference_idx if host_filter_enabled else None,
        "host_minimap2_reference_idx": host_minimap2_reference_idx if host_filter_enabled else None,
        "samples_planned": len(to_plan),
        "samples_skipped_existing": skipped_existing,
        "samples_skipped_no_reads": skipped_no_reads,
        "partitions": partition_summaries,
        "blocks": block_summaries,
        "blocks_created": len(block_summaries),
    }
