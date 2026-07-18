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
from qiita_common.host_filter_plan import (
    PoolPlanRefusal,
    SampleHostFilter,
    plan_pool_host_filter,
)
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_MINIMAP2,
    HOST_FILTER_INDEX_TYPE_RYPE,
    Platform,
)

from .dispatch import schedule_dispatch
from .fanout_dispatch import read_mask_block_cohort, top_up_dispatch
from .host_filter_resolver import resolve_host_filter_many
from .repositories.block import (
    add_block_members,
    create_block,
    create_mask_sample_pending,
    set_block_work_ticket,
)
from .repositories.mask_definition import mint_mask_definition
from .repositories.sequencing_run import fetch_sequencing_run_platform
from .runner import (
    ReferenceNotFound,
    _build_mask_params,
    _resolve_reference_index_path,
)

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
# workflow (`workflows/read-mask-block/1.0.0.yaml`, synced out-of-tree via
# `qiita-admin actions sync`). Distinct from the mask filter identity above: the
# ticket runs under "read-mask-block", but the mask it produces is minted under
# the shared "read-mask" filter identity so it collapses with the per-sample path.
BLOCK_MASK_ACTION_ID = "read-mask-block"
BLOCK_MASK_ACTION_VERSION = "1.0.0"


class AdapterMaterializationUnavailable(RuntimeError):
    """The block mask identity needs the canonical adapter-set hash (a default
    adapter reference is configured AND the read-mask workflow declares
    adapter_parquet), but no scratch staging root is available to materialize it.
    The route maps this to a 503 — a misconfiguration, not a client error."""


class BlockMaskResubmitError(RuntimeError):
    """One or more requested samples already carry a `mask_sample` gate for their
    resolved mask, so a fresh (`only_missing=False`) plan is refused:

    - a COMPLETED gate → re-planning re-masks reads already masked, double-writing
      `read_mask` rows (DuckLake has no uniqueness);
    - a still-PENDING gate → a prior plan's covering block is in-flight or failed,
      and minting a fresh same-footprint covering block would wedge the sample's
      finalize forever (`has_incomplete_covering_block` keeps seeing the stale
      non-completed block, so the gate never flips and the sample stays
      non-exportable).

    The block-compute analog of the sequenced_pool COMPLETED-resubmit rule: an
    existing gate requires an explicit DELETE before resubmission. The operator
    either DELETEs the mask first (to genuinely re-mask) or passes
    `only_missing=true` to plan only the not-yet-gated samples. The route maps this
    to 409."""

    def __init__(self, conflicting_prep_sample_idxs: list[int]):
        self.conflicting_prep_sample_idxs = conflicting_prep_sample_idxs
        super().__init__(
            f"{len(conflicting_prep_sample_idxs)} sample(s) already have a read-mask gate "
            "(pending or completed) for the resolved filtering config; a fresh plan would "
            "double-write a completed mask or wedge an in-flight one. DELETE the mask first "
            "to re-mask, or pass only_missing=true to plan only the ungated samples. "
            f"prep_sample_idxs: {conflicting_prep_sample_idxs}"
        )


async def resolve_block_mask_adapter_hash(
    pool: asyncpg.Pool,
    *,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    signing_key: bytes,
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
        signing_key=signing_key,
        workspace=workspace,
    )


class _PlanSample(NamedTuple):
    """A pool sample resolved for planning: its prep_sample_idx, the
    biosample_idx its host filtering resolves from, the prep_protocol_idx feeding
    the mask identity, and its full read range."""

    prep_sample_idx: int
    biosample_idx: int
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
    Unordered: `tile_partition` sorts by sequence_idx_start itself, so it owns the
    tiling determinism and a producer-side ORDER BY would be redundant DB work."""
    rows = await conn.fetch(
        "SELECT ss.prep_sample_idx, ps.biosample_idx, ps.prep_protocol_idx,"
        "       sr.sequence_idx_start, sr.sequence_idx_stop"
        "  FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        "  JOIN qiita.sequence_range sr ON sr.prep_sample_idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1"
        "   AND ps.retired = false",
        sequenced_pool_idx,
    )
    return [
        _PlanSample(
            prep_sample_idx=r["prep_sample_idx"],
            biosample_idx=r["biosample_idx"],
            prep_protocol_idx=r["prep_protocol_idx"],
            sample_range=SampleRange(
                r["prep_sample_idx"], r["sequence_idx_start"], r["sequence_idx_stop"]
            ),
        )
        for r in rows
    ]


class PoolHostFilterRefusal(RuntimeError):
    """A pool cannot be planned because its per-sample host-filter resolution
    refuses: one or more samples are UNRESOLVED, or the pool spans more than one
    host (its blanks have no single reference, and the multi-host union is not
    built). The block/align routes map this to a 422 that names the offending
    samples, mirroring the per-sample submit-host-filter-pool abort. `reasons`
    carries the resolver's own message for the first few offenders so the operator
    sees WHY, not just WHICH."""

    def __init__(
        self,
        refusal: PoolPlanRefusal,
        offending: tuple[int, ...],
        *,
        reasons: dict[int, str] | None = None,
        pool_host_term_idx: int | None = None,
    ) -> None:
        self.refusal = refusal
        self.offending = offending
        self.reasons = reasons or {}
        self.pool_host_term_idx = pool_host_term_idx
        super().__init__(f"{refusal.value}: {len(offending)} offending sample(s)")


class HostReferenceNotReady(RuntimeError):
    """A host reference the resolved plan points at is not usable: the reference
    row is missing, is not ACTIVE, or has no index of the required type built yet.
    Caught before any mask is minted so the whole plan is refused with one
    actionable error rather than fanning out into N failed blocks. The route maps
    it to a 422 — it is a host_filter_profile / reference-build config problem, not
    a client typo (the caller never named the reference; resolution did)."""


async def resolve_pool_sample_decisions(
    pool: asyncpg.Pool,
    *,
    samples: Sequence[_PlanSample],
    platform: Platform,
    force_decision: SampleHostFilter | None,
) -> dict[int, SampleHostFilter]:
    """Resolve each sample's host-filter decision, keyed by `prep_sample_idx`.

    THE shared seam between the block-mask and align planners: both must derive the
    identical per-sample answer, so the resolution lives here once rather than as
    two parallel implementations that could drift (the same reason
    `resolve_host_filter` and `resolve_host_filter_many` share a `_classify` core).

    `force_decision` is the `--force` override: when set, every sample gets it
    verbatim, bypassing resolution (the operator applies one reference pool-wide,
    blanks included). Otherwise each sample is resolved from its own
    `host_taxon_id` metadata + `platform` (`resolve_host_filter_many`), then the
    pool-level blank join + refusals run through the shared
    `plan_pool_host_filter`. A refusal raises `PoolHostFilterRefusal` (the route
    turns it into a 422); success returns one `SampleHostFilter` per sample.
    """
    if force_decision is not None:
        return {s.prep_sample_idx: force_decision for s in samples}

    resolutions_by_biosample = await resolve_host_filter_many(
        pool,
        biosample_idxs=[s.biosample_idx for s in samples],
        platform=platform,
    )
    # Re-key by prep_sample_idx: that is the unit the planner tiles and gates, and
    # it is what the refusal message should name. Two prep_samples sharing a
    # biosample resolve identically, which is correct.
    resolutions = {s.prep_sample_idx: resolutions_by_biosample[s.biosample_idx] for s in samples}
    plan = plan_pool_host_filter(resolutions)
    if plan.refusal is not None:
        raise PoolHostFilterRefusal(
            plan.refusal,
            plan.offending,
            reasons={key: resolutions[key].reason for key in plan.offending[:3]},
            pool_host_term_idx=plan.pool_host_term_idx,
        )
    return plan.decisions


def force_decision_from(
    *, force: bool, host_rype_reference_idx: int | None, host_minimap2_reference_idx: int | None
) -> SampleHostFilter | None:
    """Build the `--force` override decision a plan applies pool-wide, or None when
    not forcing (the normal resolve-per-sample path).

    The request's host refs are a force-only override — the model already rejects a
    host ref without `force` — so `force=False` returns None regardless. When
    forcing, the given references become one decision applied to every sample,
    blanks included (host filtering enabled exactly when a rype ref is given)."""
    if not force:
        return None
    return SampleHostFilter(
        enabled=host_rype_reference_idx is not None,
        rype_reference_idx=host_rype_reference_idx,
        minimap2_reference_idx=host_minimap2_reference_idx,
    )


def _mask_params_for(
    decision: SampleHostFilter,
    *,
    prep_protocol_idx: int | None,
    instrument_model: str | None,
    adapter_set_hash: str | None,
) -> dict[str, Any]:
    """The mask-identity params for one `(decision, prep_protocol)` combination.

    Wraps the shared `_build_mask_params` so the block-mask mint and the align
    lookup derive the SAME hash for the same effective filter — the host refs come
    from the sample's resolved `decision` (None when it disables filtering), not
    from a pool-wide flag. The block workflow is `qc -> host_filter` only, so lima
    and syndna are always absent (passed explicitly, not defaulted, so adding a
    block-path stage has to come here and say so)."""
    return _build_mask_params(
        action_id=_MASK_FILTER_WORKFLOW,
        action_version=_MASK_FILTER_VERSION,
        prep_protocol_idx=prep_protocol_idx,
        instrument_model=instrument_model,
        adapter_set_hash=adapter_set_hash,
        host_rype_reference_idx=decision.rype_reference_idx if decision.enabled else None,
        host_minimap2_reference_idx=decision.minimap2_reference_idx if decision.enabled else None,
        resolved_lima=None,
        resolved_syndna=None,
    )


async def _assert_pool_references_ready(
    pool: asyncpg.Pool, decisions: Sequence[SampleHostFilter]
) -> None:
    """Fail the whole plan (before any mint) if a reference the resolved plan
    points at is not ACTIVE with its index built.

    The per-sample submit path preflighted this client-side
    (`_assert_resolved_references_ready`); once resolution moved server-side that
    check has to move here too, or a bad profile would fail every block at the
    runner's submission stage instead of once, actionably, up front. Deduped across
    the pool: a single-host pool checks one rype (+ optional minimap2) pair no
    matter how many samples it has. A decision that disables filtering checks
    nothing."""
    rype = {d.rype_reference_idx for d in decisions if d.enabled}
    minimap2 = {
        d.minimap2_reference_idx for d in decisions if d.enabled and d.minimap2_reference_idx
    }
    checks = [(idx, HOST_FILTER_INDEX_TYPE_RYPE) for idx in sorted(rype)]
    checks += [(idx, HOST_FILTER_INDEX_TYPE_MINIMAP2) for idx in sorted(minimap2)]
    for reference_idx, index_type in checks:
        try:
            await _resolve_reference_index_path(pool, reference_idx, index_type)
        # ReferenceIndexNotBuilt is a ValueError subclass (index not built); the
        # bare ValueError also covers a non-active reference.
        except (ReferenceNotFound, ValueError) as exc:
            raise HostReferenceNotReady(
                f"reference {reference_idx} resolved by the pool's host_filter_profile is not"
                f" usable for its {index_type!r} index: {exc}. Fix the profile or build the"
                " reference index; the reference was chosen by resolution, not passed by you."
            ) from exc


def _block_action_context(
    decision: SampleHostFilter, instrument_model: str | None
) -> dict[str, Any]:
    """The `action_context` a block ticket carries for one partition's decision.

    read-mask-block is `qc -> host_filter` only (no lima chain, no syndna step), so
    those gates are always off. host filtering is on exactly when this partition's
    resolved decision enables it, and it carries THAT decision's refs — not a
    pool-wide flag. Keys match the read-mask-block `context_schema`."""
    context: dict[str, Any] = {
        "host_filter_enabled": decision.enabled,
        "lima_enabled": False,
        "syndna_enabled": False,
    }
    if decision.enabled:
        context["host_rype_reference_idx"] = decision.rype_reference_idx
        if decision.minimap2_reference_idx is not None:
            context["host_minimap2_reference_idx"] = decision.minimap2_reference_idx
    if instrument_model is not None:
        context["instrument_model"] = instrument_model
    return context


async def plan_and_submit_blocks(
    pool: asyncpg.Pool,
    *,
    app: FastAPI,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
    force_decision: SampleHostFilter | None,
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
    re-runnable without duplicating work).

    Host filtering is resolved PER SAMPLE from each sample's `host_taxon_id`
    metadata + the run's platform (`resolve_pool_sample_decisions`), not chosen
    pool-wide: samples that resolve differently get different `mask_idx`, fall into
    different partitions, and each block's `action_context` carries ITS partition's
    host refs. `force_decision` (the `--force` override) bypasses resolution and
    applies one decision to every sample. A pool that cannot resolve (an
    UNRESOLVED sample, or >1 host) raises `PoolHostFilterRefusal`; a resolved
    reference that is not built raises `HostReferenceNotReady` — both before any
    mask is minted.

    Returns a JSON-able summary (partitions with their host refs, blocks + their
    tickets, counts). Raises asyncpg errors on a genuine DB fault (fail loud); a
    sample without a sequence_range is reported in `samples_skipped_no_reads`.
    """
    # platform is the resolver's second input; instrument_model gates QC's polyG
    # and is part of the mask identity. Both are pool-constant, read once.
    platform = await fetch_sequencing_run_platform(pool, sequencing_run_idx)
    instrument_model = await pool.fetchval(
        "SELECT instrument_model FROM qiita.sequencing_run WHERE idx = $1",
        sequencing_run_idx,
    )

    all_samples = await _enumerate_pool_samples(pool, sequenced_pool_idx)

    # Per-sample host-filter decision (keyed by prep_sample_idx). Refuses an
    # UNRESOLVED / multi-host pool up front; `force_decision` bypasses resolution.
    decision_by_prep_sample = await resolve_pool_sample_decisions(
        pool, samples=all_samples, platform=platform, force_decision=force_decision
    )
    # Preflight the references the plan resolved to — one actionable error instead
    # of a whole plan's worth of failed blocks.
    await _assert_pool_references_ready(pool, list(decision_by_prep_sample.values()))

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

    # Mint mask_idx per sample. The identity now depends on BOTH the prep_protocol
    # AND the sample's resolved host-filter decision, so memoize by that pair — one
    # mint per distinct (protocol, decision), not per sample. A uniform pool
    # collapses to one mint per protocol exactly as before; a heterogeneous pool
    # mints one per distinct decision and the partitions fall out naturally.
    mask_by_prep_sample: dict[int, int] = {}
    decision_by_mask: dict[int, SampleHostFilter] = {}
    mask_by_key: dict[tuple[int | None, SampleHostFilter], int] = {}
    async with pool.acquire() as conn:
        for s in all_samples:
            decision = decision_by_prep_sample[s.prep_sample_idx]
            key = (s.prep_protocol_idx, decision)
            mask_idx = mask_by_key.get(key)
            if mask_idx is None:
                params = _mask_params_for(
                    decision,
                    prep_protocol_idx=s.prep_protocol_idx,
                    instrument_model=instrument_model,
                    adapter_set_hash=adapter_set_hash,
                )
                mask_row = await mint_mask_definition(
                    conn,
                    filter_workflow=_MASK_FILTER_WORKFLOW,
                    filter_version=_MASK_FILTER_VERSION,
                    params=params,
                    principal_idx=originator_principal_idx,
                )
                mask_idx = mask_row["mask_idx"]
                mask_by_key[key] = mask_idx
                decision_by_mask[mask_idx] = decision
            mask_by_prep_sample[s.prep_sample_idx] = mask_idx

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
            [mask_by_prep_sample[s.prep_sample_idx] for s in all_samples],
            [s.prep_sample_idx for s in all_samples],
        )
        gated_prep_sample_idxs = {r["prep_sample_idx"] for r in gated}
        to_plan = [s for s in all_samples if s.prep_sample_idx not in gated_prep_sample_idxs]
        skipped_existing = len(all_samples) - len(to_plan)

    # disallow-without-delete: on a fresh plan (only_missing=False) refuse to
    # re-plan ANY sample that already carries a mask_sample gate for its resolved
    # mask — regardless of the gate's state:
    #   - COMPLETED → re-masking double-writes its read_mask (DuckLake has no
    #     uniqueness), and the sample is already exportable.
    #   - PENDING → a prior plan's covering block is in-flight or failed; minting a
    #     fresh same-footprint block would wedge the sample's finalize forever
    #     (has_incomplete_covering_block keeps seeing the stale non-completed block,
    #     so the gate never flips). `create_mask_sample_pending` is ON CONFLICT DO
    #     NOTHING and each plan mints new block_idxes, so nothing else stops the dup.
    # Mirrors the sequenced_pool COMPLETED-resubmit gate. `only_missing` already
    # dropped ALL gated samples above (pending or completed), so this fires only
    # when only_missing is False (a fresh plan over an already-block-masked pool).
    # One batched query over the (mask_idx, prep_sample_idx) pairs; a genuine
    # re-mask DELETEs first, an interrupted plan resumes with only_missing=true.
    if to_plan:
        conflicting = await pool.fetch(
            "SELECT ms.prep_sample_idx FROM qiita.mask_sample ms"
            "  JOIN unnest($1::bigint[], $2::bigint[]) AS t(mask_idx, prep_sample_idx)"
            "    ON ms.mask_idx = t.mask_idx AND ms.prep_sample_idx = t.prep_sample_idx",
            [mask_by_prep_sample[s.prep_sample_idx] for s in to_plan],
            [s.prep_sample_idx for s in to_plan],
        )
        if conflicting:
            raise BlockMaskResubmitError(sorted(r["prep_sample_idx"] for r in conflicting))

    # Partition the to-plan samples by resolved mask_idx.
    partitions: dict[int, list[_PlanSample]] = {}
    for s in to_plan:
        partitions.setdefault(mask_by_prep_sample[s.prep_sample_idx], []).append(s)

    # Persist the whole plan in ONE transaction — a partial plan must roll back
    # (the masks minted above are idempotent and survive a rollback harmlessly).
    block_summaries: list[dict[str, Any]] = []
    partition_summaries: list[dict[str, Any]] = []
    async with pool.acquire() as conn, conn.transaction():
        for mask_idx, samples in sorted(partitions.items()):
            # Each partition carries ITS OWN host refs — the decision that minted
            # this mask_idx, not a pool-wide flag. `when:` is DEFAULT-ON and this
            # INSERTs the ticket directly (bypassing the REST route's
            # context_schema check), so the gate keys are written here explicitly.
            decision = decision_by_mask[mask_idx]
            action_context_json = json.dumps(_block_action_context(decision, instrument_model))
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
                    "  scope_target_kind, block_idx, mask_idx, action_context, dispatch_held"
                    ") VALUES ($1, $2, $3, 'block', $4, $5, $6::jsonb, true)"
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
            # Each partition's host refs are the truth for its samples (the same
            # `decision` bound above) — there is no single pool-wide answer any
            # more, so they live per partition.
            partition_summaries.append(
                {
                    "mask_idx": mask_idx,
                    "sample_count": len(samples),
                    "block_count": len(blocks),
                    "host_filter_enabled": decision.enabled,
                    "host_rype_reference_idx": (
                        decision.rype_reference_idx if decision.enabled else None
                    ),
                    "host_minimap2_reference_idx": (
                        decision.minimap2_reference_idx if decision.enabled else None
                    ),
                }
            )

    # Every block ticket was INSERTed `dispatch_held`; the pump releases up to
    # FANOUT_MAX_INFLIGHT per mask-partition cohort and refills as each block
    # finishes (dispatch._run_and_log completion hook). Post-commit, so a
    # released ticket is durable before its background task starts.
    max_inflight = app.state.settings.fanout_max_inflight
    for cohort_mask_idx in {b["mask_idx"] for b in block_summaries}:
        await top_up_dispatch(
            pool,
            read_mask_block_cohort(cohort_mask_idx),
            max_inflight=max_inflight,
            dispatch_cb=lambda idx: schedule_dispatch(app, idx),
        )

    return {
        "sequencing_run_idx": sequencing_run_idx,
        "sequenced_pool_idx": sequenced_pool_idx,
        "instrument_model": instrument_model,
        "samples_planned": len(to_plan),
        "samples_skipped_existing": skipped_existing,
        "samples_skipped_no_reads": skipped_no_reads,
        "partitions": partition_summaries,
        "blocks": block_summaries,
        "blocks_created": len(block_summaries),
    }
