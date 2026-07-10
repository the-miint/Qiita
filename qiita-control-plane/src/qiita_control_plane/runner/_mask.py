"""Runner read-mask identity (mask_idx) minting."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import asyncpg

from ..repositories.mask_definition import mint_mask_definition
from ._reference import QC_ADAPTER_BINDING, _resolve_qc_adapters
from ._upload import _submission_bad_input

# =============================================================================
# Read-mask identity (mask_idx) minting
# =============================================================================
#
# A read mask's identity is its filtering CONFIG: the filter workflow + version,
# the host reference(s) it depletes against, and the resolved QC config. The
# control plane mints a `mask_idx` deduplicated on the SHA-256 of that config so
# the same config resolves to the same mask_idx fleet-wide; the host_filter step
# stamps it onto every read_mask row. The host references are read from the
# sequenced_sample row (where they are pinned at pool fan-out); the resolved QC
# values mirror the qc job's fastp-equivalent constants so a metadata edit to a
# protocol row that doesn't change the effective filter yields the same mask.

# Binding name the runner threads the minted mask_idx under. The host_filter step
# lists it in its `params:` (mask_idx -> host_filter.Inputs.mask_idx), which both
# signals the runner to mint the mask before the step loop and carries the value
# into the step.
MASK_IDX_BINDING = "mask_idx"

# The align run's alignment_idx (mask-style identity), read off an align block
# ticket's `work_ticket.alignment_idx` and threaded into the align_sharded step's
# `params:` so every emitted row is keyed by it. None for non-align tickets.
ALIGNMENT_IDX_BINDING = "alignment_idx"

# Resolved QC config the mask hash covers — the effective fastp-equivalent
# filter the qc job applies. Mirrors the constants in
# qiita_compute_orchestrator.jobs.qc (the fastp `-l 100` defaults); kept here
# (not imported) because the control plane does not depend on the orchestrator
# package. A change to the qc filter must update both so the mask identity stays
# faithful to the filter actually applied.
_QC_RESOLVED_MIN_LENGTH = 100
_QC_RESOLVED_FILTER_TAIL = "0, 15, 40, 5, 0"


def _workflow_needs_mask(steps: list[Any]) -> bool:
    """True iff some entry threads `mask_idx` through its `params:` — the signal
    the runner must mint a read mask before the step loop. Mirrors
    `_workflow_needs_adapters` (which keys off an input binding); the mask is a
    scalar param, so it keys off `params` values instead."""
    for entry in steps:
        params = getattr(entry, "params", None) or {}
        if MASK_IDX_BINDING in params.values():
            return True
    return False


def _adapter_set_hash(adapter_parquet: Path) -> str:
    """SHA-256 hex of the materialized adapter-set Parquet's bytes — the resolved
    adapter identity for the mask config hash. Hashing the staged file (not the
    reference idx) keeps the mask identity tied to the adapter bytes actually
    applied, so a re-pointed-but-identical adapter set collapses to one mask.

    Note the hash is over the SERIALIZED Parquet bytes, not the logical sequence
    set: mint and backfill agree only because both materialize the adapter Parquet
    through the same `_write_adapter_parquet` / pyarrow writer. A writer change
    that alters the byte layout shifts this hash and would force a re-mint rather
    than collapsing to the existing mask — it is an assumption, not something the
    code enforces."""
    return hashlib.sha256(adapter_parquet.read_bytes()).hexdigest()


def _build_mask_params(
    *,
    action_id: str,
    action_version: str,
    prep_protocol_idx: int | None,
    instrument_model: str | None,
    adapter_set_hash: str | None,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
) -> dict[str, Any]:
    """Assemble the resolved-filter-config dict that `mint_mask_definition`
    hashes (canonical JSON → SHA-256 → `params_hash`) to mint/dedup a mask.

    This is the SINGLE source of truth for the mask's identity shape — the mint
    path (`_mint_read_mask`) and the block planner both call it so they derive the
    SAME hash for the SAME effective config. Every value is the EFFECTIVE filter (the host refs
    the filter applies + adapter bytes hash + thresholds), so two callers with the
    same effective config collapse to one mask even if descriptive metadata
    differs. `adapter_set_hash` is passed in already computed (the SHA-256 hex of
    the materialized adapter Parquet, via `_adapter_set_hash`) rather than a file
    path, so the backfill can supply it from a re-materialized adapter set without
    this helper touching the filesystem.

    Any change to the keys, nesting, or resolved-QC constants here changes every
    mask's identity fleet-wide — keep it deterministic and keyed only on the
    effective filter.
    """
    return {
        "filter_workflow": action_id,
        "filter_version": action_version,
        "host_rype_reference_idx": host_rype_reference_idx,
        "host_minimap2_reference_idx": host_minimap2_reference_idx,
        "prep_protocol_idx": prep_protocol_idx,
        "resolved_qc": {
            "instrument_model": instrument_model,
            "min_length": _QC_RESOLVED_MIN_LENGTH,
            "filter_read_tail": _QC_RESOLVED_FILTER_TAIL,
            "adapter_set_hash": adapter_set_hash,
        },
    }


async def _mint_read_mask(
    pool: asyncpg.Pool,
    *,
    action_id: str,
    action_version: str,
    prep_sample_idx: int,
    originator_principal_idx: int,
    instrument_model: str | None,
    adapter_parquet: Path | None,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
) -> dict[str, int]:
    """Mint (or resolve) the `mask_idx` for this filtering config and bind it.

    Run before the step loop when `_workflow_needs_mask`. The config is:
      * the filter workflow + version (this action),
      * the host reference(s) the `host_filter` step actually APPLIES, passed in
        from the same action_context values `_resolve_host_filter_indexes`
        consumes (`host_rype_reference_idx` / `host_minimap2_reference_idx`) — so
        the minted mask_idx's params describe the filter that ran. Absent host
        refs mean no host filtering, a faithful part of the config (None), and
      * the resolved QC config (instrument model gating polyG, the fastp-`-l 100`
        thresholds, and a hash of the materialized adapter set).
    `mint_mask_definition` hashes `params` (canonical JSON) and upserts on it, so
    the same effective config resolves to the same mask_idx fleet-wide.

    Like the other pre-loop resolvers, any failure raises a SUBMISSION-attributed
    BAD_INPUT the outer handler turns into a FAILED ticket: no sequenced_sample
    row (the sample must be pooled first), or an unknown originator principal.
    """
    prep_protocol_idx = await pool.fetchval(
        "SELECT ps.prep_protocol_idx"
        "  FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE ss.prep_sample_idx = $1",
        prep_sample_idx,
    )
    if prep_protocol_idx is None:
        # fetchval returns None both when no row matched and when the column is
        # NULL; distinguish by re-checking row existence so a real "not pooled"
        # error keeps its specific message and a legitimately-NULL prep protocol
        # still mints.
        row_exists = await pool.fetchval(
            "SELECT 1 FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
            prep_sample_idx,
        )
        if row_exists is None:
            raise _submission_bad_input(
                f"no sequenced_sample row for prep_sample_idx={prep_sample_idx}; the "
                "sample must be pooled (its 1:1 sequenced_sample created) before a "
                "read mask can be minted"
            )

    # Resolved config — assembled by the shared `_build_mask_params` so the mint
    # path and the block planner derive the SAME hash for the same effective
    # config. The adapter identity is the SHA-256 of the materialized adapter
    # bytes (None when this workflow uses no adapter set).
    params = _build_mask_params(
        action_id=action_id,
        action_version=action_version,
        prep_protocol_idx=prep_protocol_idx,
        instrument_model=instrument_model,
        adapter_set_hash=(
            _adapter_set_hash(adapter_parquet) if adapter_parquet is not None else None
        ),
        host_rype_reference_idx=host_rype_reference_idx,
        host_minimap2_reference_idx=host_minimap2_reference_idx,
    )

    try:
        async with pool.acquire() as conn:
            mask_row = await mint_mask_definition(
                conn,
                filter_workflow=action_id,
                filter_version=action_version,
                params=params,
                principal_idx=originator_principal_idx,
            )
    except asyncpg.ForeignKeyViolationError as exc:
        raise _submission_bad_input(
            f"could not mint read mask: originator principal "
            f"{originator_principal_idx} does not exist"
        ) from exc
    return {MASK_IDX_BINDING: mask_row["mask_idx"]}


async def _persist_mask_idx(pool: asyncpg.Pool, work_ticket_idx: int, mask_idx: int) -> None:
    """Write the minted `mask_idx` onto the ticket row (durable ticket→mask
    traceability + a cheap shared-mask guard). Idempotent: a re-mint on resume
    re-resolves to the same mask_idx via the config-hash upsert, so re-running
    this writes the same value. Like every runner DB write it fails loud — a PG
    outage raises and unwinds the run via run_workflow's catch-all."""
    await pool.execute(
        "UPDATE qiita.work_ticket SET mask_idx = $1 WHERE work_ticket_idx = $2",
        mask_idx,
        work_ticket_idx,
    )


async def _materialize_backfill_adapter_set_hash(
    pool: asyncpg.Pool,
    *,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> str | None:
    """Re-derive the canonical adapter-set hash for the backfill, once.

    Every read-mask / fastq-to-parquet ticket masks against the SAME canonical
    adapter set (`default_adapter_reference_idx`), so the `adapter_set_hash` that
    feeds `_build_mask_params` is identical across all of them. We re-materialize
    the adapter Parquet once via the same DoGet path the mint uses
    (`_resolve_qc_adapters`) and hash its bytes (`_adapter_set_hash`). The hash is
    over the SERIALIZED Parquet bytes, so this reproduces the mint's hash only as
    long as the backfill runs under the same pyarrow/Parquet writer the mint did:
    a writer change that alters the on-disk byte layout would shift the hash and
    force a re-mint rather than a backfill match. Returns None when no default
    adapter reference is configured (a deploy that mints maskless / for a test
    seam) — the caller then builds params with `adapter_set_hash=None`.
    """
    if default_adapter_reference_idx is None:
        return None
    bound = await _resolve_qc_adapters(
        pool,
        default_adapter_reference_idx=default_adapter_reference_idx,
        data_plane_url=data_plane_url,
        signing_key=signing_key,
        workspace=workspace,
    )
    return _adapter_set_hash(bound[QC_ADAPTER_BINDING])
