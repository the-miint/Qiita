"""Runner read-mask identity (mask_idx) minting."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
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

# Binding for the CP-resolved lima argument string. The `lima_export` step lists
# it in its `params:` and writes it into `lima_config.json`, which the container
# reads — a scalar cannot ride a container step's `inputs` (they are bind-mount
# paths).
LIMA_ARGS_BINDING = "lima_args"

# Resolved QC config the mask hash covers — the effective fastp-equivalent
# filter the qc job applies. Mirrors the constants in
# qiita_compute_orchestrator.jobs.qc (the fastp `-l 100` defaults); kept here
# (not imported) because the control plane does not depend on the orchestrator
# package. A change to the qc filter must update both so the mask identity stays
# faithful to the filter actually applied.
_QC_RESOLVED_MIN_LENGTH = 100
_QC_RESOLVED_FILTER_TAIL = "0, 15, 40, 5, 0"

# Canonical lima argument string per preset. The CLIENT chooses only the preset
# (`lima_preset` in action_context); the control plane resolves the arguments.
# A client-supplied arg string would let a caller forge mask identity (collide
# with any existing mask by naming its args) and pass arbitrary flags into a
# container. Adding a preset here is purely additive — existing masks hash
# unchanged.
#
# `--neighbors` is why the adapter FASTA's record ORDER is load-bearing: it keeps
# only barcode pairs that are adjacent records in the file. It is NOT implied by
# `--hifi-preset ASYMMETRIC` (lima scores barcodes all-vs-all).
_LIMA_PRESET_ARGS = {
    "ASYMMETRIC": "--hifi-preset ASYMMETRIC --neighbors --peek-guess",
    "SYMMETRIC": "--hifi-preset SYMMETRIC --peek-guess",
}

# lima version vendored into the container image. It belongs in the mask identity
# for the same reason `filter_version` does: lima decides where the adapter clip
# lands, so a version bump changes the effective filter and MUST re-mint rather
# than silently reuse a mask built by a different binary. The CI guard ties this
# to `sif-build.env`'s VERIFY_MATCH, so the constant and the installed binary
# cannot drift. Floor is the version qp-pacbio validated against.
_LIMA_VERSION = "2.13.0"

# MD5 of the Twist adapter FASTA vendored INTO the lima container image
# (`workflows/lima/twist_adapters_231010.fasta`). The control plane cannot hash a
# file inside a SIF, so this constant is how the adapter bytes enter the mask
# identity: re-vendoring a different set re-mints rather than silently reusing a
# mask built from other adapters. A CI guard asserts it equals the vendored
# file's md5, so the constant and the bytes lima sees cannot drift.
_LIMA_ADAPTER_SET_MD5 = "ace7e3019407e034ee6e6fafb36f9362"


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


def _resolved_lima(action_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """The effective lima config for the mask hash, or None when lima is off.

    Gated on `lima_enabled` so a stale `lima_preset` left in action_context by a
    disabled run cannot shift the hash. Only `preset` is client-chosen; `args`
    and `adapter_set_md5` are control-plane constants (see `_LIMA_PRESET_ARGS`).

    Returned as a NESTED block, mirroring `resolved_qc`: a future lima knob added
    inside it changes the hash only for masks that actually ran lima, leaving
    every Illumina (and non-lima PacBio) mask untouched. Flat top-level keys would
    re-mint the whole fleet on every addition.
    """
    if not action_context.get("lima_enabled"):
        return None
    preset = action_context.get("lima_preset")
    args = _LIMA_PRESET_ARGS.get(preset) if isinstance(preset, str) else None
    if args is None:
        raise _submission_bad_input(
            f"lima_enabled requires lima_preset to be one of "
            f"{sorted(_LIMA_PRESET_ARGS)}; got {preset!r}"
        )
    return {
        "version": _LIMA_VERSION,
        "preset": preset,
        "args": args,
        "adapter_set_md5": _LIMA_ADAPTER_SET_MD5,
    }


def _resolved_syndna_reference_idx(action_context: Mapping[str, Any]) -> int | None:
    """The syndna reference the mask hash carries, or None when syndna is off.

    Gated on `syndna_enabled` for the same reason as `_resolved_lima`. (The
    `host_*_reference_idx` keys below are read UNGATED — a stale value with
    `host_filter_enabled` false would enter the hash. Pre-existing; not widened
    here, since every producer writes the flag and the refs together.)
    """
    if not action_context.get("syndna_enabled"):
        return None
    return action_context.get("syndna_reference_idx")


def _build_mask_params(
    *,
    action_id: str,
    action_version: str,
    prep_protocol_idx: int | None,
    instrument_model: str | None,
    adapter_set_hash: str | None,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
    resolved_lima: dict[str, Any] | None,
    syndna_reference_idx: int | None,
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

    `resolved_lima` and `syndna_reference_idx` are what distinguish the five PacBio
    protocols. `prep_protocol_idx` cannot: it is the operator's `--prep-protocol-idx`
    flag, stamped uniformly onto every sample in a run, so it is IDENTICAL across
    the protocols. Neither does `instrument_model` (a model string, not a run id),
    and no run/pool identifier appears here BY DESIGN — a mask definition is a
    recipe that dedups fleet-wide. Without these two keys, a case-5 run (lima +
    syndna) and a case-1 run (neither) submitted weeks apart with the same operator
    flags hash identically and share one mask_idx, whose stored params then describe
    only one of them.

    Any change to the keys, nesting, or resolved-QC constants here changes every
    mask's identity fleet-wide — keep it deterministic and keyed only on the
    effective filter.
    """
    return {
        "filter_workflow": action_id,
        "filter_version": action_version,
        "host_rype_reference_idx": host_rype_reference_idx,
        "host_minimap2_reference_idx": host_minimap2_reference_idx,
        "syndna_reference_idx": syndna_reference_idx,
        "prep_protocol_idx": prep_protocol_idx,
        "resolved_qc": {
            "instrument_model": instrument_model,
            "min_length": _QC_RESOLVED_MIN_LENGTH,
            "filter_read_tail": _QC_RESOLVED_FILTER_TAIL,
            "adapter_set_hash": adapter_set_hash,
        },
        "resolved_lima": resolved_lima,
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
    resolved_lima: dict[str, Any] | None,
    syndna_reference_idx: int | None,
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
        resolved_lima=resolved_lima,
        syndna_reference_idx=syndna_reference_idx,
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
