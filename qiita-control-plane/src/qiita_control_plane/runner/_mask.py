"""Runner read-mask identity (mask_idx) minting."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import Any, NamedTuple

import asyncpg

from ..repositories.mask_definition import (
    ADAPTER_HASH_SCHEME_SEQUENCE_HASH,
    ADAPTER_SET_HASH_KEY,
    RESOLVED_QC_KEY,
    MaskDefinitionDeprecated,
    mint_mask_definition,
)
from ..repositories.reference_membership import reference_sequence_set_hash
from ._db import persist_ticket_idx
from ._reference import QC_ADAPTER_BINDING, _resolve_qc_adapters
from ._upload import _submission_bad_input

# =============================================================================
# Read-mask identity (mask_idx) minting
# =============================================================================
#
# A read mask's identity is its filtering CONFIG: the filter workflow + version, the
# host reference(s) it depletes against and the params it depletes with, and the
# resolved QC config. The control plane mints a `mask_idx` deduplicated on the SHA-256
# of that config so the same config resolves to the same mask_idx fleet-wide; the
# host_filter step stamps it onto every read_mask row. The host references are read from the
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

# rype's host-call threshold, the resolved host-filter param the mask hash covers.
# Mirrors `qiita_compute_orchestrator.jobs.host_filter._RYPE_THRESHOLD` for the same
# reason as the QC and syndna values (kept here, not imported: the control plane does
# not depend on the orchestrator package); `test_host_filter_pins` reads it out of that
# source by AST so the mirror cannot drift silently.
#
# It CHANGES which reads are called host, which is what makes it identity and not
# detail: rype emits a row per bucket scoring at or above the threshold, and the job
# calls host on ANY emitted row, so the threshold IS the rype host call.
#
# The minimap2 stage's `preset` is deliberately NOT here. It is pinned in the job to
# the preset its `.mmi` was built with ('sr') rather than chosen per mask, and the
# index is already named by `host_minimap2_reference_idx` below — so it is invariant,
# and hashing it would re-mint every mask fleet-wide to discriminate nothing. Should a
# minimap2 param ever become a per-mask choice (a caller-supplied preset, or one that
# varies by platform), it belongs in `_resolved_host_filter` alongside the threshold.
_HOST_FILTER_RYPE_THRESHOLD = 0.05

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
# (`workflows/read-mask/twist_adapters_231010.fasta`). The control plane cannot hash a
# file inside a SIF, so this constant is how the adapter bytes enter the mask
# identity: re-vendoring a different set re-mints rather than silently reusing a
# mask built from other adapters. A CI guard asserts it equals the vendored
# file's md5, so the constant and the bytes lima sees cannot drift.
_LIMA_ADAPTER_SET_MD5 = "ace7e3019407e034ee6e6fafb36f9362"

# Resolved syndna config the mask hash covers — the effective spike-in filter the
# syndna job applies. Mirrors the constants in `qiita_compute_orchestrator.jobs.syndna`
# (kept here, not imported: the control plane does not depend on the orchestrator
# package). A change to the spike-in classifier must update both, so the mask
# identity stays faithful to the filter actually applied.
_SYNDNA_ALIGNER = "minimap2"
_SYNDNA_MM2_PRESET = "map-hifi"
# The identity METHOD is part of the effective filter, not a detail: `blast` charges a
# deletion per base and `gap_compressed` charges it once, so the same read can be a
# spike-in under one and not the other. A change here must re-mint.
_SYNDNA_IDENTITY_METHOD = "blast"
_SYNDNA_MIN_IDENTITY = 0.95
# Minimum fraction of the read that aligns, against the whole PLASMID. Mirrors
# jobs/syndna._MIN_ALIGNED_FRACTION. It CHANGES which reads are called spike-in, so it
# must enter the mask identity — a mask built at 0.90 and one built at 0.0 describe
# different filters and cannot share a mask_idx.
_SYNDNA_MIN_ALIGNED_FRACTION = 0.90
# Whether a read may be called a spike-in on a NON-primary (supplementary) alignment.
# Also part of the effective filter, and a bigger lever than it sounds: it turns the rule
# from "ANY alignment >= min_identity" into "the read's BEST alignment >= min_identity",
# so the same read can be a spike-in under one and not the other. A change here must
# re-mint, which is why it is hashed and not merely a comment in the job.
_SYNDNA_PRIMARY_ONLY = True


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


class AdapterSetHashes(NamedTuple):
    """The adapter identity under both derivations, for one resolved adapter set.

    `current` is what a mask mints on; `legacy` is the lookup key that finds a
    mask minted before the derivation changed. Both None when the config carries
    no adapter set.
    """

    current: str | None
    legacy: str | None

    def scheme(self) -> str | None:
        """The `mask_definition.adapter_hash_scheme` value for a config built from
        this pair: the current derivation's name, or None when there is no adapter
        set and so no derivation to record."""
        return ADAPTER_HASH_SCHEME_SEQUENCE_HASH if self.current is not None else None


def mask_params_both_derivations(
    build: Callable[..., dict[str, Any]], hashes: AdapterSetHashes
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """One config's params under the current adapter identity, and under the legacy
    one for `mint_mask_definition`'s fallback lookup.

    `build` is `_build_mask_params` (or the block planner's `_mask_params_for`
    wrapper) with everything except `adapter_set_hash` already bound, so the two
    dicts differ in exactly that key. The legacy half is None when the config
    carries no adapter set — the two derivations agree on None, so there is
    nothing for the fallback to find.

    Used by both mint sites so they build the pair identically.
    """
    return (
        build(adapter_set_hash=hashes.current),
        build(adapter_set_hash=hashes.legacy) if hashes.legacy else None,
    )


async def _adapter_set_hashes(
    db: asyncpg.Pool | asyncpg.Connection,
    *,
    reference_idx: int | None,
    adapter_parquet: Path | None,
) -> AdapterSetHashes:
    """Both adapter-identity derivations for one resolved adapter set.

    Shared by the per-sample mint (`_mint_read_mask`) and the block planner so the
    two derive the same pair for the same adapter set. `adapter_parquet` is the
    materialized set, needed only for the legacy digest.

    Raises when the two arguments disagree about whether an adapter set exists, or
    when the reference has no members while a materialized set does: the current
    digest reads Postgres membership and the legacy one reads the bytes a DoGet of
    that same membership produced, so a disagreement means those two have drifted
    and any mask minted from it would describe an adapter set that was not
    applied.
    """
    if adapter_parquet is None and reference_idx is None:
        return AdapterSetHashes(None, None)
    if adapter_parquet is None or reference_idx is None:
        raise _submission_bad_input(
            "adapter set is half-resolved: adapter_parquet="
            f"{adapter_parquet!r}, reference_idx={reference_idx!r}; both must be "
            "present or both absent"
        )
    current = await reference_sequence_set_hash(db, reference_idx)
    if current is None:
        raise _submission_bad_input(
            f"adapter reference {reference_idx} has no members in "
            "qiita.reference_membership, but its adapter set materialized to "
            f"{adapter_parquet}"
        )
    return AdapterSetHashes(current=current, legacy=_adapter_set_hash_legacy(adapter_parquet))


def _adapter_set_hash_legacy(adapter_parquet: Path) -> str:
    """SHA-256 hex of the materialized adapter-set Parquet's bytes — the ORIGINAL
    adapter identity derivation, retained only to recognize masks minted under it.

    The pyarrow writer stamps its own version into the Parquet footer
    (`created_by = "parquet-cpp-arrow version X.Y.Z"`), so this digest is a
    function of the writer version as well as the sequences: deterministic within
    a version, different across any bump. The measured digests and the removal
    plan are in the 20260804000000 migration header.

    Nothing mints on this value. `mint_mask_definition` takes it as the legacy
    lookup key and re-keys a row it matches onto
    `repositories.reference_membership.reference_sequence_set_hash`."""
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


def _resolved_syndna(action_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """The effective syndna config for the mask hash, or None when syndna is off.

    Gated on `syndna_enabled` for the same reason as `_resolved_lima`. (The
    `host_*_reference_idx` keys below are read UNGATED — a stale value with
    `host_filter_enabled` false would enter the hash. Pre-existing; not widened
    here, since every producer writes the flag and the refs together.)

    NESTED, mirroring `resolved_lima` / `resolved_qc`: the reference alone does not
    describe the filter. A read is a spike-in when it has a PRIMARY alignment to the
    reference at >= `min_identity` under `preset` — so the preset, the identity method,
    the threshold, AND the primary-only rule are all part of the effective filter, and
    all belong in the identity. The threshold in particular is expected to move once it
    is confirmed against real data with the assay owner — when it does, masks MUST
    re-mint rather than silently reuse one built at the old cutoff.

    Only the reference idx is client-chosen; the aligner, preset, and threshold are
    control-plane constants mirroring `jobs/syndna.py` (kept here, not imported —
    the control plane does not depend on the orchestrator package; a change to
    either must update both, as with the resolved-QC constants).
    """
    if not action_context.get("syndna_enabled"):
        return None
    return {
        "reference_idx": action_context.get("syndna_reference_idx"),
        "aligner": _SYNDNA_ALIGNER,
        "preset": _SYNDNA_MM2_PRESET,
        "identity_method": _SYNDNA_IDENTITY_METHOD,
        "min_identity": _SYNDNA_MIN_IDENTITY,
        "min_aligned_fraction": _SYNDNA_MIN_ALIGNED_FRACTION,
        "primary_only": _SYNDNA_PRIMARY_ONLY,
    }


def _resolved_host_filter(host_rype_reference_idx: int | None) -> dict[str, Any] | None:
    """The effective host-filter params for the mask hash, or None when the identity
    records no rype reference to apply them to.

    Keyed off that reference rather than a separate gate, so the block can never claim
    a stage the identity does not also name a reference for. Under the two-reference
    layout that is exactly "no rype stage ran" — the job skips the stage whose index
    path is unbound, and the path comes from the reference. A LEGACY
    `host_reference_idx` ticket is the one case where rype can run with this None: that
    key is not threaded into the mint at all, so such a mask's params already describe
    no host filtering whatsoever. Pre-existing and not widened here — flagged so the
    None is not read as a claim that nothing depleted.

    NESTED and None-when-absent, mirroring `resolved_lima` / `resolved_syndna`: the
    reference names WHAT we deplete against, never HOW aggressively. A read is host
    when rype scores it at or above `rype_threshold` against that reference, so two
    masks built at different thresholds describe different filters and must not share
    a mask_idx. Nesting is what keeps a future threshold change from re-minting a mask
    that never ran rype at all.
    """
    if host_rype_reference_idx is None:
        return None
    return {"rype_threshold": _HOST_FILTER_RYPE_THRESHOLD}


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
    resolved_syndna: dict[str, Any] | None,
) -> dict[str, Any]:
    """Assemble the resolved-filter-config dict that `mint_mask_definition`
    hashes (canonical JSON → SHA-256 → `params_hash`) to mint/dedup a mask.

    This is the SINGLE source of truth for the mask's identity shape — the mint
    path (`_mint_read_mask`) and the block planner both call it so they derive the
    SAME hash for the SAME effective config. Every value is the EFFECTIVE filter
    (the host refs the filter applies + the params it applies on top of them + the
    adapter-set identity + thresholds), so two callers with the same effective config
    collapse to one mask even if descriptive metadata differs. `adapter_set_hash` is
    passed in already computed (`reference_sequence_set_hash`, over the adapter
    reference's sequence hashes) rather than as a reference idx, so a caller can also
    pass the legacy byte-derived value to build the fallback lookup key — see
    `mask_params_both_derivations`.

    `resolved_lima` and `resolved_syndna` are what distinguish the five PacBio
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
        "resolved_host_filter": _resolved_host_filter(host_rype_reference_idx),
        "prep_protocol_idx": prep_protocol_idx,
        RESOLVED_QC_KEY: {
            "instrument_model": instrument_model,
            "min_length": _QC_RESOLVED_MIN_LENGTH,
            "filter_read_tail": _QC_RESOLVED_FILTER_TAIL,
            ADAPTER_SET_HASH_KEY: adapter_set_hash,
        },
        "resolved_lima": resolved_lima,
        "resolved_syndna": resolved_syndna,
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
    default_adapter_reference_idx: int | None,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
    resolved_lima: dict[str, Any] | None,
    resolved_syndna: dict[str, Any] | None,
) -> dict[str, int]:
    """Mint (or resolve) the `mask_idx` for this filtering config and bind it.

    Run before the step loop when `_workflow_needs_mask`. The config is:
      * the filter workflow + version (this action),
      * the host reference(s) the `host_filter` step actually APPLIES, passed in
        from the same action_context values `_resolve_host_filter_indexes`
        consumes (`host_rype_reference_idx` / `host_minimap2_reference_idx`) — so
        the minted mask_idx's params describe the filter that ran. Absent host
        refs mean no host filtering, a faithful part of the config (None),
      * the params those host stages APPLY to those references (`resolved_host_filter`
        — rype's host-call threshold), because the reference says what we deplete
        against and never how aggressively, and
      * the resolved QC config (instrument model gating polyG, the fastp-`-l 100`
        thresholds, and the adapter-set identity — a hash of the adapter
        reference's sequences, not of the materialized artifact).
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
    # config. The adapter identity is the reference's sorted sequence hashes
    # (None when this workflow uses no adapter set); the legacy params carry the
    # same config under the byte derivation, as the mint's fallback lookup key.
    adapter_hashes = await _adapter_set_hashes(
        pool,
        reference_idx=default_adapter_reference_idx if adapter_parquet is not None else None,
        adapter_parquet=adapter_parquet,
    )
    params, legacy_params = mask_params_both_derivations(
        partial(
            _build_mask_params,
            action_id=action_id,
            action_version=action_version,
            prep_protocol_idx=prep_protocol_idx,
            instrument_model=instrument_model,
            host_rype_reference_idx=host_rype_reference_idx,
            host_minimap2_reference_idx=host_minimap2_reference_idx,
            resolved_lima=resolved_lima,
            resolved_syndna=resolved_syndna,
        ),
        adapter_hashes,
    )

    try:
        async with pool.acquire() as conn:
            mask_row = await mint_mask_definition(
                conn,
                filter_workflow=action_id,
                filter_version=action_version,
                params=params,
                principal_idx=originator_principal_idx,
                legacy_params=legacy_params,
                adapter_hash_scheme=adapter_hashes.scheme(),
            )
    except MaskDefinitionDeprecated as exc:
        # Same class as the FK case below: the ticket named a config that cannot be
        # minted against, which is bad input rather than a fault to retry. Left
        # unhandled it reaches run_workflow's catch-all and is recorded as
        # UNKNOWN_PERMANENT, which tells the operator nothing.
        raise _submission_bad_input(exc.detail) from exc
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
    this writes the same value."""
    await persist_ticket_idx(
        pool, column="mask_idx", work_ticket_idx=work_ticket_idx, value=mask_idx
    )


async def _materialize_adapter_set_hashes(
    pool: asyncpg.Pool,
    *,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> AdapterSetHashes:
    """Re-derive the canonical adapter set's identity, once, for a caller that has
    no materialized adapter Parquet of its own.

    Every read-mask ticket masks against the SAME canonical adapter set
    (`default_adapter_reference_idx`), so the pair that feeds `_build_mask_params`
    is identical across all of them. The current digest reads Postgres membership;
    the legacy one needs the bytes, so the adapter Parquet is re-materialized via
    the same DoGet path the mint uses (`_resolve_qc_adapters`). That
    materialization exists ONLY to feed the legacy digest and comes out with it in
    the contract phase.

    Returns both-None when no default adapter reference is configured (a deploy
    that mints maskless / for a test seam) — the caller then builds params with
    `adapter_set_hash=None`.
    """
    if default_adapter_reference_idx is None:
        return AdapterSetHashes(None, None)
    bound = await _resolve_qc_adapters(
        pool,
        default_adapter_reference_idx=default_adapter_reference_idx,
        data_plane_url=data_plane_url,
        signing_key=signing_key,
        workspace=workspace,
    )
    return await _adapter_set_hashes(
        pool,
        reference_idx=default_adapter_reference_idx,
        adapter_parquet=bound[QC_ADAPTER_BINDING],
    )
