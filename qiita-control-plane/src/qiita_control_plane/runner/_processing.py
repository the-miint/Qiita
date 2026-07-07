"""Runner processing identity (processing_idx) minting.

A processing_idx is minted before the step loop (like mask_idx) from the run's
canonical params — the workflow + version + result-affecting knobs (today the
assembler) — and threaded to the steps that record it (the assembly membership +
load). Same params -> same processing_idx (idempotent re-run); different params
-> a distinct id, so a re-run's bins never collide with a prior run's.
"""

from __future__ import annotations

from typing import Any

import asyncpg

from ..repositories.processing import mint_processing

# Binding name the runner threads the minted processing_idx under. A step lists it
# in its `params:` (processing_idx -> <job>.Inputs.processing_idx), which both
# signals the runner to mint the identity before the step loop and carries the
# value into the step. The write-assembly-membership action reads it from `bound`.
PROCESSING_IDX_BINDING = "processing_idx"


def _workflow_needs_processing(steps: list[Any]) -> bool:
    """True iff some entry threads `processing_idx` through its `params:` — the
    signal the runner mints the processing identity before the step loop. Mirrors
    `_workflow_needs_mask` (a scalar param, so it keys off `params` values)."""
    for entry in steps:
        params = getattr(entry, "params", None) or {}
        if PROCESSING_IDX_BINDING in params.values():
            return True
    return False


def _build_processing_params(
    action_id: str, action_version: str, bound: dict[str, Any]
) -> dict[str, Any]:
    """The canonical params a processing_idx hashes — the SINGLE source of truth
    for the run's identity shape. RESULT-AFFECTING knobs only (non-result params
    like threads/mem never enter the hash). Today: the assembler, defaulting to
    the workflow's default when the submitter omits it (so an omitted-vs-explicit
    default collapse to one identity). As more result-affecting params are
    parameterized (min-contig-length, DAS_Tool threshold, LCG cutoff) they are
    added here, and every processing_idx re-hashes fleet-wide."""
    return {
        "workflow": action_id,
        "version": action_version,
        "assembler": bound.get("assembler") or "hifiasm_meta",
    }


async def _mint_processing_idx(
    pool: asyncpg.Pool,
    *,
    action_id: str,
    action_version: str,
    bound: dict[str, Any],
) -> dict[str, int]:
    """Mint (or resolve) the processing_idx for this run's params and bind it.

    Run before the step loop when `_workflow_needs_processing`.
    `mint_processing` hashes the canonical params (canonical JSON) and upserts on
    it, so the same params resolve to the same processing_idx fleet-wide."""
    params = _build_processing_params(action_id, action_version, bound)
    async with pool.acquire() as conn:
        row = await mint_processing(conn, workflow=action_id, version=action_version, params=params)
    return {PROCESSING_IDX_BINDING: row["processing_idx"]}
