"""Runner processing identity (processing_idx) minting.

A processing_idx is minted before the step loop (like mask_idx) from the run's
canonical params — the workflow + version + the inputs and knobs that change the
RESULT: the mask_idx that selects WHICH reads are assembled, and the assembler.
Same params -> same processing_idx (idempotent re-run); different params -> a
distinct id, so a re-run's bins never collide with a prior run's, and assembling a
DIFFERENT mask's pass-set is a distinct identity rather than a false duplicate that
disallow-without-delete would wrongly block.
"""

from __future__ import annotations

from typing import Any

import asyncpg

from ..repositories.processing import mint_processing
from ._mask import MASK_IDX_BINDING

# Binding name the runner threads the minted processing_idx under. A step lists it
# in its `params:` (processing_idx -> <job>.Inputs.processing_idx), which both
# signals the runner to mint the identity before the step loop and carries the
# value into the step. The write-assembly-membership action reads it from `bound`.
PROCESSING_IDX_BINDING = "processing_idx"

# action_context key naming the step-1 assembler. Its default is single-sourced
# from the action's context_schema (see `_mint_processing_idx`), never hardcoded
# here — a re-declared literal would let the hash pick a different assembler than
# the container runs.
ASSEMBLER_BINDING = "assembler"


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
    action_id: str,
    action_version: str,
    bound: dict[str, Any],
    *,
    assembler_default: str | None = None,
) -> dict[str, Any]:
    """the result-affecting params a processing_idx hashes.

    each candidate is entered only when the workflow binds it (None dropped), so
    each workflow's identity carries just its own knobs and neither pollutes the
    other: assembly hashes {mask_idx, assembler}, amplicon hashes {trim, primer,
    orient_primer, sortmerna_reference_idx}. add new knobs here.

    sortmerna_reference_idx is the stable reference_idx, not the materialized path."""
    candidates = {
        "mask_idx": bound.get(MASK_IDX_BINDING),
        "assembler": bound.get(ASSEMBLER_BINDING) or assembler_default,
        "trim": bound.get("trim"),
        "primer": bound.get("primer"),
        "orient_primer": bound.get("orient_primer"),
        "sortmerna_reference_idx": bound.get("sortmerna_reference_idx"),
    }
    params: dict[str, Any] = {"workflow": action_id, "version": action_version}
    params.update({k: v for k, v in candidates.items() if v is not None})
    return params


async def _mint_processing_idx(
    pool: asyncpg.Pool,
    *,
    action_id: str,
    action_version: str,
    bound: dict[str, Any],
    assembler_default: str | None = None,
) -> dict[str, Any]:
    """Mint (or resolve) the processing_idx for this run's params and bind it.

    Run before the step loop when `_workflow_needs_processing`.
    `mint_processing` hashes the canonical params (canonical JSON) and upserts on
    it, so the same params resolve to the same processing_idx fleet-wide.

    Also binds the RESOLVED assembler back into `bound`, so the native step that
    writes run_config.json (and thus the container that assembles) runs exactly the
    assembler the identity hashed. Without this, an omitted assembler would hash the
    context_schema default while the container fell back to the native job's own
    Inputs default — a silent hash≠reality drift the moment those two defaults
    diverge."""
    params = _build_processing_params(
        action_id, action_version, bound, assembler_default=assembler_default
    )
    async with pool.acquire() as conn:
        row = await mint_processing(conn, workflow=action_id, version=action_version, params=params)
    bindings: dict[str, Any] = {PROCESSING_IDX_BINDING: row["processing_idx"]}
    # absent for non-assembly workflows; only bind when it was hashed.
    if params.get("assembler") is not None:
        bindings[ASSEMBLER_BINDING] = params["assembler"]
    return bindings
