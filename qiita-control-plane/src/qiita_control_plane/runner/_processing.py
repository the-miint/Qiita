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
    """The canonical params a processing_idx hashes — the SINGLE source of truth
    for the run's identity shape. RESULT-AFFECTING inputs only (non-result params
    like threads/mem never enter the hash):

      - mask_idx: WHICH masked pass-set is assembled. This is the gating input
        predicate — assembling mask A vs mask B for the same sample+assembler must
        be two DISTINCT identities, not a false duplicate. Read from `bound` (the
        masked-reads resolver binds it before this runs).
      - assembler: the step-1 assembler, defaulting to the action's context_schema
        default when the submitter omits it (so omitted-vs-explicit-default
        collapse to one identity). `assembler_default` is passed by the caller
        straight off `context_schema`, so the default literal lives in ONE place.

    As more result-affecting params are parameterized (min-contig-length, DAS_Tool
    threshold, LCG cutoff) they are added here, and every processing_idx re-hashes
    fleet-wide."""
    return {
        "workflow": action_id,
        "version": action_version,
        "mask_idx": bound.get(MASK_IDX_BINDING),
        "assembler": bound.get(ASSEMBLER_BINDING) or assembler_default,
    }


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
    if params["assembler"] is not None:
        bindings[ASSEMBLER_BINDING] = params["assembler"]
    return bindings
