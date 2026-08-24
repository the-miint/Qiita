"""Runner processing identity (processing_idx) minting, and the two
qiita.assembly_sample gate writes the runner owns (the gate is keyed on the
processing_idx minted here; the third write is the terminal
`finalize-assembly-sample` library action).

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
from qiita_common.actions import (
    PROCESSING_IDX_BINDING,
    WorkflowAction,
    action_threads_processing_idx,
)
from qiita_common.api_paths import LibraryPrimitive

from ..repositories.assembly import (
    create_assembly_sample_pending,
    upsert_assembly_sample_no_data,
)
from ..repositories.processing import mint_processing
from ._base import _log
from ._mask import MASK_IDX_BINDING

# action_context key naming the step-1 assembler. Its default is single-sourced
# from the action's context_schema (see `_mint_processing_idx`), never hardcoded
# here — a re-declared literal would let the hash pick a different assembler than
# the container runs.
ASSEMBLER_BINDING = "assembler"


def _workflow_needs_processing(steps: list[Any]) -> bool:
    """The runner's signal to mint the processing identity before the step loop.

    Reads `qiita_common.actions.action_threads_processing_idx`, which owns the
    rule and which the load-time validator on ActionDefinition reads too. Named
    here to sit beside the runner's other pre-loop signals (`_workflow_needs_mask`,
    `_workflow_writes_assembly_gate`).
    """
    return action_threads_processing_idx(steps)


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
        be two DISTINCT identities, not a false duplicate. Read from `bound`, where
        it arrives either from action_context (assembly, bound and checked non-NULL
        before the masked-reads resolver runs) or from the read-mask minting branch
        — so the mint this feeds has to stay after both.
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


def _workflow_writes_assembly_gate(steps: list[Any]) -> bool:
    """True iff some entry is the `finalize-assembly-sample` action.

    The signal for both of the runner's own qiita.assembly_sample writes: the
    'pending' row after the processing_idx mint, and the 'no_data' row in the
    StepNoData handler. Keying on the terminal action's presence rather than on
    the action_id means a workflow gets the runner's two writes exactly when it
    declares the third.
    """
    gate = LibraryPrimitive.FINALIZE_ASSEMBLY_SAMPLE
    return any(isinstance(entry, WorkflowAction) and entry.name == gate for entry in steps)


async def _create_assembly_gate_pending(
    pool: asyncpg.Pool,
    *,
    processing_idx: int,
    prep_sample_idx: int,
) -> None:
    """Materialize this run's assembly_sample gate row at 'pending'.

    Run immediately after the processing_idx mint, which is the earliest point
    the gate's key exists: the identity is a hash of the run's params, so there
    is nothing to key on at HTTP submit. Idempotent; what it does to a row a
    previous run left closed is stated on
    `repositories.assembly.create_assembly_sample_pending`.
    """
    async with pool.acquire() as conn, conn.transaction():
        await create_assembly_sample_pending(
            conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )


async def _record_assembly_gate_no_data(
    pool: asyncpg.Pool,
    *,
    processing_idx: int,
    prep_sample_idx: int,
) -> None:
    """Close this run's assembly_sample gate at 'no_data'.

    Run from the StepNoData handler. assembly_hash raises StepNoData when the
    sample produced no contig of any kind, which abandons the remaining entries —
    including `finalize-assembly-sample` — so without this write the row would sit
    at 'pending' for a run that has ended and will never move again.

    The write's guard leaves a standing 'completed' row from an earlier run of the
    same identity alone (reasoning on
    `repositories.assembly.upsert_assembly_sample_no_data`); this logs when that
    happens.
    """
    async with pool.acquire() as conn, conn.transaction():
        written = await upsert_assembly_sample_no_data(
            conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )
    if not written:
        _log.warning(
            "assembly_sample gate (processing %d, prep_sample %d) stays 'completed':"
            " an earlier run of this identity assembled contigs, so this run's"
            " 'no_data' is not written",
            processing_idx,
            prep_sample_idx,
        )
