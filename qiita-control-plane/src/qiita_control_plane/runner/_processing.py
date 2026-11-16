"""Runner processing identity (processing_idx) minting, and the two
qiita.assembly_sample gate writes the runner owns (the gate is keyed on the
processing_idx minted here; the third write is the terminal
`finalize-assembly-sample` library action).

A processing_idx is minted before the step loop (like mask_idx) from the run's
canonical params — the workflow + version + the inputs and knobs that change the
RESULT: for assembly the mask_idx that selects WHICH reads are assembled and the
assembler; for amplicon the trim, sortmerna_reference_idx, and orient_primer (plus
the primer only when orienting). Same params -> same processing_idx (idempotent
re-run); different params -> a
distinct id, so a re-run's bins never collide with a prior run's, and assembling a
DIFFERENT mask's pass-set is a distinct identity rather than a false duplicate that
disallow-without-delete would wrongly block.
"""

from __future__ import annotations

from typing import Any

import asyncpg
from qiita_common.actions import (
    PROCESSING_IDX_BINDING,
    action_threads_processing_idx,
)
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.hashing import canonical_params_hash

from ..repositories import require_transaction
from ..repositories.assembly import (
    create_assembly_sample_pending,
    upsert_assembly_sample_no_data,
)
from ..repositories.processing import ProcessingDeprecated, mint_processing
from ._base import _log, _workflow_declares_action
from ._mask import MASK_IDX_BINDING
from ._upload import _submission_bad_input

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
    like threads/mem never enter the hash). Each candidate is entered only when the
    workflow binds it (None dropped), so assembly hashes {mask_idx, assembler} and
    amplicon hashes {trim, primer, orient_primer, sortmerna_reference_idx} without
    polluting each other:

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
    threshold, LCG cutoff, `residue_split._MIN_RESIDUE_LENGTH_BP`) they are added
    here, and every processing_idx re-hashes fleet-wide.

    The container images are NOT in here. `version` is the workflow YAML's, so two
    runs of the same version are one identity even when the images between them
    changed — an edited entrypoint or def rebuilds the SIF (`HASH_INPUTS`, see
    docs/container-images.md) without any of this moving. A fix that lives in an
    image therefore does not reach runs that already exist; correcting those takes a
    new workflow version, which re-hashes them as a distinct run.

    Keying identity on the SIF build hash instead would re-mint on rebuilds that
    change no output — a comment or a pinned-version bump is enough — orphaning the
    artifacts stored against the old processing_idx. The granularity is also
    per-image, so editing one tool's def would re-mint runs whose assembly is
    byte-identical.

    sortmerna_reference_idx is the stable reference_idx, not the materialized path."""
    candidates: dict[str, Any] = {
        "mask_idx": bound.get(MASK_IDX_BINDING),
        "assembler": bound.get(ASSEMBLER_BINDING) or assembler_default,
    }
    # amplicon knobs, gated on `trim` (amplicon-required, assembly never binds it) so
    # they never enter an assembly identity. orient_primer's default (off) is applied
    # here, and primer is hashed ONLY when orienting — otherwise it's a no-op knob
    # (`_set_session_vars` ignores it) and an omitted-vs-explicit primer must not
    # split the identity.
    if bound.get("trim") is not None:
        orient = bool(bound.get("orient_primer"))
        candidates["trim"] = bound.get("trim")
        candidates["sortmerna_reference_idx"] = bound.get("sortmerna_reference_idx")
        candidates["orient_primer"] = orient
        if orient:
            candidates["primer"] = bound.get("primer")
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
    work_ticket_idx: int | None = None,
) -> dict[str, Any]:
    """Mint (or resolve) the processing_idx for this run's params and bind it.

    Run before the step loop when `_workflow_needs_processing`.
    `mint_processing` hashes the canonical params (canonical JSON) and upserts on
    it, so the same params resolve to the same processing_idx fleet-wide — unless
    the row it resolves to has been deprecated, which is refused as bad submission
    input. That is the guard that stops a run built from a pass-set judged unsound
    from assembling further samples: the params are the identity, so a re-submit
    under the same ones IS the deprecated run.

    Also binds the RESOLVED assembler back into `bound`, so the native step that
    writes run_config.json (and thus the container that assembles) runs exactly the
    assembler the identity hashed. Without this, an omitted assembler would hash the
    context_schema default while the container fell back to the native job's own
    Inputs default — a silent hash≠reality drift the moment those two defaults
    diverge.

    With `work_ticket_idx`, the same transaction records the processing on the ticket
    (`work_ticket.processing_idx`), holding the processing row `FOR SHARE` from the
    mint through that write (`_mint_and_hold`). A settings edit holds the row
    `FOR UPDATE` while it checks for unfinished tickets (`definition_settings`), so
    the edit and this mint-and-record cannot both commit unseen by the other, and the
    ticket is never recorded under settings it did not hash.

    A redrive resumes a ticket keeping its completed steps' outputs and its assembly
    gate row, both keyed on the processing it recorded on its first attempt. When that
    processing's settings were edited since, the mint resolves to another row; the
    ticket is then failed as bad submission input rather than re-recorded, so no run
    mixes outputs of two processings. A new ticket runs under the edited settings."""
    params = _build_processing_params(
        action_id, action_version, bound, assembler_default=assembler_default
    )
    try:
        async with pool.acquire() as conn, conn.transaction():
            row = await _mint_and_hold(
                conn, workflow=action_id, version=action_version, params=params
            )
            if work_ticket_idx is not None:
                recorded = await conn.fetchval(
                    "SELECT processing_idx FROM qiita.work_ticket"
                    " WHERE work_ticket_idx = $1 FOR UPDATE",
                    work_ticket_idx,
                )
                if recorded is not None and recorded != row["processing_idx"]:
                    raise _submission_bad_input(
                        f"work ticket {work_ticket_idx} first ran under processing"
                        f" {recorded}, whose settings were edited since; its params now"
                        f" resolve to processing {row['processing_idx']}. Its completed"
                        " steps and assembly gate are keyed on the first, so it cannot"
                        " resume; submit a new ticket"
                    )
                await conn.execute(
                    "UPDATE qiita.work_ticket SET processing_idx = $1 WHERE work_ticket_idx = $2",
                    row["processing_idx"],
                    work_ticket_idx,
                )
    except ProcessingDeprecated as exc:
        # The ticket named a run config that cannot be minted against, which is bad
        # input rather than a fault to retry. Left unhandled it reaches
        # run_workflow's catch-all and is recorded as UNKNOWN_PERMANENT, which tells
        # the operator nothing. Same shape as the mask twin in `_mask.py`.
        raise _submission_bad_input(exc.detail) from exc
    bindings: dict[str, Any] = {PROCESSING_IDX_BINDING: row["processing_idx"]}
    # absent for non-assembly workflows; only bind when it was hashed.
    if params.get("assembler") is not None:
        bindings[ASSEMBLER_BINDING] = params["assembler"]
    return bindings


# A settings edit that commits between a mint and its hold re-keys the row the mint
# resolved; the next mint then resolves (or inserts) the row for these params. One
# retry covers one such edit; a second in the same window raises RuntimeError, which
# fails the ticket.
_MINT_HOLD_ATTEMPTS = 2


async def _mint_and_hold(
    conn: asyncpg.Connection, *, workflow: str, version: str, params: dict[str, Any]
) -> asyncpg.Record:
    """`mint_processing`, then hold the minted row `FOR SHARE` for the caller's
    transaction, re-minting when the held row's `params_hash` is no longer the hash
    of `params`: a settings edit committed between the mint and the hold. Requires an
    open transaction: outside one the `FOR SHARE` ends with its statement."""
    require_transaction(conn)
    params_hash = canonical_params_hash(params)
    for _ in range(_MINT_HOLD_ATTEMPTS):
        row = await mint_processing(conn, workflow=workflow, version=version, params=params)
        held = await conn.fetchval(
            "SELECT params_hash FROM qiita.processing WHERE processing_idx = $1 FOR SHARE",
            row["processing_idx"],
        )
        if held == params_hash:
            return row
    raise RuntimeError(
        f"processing for {workflow} {version}: the minted row's settings were edited"
        f" between the mint and its hold on each of {_MINT_HOLD_ATTEMPTS} attempts"
    )


def _workflow_writes_assembly_gate(steps: list[Any]) -> bool:
    """True iff some entry is the `finalize-assembly-sample` action.

    The signal for both of the runner's own qiita.assembly_sample writes: the
    'pending' row after the processing_idx mint, and the 'no_data' row in the
    StepNoData handler. Keying on the terminal action's presence rather than on
    the action_id means a workflow gets the runner's two writes exactly when it
    declares the third.
    """
    return _workflow_declares_action(steps, LibraryPrimitive.FINALIZE_ASSEMBLY_SAMPLE)


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

    The write's guard leaves a standing 'completed' or 'invalidated' row alone
    (reasoning on `repositories.assembly.upsert_assembly_sample_no_data`); this
    logs which one held when that happens.
    """
    async with pool.acquire() as conn, conn.transaction():
        standing = await upsert_assembly_sample_no_data(
            conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )
    if standing is not None:
        _log.warning(
            "assembly_sample gate (processing %d, prep_sample %d) stays %r, so this"
            " run's 'no_data' is not written",
            processing_idx,
            prep_sample_idx,
            standing,
        )
