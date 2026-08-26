"""Runner de novo alignment identity (alignment_idx) minting, and the
qiita.alignment_sample gate row the runner owns.

The per-sample counterpart to `align_planner`, which resolves a BLOCK ticket's
alignment_idx at plan time and stores it on the ticket. A prep_sample-scoped alignment
workflow has no planner: one ticket is one sample, so the identity is minted before the
step loop from the run's canonical params, exactly as `_processing` mints a
processing_idx. Same params -> same alignment_idx (idempotent re-run); different params
-> a distinct id, so a re-run at a different threshold lands beside the first rather
than overwriting it.

The signal is the terminal `finalize-alignment-sample` action, mirroring
`_workflow_writes_assembly_gate`: a workflow gets the two writes here exactly when it
declares the third.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
from qiita_common.actions import (
    ALIGNMENT_IDX_BINDING,
    WorkflowAction,
    context_schema_default,
)
from qiita_common.analytic import MAX_SECONDARY
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.backend_failure import StepNoData

from ..repositories.alignment_definition import (
    ParamsDoNotSurviveStorageError,
    fetch_alignment_definition_by_idx,
    mint_alignment_definition,
)
from ..repositories.assembly import fetch_assembly_sample_state
from ..repositories.block import (
    create_alignment_sample_pending,
    fetch_mask_sample_state,
)
from ._db import persist_ticket_idx
from ._upload import _submission_bad_input

# action_context keys the de novo resolver reads. Prefixed like `align/1.0.0.yaml`'s
# own keys where the bare name is already a binding the runner acts on: `mask_idx` in a
# step's `params:` makes `_workflow_needs_mask` MINT a mask, and `processing_idx` there
# makes `_workflow_needs_processing` mint a processing identity. This workflow CONSUMES
# both, so neither may appear as a params value.
ASSEMBLY_PROCESSING_IDX_BINDING = "assembly_processing_idx"
ALIGN_MASK_IDX_BINDING = "align_mask_idx"

# The caller-settable knobs. Each is hashed into the alignment identity, so a run at a
# different value is a different alignment rather than a silent overwrite of the first.
# Their DEFAULTS are read off the action's context_schema, never redeclared here — the
# hash and the job must see the same value, and a second literal is how those drift.
PRESET_BINDING = "preset"
MIN_IDENTITY_BINDING = "min_identity"
MIN_QUERY_COVERAGE_BINDING = "min_query_coverage"

# The two identities this workflow CONSUMES, mapped to the name each takes in the
# hashed params. Neither has a context_schema default — both are `required` — so they
# are resolved and coerced separately from the knobs above.
_SELECTOR_KEYS = {
    ASSEMBLY_PROCESSING_IDX_BINDING: "processing_idx",
    ALIGN_MASK_IDX_BINDING: "mask_idx",
}

_KNOB_BINDINGS = (PRESET_BINDING, MIN_IDENTITY_BINDING, MIN_QUERY_COVERAGE_BINDING)
_KNOB_TYPES = {
    PRESET_BINDING: str,
    MIN_IDENTITY_BINDING: float,
    MIN_QUERY_COVERAGE_BINDING: float,
}

# What the reads are aligned AGAINST, as an identity term: the sample's own contigs.
_SUBJECT_ASSEMBLY = "assembly"

# The aligner, as an identity term. Fixed rather than a knob: the de novo subject is a
# long-read assembly and `align_denovo` calls minimap2. It is hashed anyway so that
# adding a second aligner later re-mints instead of reusing an identity built by a
# different one.
_ALIGNER = "minimap2"


def _workflow_writes_alignment_gate(steps: list[Any]) -> bool:
    """True iff some entry is the `finalize-alignment-sample` action.

    The signal for everything in this module: the alignment_idx mint, the ticket
    column write, and the 'pending' gate row. Keying on the terminal action's presence
    rather than on the action_id means a workflow gets them exactly when it declares
    it. Mirrors `_processing._workflow_writes_assembly_gate`.
    """
    gate = LibraryPrimitive.FINALIZE_ALIGNMENT_SAMPLE
    return any(isinstance(entry, WorkflowAction) and entry.name == gate for entry in steps)


def _knob_defaults(context_schema: dict[str, Any]) -> dict[str, Any]:
    """Each knob's context_schema default, so the hash and the job read one literal."""
    return {key: context_schema_default(context_schema, key) for key in _KNOB_BINDINGS}


def _build_denovo_alignment_params(
    action_id: str,
    action_version: str,
    bound: dict[str, Any],
    *,
    defaults: dict[str, Any],
) -> dict[str, Any]:
    """The canonical params a de novo alignment_idx hashes — the one place this
    identity's shape is defined. Result-affecting inputs only:

      - subject / processing_idx: which contigs the reads are aligned against. The
        assembly run, not the sample, because a sample re-assembled under different
        params has different contigs and so a different alignment.
      - mask_idx: which reads are aligned. Aligning mask A's pass-set and mask B's
        against the same contigs must be two identities, not a false duplicate.
      - aligner / preset: how they are aligned.
      - min_identity / min_query_coverage: what a read must score for its records to
        be persisted at all. A threshold change does not re-filter an existing
        alignment's rows, it produces a different set of them.

    A submitter-supplied knob overrides the action's default; an omitted one collapses
    onto that default, so omitted-vs-explicit-default are one identity.

    **Every value is coerced to its declared type first**, the two consumed identities
    included, because the hash is over canonical JSON. A JSON-schema `type: number`
    admits both `1` and `1.0`, and `type: integer` admits `42.0` as well — it matches
    any number with a zero fractional part, so `{"minimum": 1, "type": "integer"}`
    validates a float. `json.dumps` renders each pair as two different strings, so one
    config would take two alignment_idx. `_assert_params_survive_storage` cannot catch
    it: `42.0` round-trips jsonb unchanged, hashing the same before and after.

    **This key set is disjoint from the block path's** (`align_planner` hashes
    `{reference_idx, aligner, mask_idx, shard_ids}`), so a de novo alignment_idx can
    never collide with a sharded one. Two sites rely on that: the per-sample gate flip
    makes no cross-path refusal, and the per-sample delete cannot reach a block's rows.
    """
    resolved = {
        key: bound[key] if bound.get(key) is not None else defaults[key] for key in _KNOB_BINDINGS
    }
    missing = sorted(key for key, value in resolved.items() if value is None)
    if missing:
        raise _submission_bad_input(
            f"de novo alignment params are incomplete: {missing} resolved to None. "
            "Each is either supplied in action_context or defaulted by the action's "
            "context_schema; a missing default means the action declaration is wrong, "
            "not the submission"
        )
    absent = sorted(binding for binding in _SELECTOR_KEYS if bound.get(binding) is None)
    if absent:
        raise _submission_bad_input(
            f"de novo alignment selectors are incomplete: {absent} absent from "
            "action_context. Each names an identity this workflow consumes — which "
            "assembly run's contigs, which mask's reads — and neither has a default to "
            "fall back on"
        )
    return {
        "subject": _SUBJECT_ASSEMBLY,
        "workflow": action_id,
        "version": action_version,
        "aligner": _ALIGNER,
        # Fixed, and hashed for the same reason `aligner` is: it decides WHICH
        # alignments the run collects, so two runs at identical thresholds under
        # different caps are different results. `qiita_common.analytic.MAX_SECONDARY`
        # is the one copy — the job passes the same constant to `align_minimap2`.
        "max_secondary": MAX_SECONDARY,
        **{key: int(bound[binding]) for binding, key in _SELECTOR_KEYS.items()},
        **{key: _KNOB_TYPES[key](value) for key, value in resolved.items()},
    }


async def _persist_alignment_idx(
    pool: asyncpg.Pool, work_ticket_idx: int, alignment_idx: int
) -> None:
    """Write the minted `alignment_idx` onto the ticket row.

    `delete-alignment-sample` and `finalize-alignment-sample` both read this COLUMN
    rather than action_context, and refuse on NULL; so does the resume path in
    `_resolve_denovo_alignment_idx`, which re-attaches to it instead of re-deriving.
    Writing it again on a resume writes the same value."""
    await persist_ticket_idx(
        pool, column="alignment_idx", work_ticket_idx=work_ticket_idx, value=alignment_idx
    )


async def _create_alignment_gate_pending(
    pool: asyncpg.Pool, *, alignment_idx: int, prep_sample_idx: int
) -> None:
    """Materialize this run's alignment_sample gate row at 'pending'.

    Run immediately after the mint, which is the earliest point the row's key exists.
    Idempotent, and it never resurrects a row an earlier run flipped to 'completed'
    (`create_alignment_sample_pending`'s ON CONFLICT DO NOTHING).
    """
    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[prep_sample_idx]
        )


async def _require_assembly_subject(
    pool: asyncpg.Pool, *, processing_idx: int, prep_sample_idx: int
) -> None:
    """Refuse to align unless this assembly run finished and produced contigs.

    Reads the gate, whose contract `fetch_assembly_sample_state` states and which other
    consumers point at rather than restate. This maps its states to the two outcomes a
    submission has, so no caller has to: 'completed' proceeds, 'no_data' is the
    terminal-but-not-a-failure `StepNoData` (the run assembled nothing, so this sample
    has no subject), and everything else is bad input.

    Absence gets its own message rather than falling into the catch-all, because it is
    reachable for a sample that really did assemble: this is the first consumer to read
    the gate, and runs that finished before it existed wrote no row (see the
    `long-read-assembly` entry in `DEPLOY_CHECKLIST.md`). "gate reads None" would send
    the operator looking for a stalled run instead of at the remedy.
    """
    state = await fetch_assembly_sample_state(
        pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
    )
    if state == "completed":
        return
    if state is None:
        raise _submission_bad_input(
            f"no assembly_sample gate row exists for assembly run {processing_idx} and "
            f"prep_sample {prep_sample_idx}. Absence is never 'assembled', so there is "
            "nothing to align against. Two ways to get here: the run never reached this "
            "sample, or it assembled before the gate existed and so wrote no row. "
            "Either way the remedy is the same — re-submit long-read-assembly for this "
            "sample, which is admitted and writes the row."
        )
    if state == "no_data":
        raise StepNoData(
            reason=(
                f"assembly run {processing_idx} produced no contigs for prep_sample "
                f"{prep_sample_idx}; there is nothing to align against"
            ),
        )
    raise _submission_bad_input(
        f"assembly run {processing_idx} for prep_sample {prep_sample_idx} is not "
        f"complete (assembly_sample gate reads {state!r}); its contigs are the subject "
        "this workflow aligns against, so there is nothing to align to until the gate "
        "reads 'completed'"
    )


async def _require_masked_query(pool: asyncpg.Pool, *, mask_idx: int, prep_sample_idx: int) -> None:
    """Refuse to align unless this sample's pass-set under `mask_idx` is complete.

    Reads the gate, whose contract `fetch_mask_sample_state` states; the invariant
    there is that a consumer which must not read an absent, partial or withdrawn
    pass-set proceeds on 'completed' alone. This is such a consumer: the pass-set is
    the QUERY the job aligns, so a 'pending' row would align a fraction of the sample's
    reads and record the result as the whole of it.

    `routes/read_masked.py` applies the same gate when it mints the DoGet ticket, but
    that runs when the job asks for it — from a compute node already holding an 8-cpu
    allocation, and after the mint has written both ticket columns and an
    alignment_sample row at 'pending' that no reconcile sweeps. Checking here refuses
    the submission before any of that. `_read_ingest` checks the same gate before its
    own read stream for the same reason.
    """
    state = await fetch_mask_sample_state(pool, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)
    if state != "completed":
        raise _submission_bad_input(
            f"{ALIGN_MASK_IDX_BINDING} {mask_idx} is not masked-complete for prep_sample "
            f"{prep_sample_idx} (mask_sample.state={state!r}); its pass-set is the query "
            "this workflow aligns, and an absent or partial one would align a fraction "
            "of the sample's reads as though it were all of them. Resubmit once masking "
            "is completed."
        )


async def _resolve_denovo_alignment_idx(
    pool: asyncpg.Pool,
    *,
    action_id: str,
    action_version: str,
    context_schema: dict[str, Any],
    bound: dict[str, Any],
    originator_principal_idx: int,
    work_ticket_idx: int,
) -> dict[str, Any]:
    """Bind this run's alignment_idx and the knob values it was hashed over.

    **A ticket that already carries the column is re-attached, never re-minted** — the
    same rule the block arm of the read-mask resolver applies to a plan-time mask_idx,
    and for a sharper reason here: this one identity is simultaneously the delete key,
    the register key and the gate key. The runner re-enters this whole block on every
    resume, and re-deriving would key those three off whatever the action declares NOW.
    `qiita-admin actions sync` upserts `context_schema` in place, so a deploy that edits
    a knob's default without a version bump moves what a re-derivation produces; the
    ticket would then finalize a gate under one identity over rows stamped with
    another, which is exactly what `_alignment_gate_threads_its_identity` exists to
    prevent.

    So the knobs are read back from the STORED params rather than re-resolved, and the
    job runs the configuration the identity actually names. A ticket whose definition
    was deleted mid-flight does not reach this branch: the FK is ON DELETE SET NULL, so
    the column is NULL and the mint below runs.

    Like the other pre-loop resolvers, any failure raises a SUBMISSION-attributed
    BAD_INPUT the outer handler turns into a FAILED ticket.
    """
    existing = await pool.fetchval(
        "SELECT alignment_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if existing is not None:
        row = await fetch_alignment_definition_by_idx(pool, existing)
        stored = json.loads(row["params"])
        absent = sorted(key for key in _KNOB_BINDINGS if key not in stored)
        if absent:
            raise _submission_bad_input(
                f"alignment definition {existing} on this ticket was minted under an "
                f"older params shape: {absent} are missing from its stored config, so "
                "the run this ticket resumes cannot be reconstructed. Delete the "
                "definition and resubmit"
            )
        return {
            ALIGNMENT_IDX_BINDING: existing,
            **{key: stored[key] for key in _KNOB_BINDINGS},
        }

    params = _build_denovo_alignment_params(
        action_id, action_version, bound, defaults=_knob_defaults(context_schema)
    )
    try:
        async with pool.acquire() as conn:
            row = await mint_alignment_definition(
                conn, params=params, principal_idx=originator_principal_idx
            )
    except ParamsDoNotSurviveStorageError as exc:
        raise _submission_bad_input(str(exc)) from exc
    except asyncpg.ForeignKeyViolationError as exc:
        raise _submission_bad_input(
            f"could not mint the de novo alignment definition: originator principal "
            f"{originator_principal_idx} does not exist"
        ) from exc
    return {
        ALIGNMENT_IDX_BINDING: row["alignment_idx"],
        **{key: params[key] for key in _KNOB_BINDINGS},
    }
