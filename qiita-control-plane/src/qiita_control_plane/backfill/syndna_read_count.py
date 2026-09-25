"""Write qiita.syndna_read_count for prep_samples masked before the read-mask workflow
persisted it.

A read-mask ticket's `syndna` step leaves its alignment in the ticket's scratch
workspace, and the `persist-syndna-read-count` action reduces it to per-insert
counts. A prep_sample whose mask completed without that action has a 'completed'
gate and no count rows, and the export refuses it.

**Re-reads the scratch file the step left, and nothing else.** The ticket's
COMPLETED `work_ticket_step` row for `syndna` gives the attempt, and that attempt's
`output/manifest.json` names the file bound to `alignment`; the path is joined as
the manifest gives it, with none of the orchestrator verifier's checks, because the
manifest is our own step's output. The counting and the write are
`actions.library.persist_syndna_read_count` — the function the workflow calls — so a
backfilled row and a workflow-written row cannot differ.

**Residue, not a guess.** A prep_sample whose ticket, step row, manifest or file
cannot be found is listed with the reason and left alone: scratch workspaces are not
permanent, and the only other source for the counts is a re-mask.

Contract per this package: dry-run by default, and idempotent — a pair with count
rows is out of the query.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import asyncpg
from qiita_common.actions import READ_MASK_ACTION_ID
from qiita_common.models import StepProgressState, WorkTicketState

from ..actions.library import persist_syndna_read_count
from ..repositories.block import MASK_SAMPLE_COMPLETED
from ..repositories.syndna_read_count import SYNDNA_REFERENCE_SQL
from ..workspace import (
    STEP_MANIFEST_FILENAME,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

# The read-mask entry that writes the alignment, and the output binding it writes it
# under — the names in workflows/read-mask/<version>.yaml.
_SYNDNA_STEP = "syndna"
_ALIGNMENT_BINDING = "alignment"

# Every completed (mask, prep_sample) pair under a SynDNA mask with no count rows, with the
# newest completed read-mask ticket for the pair and the attempt its `syndna` step
# completed on (NULL when either is missing — residue).
_UNCOUNTED_SQL = f"""
SELECT ms.mask_idx, ms.prep_sample_idx, t.work_ticket_idx, s.attempt
  FROM qiita.mask_sample ms
  JOIN qiita.mask_definition md ON md.mask_idx = ms.mask_idx
  LEFT JOIN LATERAL (
        SELECT wt.work_ticket_idx
          FROM qiita.work_ticket wt
         WHERE wt.mask_idx = ms.mask_idx
           AND wt.prep_sample_idx = ms.prep_sample_idx
           AND wt.action_id = $2
           AND wt.state = $3::qiita.work_ticket_state
         ORDER BY wt.created_at DESC
         LIMIT 1
       ) t ON true
  LEFT JOIN LATERAL (
        SELECT wts.attempt
          FROM qiita.work_ticket_step wts
         WHERE wts.work_ticket_idx = t.work_ticket_idx
           AND wts.step_name = $4
           AND wts.state = $5
         ORDER BY wts.attempt DESC
         LIMIT 1
       ) s ON true
 WHERE ms.state = $1
   AND {SYNDNA_REFERENCE_SQL} IS NOT NULL
   AND NOT EXISTS (
         SELECT 1 FROM qiita.syndna_read_count c
          WHERE c.mask_idx = ms.mask_idx AND c.prep_sample_idx = ms.prep_sample_idx
       )
 ORDER BY ms.mask_idx, ms.prep_sample_idx
"""


@dataclass(frozen=True, slots=True)
class Pair:
    """One uncounted (mask, prep_sample) pair, with the file to count or why there is none."""

    mask_idx: int
    prep_sample_idx: int
    alignment_path: Path | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class BackfillPlan:
    pairs: list[Pair] = field(default_factory=list)

    def writable(self) -> list[Pair]:
        return [p for p in self.pairs if p.alignment_path is not None]

    def residue(self) -> list[Pair]:
        return [p for p in self.pairs if p.alignment_path is None]


def _alignment_from_manifest(attempt_dir: Path) -> tuple[Path | None, str | None]:
    """The file the attempt's manifest binds to `alignment`, or why it cannot be had."""
    manifest = step_output_dir(attempt_dir) / STEP_MANIFEST_FILENAME
    if not manifest.is_file():
        return None, f"no manifest at {manifest}"
    outputs = json.loads(manifest.read_text()).get("outputs", {})
    relative = outputs.get(_ALIGNMENT_BINDING)
    if relative is None:
        return None, f"{manifest} binds no {_ALIGNMENT_BINDING!r} output"
    path = step_output_dir(attempt_dir) / relative
    if not path.is_file():
        return None, f"{path} is gone"
    return path, None


async def plan_backfill(pool: asyncpg.Pool, *, ticket_root: Path) -> BackfillPlan:
    """Read-only: every uncounted pair, resolved to its alignment file or a reason.

    `ticket_root` is the per-ticket workspace root (`PATH_SCRATCH/ticket`).
    """
    rows = await pool.fetch(
        _UNCOUNTED_SQL,
        MASK_SAMPLE_COMPLETED,
        READ_MASK_ACTION_ID,
        WorkTicketState.COMPLETED.value,
        _SYNDNA_STEP,
        StepProgressState.COMPLETED.value,
    )
    pairs = []
    for r in rows:
        path: Path | None = None
        if r["work_ticket_idx"] is None:
            reason = "no completed read-mask ticket"
        elif r["attempt"] is None:
            reason = f"ticket {r['work_ticket_idx']} has no completed {_SYNDNA_STEP!r} step"
        else:
            attempt_dir = step_attempt_dir(
                ticket_workspace(ticket_root, r["work_ticket_idx"]), _SYNDNA_STEP, r["attempt"]
            )
            path, reason = _alignment_from_manifest(attempt_dir)
        pairs.append(Pair(r["mask_idx"], r["prep_sample_idx"], path, reason))
    return BackfillPlan(pairs=pairs)


async def apply_backfill(pool: asyncpg.Pool, plan: BackfillPlan) -> int:
    """Count and write every writable pair; return the pairs written. One
    transaction per pair (inside `persist_syndna_read_count`), so an interrupted run
    leaves whole pairs written and the next plan picks up the rest."""
    written = 0
    for pair in plan.writable():
        await persist_syndna_read_count(
            pool,
            mask_idx=pair.mask_idx,
            prep_sample_idx=pair.prep_sample_idx,
            alignment_path=Path(pair.alignment_path),
        )
        written += 1
    return written
