"""Call genes for one long-read-assembly run that completed without a gene_call step,
from the run's own assemble and bin_refine outputs. A one-off; the
`assembly-gene-call-recover` workflow's header says when it is removed.

The calling is `gene_call.execute`'s; this job only finds its two inputs in the source
ticket's workspace (`_recover_source`).
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from . import gene_call
from ._recover_source import ASSEMBLE_STEP, BIN_REFINE_STEP, source_ticket_dir, step_outputs


class Inputs(BaseModel):
    """`prep_sample_idx` / `work_ticket_idx` are framework-injected. The rest ride the
    workflow's `params:`: the completed long-read-assembly ticket, and the completed
    attempt of each step read from it. The runner numbers attempts from 0."""

    prep_sample_idx: int
    work_ticket_idx: int
    source_work_ticket_idx: int = Field(gt=0)
    assemble_attempt: int = Field(ge=0)
    bin_refine_attempt: int = Field(ge=0)


def source_inputs(inputs: Inputs) -> gene_call.Inputs:
    """`gene_call.Inputs` over the source ticket's outputs."""
    ticket = source_ticket_dir(inputs.source_work_ticket_idx)
    assemble = step_outputs(ticket, ASSEMBLE_STEP, inputs.assemble_attempt, ("genomes_dir",))
    bin_refine = step_outputs(
        ticket, BIN_REFINE_STEP, inputs.bin_refine_attempt, ("refined_bins_dir",)
    )
    return gene_call.Inputs(
        genomes_dir=assemble["genomes_dir"],
        refined_bins_dir=bin_refine["refined_bins_dir"],
        prep_sample_idx=inputs.prep_sample_idx,
        work_ticket_idx=inputs.work_ticket_idx,
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    return await gene_call.execute(source_inputs(inputs), workspace)
