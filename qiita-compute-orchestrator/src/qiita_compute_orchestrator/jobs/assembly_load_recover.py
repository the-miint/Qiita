"""Stage one long-read-assembly run's lake rows again, with its gene calls and
genome_idx, under the run's existing processing_idx. A one-off; the
`assembly-gene-call-recover` workflow's header says when it is removed.

The staging is `assembly_load.execute`'s. This job finds the run's assemble,
bin_refine, checkm, assembly_hash and mint-features outputs in the source ticket's
workspace (`_recover_source`), and takes the three gene inputs from this ticket.

Of what assembly_load stages, only `RECOVERED_TABLES` are registered: the run's
contig sequences and myloasm provenance are already in the lake, and registering the
sequences again would replace a contig shared with a later run with this run's strand
(`flight_service::registration`). The rest is removed from the staging directory.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pydantic import BaseModel, Field
from qiita_common.actions import MINT_FEATURES_OUTPUT_BASENAME
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.gene_call_constants import GENE_CALL_CONTIG_TABLE, GENE_CALL_TABLE

from . import assembly_hash, assembly_load
from ._recover_source import (
    ASSEMBLE_STEP,
    BIN_REFINE_STEP,
    CHECKM_STEP,
    action_output,
    source_ticket_dir,
    step_outputs,
)


class Inputs(BaseModel):
    """`gene_calls`, `gene_feature_map` and `subject_genome_map` are this ticket's
    upstream outputs. `prep_sample_idx` / `work_ticket_idx` are framework-injected.
    The rest ride the workflow's `params:`: `assembly_processing_idx` names the run
    (the control plane's `runner._alignment` says why it is not `processing_idx`), and
    the source ticket and the completed attempt of each entry read from it. The runner
    numbers attempts from 0."""

    gene_calls: Path
    gene_feature_map: Path
    subject_genome_map: Path
    prep_sample_idx: int
    work_ticket_idx: int
    assembly_processing_idx: int = Field(gt=0)
    source_work_ticket_idx: int = Field(gt=0)
    assemble_attempt: int = Field(ge=0)
    bin_refine_attempt: int = Field(ge=0)
    checkm_attempt: int = Field(ge=0)
    assembly_hash_attempt: int = Field(ge=0)
    mint_features_attempt: int = Field(ge=0)


def source_inputs(inputs: Inputs) -> assembly_load.Inputs:
    """`assembly_load.Inputs` over the source ticket's outputs and this ticket's gene
    inputs, under the run's processing_idx."""
    ticket = source_ticket_dir(inputs.source_work_ticket_idx)
    assemble = step_outputs(ticket, ASSEMBLE_STEP, inputs.assemble_attempt, ("genomes_dir",))
    bin_refine = step_outputs(
        ticket, BIN_REFINE_STEP, inputs.bin_refine_attempt, ("refined_bins_dir",)
    )
    checkm = step_outputs(ticket, CHECKM_STEP, inputs.checkm_attempt, ("checkm_dir",))
    hashed = step_outputs(
        ticket,
        assembly_hash.YAML_STEP_NAME,
        inputs.assembly_hash_attempt,
        ("manifest", "assembly_chunks", "bin_map"),
    )
    feature_map = action_output(
        ticket,
        LibraryPrimitive.MINT_FEATURES,
        inputs.mint_features_attempt,
        MINT_FEATURES_OUTPUT_BASENAME,
    )
    return assembly_load.Inputs(
        manifest=hashed["manifest"],
        feature_map=feature_map,
        assembly_chunks=hashed["assembly_chunks"],
        bin_map=hashed["bin_map"],
        genomes_dir=assemble["genomes_dir"],
        checkm_dir=checkm["checkm_dir"],
        refined_bins_dir=bin_refine["refined_bins_dir"],
        gene_calls=inputs.gene_calls,
        gene_feature_map=inputs.gene_feature_map,
        subject_genome_map=inputs.subject_genome_map,
        processing_idx=inputs.assembly_processing_idx,
        prep_sample_idx=inputs.prep_sample_idx,
        work_ticket_idx=inputs.work_ticket_idx,
    )


# The tables a recovery registers: the run-keyed ones it changes, which are also all
# that the gene-call tables need beside them (their replace key is
# assembly_membership's).
RECOVERED_TABLES = frozenset(
    {"assembly_membership", "bin_quality", GENE_CALL_TABLE, GENE_CALL_CONTIG_TABLE}
)


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    out = await assembly_load.execute(source_inputs(inputs), workspace)
    staging = out["staging_dir"]
    staged = {p.name for p in staging.iterdir()}
    missing = {f"{t}.parquet" for t in RECOVERED_TABLES} - staged
    if missing:
        raise RuntimeError(f"assembly_load staged no {sorted(missing)} in {staging}")
    for path in staging.iterdir():
        if path.suffix == ".parquet" and path.stem in RECOVERED_TABLES:
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    return out
