"""The runner's resume paths and run binding for mint-gene-features and
export-subject-genome-map: a resumed ticket rebuilds each output where the primitive
wrote it, and the subject genome map reads exactly one assembly run."""

from __future__ import annotations

import pytest
from qiita_common.actions import PROCESSING_IDX_BINDING, WorkflowAction
from qiita_common.api_paths import LibraryPrimitive

from qiita_control_plane.actions.library import (
    MINT_GENE_FEATURES_OUTPUT_BASENAME,
    SUBJECT_GENOME_MAP_OUTPUT_BASENAME,
)
from qiita_control_plane.runner import ASSEMBLY_PROCESSING_IDX_BINDING
from qiita_control_plane.runner._reconstruct import (
    _assembly_processing_idx,
    _reconstruct_action_outputs,
)


@pytest.mark.parametrize(
    "name,output,basename",
    [
        (
            LibraryPrimitive.MINT_GENE_FEATURES,
            "gene_feature_map",
            MINT_GENE_FEATURES_OUTPUT_BASENAME,
        ),
        (
            LibraryPrimitive.EXPORT_SUBJECT_GENOME_MAP,
            "subject_genome_map",
            SUBJECT_GENOME_MAP_OUTPUT_BASENAME,
        ),
    ],
)
def test_resume_rebuilds_the_output_where_the_primitive_wrote_it(tmp_path, name, output, basename):
    entry = WorkflowAction(kind="action", name=name, outputs=[output])
    assert _reconstruct_action_outputs(entry, tmp_path) == {output: tmp_path / basename}


def test_the_genome_map_reads_the_run_this_ticket_minted_or_the_one_it_names():
    assert _assembly_processing_idx({PROCESSING_IDX_BINDING: 5}) == 5
    assert _assembly_processing_idx({ASSEMBLY_PROCESSING_IDX_BINDING: 6}) == 6


@pytest.mark.parametrize(
    "bound", [{}, {PROCESSING_IDX_BINDING: 5, ASSEMBLY_PROCESSING_IDX_BINDING: 6}]
)
def test_the_genome_map_refuses_no_run_or_two(bound):
    with pytest.raises(RuntimeError, match="exactly one"):
        _assembly_processing_idx(bound)
