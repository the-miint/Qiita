"""Pure-unit tests for the runner processing identity (processing_idx) helpers.

The DB-bound mint (qiita.mint_processing upsert) is exercised in the db tier;
here we cover the pure params-shape + gate logic.
"""

from __future__ import annotations

from qiita_common.actions import WorkflowAction, WorkflowStep

from qiita_control_plane.runner._processing import (
    _build_processing_params,
    _workflow_needs_processing,
    _workflow_writes_assembly_gate,
)


def _step(params: dict[str, str] | None = None) -> WorkflowStep:
    return WorkflowStep(
        kind="step",
        name="assembly_load",
        step_type="singleton",
        module="qiita_compute_orchestrator.jobs.assembly_load",
        params=params or {},
        baseline_resources={"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    )


def test_workflow_needs_processing_gate():
    """A step threading processing_idx via params: signals the runner to mint."""
    threads = [_step({"processing_idx": "processing_idx"})]
    assert _workflow_needs_processing(threads) is True

    other = [_step({"assembler": "assembler"})]
    assert _workflow_needs_processing(other) is False

    none = [_step()]
    assert _workflow_needs_processing(none) is False

    # An `action:` entry has no `params:` field at all, so it can never carry the
    # signal — what lets the shared predicate narrow on WorkflowStep.
    gate_only = [WorkflowAction(kind="action", name="finalize-assembly-sample")]
    assert _workflow_needs_processing(gate_only) is False


def test_build_processing_params_shape_and_assembler_default():
    """The canonical params carry workflow+version+mask_idx+assembler; an omitted
    assembler collapses to the passed context_schema default (so omitted ==
    explicit-default)."""
    explicit = _build_processing_params(
        "long-read-assembly",
        "1.0.0",
        {"mask_idx": 7, "assembler": "myloasm"},
        assembler_default="hifiasm_meta",
    )
    assert explicit == {
        "workflow": "long-read-assembly",
        "version": "1.0.0",
        "mask_idx": 7,
        "assembler": "myloasm",
    }

    omitted = _build_processing_params(
        "long-read-assembly", "1.0.0", {"mask_idx": 7}, assembler_default="hifiasm_meta"
    )
    assert omitted["assembler"] == "hifiasm_meta"
    # Omitted and explicit-default hash-collapse (same dict -> same processing_idx).
    assert omitted == _build_processing_params(
        "long-read-assembly",
        "1.0.0",
        {"mask_idx": 7, "assembler": "hifiasm_meta"},
        assembler_default="hifiasm_meta",
    )


def test_amplicon_params_carry_only_amplicon_knobs():
    """An amplicon run hashes its own knobs (trim/primer/orient_primer/
    sortmerna_reference_idx) and NOT the assembly ones — a None candidate is
    dropped, so the two families never pollute each other's identity."""
    amplicon = _build_processing_params(
        "amplicon",
        "1.0.0",
        {
            "trim": 150,
            "primer": "GTGYCAGCMGCCGCGGTAA",
            "orient_primer": False,
            "sortmerna_reference_idx": 3,
        },
    )
    assert amplicon == {
        "workflow": "amplicon",
        "version": "1.0.0",
        "trim": 150,
        "primer": "GTGYCAGCMGCCGCGGTAA",
        "orient_primer": False,
        "sortmerna_reference_idx": 3,
    }
    # No assembly knobs leaked in (mask_idx / assembler absent).
    assert "mask_idx" not in amplicon and "assembler" not in amplicon


def test_amplicon_trim_and_reference_are_part_of_the_identity():
    """Different truncation length OR different SortMeRNA reference -> distinct
    params (distinct processing_idx): both change the denoised result."""
    base = {"trim": 150, "primer": "GTGYCAGCMGCCGCGGTAA", "sortmerna_reference_idx": 3}
    trim_a = _build_processing_params("amplicon", "1.0.0", {**base, "trim": 150})
    trim_b = _build_processing_params("amplicon", "1.0.0", {**base, "trim": 100})
    assert trim_a != trim_b
    ref_b = _build_processing_params("amplicon", "1.0.0", {**base, "sortmerna_reference_idx": 4})
    assert trim_a != ref_b


def test_mask_idx_is_part_of_the_identity():
    """mask_idx is the gating input predicate: the same sample+assembler assembled
    from two different masks must yield DISTINCT params (distinct processing_idx),
    never a false duplicate that disallow-without-delete would block."""
    mask_a = _build_processing_params(
        "long-read-assembly", "1.0.0", {"mask_idx": 1}, assembler_default="hifiasm_meta"
    )
    mask_b = _build_processing_params(
        "long-read-assembly", "1.0.0", {"mask_idx": 2}, assembler_default="hifiasm_meta"
    )
    assert mask_a != mask_b
    assert mask_a["mask_idx"] == 1 and mask_b["mask_idx"] == 2


def test_workflow_writes_assembly_gate_keys_on_the_terminal_action():
    """Declaring the `finalize-assembly-sample` action is the single signal for all
    three assembly_sample writes (pending at mint, completed at the action, no_data
    on the StepNoData path). A workflow that mints a processing_idx but declares no
    gate action gets no gate row."""
    gated = [WorkflowAction(kind="action", name="finalize-assembly-sample")]
    assert _workflow_writes_assembly_gate(gated) is True

    other_action = [WorkflowAction(kind="action", name="register-files")]
    assert _workflow_writes_assembly_gate(other_action) is False

    # A `step:` entry never triggers the gate even when its name collides with
    # the primitive's — only an `action:` entry declares it.
    colliding_step = [
        WorkflowStep(
            kind="step",
            name="finalize-assembly-sample",
            step_type="singleton",
            module="qiita_compute_orchestrator.jobs.assembly_load",
            baseline_resources={"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
        )
    ]
    assert _workflow_writes_assembly_gate(colliding_step) is False
