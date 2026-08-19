"""Pure-unit tests for the runner processing identity (processing_idx) helpers.

The DB-bound mint (qiita.mint_processing upsert) is exercised in the db tier;
here we cover the pure params-shape + gate logic.
"""

from __future__ import annotations

from types import SimpleNamespace

from qiita_common.actions import WorkflowAction

from qiita_control_plane.runner._processing import (
    _build_processing_params,
    _workflow_needs_processing,
    _workflow_writes_assembly_gate,
)


def _step(**kw) -> SimpleNamespace:
    return SimpleNamespace(params=kw.get("params", {}))


def test_workflow_needs_processing_gate():
    """A step threading processing_idx via params: signals the runner to mint."""
    threads = [_step(params={"processing_idx": "processing_idx"})]
    assert _workflow_needs_processing(threads) is True

    other = [_step(params={"assembler": "assembler"})]
    assert _workflow_needs_processing(other) is False

    none = [_step()]
    assert _workflow_needs_processing(none) is False


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
    gate action gets no gate row — the two signals are independent."""
    gated = [WorkflowAction(kind="action", name="finalize-assembly-sample")]
    assert _workflow_writes_assembly_gate(gated) is True

    other_action = [WorkflowAction(kind="action", name="register-files")]
    assert _workflow_writes_assembly_gate(other_action) is False

    # A `step:` entry, which carries no `kind="action"`, never triggers the gate
    # even if its name collides with the primitive's.
    assert _workflow_writes_assembly_gate([_step()]) is False
    assert (
        _workflow_writes_assembly_gate(
            [SimpleNamespace(kind="step", name="finalize-assembly-sample")]
        )
        is False
    )
