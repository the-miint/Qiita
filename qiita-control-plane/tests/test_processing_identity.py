"""Pure-unit tests for the runner processing identity (processing_idx) helpers.

The DB-bound mint (qiita.mint_processing upsert) is exercised in the db tier;
here we cover the pure params-shape + gate logic.
"""

from __future__ import annotations

from types import SimpleNamespace

from qiita_control_plane.runner._processing import (
    _build_processing_params,
    _workflow_needs_processing,
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
    """The canonical params carry workflow+version+assembler; an omitted assembler
    collapses to the workflow default (so omitted == explicit-default)."""
    explicit = _build_processing_params("long-read-assembly", "1.0.0", {"assembler": "myloasm"})
    assert explicit == {
        "workflow": "long-read-assembly",
        "version": "1.0.0",
        "assembler": "myloasm",
    }

    omitted = _build_processing_params("long-read-assembly", "1.0.0", {})
    assert omitted["assembler"] == "hifiasm_meta"
    # Omitted and explicit-default hash-collapse (same dict -> same processing_idx).
    assert omitted == _build_processing_params(
        "long-read-assembly", "1.0.0", {"assembler": "hifiasm_meta"}
    )
