"""tests for the amplicon_deblur Inputs contract (no miint).

pin the optionality the live e2e exposed: a submit passes only sortmerna_ref and
trim and lets the runner stream reads, so `reads` must be optional.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from qiita_compute_orchestrator.jobs.amplicon_deblur import Inputs

_SCOPE = {"sequenced_pool_idx": 1, "sequencing_run_idx": 1, "work_ticket_idx": 1}


def test_inputs_defaults_allow_a_minimal_submit():
    """The set the amplicon workflow binds: sortmerna_ref + trim + the framework
    scope scalars; `reads` streams, so it defaults."""
    inp = Inputs(sortmerna_ref=Path("/tmp/ref.fasta"), trim=150, **_SCOPE)
    assert inp.reads is None
    assert inp.orient_primer is False


def test_inputs_accept_workflow_1_0_0_params_without_orienting():
    """Workflow 1.0.0 still binds `primer` and `orient_primer`; a ticket that left
    orienting off runs, its primer ignored."""
    inp = Inputs(
        sortmerna_ref=Path("/tmp/ref.fasta"),
        trim=150,
        primer="GTGYCAGCMGCCGCGGTAA",
        orient_primer=False,
        **_SCOPE,
    )
    assert inp.trim == 150


def test_inputs_refuse_orienting():
    """A 1.0.0 ticket that asked to orient fails loud rather than running unoriented."""
    with pytest.raises(ValidationError, match="primer orienting was removed"):
        Inputs(sortmerna_ref=Path("/tmp/ref.fasta"), trim=150, orient_primer=True, **_SCOPE)
