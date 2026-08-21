"""Pure-unit tests for the amplicon_deblur Inputs contract (no miint).

Pins the optionality the live e2e exposed: a submit passes only the required
context (sortmerna_reference_idx→sortmerna_ref, trim) and lets the runner stream
reads, so `reads` and `primer` MUST be optional — the workflow YAML's
context_schema defaults are NOT auto-applied to action_context, so a missing
`primer` would otherwise fail Inputs validation before the job runs.
"""

from __future__ import annotations

from pathlib import Path

from qiita_compute_orchestrator.jobs.amplicon_deblur import Inputs


def test_inputs_defaults_allow_a_minimal_submit():
    """The minimal set the amplicon workflow binds: sortmerna_ref + trim + the
    framework scope scalars. `reads` (streamed) and `primer`/`orient_primer`
    (dead unless orienting) default, so this must validate."""
    inp = Inputs(
        sortmerna_ref=Path("/tmp/ref.fasta"),
        trim=150,
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        work_ticket_idx=1,
    )
    assert inp.reads is None
    assert inp.primer == "GTGYCAGCMGCCGCGGTAA"
    assert inp.orient_primer is False


def test_inputs_accept_explicit_orient_knobs():
    inp = Inputs(
        sortmerna_ref=Path("/tmp/ref.fasta"),
        trim=100,
        primer="GTGCCAGCAGCCGCGGTAA",
        orient_primer=True,
        sequenced_pool_idx=2,
        sequencing_run_idx=2,
        work_ticket_idx=2,
    )
    assert inp.orient_primer is True
    assert inp.primer == "GTGCCAGCAGCCGCGGTAA"
