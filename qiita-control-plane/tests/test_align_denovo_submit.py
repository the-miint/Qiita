"""The submit-side half of `align-denovo`: the on-disk workflow's shape, and the
runner's pre-loop de novo alignment resolver.

Everything here is what a ticket must have settled BEFORE the job runs — the
identity it stamps on every row, the ticket columns two later actions read, and the
gate row the terminal action flips. The job itself is pinned against real miint in
`qiita-compute-orchestrator/tests/jobs/test_align_denovo.py`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from qiita_common.actions import ALIGNMENT_IDX_BINDING, WorkflowAction, WorkflowStep
from qiita_common.analytic import CIRCULAR_MIN_COVERAGE, CIRCULAR_MIN_IDENTITY
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane.actions import load_actions
from qiita_control_plane.runner._alignment import (
    ALIGN_MASK_IDX_BINDING,
    ASSEMBLY_PROCESSING_IDX_BINDING,
    MIN_IDENTITY_BINDING,
    MIN_QUERY_COVERAGE_BINDING,
    PRESET_BINDING,
    _build_denovo_alignment_params,
    _knob_defaults,
    _workflow_writes_alignment_gate,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def actions():
    return {a.action_id: a for a in load_actions(_REPO_ROOT / "workflows")}


@pytest.fixture(scope="module")
def denovo(actions):
    return actions["align-denovo"]


def test_the_on_disk_workflow_has_the_per_sample_alignment_shape(denovo):
    """prep_sample-scoped, and the four entries in the order the replace discipline
    needs: align, then delete THIS sample's prior rows, then register, then flip the
    gate. Registering before the delete would double-count a re-run; flipping the gate
    before registering would let a consumer read 'completed' over rows that are not in
    the lake yet."""
    assert denovo.target_kind == "prep_sample"
    assert [entry.name for entry in denovo.steps] == [
        "align_denovo",
        LibraryPrimitive.DELETE_ALIGNMENT_SAMPLE,
        LibraryPrimitive.REGISTER_FILES,
        LibraryPrimitive.FINALIZE_ALIGNMENT_SAMPLE,
    ]
    step = denovo.steps[0]
    assert isinstance(step, WorkflowStep)
    assert step.module == "qiita_compute_orchestrator.jobs.align_denovo"
    assert step.container is None
    # Both Parquets are outputs, and both land in the staging dir register-files loads.
    assert step.outputs == ["alignment", "alignment_origin_spanning", "alignment_staging_dir"]
    assert denovo.steps[2].inputs == ["alignment_staging_dir"]


def test_the_workflow_streams_its_data_rather_than_staging_it(denovo):
    """The job declares no `inputs:` at all: it holds identifiers and opens both
    streams itself. A bound path here would mean something was materialized onto
    shared scratch at submit time, which is what the streaming seams exist to avoid."""
    assert denovo.steps[0].inputs == []


def test_neither_consumed_identity_is_threaded_under_its_minting_name(denovo):
    """A step naming `mask_idx` or `processing_idx` as a params FIELD is the runner's
    signal to MINT that identity before the loop. This workflow consumes an existing
    one of each, so both ride prefixed names — get this wrong and every ticket mints a
    fresh mask or processing identity and aligns against the wrong thing."""
    fields = set(denovo.steps[0].params.values())
    assert "mask_idx" not in fields
    assert "processing_idx" not in fields
    assert {ALIGN_MASK_IDX_BINDING, ASSEMBLY_PROCESSING_IDX_BINDING} <= fields
    assert ALIGNMENT_IDX_BINDING in fields


def test_the_gate_thresholds_default_to_the_analytic_constants(denovo):
    """The YAML defaults are what the identity hashes and what the job applies — the
    job declares no defaults of its own — so these are the live values. They are the
    same two the analytic's circular gate carries; pinned so the caller-facing default
    and the library default cannot drift into two different gates."""
    defaults = _knob_defaults(denovo.context_schema)
    assert defaults[MIN_IDENTITY_BINDING] == CIRCULAR_MIN_IDENTITY
    assert defaults[MIN_QUERY_COVERAGE_BINDING] == CIRCULAR_MIN_COVERAGE
    assert defaults[PRESET_BINDING] == "map-hifi"


def test_only_the_two_selectors_are_required(denovo):
    """Which contigs and which reads have no sensible default; the three knobs do."""
    assert denovo.context_schema["required"] == [
        ASSEMBLY_PROCESSING_IDX_BINDING,
        ALIGN_MASK_IDX_BINDING,
    ]


def test_the_resolver_fires_on_the_terminal_gate_action(actions, denovo):
    """The signal for the mint, the two ticket-column writes and the 'pending' gate
    row. Keying on the declared entry rather than the action_id is what makes a
    workflow get them exactly when it declares the action that needs them — and it is
    why the block `align` workflow, which resolves its identity at plan time, does not
    trip it."""
    assert _workflow_writes_alignment_gate(denovo.steps) is True
    assert _workflow_writes_alignment_gate(actions["align"].steps) is False
    assert _workflow_writes_alignment_gate(actions["long-read-assembly"].steps) is False
    # A `step:` entry that happened to carry the same name is not the signal — the
    # gate is a control-plane primitive, and a compute step by that name would run
    # something else entirely.
    named = {"kind": "step", "name": str(LibraryPrimitive.FINALIZE_ALIGNMENT_SAMPLE)}
    assert (
        _workflow_writes_alignment_gate(
            [
                WorkflowStep(
                    **named,
                    step_type="singleton",
                    module="qiita_compute_orchestrator.jobs.align_denovo",
                    baseline_resources={"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
                )
            ]
        )
        is False
    )
    assert _workflow_writes_alignment_gate([WorkflowAction(**{**named, "kind": "action"})]) is True


def _params(bound, defaults=None):
    return _build_denovo_alignment_params(
        "align-denovo",
        "1.0.0",
        bound,
        defaults=defaults
        or {
            PRESET_BINDING: "map-hifi",
            MIN_IDENTITY_BINDING: 0.95,
            MIN_QUERY_COVERAGE_BINDING: 0.90,
        },
    )


def test_the_identity_hashes_every_result_affecting_input():
    """What the alignment_idx is: the assembly run, the mask, the aligner, the secondary
    cap, the preset and both thresholds. Written out rather than checked by count,
    because a key added here re-mints every de novo alignment fleet-wide and a key
    dropped silently merges two runs that produced different rows.

    `max_secondary` is in the set because it decides WHICH alignments the run collects:
    two runs at identical thresholds under different caps hold different rows, and
    without it the second would resolve the first's id and replace them."""
    params = _params({ASSEMBLY_PROCESSING_IDX_BINDING: 88, ALIGN_MASK_IDX_BINDING: 99})
    assert params == {
        "subject": "assembly",
        "workflow": "align-denovo",
        "version": "1.0.0",
        "processing_idx": 88,
        "mask_idx": 99,
        "aligner": "minimap2",
        "max_secondary": 100,
        "preset": "map-hifi",
        "min_identity": 0.95,
        "min_query_coverage": 0.90,
    }


def test_the_de_novo_identity_cannot_collide_with_a_sharded_one():
    """A block alignment hashes `{reference_idx, aligner, mask_idx, shard_ids}`. The
    two key sets are disjoint — `subject` is in one and `reference_idx` in the other —
    which is what lets `finalize_alignment_sample_gate` skip the covering-block refusal
    its mask-gate twin has to make."""
    params = _params({ASSEMBLY_PROCESSING_IDX_BINDING: 88, ALIGN_MASK_IDX_BINDING: 99})
    assert "reference_idx" not in params
    assert "shard_ids" not in params
    assert params["subject"] == "assembly"


def test_an_omitted_knob_collapses_onto_the_action_default():
    """Omitted and explicitly-at-the-default must be ONE identity, or the same run
    submitted two ways lands two alignments over the same rows."""
    base = {ASSEMBLY_PROCESSING_IDX_BINDING: 88, ALIGN_MASK_IDX_BINDING: 99}
    assert _params(base) == _params(
        {**base, PRESET_BINDING: "map-hifi", MIN_IDENTITY_BINDING: 0.95}
    )


def test_a_float_selector_and_an_integer_selector_are_one_identity():
    """`{"type": "integer"}` admits `42.0` as well as `42` — JSON Schema matches any
    number with a zero fractional part — so a client that renders its integers as
    floats validates. Uncoerced, the two canonicalize to `42.0` and `42`, giving one
    config two alignment_idx: a re-run neither replaces its own rows nor is recognized
    as the same alignment. The jsonb round-trip guard on the mint cannot catch it,
    because `42.0` stores and re-reads unchanged."""
    ints = _params({ASSEMBLY_PROCESSING_IDX_BINDING: 88, ALIGN_MASK_IDX_BINDING: 99})
    floats = _params({ASSEMBLY_PROCESSING_IDX_BINDING: 88.0, ALIGN_MASK_IDX_BINDING: 99.0})
    assert ints == floats
    assert canonical_params_hash(ints) == canonical_params_hash(floats)


def test_a_selector_absent_from_action_context_is_refused():
    """Unlike a knob, neither consumed identity has a context_schema default, so an
    absent one has nothing to collapse onto. Hashing it as null would mint a real
    alignment_idx keyed on "no assembly run"."""
    with pytest.raises(Exception, match="selectors are incomplete") as absent_run:
        _params({ASSEMBLY_PROCESSING_IDX_BINDING: 88})
    with pytest.raises(Exception, match="selectors are incomplete"):
        _params({ALIGN_MASK_IDX_BINDING: 99})

    # The refusal names the read that produces each identity, so a submitter who
    # got one wrong is not left to find the verb themselves. Pinned because the
    # spelling is a CLI surface this module cannot see: rename the verb and this
    # message goes stale silently.
    assert "qiita processing list" in str(absent_run.value)
    assert "qiita mask list" in str(absent_run.value)


def test_a_supplied_knob_overrides_the_action_default():
    """And the override reaches the hash, so a run at a different threshold is a
    distinct alignment rather than a silent overwrite of the first."""
    base = {ASSEMBLY_PROCESSING_IDX_BINDING: 88, ALIGN_MASK_IDX_BINDING: 99}
    relaxed = _params({**base, MIN_QUERY_COVERAGE_BINDING: 0.5})
    assert relaxed["min_query_coverage"] == 0.5
    assert relaxed != _params(base)
