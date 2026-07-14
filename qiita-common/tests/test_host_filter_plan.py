"""Tests for the pool-level host-filter plan.

This is where a blank's fate is decided, and a blank is the one sample whose
answer cannot come from itself. Getting it wrong is not symmetric:

  * refusing a pool that should have submitted is annoying;
  * depleting a blank against nothing, or against the wrong host, is a silently
    wrong result that nobody notices until someone asks why the blank has reads.

So the cases below pin both directions — what it submits AND what it refuses.
"""

import pytest

from qiita_common.host_filter_plan import (
    PoolPlanRefusal,
    SampleHostFilter,
    plan_pool_host_filter,
)
from qiita_common.models import HostFilterOutcome, HostFilterResolution

_HUMAN = 9606
_MOUSE = 10090


def _filter(host=_HUMAN, rype=1, minimap2=2):
    return HostFilterResolution(
        outcome=HostFilterOutcome.FILTER,
        host_term_idx=host,
        rype_reference_idx=rype,
        minimap2_reference_idx=minimap2,
        reason="has a host with a profile",
    )


def _resolution(outcome):
    return HostFilterResolution(outcome=outcome, reason=f"{outcome}")


_CONTROL = _resolution(HostFilterOutcome.CONTROL)
_PASS_THROUGH = _resolution(HostFilterOutcome.PASS_THROUGH)
_UNRESOLVED = _resolution(HostFilterOutcome.UNRESOLVED)


# ---------------------------------------------------------------------------
# The normal case — and it is the ONLY case on our current data
# ---------------------------------------------------------------------------


def test_blank_is_depleted_against_the_pools_sole_host():
    """Every pool we hold is human-gut samples + blanks. The blank inherits the
    pool's host, which is what happens today (pool-wide filtering hits blanks
    too), so this is not a behaviour change for blanks — it is the same answer,
    now derived rather than assumed."""
    plan = plan_pool_host_filter({"S1": _filter(), "S2": _filter(), "BLANK.1": _CONTROL})

    assert plan.refusal is None
    assert plan.pool_host_term_idx == _HUMAN
    assert plan.decisions["BLANK.1"] == SampleHostFilter(
        enabled=True, rype_reference_idx=1, minimap2_reference_idx=2
    )
    # ...and it is the SAME decision the real samples get, so they collapse to one
    # mask_idx and one block partition.
    assert plan.decisions["BLANK.1"] == plan.decisions["S1"]


def test_blanks_do_not_vote_on_the_pools_host():
    """A pool of blanks and ONE real sample still has exactly one host. If controls
    counted toward the host set the answer would be circular — a blank's host is
    what we are trying to work out."""
    plan = plan_pool_host_filter({"S1": _filter(), **{f"BLANK.{i}": _CONTROL for i in range(20)}})

    assert plan.refusal is None
    assert plan.pool_host_term_idx == _HUMAN
    assert all(plan.decisions[f"BLANK.{i}"].enabled for i in range(20))


# ---------------------------------------------------------------------------
# The empty-union case — real: the PacBio eDNA pool
# ---------------------------------------------------------------------------


def test_a_pool_with_no_host_at_all_passes_its_blanks_through():
    """Seawater + a blank: the pool's non-control samples have NO host, so there is
    nothing for the blank to be depleted against.

    This is the case the original spec missed — it covered "exactly one host" and
    "more than one" but not "none" — and it is not hypothetical: the PacBio eDNA
    pool is 25 seawater samples and 1 blank."""
    plan = plan_pool_host_filter({"W1": _PASS_THROUGH, "W2": _PASS_THROUGH, "BLANK.1": _CONTROL})

    assert plan.refusal is None
    assert plan.pool_host_term_idx is None
    assert plan.decisions["BLANK.1"] == SampleHostFilter(enabled=False)
    assert plan.decisions["W1"] == SampleHostFilter(enabled=False)


def test_a_pool_of_only_blanks_passes_through():
    """Degenerate but reachable (a control-only plate). No host, so no depletion —
    rather than crashing on an empty host set."""
    plan = plan_pool_host_filter({"BLANK.1": _CONTROL, "BLANK.2": _CONTROL})

    assert plan.refusal is None
    assert plan.decisions["BLANK.1"] == SampleHostFilter(enabled=False)


# ---------------------------------------------------------------------------
# Refusals — fail closed
# ---------------------------------------------------------------------------


def test_an_unresolved_sample_refuses_the_whole_pool():
    """One sample we cannot place aborts the submission, and the plan comes back
    EMPTY — not partial. A caller must not be able to submit the resolvable 383 of
    384 and quietly leave one behind."""
    plan = plan_pool_host_filter({"S1": _filter(), "S2": _UNRESOLVED, "BLANK.1": _CONTROL})

    assert plan.refusal is PoolPlanRefusal.UNRESOLVED_SAMPLES
    assert plan.offending == ("S2",)
    assert plan.decisions == {}


def test_unresolved_is_checked_before_the_host_union():
    """An unresolved sample in a pool that ALSO spans two hosts must report the
    unresolved one: it is the actionable problem (curate the sample), whereas
    multi-host is a feature request. Reporting the wrong one sends the operator
    down the wrong path."""
    plan = plan_pool_host_filter(
        {"S1": _filter(host=_HUMAN), "S2": _filter(host=_MOUSE), "S3": _UNRESOLVED}
    )

    assert plan.refusal is PoolPlanRefusal.UNRESOLVED_SAMPLES
    assert plan.offending == ("S3",)


def test_a_multi_host_pool_refuses_and_names_the_samples_that_caused_it():
    """Two hosts means a blank has no single answer. Refuse rather than pick.

    The offending list names the samples that ESTABLISHED the competing hosts, not
    the blanks — the blanks are the victims, and telling an operator to go look at
    a blank would be a wild goose chase."""
    plan = plan_pool_host_filter(
        {"HUMAN.1": _filter(host=_HUMAN), "MOUSE.1": _filter(host=_MOUSE), "BLANK.1": _CONTROL}
    )

    assert plan.refusal is PoolPlanRefusal.MULTI_HOST
    assert plan.offending == ("HUMAN.1", "MOUSE.1")
    assert plan.decisions == {}


def test_multi_host_refuses_even_with_no_blanks_to_protect():
    """A blank is what makes multi-host UNANSWERABLE, but a mixed-host pool is
    still outside what the mask identity and the host-filter chain express, so it
    refuses regardless. Submitting it would produce two mask identities for one
    pool with no way to say which is which."""
    plan = plan_pool_host_filter({"HUMAN.1": _filter(host=_HUMAN), "MOUSE.1": _filter(host=_MOUSE)})

    assert plan.refusal is PoolPlanRefusal.MULTI_HOST


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------


def test_pass_through_is_explicitly_disabled_not_merely_absent():
    """The read-mask `when:` gates are DEFAULT-ON — an absent key RUNS the step. A
    pass-through sample must therefore carry enabled=False explicitly, or it would
    be host-filtered against nothing."""
    plan = plan_pool_host_filter({"W1": _PASS_THROUGH})

    assert "W1" in plan.decisions
    assert plan.decisions["W1"].enabled is False


def test_every_input_key_appears_in_the_output():
    keys = {"S1": _filter(), "W1": _PASS_THROUGH, "B1": _CONTROL}
    plan = plan_pool_host_filter(keys)
    assert set(plan.decisions) == set(keys)


@pytest.mark.parametrize("minimap2", [None, 2])
def test_the_second_stage_rides_along_when_the_profile_has_one(minimap2):
    """A long-read profile has no minimap2 stage; a short-read one does. The plan
    carries whatever the profile declared — including for the blanks, which inherit
    the pool host's full stage set, not just its rype index."""
    plan = plan_pool_host_filter({"S1": _filter(minimap2=minimap2), "BLANK.1": _CONTROL})

    assert plan.decisions["S1"].minimap2_reference_idx == minimap2
    assert plan.decisions["BLANK.1"].minimap2_reference_idx == minimap2
