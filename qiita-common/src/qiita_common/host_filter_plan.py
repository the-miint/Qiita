"""Turn a pool's per-sample host-filter resolutions into a submittable plan.

The resolver answers one sample at a time, and for most samples that is the whole
answer. But a CONTROL (a blank) is deliberately NOT answered there: it has no host
of its own, so what it should be depleted against is a property of the POOL it
rode in — its neighbours' host. That join is what this module does.

It is pure, and it lives in the contract layer, because there are TWO submitters
that must not disagree about the same pool: the per-sample CLI fan-out
(`submit-host-filter-pool`) and the bulk-block planner. Two implementations of
"what does this blank get filtered against" is exactly the drift that would let
one pool be masked two ways.

The pool rule, over the NON-control samples' hosts:

    exactly one host   -> blanks are depleted against it. THE NORMAL CASE: every
                          pool we hold is human-gut samples plus blanks.
    no host at all     -> nothing to deplete against, so blanks pass through too.
                          Real: the PacBio eDNA pool is seawater + one blank.
    more than one host -> refuse. A blank in a multi-host pool has no single
                          answer, and filtering it against a union is a feature
                          that does not exist yet.

Anything UNRESOLVED refuses as well. That is the fail-closed contract the whole
arc rests on: we would rather abort a submission than silently pass an
un-depleted human sample through.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from qiita_common.models.host_filter_profile import (
    HostFilterOutcome,
    HostFilterResolution,
)


class PoolPlanRefusal(StrEnum):
    """Why a pool cannot be submitted. Each maps to an operator-facing abort."""

    # One or more samples' host could not be determined. Curate them, or override.
    UNRESOLVED_SAMPLES = "unresolved_samples"
    # The pool's non-control samples span more than one host, so its blanks have
    # no single answer. Depleting a blank against the union of hosts is not built.
    MULTI_HOST = "multi_host"


@dataclass(frozen=True, slots=True)
class SampleHostFilter:
    """What one sample's read-mask ticket should carry."""

    enabled: bool
    rype_reference_idx: int | None = None
    minimap2_reference_idx: int | None = None


@dataclass(frozen=True, slots=True)
class PoolHostFilterPlan:
    """The whole pool's decision, or a refusal with the samples to blame.

    `refusal` is None on success. When it is set, `decisions` is empty and
    `offending` names the samples that caused it — an abort message that says
    WHICH samples is the difference between an operator fixing three rows and an
    operator staring at a pool of 384.
    """

    decisions: dict[str, SampleHostFilter]
    refusal: PoolPlanRefusal | None = None
    offending: tuple[str, ...] = ()
    # The pool's sole host, when there is one. Carried for the abort/summary text.
    pool_host_term_idx: int | None = None


def plan_pool_host_filter(
    resolutions: dict[str, HostFilterResolution],
) -> PoolHostFilterPlan:
    """Resolve a pool's blanks against its neighbours and return the per-sample plan.

    `resolutions` maps an opaque per-sample key (the caller's — the CLI uses
    `sequenced_pool_item_id`, the block planner would use `prep_sample_idx`) to
    that sample's resolution. Keys are returned untouched.

    Pure: no DB, no HTTP. The refusal cases return an empty plan rather than a
    partial one, so a caller cannot half-submit a pool it should have refused.
    """
    unresolved = sorted(
        key for key, r in resolutions.items() if r.outcome is HostFilterOutcome.UNRESOLVED
    )
    if unresolved:
        return PoolHostFilterPlan(
            decisions={},
            refusal=PoolPlanRefusal.UNRESOLVED_SAMPLES,
            offending=tuple(unresolved),
        )

    # The pool's host set, over the samples that HAVE a host of their own. Blanks
    # are excluded by construction — they are what we are trying to answer, so
    # letting them vote would be circular.
    filtering = {key: r for key, r in resolutions.items() if r.outcome is HostFilterOutcome.FILTER}
    hosts = {r.host_term_idx for r in filtering.values()}

    if len(hosts) > 1:
        # Name the samples that established the competing hosts, not the blanks —
        # the blanks are the victims, not the cause.
        return PoolHostFilterPlan(
            decisions={},
            refusal=PoolPlanRefusal.MULTI_HOST,
            offending=tuple(sorted(filtering)),
        )

    # The blanks' answer: the pool's sole host, or nothing when the pool has none.
    pool_host_term_idx = next(iter(hosts)) if hosts else None
    control_decision = SampleHostFilter(enabled=False)
    if pool_host_term_idx is not None:
        # Every FILTER sample with this host resolved through the same
        # (host, platform) profile, so they all carry the same references — take
        # them from any of them.
        exemplar = next(r for r in filtering.values() if r.host_term_idx == pool_host_term_idx)
        control_decision = SampleHostFilter(
            enabled=True,
            rype_reference_idx=exemplar.rype_reference_idx,
            minimap2_reference_idx=exemplar.minimap2_reference_idx,
        )

    decisions: dict[str, SampleHostFilter] = {}
    for key, r in resolutions.items():
        if r.outcome is HostFilterOutcome.FILTER:
            decisions[key] = SampleHostFilter(
                enabled=True,
                rype_reference_idx=r.rype_reference_idx,
                minimap2_reference_idx=r.minimap2_reference_idx,
            )
        elif r.outcome is HostFilterOutcome.PASS_THROUGH:
            # A deliberate no-host sample. Explicitly disabled, not merely absent:
            # the read-mask `when:` gates are DEFAULT-ON, so an unset key RUNS the
            # step.
            decisions[key] = SampleHostFilter(enabled=False)
        else:  # CONTROL — answered by the pool, above.
            decisions[key] = control_decision

    return PoolHostFilterPlan(
        decisions=decisions,
        pool_host_term_idx=pool_host_term_idx,
    )
