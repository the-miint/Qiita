"""Host filtering: the (host taxon, platform) -> reference-build mapping, and the
per-sample resolution derived from it.

`HostFilterProfile` mirrors the `qiita.host_filter_profile` table — the config
layer resolving a host ORGANISM (biosample metadata, the `host_taxon_id` global
field) to the host reference BUILD we deplete against.

`HostFilterOutcome` / `HostFilterResolution` are the ANSWER that resolution
produces for one sample. They live here, in the contract layer, rather than
beside the resolver in the control plane, because they are on the wire: the pool
roster reports a resolution per sample so an operator can see what a submission
*would* do before running it.
"""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from qiita_common.models.reference import Platform


class HostFilterProfile(BaseModel):
    """One row of `qiita.host_filter_profile`.

    `rype_reference_idx` is required — the existence of a profile row *means*
    "deplete this host", so there is always a stage-1 index to deplete against.
    `minimap2_reference_idx` is the optional stage 2; None means this profile
    stops after stage 1.

    Both fields name a `qiita.reference`, not an on-disk path. Whether that
    reference is ACTIVE and its index actually built is a run-time question,
    answered later by the runner's reference resolution — a profile pins
    identity, not readiness.
    """

    model_config = ConfigDict(extra="forbid")

    idx: int
    host_term_idx: int
    platform: Platform
    rype_reference_idx: int
    minimap2_reference_idx: int | None = None


class HostFilterOutcome(StrEnum):
    """What should happen to one biosample's reads, host-filtering-wise.

    Python-only. Nothing persists an outcome — it is recomputed from metadata +
    config at read time — so there is no Postgres twin and this is out of scope
    for the enum-parity tests.
    """

    # The sample has a host, and that host has a reference build on this
    # platform. Deplete against it.
    FILTER = "filter"
    # The sample deliberately has no host ('not applicable' — e.g. a water or
    # soil sample). Nothing to deplete; this is a decision, not a gap.
    PASS_THROUGH = "pass_through"
    # A control/blank. It has no host of its own, but it is not "pass-through"
    # either: what it gets filtered against is decided at the pool level, by its
    # neighbours. A marker for the caller, not an answer.
    CONTROL = "control"
    # We cannot tell. Abort unless the caller supplies an explicit override.
    UNRESOLVED = "unresolved"


class HostFilterResolution(BaseModel):
    """What host filtering one sample would get — the resolver's answer.

    One type, used both as the resolver's return value inside the control plane
    and as the wire shape on the pool roster — which is where the submit path
    reads it. It is the SAMPLE'S OWN view, derived from its `host_taxon_id`
    metadata plus the run's platform.

    A sample's host is a property of the SAMPLE — this is derived from its own
    metadata, not from the project it was booked under.

    NOTE this is only PART of the answer for a control/blank, which resolves
    CONTROL — a blank has no host of its own, so what it gets depleted against
    comes from its pool. `qiita_common.host_filter_plan` does that join.

    Frozen: a resolution is a computed answer, not a mutable accumulator. `reason`
    is always populated and is written for a human — it is the explanation an
    operator reads when a sample comes back UNRESOLVED and needs fixing.

    The reference idxs are populated only on FILTER. They name `qiita.reference`
    rows, not on-disk paths: whether an index is actually built is a run-time
    question answered at submit, not here.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    outcome: HostFilterOutcome
    host_term_idx: int | None = None
    rype_reference_idx: int | None = None
    minimap2_reference_idx: int | None = None
    reason: str
