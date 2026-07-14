"""Host-filter resolver: what host filtering should happen for one biosample.

Answers a single question — given a biosample's `host_taxon_id` metadata and the
platform it was sequenced on, do we deplete a host, deliberately not deplete, or
refuse to proceed? The two facts live in different places by design: the ORGANISM
is sample metadata, and the reference BUILD is submission-time config
(`qiita.host_filter_profile`). This module is the join.

Pure and single-sample on purpose. It takes a `biosample_idx`, not a prep_sample
or a pool — the prep_sample -> biosample join and the pool-level fan-out belong
to the callers. Two seams it deliberately does not cross:

  * CONTROL is a MARKER, not a decision. A blank/control sample has no host of
    its own; what it should be filtered against is a property of the POOL it
    rode in (the union of its neighbours' hosts). This module reports "this is a
    control" and stops; the pool-level union lives with the pool-level caller.

  * It resolves reference IDENTITY, not on-disk READINESS. The returned idxs name
    `qiita.reference` rows. Whether those references are ACTIVE and their indexes
    actually built is a run-time question the runner already answers
    (`runner/_reference.py`), and duplicating it here would let the two drift.

Fail-closed is the rule for anything ambiguous: an absent field, an unrecognised
missing-reason, or a host with no profile for the platform all resolve to
UNRESOLVED rather than to "no filtering". Silently passing an un-depleted human
sample through is the one outcome we cannot take back.
"""

from dataclasses import dataclass
from enum import StrEnum

import asyncpg
from qiita_common.models import HostFilterProfile, Platform

from .repositories.host_filter_profile import get_host_filter_profile

# The biosample global field carrying the host organism, seeded as a
# terminology-typed field bound to NCBI Taxonomy. Because it is
# terminology-typed, a value is only ever a term or a missing-reason — the
# field-contract trigger rejects a value_text on it — so this module has no
# free-text branch to consider.
_HOST_TAXON_FIELD = "host_taxon_id"

# `missing_value_reason.name` values, seeded by the terminology-lifecycle migration.
_REASON_NOT_APPLICABLE = "not applicable"
_REASON_CONTROL_SAMPLE = "missing: control sample"


class HostFilterOutcome(StrEnum):
    """What should happen to one biosample's reads, host-filtering-wise.

    Python-only. There is no Postgres twin (nothing persists an outcome — it is
    recomputed at submit), so this is out of scope for the enum-parity tests.
    """

    # The sample has a host, and that host has a reference build on this
    # platform. Deplete against it.
    FILTER = "filter"
    # The sample deliberately has no host ('not applicable' — e.g. a water or
    # soil sample). Nothing to deplete; this is a decision, not a gap.
    PASS_THROUGH = "pass_through"
    # A control/blank. It has no host of its own, but it is not
    # "pass-through" either: what it gets filtered against is decided at the
    # pool level, by its neighbours. A marker for the caller, not an answer.
    CONTROL = "control"
    # We cannot tell. Abort unless the caller supplies an explicit override.
    UNRESOLVED = "unresolved"


# The missing-reasons that say something DEFINITE about whether a host exists,
# each with the outcome it means and the clause explaining it.
#
# This table IS the fail-closed rule. A reason listed here is a decision; every
# reason NOT listed ('not collected', 'not provided', 'restricted access', ...)
# falls through to UNRESOLVED, because those all mean "we don't know" — and "we
# don't know whether this sample has a host" must never silently become "don't
# filter it". Widening this dict is therefore the single, deliberate place a
# missing-reason can be promoted from "abort" to "proceed".
_RECOGNISED_MISSING_REASON: dict[str, tuple[HostFilterOutcome, str]] = {
    _REASON_NOT_APPLICABLE: (
        HostFilterOutcome.PASS_THROUGH,
        "the sample deliberately has no host",
    ),
    _REASON_CONTROL_SAMPLE: (
        HostFilterOutcome.CONTROL,
        "a control sample; what it filters against is decided pool-side",
    ),
}


@dataclass(frozen=True, slots=True)
class HostFilterResolution:
    """The resolver's answer for one biosample.

    `reason` is always populated and is written to be shown to a human — it is
    the body of the abort message when the outcome is UNRESOLVED, and the
    explanation the read-only preview endpoint renders for the other outcomes.

    `host_term_idx` is set whenever the sample named a host, even if no profile
    was found for it: an UNRESOLVED "taxon 9606 has no pacbio_smrt profile" is a
    far more actionable message than "unresolved", and the caller needs the term
    to offer a fix. The reference idxs are set only on FILTER.
    """

    outcome: HostFilterOutcome
    host_term_idx: int | None
    rype_reference_idx: int | None
    minimap2_reference_idx: int | None
    reason: str


async def resolve_host_filter(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    biosample_idx: int,
    platform: Platform,
) -> HostFilterResolution:
    """Resolve what host filtering `biosample_idx` should get on `platform`.

    `platform` is the sample's sequencing platform (e.g. from
    `sequencing_run.platform`), typed as the enum rather than a bare str so a
    bad value is caught at the caller rather than as an
    `InvalidTextRepresentation` from Postgres.
    Never raises for ordinary "cannot resolve" cases — those come back as
    UNRESOLVED with a reason, because whether that is fatal is the caller's call
    (a submit aborts; the preview endpoint renders it). asyncpg errors from a
    genuinely broken query propagate.
    """
    # Read the host_taxon_id value via the trigger-maintained global_field_idx,
    # which is what makes this a cross-study read: it resolves the same field no
    # matter which study's local field the value was written against.
    #
    # fetchrow (not fetch) is safe: the partial unique index
    # biosample_metadata_one_value_per_global_field guarantees at most ONE row
    # per (biosample, global field), so a biosample linked to several studies
    # still cannot carry two conflicting host_taxon_id values.
    row = await conn.fetchrow(
        "SELECT bm.value_terminology_term_idx,"
        "       bm.value_missing_reason_idx,"
        "       mvr.name AS missing_reason"
        "  FROM qiita.biosample_metadata bm"
        "  JOIN qiita.biosample_global_field bgf"
        "    ON bgf.idx = bm.global_field_idx AND bgf.internal_name = $2"
        "  LEFT JOIN qiita.missing_value_reason mvr"
        "    ON mvr.idx = bm.value_missing_reason_idx"
        " WHERE bm.biosample_idx = $1",
        biosample_idx,
        _HOST_TAXON_FIELD,
    )

    # The field was never set. Not the same as "no host" — we simply were not
    # told, so we refuse rather than guess.
    if row is None:
        return _without_references(
            HostFilterOutcome.UNRESOLVED,
            f"{_HOST_TAXON_FIELD} is not set on biosample {biosample_idx}",
        )

    host_term_idx = row["value_terminology_term_idx"]

    # A named host. Its build is config, so ask the profile table.
    if host_term_idx is not None:
        profile = await get_host_filter_profile(
            conn, host_term_idx=host_term_idx, platform=platform
        )
        if profile is None:
            # The term rides along even on the failure: "taxon N has no build on
            # this platform" is far more actionable than a bare "unresolved", and
            # the caller needs the term to offer a fix.
            return _without_references(
                HostFilterOutcome.UNRESOLVED,
                f"no host_filter_profile for terminology term {host_term_idx}"
                f" on platform {platform!r}",
                host_term_idx=host_term_idx,
            )
        return _filter_against(host_term_idx, profile)

    missing_reason = row["missing_reason"]

    # A missing-reason that says something definite: 'not applicable' (no host by
    # design) or 'missing: control sample' (pool decides). Anything else falls
    # through — see _RECOGNISED_MISSING_REASON.
    recognised = _RECOGNISED_MISSING_REASON.get(missing_reason)
    if recognised is not None:
        outcome, explanation = recognised
        return _without_references(
            outcome, f"{_HOST_TAXON_FIELD} is {missing_reason!r}: {explanation}"
        )

    # Neither a term nor a missing reason. Unreachable through the field-contract
    # trigger (a terminology field's value must be one or the other); handled
    # rather than asserted so a broken row aborts THIS submit with a legible
    # message instead of 500ing every caller.
    if missing_reason is None:
        return _without_references(
            HostFilterOutcome.UNRESOLVED,
            f"{_HOST_TAXON_FIELD} on biosample {biosample_idx} has neither a"
            " terminology term nor a missing reason",
        )

    # An unrecognised missing reason — "we don't know", which we refuse to read
    # as "no host".
    return _without_references(
        HostFilterOutcome.UNRESOLVED,
        f"{_HOST_TAXON_FIELD} is {missing_reason!r}, which does not say whether"
        " the sample has a host",
    )


def _filter_against(host_term_idx: int, profile: HostFilterProfile) -> HostFilterResolution:
    """Build the FILTER resolution for a host that has a profile."""
    second_stage = (
        f" then reference {profile.minimap2_reference_idx} (minimap2)"
        if profile.minimap2_reference_idx is not None
        else " (no minimap2 stage in this profile)"
    )
    return HostFilterResolution(
        outcome=HostFilterOutcome.FILTER,
        host_term_idx=host_term_idx,
        rype_reference_idx=profile.rype_reference_idx,
        minimap2_reference_idx=profile.minimap2_reference_idx,
        reason=(
            f"host terminology term {host_term_idx} filters against"
            f" reference {profile.rype_reference_idx} (rype){second_stage}"
        ),
    )


def _without_references(
    outcome: HostFilterOutcome,
    reason: str,
    *,
    host_term_idx: int | None = None,
) -> HostFilterResolution:
    """Build any resolution that names no references — PASS_THROUGH, CONTROL, and
    every UNRESOLVED. All three are "nothing to deplete against, here is why", so
    they differ only in outcome and reason; keeping one builder means a new
    no-reference outcome cannot accidentally ship with a stale reference idx
    copied from the branch above it.
    """
    return HostFilterResolution(
        outcome=outcome,
        host_term_idx=host_term_idx,
        rype_reference_idx=None,
        minimap2_reference_idx=None,
        reason=reason,
    )
