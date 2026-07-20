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

from collections.abc import Sequence

import asyncpg
from qiita_common.models import (
    BIOSAMPLE_FIELD_HOST_TAXON_ID,
    MISSING_REASON_CONTROL_SAMPLE,
    MISSING_REASON_NOT_APPLICABLE,
    HostFilterOutcome,
    HostFilterProfile,
    HostFilterResolution,
    Platform,
)

from .repositories.host_filter_profile import (
    get_host_filter_profile,
    list_host_filter_profiles,
)

# The columns every host_taxon_id read selects, shared by the single-sample and
# batch queries so the two cannot drift into classifying different shapes.
#
# host_taxon_id is terminology-typed, so a value is only ever a term or a
# missing-reason — the field-contract trigger rejects a value_text on it — which
# is why there is no free-text branch below.
_METADATA_COLUMNS = (
    "bm.biosample_idx,"
    " bm.value_terminology_term_idx,"
    " bm.value_missing_reason_idx,"
    " mvr.name AS missing_reason"
)
_METADATA_FROM = (
    " FROM qiita.biosample_metadata bm"
    " JOIN qiita.biosample_global_field bgf"
    "   ON bgf.idx = bm.global_field_idx AND bgf.internal_name = $1"
    " LEFT JOIN qiita.missing_value_reason mvr"
    "   ON mvr.idx = bm.value_missing_reason_idx"
)


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
    MISSING_REASON_NOT_APPLICABLE: (
        HostFilterOutcome.PASS_THROUGH,
        "the sample deliberately has no host",
    ),
    MISSING_REASON_CONTROL_SAMPLE: (
        HostFilterOutcome.CONTROL,
        "a control sample; what it filters against is decided pool-side",
    ),
}


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

    Resolving a whole pool? Use `resolve_host_filter_many` — this one costs two
    round trips per sample, which a large roster cannot afford.
    """
    # Coerce at the boundary rather than trusting the annotation: asyncpg hands a
    # qiita.platform column back as a plain str, so a caller reading one straight
    # out of a row would otherwise reach the SQL untyped, and a bad value would
    # surface as an InvalidTextRepresentation from Postgres instead of here.
    platform = Platform(platform)

    row = await _fetch_host_taxon_metadata_row(conn, biosample_idx)

    # Look the profile up only when the sample actually named a host; the
    # classifier below needs it for exactly that branch.
    profile = None
    if row is not None and row["value_terminology_term_idx"] is not None:
        profile = await get_host_filter_profile(
            conn, host_term_idx=row["value_terminology_term_idx"], platform=platform
        )
    return _classify(biosample_idx, row, profile, platform)


async def _fetch_host_taxon_metadata_row(
    conn: asyncpg.Pool | asyncpg.Connection, biosample_idx: int
) -> asyncpg.Record | None:
    """The single host_taxon_id metadata row for a biosample (or None if unset),
    read via the trigger-maintained global_field_idx — which is what makes this a
    cross-study read: it resolves the same field no matter which study's local
    field the value was written against. Shared by `resolve_host_filter` and
    `is_control_sample` so the row SHAPE they classify can't drift.

    fetchrow (not fetch) is safe: the partial unique index
    biosample_metadata_one_value_per_global_field guarantees at most ONE row
    per (biosample, global field), so a biosample linked to several studies
    still cannot carry two conflicting host_taxon_id values.
    """
    return await conn.fetchrow(
        f"SELECT {_METADATA_COLUMNS}{_METADATA_FROM} WHERE bm.biosample_idx = $2",
        BIOSAMPLE_FIELD_HOST_TAXON_ID,
        biosample_idx,
    )


async def is_control_sample(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    biosample_idx: int,
) -> bool:
    """True when `biosample_idx` is an expected-empty control — a blank / no-template
    control whose host_taxon_id carries the control missing-reason
    (`MISSING_REASON_CONTROL_SAMPLE`, the marker the control-sample backfill sets).

    Platform-independent, and deliberately so: the control classification never
    consults a host_filter_profile (that lookup only feeds the host-*present*
    branch), so this reads the single metadata row (via the shared
    `_fetch_host_taxon_metadata_row`) and reuses the SAME `_RECOGNISED_MISSING_REASON`
    table `resolve_host_filter` classifies against — the two cannot drift about what
    "control" means. A biosample with no host_taxon_id row, a named host, or any
    non-control missing-reason returns False.

    The caller is the read-mask reads binder: a control well legitimately yields
    zero reads (a benign terminal `no_data`), whereas a data well with zero reads
    is a genuine failure — so the two must be told apart before disposing of a
    zero-read ticket.
    """
    row = await _fetch_host_taxon_metadata_row(conn, biosample_idx)
    if row is None or row["value_terminology_term_idx"] is not None:
        return False
    recognised = _RECOGNISED_MISSING_REASON.get(row["missing_reason"])
    return recognised is not None and recognised[0] is HostFilterOutcome.CONTROL


async def resolve_host_filter_many(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    biosample_idxs: Sequence[int],
    platform: Platform,
) -> dict[int, HostFilterResolution]:
    """Resolve a whole pool at once. Returns {biosample_idx: resolution}.

    Same answers as calling `resolve_host_filter` per sample — they share the
    `_classify` core, so the two can't drift — but in TWO queries total rather
    than two per sample. A pool holds hundreds of samples, so the per-sample path
    would turn one roster GET into a four-figure count of round trips (each also
    acquiring a connection when handed a pool).

    The profile side is fetched as one platform-scoped list rather than one
    lookup per distinct host: `qiita.host_filter_profile` holds a handful of rows
    (one per host per platform), so fetching all of them for this platform is
    cheaper than an IN-list and keeps the query count fixed at two regardless of
    how many distinct hosts the pool spans.

    Every requested idx appears in the result. A biosample with no host_taxon_id
    row is not silently dropped — it comes back UNRESOLVED, which is the whole
    point of the fail-closed contract.
    """
    # Coerce before anything else — see resolve_host_filter. It matters more here:
    # `profile_by_term` below is keyed on host_term_idx ALONE, which is only
    # unambiguous because every row in `profiles` is for one platform. A None
    # platform would make list_host_filter_profiles return every platform's rows
    # and the dict would silently keep the last one per host — resolving a sample
    # against another platform's build. Platform(None) raises instead.
    platform = Platform(platform)

    if not biosample_idxs:
        return {}

    rows = await conn.fetch(
        f"SELECT {_METADATA_COLUMNS}{_METADATA_FROM} WHERE bm.biosample_idx = ANY($2::bigint[])",
        BIOSAMPLE_FIELD_HOST_TAXON_ID,
        list(biosample_idxs),
    )
    row_by_biosample = {r["biosample_idx"]: r for r in rows}

    profiles = await list_host_filter_profiles(conn, platform=platform)
    profile_by_term = {p.host_term_idx: p for p in profiles}

    resolutions: dict[int, HostFilterResolution] = {}
    for biosample_idx in biosample_idxs:
        row = row_by_biosample.get(biosample_idx)
        term = row["value_terminology_term_idx"] if row is not None else None
        profile = profile_by_term.get(term) if term is not None else None
        resolutions[biosample_idx] = _classify(biosample_idx, row, profile, platform)
    return resolutions


def _classify(
    biosample_idx: int,
    row: asyncpg.Record | None,
    profile: HostFilterProfile | None,
    platform: Platform,
) -> HostFilterResolution:
    """The whole decision, as a pure function of the two facts it needs: the
    sample's host_taxon_id row (or its absence) and the profile for whatever host
    that row names (or its absence).

    Both the single-sample and batch entry points fetch those two facts their own
    way — one round trip each vs. two queries for a whole pool — and then land
    here. Keeping the branching in one place is what guarantees a pool roster and
    a per-sample submit cannot disagree about the same sample.
    """
    # The field was never set. Not the same as "no host" — we simply were not
    # told, so we refuse rather than guess.
    if row is None:
        return _without_references(
            HostFilterOutcome.UNRESOLVED,
            f"{BIOSAMPLE_FIELD_HOST_TAXON_ID} is not set on biosample {biosample_idx}",
        )

    host_term_idx = row["value_terminology_term_idx"]

    # A named host. Its build is config, so the caller looked it up in the
    # profile table.
    if host_term_idx is not None:
        if profile is None:
            # The term rides along even on the failure: "taxon N has no build on
            # this platform" is far more actionable than a bare "unresolved", and
            # the caller needs the term to offer a fix.
            return _without_references(
                HostFilterOutcome.UNRESOLVED,
                f"no host_filter_profile for terminology term {host_term_idx}"
                f" on platform {platform}",
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
            outcome, f"{BIOSAMPLE_FIELD_HOST_TAXON_ID} is {missing_reason!r}: {explanation}"
        )

    # Neither a term nor a missing reason. Unreachable through the field-contract
    # trigger (a terminology field's value must be one or the other); handled
    # rather than asserted so a broken row aborts THIS submit with a legible
    # message instead of 500ing every caller.
    if missing_reason is None:
        return _without_references(
            HostFilterOutcome.UNRESOLVED,
            f"{BIOSAMPLE_FIELD_HOST_TAXON_ID} on biosample {biosample_idx} has neither a"
            " terminology term nor a missing reason",
        )

    # An unrecognised missing reason — "we don't know", which we refuse to read
    # as "no host".
    return _without_references(
        HostFilterOutcome.UNRESOLVED,
        f"{BIOSAMPLE_FIELD_HOST_TAXON_ID} is {missing_reason!r}, which does not say whether"
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
