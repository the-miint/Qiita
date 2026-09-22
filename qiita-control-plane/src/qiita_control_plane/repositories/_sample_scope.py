"""The per-sample narrowing every gate-roster read applies.

Two reads answer the same shape of question over two different gates — which
samples are masked under a `mask_idx` (`repositories.mask_definition`), which are
assembled under a `processing_idx` (`repositories.processing`) — and both have to
narrow the sample set identically: exclude entity-retired prep_samples, optional
`sequenced_pool_idx` / `prep_sample_idx` filters, and the per-study visibility
policy for a caller below the bypass role. This module owns that one copy;
neither reader restates it.

Every fragment correlates on `{alias}.prep_sample_idx`, where `{alias}` is the
caller's gate CTE. Nothing else about the gate is assumed.
"""

from qiita_common.models import Tier

# The per-study narrowing predicate for a plain user: a correlated NOT EXISTS over
# the roster's `prep_sample_idx`, admitting a sample only when no linked study
# denies it.
#
# SQL restatement of the policy the submission gate applies in Python
# (`auth.guards.require_caller_has_admin_on_all_studies`, as reached from
# routes/work_ticket.py). Arm for arm: the caller must hold Tier.ADMIN — or own the
# study — on EVERY non-retired link; a study row that does not exist is skipped
# (inner JOIN); a sample whose links are all retired is admitted (vacuous NOT
# EXISTS). Nothing pins the two together, so a change to either belongs in a PR
# that changes both — the guard carries the reciprocal note.
#
# Composition note: as a GATE the orphan case means "you already named the sample".
# As a FILTER it means every study-less prep_sample is visible to every caller.
#
# `{caller}` is the positional parameter holding the caller's principal_idx;
# `{tier}` the one holding the required tier; `{alias}` the roster CTE's alias.
_CALLER_MAY_SEE_SAMPLE = """
    NOT EXISTS (
        SELECT 1
          FROM qiita.prep_sample_to_study pts
          JOIN qiita.study s ON s.idx = pts.study_idx
          LEFT JOIN qiita.study_access sa
            ON sa.study_idx = pts.study_idx AND sa.principal_idx = {caller}
         WHERE pts.prep_sample_idx = {alias}.prep_sample_idx
           AND pts.retired = false
           AND s.owner_idx IS DISTINCT FROM {caller}
           AND (sa.access_tier IS NULL OR sa.access_tier <> {tier}::qiita.tier)
    )
"""

# Both reads on a gate exclude an entity-retired prep_sample, so a list's tally and
# the roster it invites the caller to fetch count the same set. Applied to the
# shared roster CTE, not to either query alone. Distinct from the
# `prep_sample_to_study.retired` LINK flag inside the predicate above.
_SAMPLE_NOT_RETIRED = """
    EXISTS (
        SELECT 1 FROM qiita.prep_sample ps_live
         WHERE ps_live.idx = {alias}.prep_sample_idx AND ps_live.retired = false
    )
"""


def sample_scope_sql(
    *,
    alias: str,
    args: list,
    sequenced_pool_idx: int | None,
    prep_sample_idx: int | None,
    visible_to_principal_idx: int | None,
) -> tuple[str, bool]:
    """Build the roster-narrowing clauses, appending each bound value to `args`.
    Returns (sql, narrowed), where `narrowed` is True iff a caller-supplied
    narrowing was applied.

    The SQL is ANDed onto a query whose FROM carries the roster CTE aliased
    `alias`. The retirement exclusion is unconditional and does not count as a
    narrowing — it bounds both reads identically rather than reflecting anything
    the caller asked for. The three that do: `sequenced_pool_idx` joins through
    qiita.sequenced_sample, `prep_sample_idx` matches directly, and
    `visible_to_principal_idx` applies the per-study predicate. Pass None for
    `visible_to_principal_idx` only for a caller holding the bypass role — it
    means "see every sample".
    """
    # `alias` is interpolated into SQL, not bound. Both callers pass a module
    # constant, so nothing reaches this from a request today; the check is what
    # keeps that true of a third caller.
    if not alias.isidentifier():
        raise ValueError(f"roster alias must be a bare SQL identifier, got {alias!r}")
    clauses = " AND " + _SAMPLE_NOT_RETIRED.format(alias=alias)
    narrowed = False
    if sequenced_pool_idx is not None:
        narrowed = True
        args.append(sequenced_pool_idx)
        clauses += (
            f" AND EXISTS (SELECT 1 FROM qiita.sequenced_sample ss"
            f"              WHERE ss.prep_sample_idx = {alias}.prep_sample_idx"
            f"                AND ss.sequenced_pool_idx = ${len(args)})"
        )
    if prep_sample_idx is not None:
        narrowed = True
        args.append(prep_sample_idx)
        clauses += f" AND {alias}.prep_sample_idx = ${len(args)}"
    if visible_to_principal_idx is not None:
        narrowed = True
        args.append(visible_to_principal_idx)
        caller_param = f"${len(args)}"
        args.append(Tier.ADMIN.value)
        clauses += " AND " + _CALLER_MAY_SEE_SAMPLE.format(
            alias=alias, caller=caller_param, tier=f"${len(args)}"
        )
    return clauses, narrowed
