"""Reference-row mutations callable from both routes and the runner.

The runner (in-process) and the PATCH /reference/{idx}/status route share
this transition logic so the validation matrix and TOCTOU-safe UPDATE
have one home.
"""

from __future__ import annotations

import asyncpg
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    TERMINAL_WORK_TICKET_STATES,
    VALID_STATUS_TRANSITIONS,
    ReferenceResponse,
    ReferenceStatus,
)

# Column projection backing every ReferenceResponse. Imported by
# routes/reference.py too, so the two callers can't drift (they previously
# kept hand-synced copies).
REFERENCE_RETURNING = (
    "reference_idx, name, version, kind, status, is_host, created_by_idx, created_at"
)


class ReferenceNotFound(Exception):
    """Raised when the reference_idx doesn't exist."""


# Work-ticket states that block a reference delete. In-flight states block
# unconditionally (a running job is reading/writing the reference's data);
# terminal states block only without `force` (a completed test run is exactly
# what an admin purging a test reference wants gone).
#
# Between them these cover EVERY state, which is the point: a state in neither
# arm is invisible to the gate, and since the cascade below is state-blind, the
# delete would proceed unforced and purge its tickets anyway.
_WORK_TICKET_IN_FLIGHT_STATES = NON_TERMINAL_WORK_TICKET_STATES
_WORK_TICKET_TERMINAL_STATES = TERMINAL_WORK_TICKET_STATES


class ReferenceDeleteBlocked(Exception):
    """Raised when a reference cannot be deleted. `alignment_definitions` blocks
    UNCONDITIONALLY (even with force); `in_flight` work tickets always block;
    `terminal` work tickets block only when the caller did not pass force=True."""

    def __init__(
        self,
        *,
        reference_idx: int,
        in_flight: int,
        terminal: int,
        alignment_definitions: int = 0,
    ) -> None:
        self.reference_idx = reference_idx
        self.in_flight = in_flight
        self.terminal = terminal
        self.alignment_definitions = alignment_definitions
        # Alignment definitions are the force-proof reason and the one the operator
        # most needs to act on, so it is reported first when present. A reference
        # delete cascades neither `alignment_definition` nor the DuckLake `alignment`
        # rows it owns (those are keyed on `feature_idx`, which this delete would
        # orphan-GC in both stores), and the data plane's delete_reference does not
        # touch `alignment` — so force cannot make this delete safe. The
        # DELETE /alignment-definition/{idx} route DOES purge the lake rows and
        # cascade its gates, so send the operator there first.
        if alignment_definitions:
            reason = (
                f"{alignment_definitions} alignment definition(s) align against it; "
                "force cannot be used because it cannot clean the DuckLake "
                "`alignment` rows they own (keyed on feature_idx this delete would "
                "orphan). Delete each via DELETE /alignment-definition/{idx} first "
                "(that route purges the lake rows and cascades its gates), then retry"
            )
        elif in_flight:
            reason = (
                f"{in_flight} in-flight work ticket(s) "
                f"({'/'.join(_WORK_TICKET_IN_FLIGHT_STATES)}) reference it; "
                "wait for them to finish or cancel them"
            )
        else:
            reason = (
                f"{terminal} terminal work ticket(s) "
                f"({'/'.join(_WORK_TICKET_TERMINAL_STATES)}) reference it; "
                "re-issue with force=true to delete them too"
            )
        super().__init__(f"Reference {reference_idx} cannot be deleted: {reason}")


def _rowcount(status: str) -> int:
    """Parse the affected-row count out of an asyncpg command tag
    (e.g. 'DELETE 5' → 5). Returns 0 for an unparseable tag rather than
    raising — the count is informational, not a control signal."""
    try:
        return int(status.rsplit(" ", 1)[1])
    except IndexError, ValueError:
        return 0


async def assert_reference_deletable(
    conn: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    *,
    force: bool,
) -> str:
    """Existence + work-ticket gating precheck for a reference delete.

    Returns the reference's current status on success. Raises
    ReferenceNotFound if it doesn't exist, or ReferenceDeleteBlocked if work
    tickets reference it (in-flight always; terminal unless force) or if any
    alignment definition aligns against it (always, even with force). Run this
    *before* any destructive step so a blocked delete touches nothing."""
    status = await conn.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if status is None:
        raise ReferenceNotFound(reference_idx)
    rows = await conn.fetch(
        "SELECT state, count(*) AS n FROM qiita.work_ticket"
        " WHERE reference_idx = $1 GROUP BY state",
        reference_idx,
    )
    counts = {r["state"]: r["n"] for r in rows}
    in_flight = sum(counts.get(s, 0) for s in _WORK_TICKET_IN_FLIGHT_STATES)
    terminal = sum(counts.get(s, 0) for s in _WORK_TICKET_TERMINAL_STATES)
    # Align-block work tickets are block-scoped and leave work_ticket.reference_idx
    # NULL, so the gate above cannot see them — the reference↔alignment link lives
    # only in alignment_definition.params (JSON `reference_idx`, an int; there is no
    # FK). Gate on that row directly: it is the durable record that an alignment was
    # built against this reference, and it outlives the (transient) work tickets.
    # This blocks EVEN WITH force, because neither the Postgres cascade nor the data
    # plane's delete_reference touches the DuckLake `alignment` rows those
    # definitions own (keyed on feature_idx this delete would orphan-GC in both
    # stores) — so no force can make the delete safe. DELETE /alignment-definition
    # first purges the lake rows and cascades the gates.
    alignment_definitions = await conn.fetchval(
        "SELECT count(*) FROM qiita.alignment_definition"
        " WHERE (params->>'reference_idx')::bigint = $1",
        reference_idx,
    )
    if alignment_definitions or in_flight or (terminal and not force):
        raise ReferenceDeleteBlocked(
            reference_idx=reference_idx,
            in_flight=in_flight,
            terminal=terminal,
            alignment_definitions=alignment_definitions,
        )
    return status


async def delete_reference_cascade(
    conn: asyncpg.Connection,
    reference_idx: int,
) -> dict[str, int]:
    """Tear down every Postgres row owned by a reference, in FK-dependency
    order, ending with the `qiita.reference` row itself. Must run inside the
    caller's transaction; the caller must have already gated via
    `assert_reference_deletable`.

    The schema uses ON DELETE RESTRICT throughout (no cascades), so order is
    explicit: work_ticket (→ work_ticket_step CASCADEs) → reference_index →
    reference_membership → annotation_to_term → reference_annotation → orphan
    annotation_term → orphan feature_genome/feature → orphan genome → reference.
    Features, genomes and terms are deleted only when *orphaned* — claimed by no
    other reference — so a shared one survives.

    Returns the per-table delete counts for the caller's response."""
    # Orphan features: this reference's features that no other reference claims.
    #
    # A reference claims a feature in TWO ways, and both count: as a member
    # (reference_membership — a whole sequence, indexed and aligned against) or as
    # an annotated interval (reference_annotation — a SynDNA insert on its plasmid,
    # minted its own feature_idx but deliberately kept OUT of membership). Reading
    # only membership here would leave every annotated feature_idx behind forever,
    # referenced by nothing; reading only membership on the *right* side of the
    # EXCEPT would delete a feature another reference still annotates.
    #
    # Computed before the DELETEs below (the EXCEPT needs this reference's rows
    # present). This set MUST match the data-plane orphan computation in
    # qiita-data-plane's flight_service.rs::delete_reference — the two stores GC the
    # same features independently, so a change to either query must change the other
    # or sequences/features desync across stores.
    orphan_features = [
        r["feature_idx"]
        for r in await conn.fetch(
            "  SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1"
            "  UNION"
            "  SELECT feature_idx FROM qiita.reference_annotation WHERE reference_idx = $1"
            " EXCEPT"
            " ( SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx <> $1"
            "  UNION"
            "  SELECT feature_idx FROM qiita.reference_annotation WHERE reference_idx <> $1)",
            reference_idx,
        )
    ]

    work_ticket_deleted = _rowcount(
        await conn.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx)
    )
    index_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )
    )
    membership_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
        )
    )
    # The terms THIS reference cites. Captured BEFORE the junction rows go, because the
    # junction is the only thing that records the citation — after the delete below there
    # is no way back from a reference to the terms it used to name.
    cited_terms = [
        r["annotation_term_idx"]
        for r in await conn.fetch(
            "SELECT DISTINCT l.annotation_term_idx FROM qiita.annotation_to_term l"
            " JOIN qiita.reference_annotation ra ON ra.annotation_idx = l.annotation_idx"
            " WHERE ra.reference_idx = $1",
            reference_idx,
        )
    ]

    # Must precede the reference_annotation delete: annotation_to_term FKs
    # annotation_idx with ON DELETE RESTRICT.
    annotation_term_link_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.annotation_to_term l"
            " USING qiita.reference_annotation ra"
            " WHERE l.annotation_idx = ra.annotation_idx AND ra.reference_idx = $1",
            reference_idx,
        )
    )
    # Must precede the qiita.feature delete below: reference_annotation FKs BOTH
    # feature_idx and parent_feature_idx with ON DELETE RESTRICT, so an orphan
    # feature that is still claimed by one of these rows cannot be removed.
    annotation_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.reference_annotation WHERE reference_idx = $1", reference_idx
        )
    )
    # Orphan terms: a term is GLOBAL (deduplicated on (system, system_id) across every
    # reference), so it goes only once NO annotation anywhere still cites it — the same
    # orphan rule qiita.feature and qiita.genome get.
    #
    # Scoped to the terms THIS reference cited (captured before the junction delete
    # above), not a whole-table sweep: an unscoped DELETE would make the returned count
    # unattributable to this reference, and would quietly become this endpoint's
    # responsibility to GC terms leaked by some other path.
    annotation_term_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.annotation_term t"
            " WHERE t.annotation_term_idx = ANY($1::bigint[])"
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM qiita.annotation_to_term l"
            "     WHERE l.annotation_term_idx = t.annotation_term_idx)",
            cited_terms,
        )
    )

    if orphan_features:
        # Capture the genomes these features mapped to before deleting the
        # junction rows, so we can GC any genome left with no features.
        candidate_genomes = [
            r["genome_idx"]
            for r in await conn.fetch(
                "SELECT DISTINCT genome_idx FROM qiita.feature_genome"
                " WHERE feature_idx = ANY($1::bigint[])",
                orphan_features,
            )
        ]
        await conn.execute(
            "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
            orphan_features,
        )
        await conn.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
            orphan_features,
        )
        if candidate_genomes:
            await conn.execute(
                "DELETE FROM qiita.genome g WHERE g.genome_idx = ANY($1::bigint[])"
                " AND NOT EXISTS ("
                "   SELECT 1 FROM qiita.feature_genome fg WHERE fg.genome_idx = g.genome_idx"
                ")",
                candidate_genomes,
            )

    await conn.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)

    return {
        "membership_deleted": membership_deleted,
        "annotation_deleted": annotation_deleted,
        "annotation_term_link_deleted": annotation_term_link_deleted,
        "annotation_term_deleted": annotation_term_deleted,
        "index_deleted": index_deleted,
        "work_ticket_deleted": work_ticket_deleted,
        "orphan_feature_count": len(orphan_features),
    }


class IllegalStatusTransition(Exception):
    """Raised when the current status can't transition to the target."""

    def __init__(self, *, current: str | None, target: ReferenceStatus) -> None:
        super().__init__(f"Cannot transition from {current!r} to {target!r}")
        self.current = current
        self.target = target


async def transition_reference_status(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    target: ReferenceStatus,
) -> ReferenceResponse:
    """Atomically transition a reference's status, validated against
    qiita_common.models.VALID_STATUS_TRANSITIONS.

    Raises ReferenceNotFound if the row doesn't exist; IllegalStatusTransition
    if no source status maps to `target`, or if the row is in a state that
    cannot reach `target`.
    """
    valid_sources = [
        str(src) for src, targets in VALID_STATUS_TRANSITIONS.items() if target in targets
    ]
    if not valid_sources:
        raise IllegalStatusTransition(current=None, target=target)

    row = await pool.fetchrow(
        "UPDATE qiita.reference SET status = $1"
        " WHERE reference_idx = $2 AND status = ANY($3::text[])"
        f" RETURNING {REFERENCE_RETURNING}",
        str(target),
        reference_idx,
        valid_sources,
    )
    if row is not None:
        return ReferenceResponse(**dict(row))

    current = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if current is None:
        raise ReferenceNotFound(reference_idx)
    raise IllegalStatusTransition(current=current, target=target)
