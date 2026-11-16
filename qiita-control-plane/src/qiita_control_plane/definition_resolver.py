"""Per-sample resolution across a definition group: for each prep_sample, the member
of a mask, alignment or processing group whose results are used, or why none is.
The rules are stated here and nowhere else.

**A run** of a definition for a prep_sample is read from the definition's gate row
(`mask_sample`, `alignment_sample`, `assembly_sample`) and from the tickets that
produce it:
  - a prep_sample-scoped ticket that names the definition: the prep_sample's newest such
    ticket that is not cancelled (an older failed ticket that a later one
    superseded does not count);
  - every block ticket whose block covers the prep_sample and is not abandoned.
  Cancelled tickets are ignored. A ticket counts only once the runner has written
  its definition on it (a per-sample ticket not yet started names none, so it does
  not make a prep_sample incomplete). A ticket is *unfinished* when it is waiting,
  running or failed.
  A failed ticket is *stale*, and ignored, when the gate row is `completed`,
  `no_data` or `invalidated` and changed after the ticket was created (the gate's
  `updated_at` is later than the ticket's `created_at`). Withdrawing a run, re-running
  it, which fails because the withdrawn gate does not reopen, and then restoring the
  gate leaves such a ticket behind the restored run. A failed ticket under a
  `pending` gate, or with no gate row, is never stale: rerun-block re-arms a block
  ticket in place, keeping its `created_at`, after setting its gates `pending`.
  Waiting and running tickets are never stale.

**Which member.** Of the anchor's group (`Groups.group_of`), the highest-id member
under which the prep_sample has a gate `completed`, `no_data` (assembly) or
`invalidated`, or an unfinished ticket. A `pending` gate with no unfinished ticket
(its tickets were cancelled) is not a run. A de novo alignment with none of these
whose assembly (its `processing_idx`) has gate `no_data` for the prep_sample, and no
unfinished ticket, is a candidate too: the assembly produced no contigs, so there is
nothing to align. No member → `not_processed` / `no_run`.

**That member's state**, first match wins:
  1. an unfinished ticket → `incomplete`: `in_progress` when one is waiting or
     running, else `failed`. This holds over a completed gate too (a same-version
     re-run, or a block re-armed by rerun-block).
  2. gate `invalidated` → `not_processed` / `withdrawn`.
  3. the definition deprecated → `not_processed` / `deprecated`.
  4. an upstream definition deprecated → `not_processed` / `upstream_deprecated`.
     Upstreams: an alignment's and a processing's `mask_idx`, and a de novo
     alignment's `processing_idx` (`definition_similarity.upstream_leaves`).
  5. each upstream, in that order, resolved for the prep_sample in the upstream's group:
       - processed under another member → `incomplete` / `upstream_moved` (the
         prep_sample needs re-running on that other member);
       - incomplete → `incomplete` / `upstream_incomplete`;
       - not processed → `not_processed` / `upstream_withdrawn`,
         `upstream_deprecated` or `upstream_no_run`.
  6. otherwise `processed`, or, for a de novo alignment chosen because its assembly
     produced no contigs, `not_processed` / `no_data`: there is nothing to read.
A not-processed member is not replaced by an older one.

**Exact mode** (`resolve_exact`) reads one definition's own runs, for a caller that
names a deprecated definition on purpose. The member is the named definition only,
never another of its group; rules 1, 2, 5 and 6 apply as above; rules 3 and 4 (the
definition or an upstream deprecated) are not checked; and each upstream is
resolved in exact mode too (its own definition, no group), so rule 5's
`upstream_moved` cannot arise.

**Public API**:
  - `async resolve_group(conn, *, kind, definition_idx, prep_sample_idxs, groups=None)
    -> GroupResolution`, in the caller's transaction, which must be REPEATABLE READ;
    loads the groups when not given them. Raises `DefinitionNotFound`.
    `resolve_exact`, with the same signature, resolves in exact mode.
  - `GroupResolution.samples[prep_sample_idx] -> SampleResolution` (the
    qiita-common wire model) and `GroupResolution.not_resolved_to(idx)`.
  - `require_observable(groups, kind, definition_idx)`: raises
    `DefinitionNotObservable` for a group whose resolution would read a processing of
    a workflow whose runs are not read here (`PROCESSING_RUN_WORKFLOWS`); every
    prep_sample would read `no_run`. `resolve_group` calls it.
  - `cohort_refusal(resolution, definition_idx)`: the `DefinitionGroupRefusal` for
    a prep_sample that is not processed under exactly `definition_idx`, or None.
  - Pure core: `build_observations`, `resolve_sample`, `needed_definitions`.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import get_args

import asyncpg
from qiita_common.models import (
    AlignmentSampleState,
    AssemblySampleState,
    DefinitionGroupRefusal,
    DefinitionKind,
    MaskSampleState,
    SampleResolution,
    SampleResolutionReason,
    SampleResolutionState,
    WorkTicketState,
)

from .definition_similarity import (
    DefinitionNotFound,
    Groups,
    is_definition_ref,
    load_groups,
    upstream_leaves,
)
from .repositories import SNAPSHOT_ISOLATION_LEVELS, require_transaction, transaction_isolation
from .repositories.alignment_definition import (
    ALIGNMENT_SAMPLE_COMPLETED,
    ALIGNMENT_SAMPLE_INVALIDATED,
    ALIGNMENT_SAMPLE_PENDING,
)
from .repositories.assembly import (
    ASSEMBLY_SAMPLE_COMPLETED,
    ASSEMBLY_SAMPLE_INVALIDATED,
    ASSEMBLY_SAMPLE_NO_DATA,
    ASSEMBLY_SAMPLE_PENDING,
)
from .repositories.block import (
    MASK_SAMPLE_COMPLETED,
    MASK_SAMPLE_INVALIDATED,
    MASK_SAMPLE_PENDING,
)
from .repositories.definition_run import (
    PROCESSING_RUN_WORKFLOWS,
    SOURCE_BLOCK_TICKET,
    SOURCE_GATE,
    SOURCE_SAMPLE_TICKET,
    RunRow,
    fetch_run_rows,
)

__all__ = [
    "DefinitionNotObservable",
    "GroupResolution",
    "RunObservation",
    "build_observations",
    "cohort_refusal",
    "needed_definitions",
    "require_observable",
    "resolve_exact",
    "resolve_group",
    "resolve_sample",
    "upstreams",
]


class _GateRole(Enum):
    NONE = "none"  # a gate row with no run behind it (its tickets were cancelled)
    DONE = "done"
    WITHDRAWN = "withdrawn"


class _TicketRole(Enum):
    RUNNING = "running"
    FAILED = "failed"
    FINISHED = "finished"
    IGNORED = "ignored"


# Every gate state of each kind, mapped; checked against the gate's declared states
# at import, so a state added there fails here instead of reading as no run.
# Assembly's `no_data` is done: the run ended with nothing to assemble.
_GATE_ROLE: dict[DefinitionKind, dict[str, _GateRole]] = {
    DefinitionKind.MASK: {
        MASK_SAMPLE_PENDING: _GateRole.NONE,
        MASK_SAMPLE_COMPLETED: _GateRole.DONE,
        MASK_SAMPLE_INVALIDATED: _GateRole.WITHDRAWN,
    },
    DefinitionKind.ALIGNMENT: {
        ALIGNMENT_SAMPLE_PENDING: _GateRole.NONE,
        ALIGNMENT_SAMPLE_COMPLETED: _GateRole.DONE,
        ALIGNMENT_SAMPLE_INVALIDATED: _GateRole.WITHDRAWN,
    },
    DefinitionKind.PROCESSING: {
        ASSEMBLY_SAMPLE_PENDING: _GateRole.NONE,
        ASSEMBLY_SAMPLE_COMPLETED: _GateRole.DONE,
        ASSEMBLY_SAMPLE_NO_DATA: _GateRole.DONE,
        ASSEMBLY_SAMPLE_INVALIDATED: _GateRole.WITHDRAWN,
    },
}
for _kind, _declared in (
    (DefinitionKind.MASK, MaskSampleState),
    (DefinitionKind.ALIGNMENT, AlignmentSampleState),
    (DefinitionKind.PROCESSING, AssemblySampleState),
):
    if set(_GATE_ROLE[_kind]) != set(get_args(_declared)):
        raise RuntimeError(f"_GATE_ROLE[{_kind.value}] does not map every declared gate state")

# Every ticket state, mapped; checked against WorkTicketState at import.
_TICKET_ROLE: dict[WorkTicketState, _TicketRole] = {
    WorkTicketState.PENDING: _TicketRole.RUNNING,
    WorkTicketState.QUEUED: _TicketRole.RUNNING,
    WorkTicketState.PROCESSING: _TicketRole.RUNNING,
    WorkTicketState.FAILED: _TicketRole.FAILED,
    WorkTicketState.COMPLETED: _TicketRole.FINISHED,
    WorkTicketState.NO_DATA: _TicketRole.FINISHED,
    WorkTicketState.CANCELLED: _TicketRole.IGNORED,
}
if set(_TICKET_ROLE) != set(WorkTicketState):
    raise RuntimeError("_TICKET_ROLE does not map every WorkTicketState")


def _gate_role(kind: DefinitionKind, gate: str | None) -> _GateRole:
    if gate is None:
        return _GateRole.NONE
    try:
        return _GATE_ROLE[kind][gate]
    except KeyError:
        raise ValueError(f"unknown {kind.value} gate state {gate!r}") from None


# How an upstream's not-processed reason reads on the downstream prep_sample.
_UPSTREAM_NOT_PROCESSED = {
    SampleResolutionReason.NO_RUN: SampleResolutionReason.UPSTREAM_NO_RUN,
    SampleResolutionReason.UPSTREAM_NO_RUN: SampleResolutionReason.UPSTREAM_NO_RUN,
    SampleResolutionReason.WITHDRAWN: SampleResolutionReason.UPSTREAM_WITHDRAWN,
    SampleResolutionReason.UPSTREAM_WITHDRAWN: SampleResolutionReason.UPSTREAM_WITHDRAWN,
    SampleResolutionReason.DEPRECATED: SampleResolutionReason.UPSTREAM_DEPRECATED,
    SampleResolutionReason.UPSTREAM_DEPRECATED: SampleResolutionReason.UPSTREAM_DEPRECATED,
}


# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunObservation:
    """What is known of one `(definition, prep_sample)`: its gate state (None when no
    row) and its unfinished tickets as `(work_ticket_idx, state)`, stale failed
    tickets left out."""

    gate: str | None = None
    unfinished: tuple[tuple[int, WorkTicketState], ...] = ()


Observations = Mapping[DefinitionKind, Mapping[tuple[int, int], RunObservation]]


def build_observations(
    kind: DefinitionKind, rows: Iterable[RunRow]
) -> dict[tuple[int, int], RunObservation]:
    """One kind's observations, keyed `(definition_idx, prep_sample_idx)`, from its
    gate rows and producing tickets (cancelled ones already excluded). Of a
    prep_sample's per-sample tickets only the newest counts; every block ticket counts.
    A stale failed ticket (module docstring) is then dropped."""
    gates: dict[tuple[int, int], RunRow] = {}
    newest_sample_ticket: dict[tuple[int, int], RunRow] = {}
    block_tickets: dict[tuple[int, int], list[RunRow]] = {}
    for row in rows:
        key = (row.definition_idx, row.prep_sample_idx)
        if row.source == SOURCE_GATE:
            gates[key] = row
            continue
        if row.work_ticket_idx is None:
            raise ValueError(f"a {row.source} row carries no work_ticket_idx: {row}")
        if row.source == SOURCE_SAMPLE_TICKET:
            newest = newest_sample_ticket.get(key)
            if newest is None or row.work_ticket_idx > newest.work_ticket_idx:
                newest_sample_ticket[key] = row
        elif row.source == SOURCE_BLOCK_TICKET:
            block_tickets.setdefault(key, []).append(row)
        else:
            raise ValueError(f"unknown run row source {row.source!r}")
    out: dict[tuple[int, int], RunObservation] = {}
    for key in gates.keys() | newest_sample_ticket.keys() | block_tickets.keys():
        gate = gates.get(key)
        tickets = list(block_tickets.get(key, ()))
        if key in newest_sample_ticket:
            tickets.append(newest_sample_ticket[key])
        unfinished: set[tuple[int, WorkTicketState]] = set()
        for t in tickets:
            state = WorkTicketState(t.state)
            role = _TICKET_ROLE[state]
            if role is _TicketRole.RUNNING or (
                role is _TicketRole.FAILED and not _stale(kind, t, gate)
            ):
                unfinished.add((t.work_ticket_idx, state))
        out[key] = RunObservation(
            gate=None if gate is None else gate.state, unfinished=tuple(sorted(unfinished))
        )
    return out


def _stale(kind: DefinitionKind, failed: RunRow, gate: RunRow | None) -> bool:
    """Whether a failed ticket is stale: its gate row is not `pending` and changed
    after the ticket was created (module docstring, **A run**)."""
    return (
        gate is not None
        and _gate_role(kind, gate.state) is not _GateRole.NONE
        and gate.at > failed.at
    )


# ---------------------------------------------------------------------------
# Resolution (pure)
# ---------------------------------------------------------------------------


def upstreams(groups: Groups, kind: DefinitionKind, idx: int) -> list[tuple[DefinitionKind, int]]:
    """The definitions `kind` `idx` consumed, mask first."""
    params = groups.params_of(kind, idx)
    return [
        (up_kind, params[path])
        for path, up_kind in upstream_leaves(kind, params).items()
        if is_definition_ref(params.get(path))
    ]


def _members(groups: Groups, kind: DefinitionKind, idx: int, *, exact: bool) -> set[int]:
    """The definitions a resolution of `kind` `idx` chooses among: its group, or in
    exact mode `idx` alone."""
    return {idx} if exact else set(groups.group_of(kind, idx))


def needed_definitions(
    groups: Groups, kind: DefinitionKind, definition_idx: int, *, exact: bool = False
) -> dict[DefinitionKind, set[int]]:
    """Every definition that resolving `kind` `definition_idx`'s group reads runs of:
    the group's members and, recursively, the groups of their upstreams (in exact
    mode, the definition and its upstreams only)."""
    out: dict[DefinitionKind, set[int]] = {}
    stack = [(kind, definition_idx)]
    while stack:
        k, idx = stack.pop()
        if idx in out.get(k, ()):
            continue
        members = _members(groups, k, idx, exact=exact)
        out.setdefault(k, set()).update(members)
        for member in members:
            stack.extend(upstreams(groups, k, member))
    return out


def _has_run(kind: DefinitionKind, obs: RunObservation) -> bool:
    return bool(obs.unfinished) or _gate_role(kind, obs.gate) is not _GateRole.NONE


def _assembled_nothing(
    groups: Groups,
    kind: DefinitionKind,
    idx: int,
    prep_sample_idx: int,
    observations: Observations,
) -> bool:
    """True for a de novo alignment whose assembly has gate `no_data` and no unfinished
    ticket for the prep_sample."""
    if kind is not DefinitionKind.ALIGNMENT:
        return False
    for up_kind, up_idx in upstreams(groups, kind, idx):
        if up_kind is DefinitionKind.PROCESSING:
            obs = observations.get(up_kind, {}).get((up_idx, prep_sample_idx))
            return obs is not None and obs.gate == ASSEMBLY_SAMPLE_NO_DATA and not obs.unfinished
    return False


def _resolution(
    kind: DefinitionKind,
    prep_sample_idx: int,
    state: SampleResolutionState,
    *,
    groups: Groups,
    definition_idx: int | None = None,
    reason: SampleResolutionReason | None = None,
    work_ticket_idx: int | None = None,
    upstream: tuple[DefinitionKind, int | None] | None = None,
) -> SampleResolution:
    mask_idx = None
    if definition_idx is not None and kind is not DefinitionKind.MASK:
        value = groups.params_of(kind, definition_idx).get("mask_idx")
        mask_idx = value if is_definition_ref(value) else None
    return SampleResolution(
        prep_sample_idx=prep_sample_idx,
        state=state,
        definition_kind=kind,
        definition_idx=definition_idx,
        mask_idx=mask_idx,
        reason=reason,
        work_ticket_idx=work_ticket_idx,
        upstream_kind=None if upstream is None else upstream[0],
        upstream_definition_idx=None if upstream is None else upstream[1],
    )


def resolve_sample(
    groups: Groups,
    kind: DefinitionKind,
    definition_idx: int,
    prep_sample_idx: int,
    observations: Observations,
    *,
    exact: bool = False,
) -> SampleResolution:
    """`prep_sample_idx`'s resolution in `kind` `definition_idx`'s group, by the
    rules in the module docstring (in exact mode with `exact`). `observations` must
    cover `needed_definitions(groups, kind, definition_idx, exact=exact)`; a missing
    entry reads as no gate row and no ticket."""
    kind_obs = observations.get(kind, {})
    empty = RunObservation()
    with_run: list[int] = []
    nothing_to_align: list[int] = []
    for m in sorted(_members(groups, kind, definition_idx, exact=exact)):
        if _has_run(kind, kind_obs.get((m, prep_sample_idx), empty)):
            with_run.append(m)
        elif _assembled_nothing(groups, kind, m, prep_sample_idx, observations):
            nothing_to_align.append(m)

    def result(state, **kw) -> SampleResolution:
        return _resolution(kind, prep_sample_idx, state, groups=groups, **kw)

    if not with_run and not nothing_to_align:
        return result(SampleResolutionState.NOT_PROCESSED, reason=SampleResolutionReason.NO_RUN)
    member = max(with_run + nothing_to_align)
    obs = kind_obs.get((member, prep_sample_idx), empty)

    if obs.unfinished:
        running = [t for t, s in obs.unfinished if _TICKET_ROLE[s] is _TicketRole.RUNNING]
        failed = [t for t, s in obs.unfinished if _TICKET_ROLE[s] is _TicketRole.FAILED]
        return result(
            SampleResolutionState.INCOMPLETE,
            definition_idx=member,
            reason=SampleResolutionReason.IN_PROGRESS if running else SampleResolutionReason.FAILED,
            work_ticket_idx=max(running or failed),
        )
    if _gate_role(kind, obs.gate) is _GateRole.WITHDRAWN:
        return result(
            SampleResolutionState.NOT_PROCESSED,
            definition_idx=member,
            reason=SampleResolutionReason.WITHDRAWN,
        )
    definition = groups.definitions.get((kind, member))
    if not exact and definition is not None and definition.deprecated:
        return result(
            SampleResolutionState.NOT_PROCESSED,
            definition_idx=member,
            reason=SampleResolutionReason.DEPRECATED,
        )
    ups = upstreams(groups, kind, member)
    for up_kind, up_idx in ups:
        up_definition = groups.definitions.get((up_kind, up_idx))
        if not exact and up_definition is not None and up_definition.deprecated:
            return result(
                SampleResolutionState.NOT_PROCESSED,
                definition_idx=member,
                reason=SampleResolutionReason.UPSTREAM_DEPRECATED,
                upstream=(up_kind, up_idx),
            )
    for up_kind, up_idx in ups:
        up = resolve_sample(groups, up_kind, up_idx, prep_sample_idx, observations, exact=exact)
        if up.state is SampleResolutionState.PROCESSED:
            if up.definition_idx == up_idx:
                continue
            return result(
                SampleResolutionState.INCOMPLETE,
                definition_idx=member,
                reason=SampleResolutionReason.UPSTREAM_MOVED,
                upstream=(up_kind, up.definition_idx),
            )
        if up.state is SampleResolutionState.INCOMPLETE:
            return result(
                SampleResolutionState.INCOMPLETE,
                definition_idx=member,
                reason=SampleResolutionReason.UPSTREAM_INCOMPLETE,
                work_ticket_idx=up.work_ticket_idx,
                upstream=(up_kind, up.definition_idx),
            )
        return result(
            SampleResolutionState.NOT_PROCESSED,
            definition_idx=member,
            reason=_UPSTREAM_NOT_PROCESSED[up.reason],
            upstream=(up_kind, up.definition_idx if up.definition_idx is not None else up_idx),
        )
    if member in nothing_to_align:
        return result(
            SampleResolutionState.NOT_PROCESSED,
            definition_idx=member,
            reason=SampleResolutionReason.NO_DATA,
        )
    return result(SampleResolutionState.PROCESSED, definition_idx=member)


def cohort_refusal(
    resolution: SampleResolution, definition_idx: int
) -> DefinitionGroupRefusal | None:
    """None when `resolution` is `processed` under exactly `definition_idx`; else
    why a cohort naming `definition_idx` may not include the prep_sample."""
    if resolution.state is SampleResolutionState.PROCESSED:
        if resolution.definition_idx == definition_idx:
            return None
        return DefinitionGroupRefusal.RESOLVED_ELSEWHERE
    if resolution.state is SampleResolutionState.INCOMPLETE:
        return DefinitionGroupRefusal.INCOMPLETE
    return DefinitionGroupRefusal.NOT_PROCESSED


# ---------------------------------------------------------------------------
# Resolution (database)
# ---------------------------------------------------------------------------


class DefinitionNotObservable(Exception):
    """A group's resolution would read a processing whose runs the resolver does not
    read (`PROCESSING_RUN_WORKFLOWS`)."""

    def __init__(self, kind: DefinitionKind, idx: int, processing_idx: int, workflow: str | None):
        super().__init__(
            f"{kind.value} {idx}: resolving it reads processing {processing_idx}, a"
            f" {workflow!r} run; only runs of {', '.join(PROCESSING_RUN_WORKFLOWS)} are"
            f" read per prep_sample, so every prep_sample would read as having no run"
        )
        self.kind = kind
        self.idx = idx
        self.processing_idx = processing_idx
        self.workflow = workflow


def require_observable(
    groups: Groups, kind: DefinitionKind, definition_idx: int, *, exact: bool = False
) -> None:
    """Raise DefinitionNotObservable when resolving `kind` `definition_idx` would read a
    processing (a member, or an upstream of one) of a workflow outside
    `PROCESSING_RUN_WORKFLOWS`."""
    needed = needed_definitions(groups, kind, definition_idx, exact=exact)
    for processing_idx in sorted(needed.get(DefinitionKind.PROCESSING, ())):
        workflow = groups.definitions[(DefinitionKind.PROCESSING, processing_idx)].workflow
        if workflow not in PROCESSING_RUN_WORKFLOWS:
            raise DefinitionNotObservable(kind, definition_idx, processing_idx, workflow)


@dataclass(frozen=True)
class GroupResolution:
    """The resolution of `prep_sample_idxs` in one group. `group` is the members,
    ascending (the named definition alone when `exact`); `samples` is keyed by
    prep_sample_idx, ascending."""

    kind: DefinitionKind
    definition_idx: int
    group: tuple[int, ...]
    samples: Mapping[int, SampleResolution]
    exact: bool = False

    def not_resolved_to(self, definition_idx: int) -> list[SampleResolution]:
        """The prep_samples not `processed` under exactly `definition_idx`, ascending."""
        return [r for r in self.samples.values() if cohort_refusal(r, definition_idx) is not None]


async def _load_observations(
    conn: asyncpg.Connection,
    needed: Mapping[DefinitionKind, set[int]],
    prep_sample_idxs: Sequence[int],
) -> dict[DefinitionKind, dict[tuple[int, int], RunObservation]]:
    out: dict[DefinitionKind, dict[tuple[int, int], RunObservation]] = {}
    for kind, idxs in needed.items():
        out[kind] = build_observations(
            kind, await fetch_run_rows(conn, kind, sorted(idxs), prep_sample_idxs)
        )
    return out


async def resolve_group(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    definition_idx: int,
    prep_sample_idxs: Iterable[int],
    groups: Groups | None = None,
) -> GroupResolution:
    """Resolve each of `prep_sample_idxs` in `kind` `definition_idx`'s group, in the
    caller's REPEATABLE READ transaction. `groups` is loaded when not given; a caller
    passing its own must have loaded it in this transaction. Raises
    DefinitionNotFound when `definition_idx` is not a `kind` definition, and
    DefinitionNotObservable (`require_observable`)."""
    return await _resolve(
        conn,
        kind=kind,
        definition_idx=definition_idx,
        prep_sample_idxs=prep_sample_idxs,
        groups=groups,
        exact=False,
    )


async def resolve_exact(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    definition_idx: int,
    prep_sample_idxs: Iterable[int],
    groups: Groups | None = None,
) -> GroupResolution:
    """`resolve_group` in exact mode (module docstring): `definition_idx`'s own runs,
    its deprecation and its upstreams' not checked."""
    return await _resolve(
        conn,
        kind=kind,
        definition_idx=definition_idx,
        prep_sample_idxs=prep_sample_idxs,
        groups=groups,
        exact=True,
    )


async def _resolve(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    definition_idx: int,
    prep_sample_idxs: Iterable[int],
    groups: Groups | None,
    exact: bool,
) -> GroupResolution:
    require_transaction(conn)
    # The gates, tickets and groups are read in several statements; below REPEATABLE
    # READ each takes its own snapshot, and a gate read `pending` before its ticket
    # completed would read as no run at all.
    isolation = await transaction_isolation(conn)
    if isolation not in SNAPSHOT_ISOLATION_LEVELS:
        raise RuntimeError(
            f"resolve_group needs a REPEATABLE READ transaction; this one is {isolation!r}"
        )
    if groups is None:
        groups = await load_groups(conn)
    if (kind, definition_idx) not in groups.definitions:
        raise DefinitionNotFound(kind, definition_idx)
    require_observable(groups, kind, definition_idx, exact=exact)
    samples = sorted(set(prep_sample_idxs))
    needed = needed_definitions(groups, kind, definition_idx, exact=exact)
    observations = await _load_observations(conn, needed, samples)
    return GroupResolution(
        kind=kind,
        definition_idx=definition_idx,
        group=tuple(sorted(_members(groups, kind, definition_idx, exact=exact))),
        samples={
            s: resolve_sample(groups, kind, definition_idx, s, observations, exact=exact)
            for s in samples
        },
        exact=exact,
    )
