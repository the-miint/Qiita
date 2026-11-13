"""Per-sample resolution across a definition group: for each prep_sample, the member
of a mask, alignment or processing group whose results are used, or why none is.
The rules are stated here and nowhere else.

**A run** of a definition for a sample is read from the definition's gate row
(`mask_sample`, `alignment_sample`, `assembly_sample`) and from the tickets that
produce it:
  - a prep_sample-scoped ticket that names the definition: the sample's newest such
    ticket that is not cancelled (an older failed ticket that a later one
    superseded does not count);
  - every block ticket whose block covers the sample and is not abandoned;
  - a long-read-assembly ticket, which records no processing_idx: it counts for the
    processing whose params equal the params that the runner mints for it,
    recomputed from the ticket with the runner's own builder.
  Cancelled tickets are ignored. A ticket counts only once the runner has written
  its definition on it (a per-sample ticket not yet started names none, so it does
  not make a sample incomplete). A ticket is *unfinished* when it is waiting,
  running or failed.

**Which member.** Of the anchor's group (`Groups.group_of`), the highest-id member
under which the sample has a gate `completed`, `no_data` (assembly) or
`invalidated`, or an unfinished ticket. A `pending` gate with no unfinished ticket
(its tickets were cancelled) is not a run. No member → `not_processed` / `no_run`.

**That member's state**, first match wins:
  1. an unfinished ticket → `incomplete`: `in_progress` when one is waiting or
     running, else `failed`. This holds over a completed gate too (a same-version
     re-run, or a block re-armed by rerun-block).
  2. gate `invalidated` → `not_processed` / `withdrawn`.
  3. the definition deprecated → `not_processed` / `deprecated`.
  4. an upstream definition deprecated → `not_processed` / `upstream_deprecated`.
     Upstreams: an alignment's and a processing's `mask_idx`, and a de novo
     alignment's `processing_idx` (`definition_similarity.upstream_leaves`).
  5. each upstream, in that order, resolved for the sample in the upstream's group:
       - processed under another member → `incomplete` / `upstream_moved` (the
         sample needs re-running on that other member);
       - incomplete → `incomplete` / `upstream_incomplete`;
       - not processed → `not_processed` / `upstream_withdrawn`,
         `upstream_deprecated` or `upstream_no_run`.
  6. otherwise `processed`.
A not-processed member is not replaced by an older one.

**Public API**:
  - `async resolve_group(conn, *, kind, definition_idx, prep_sample_idxs, groups=None)
    -> GroupResolution`, in the caller's REPEATABLE READ transaction (the reads
    span several statements); loads the groups when not given them. Raises
    `DefinitionNotFound`.
  - `GroupResolution.samples[prep_sample_idx] -> SampleResolution` (the
    qiita-common wire model) and `GroupResolution.not_resolved_to(idx)`.
  - `cohort_refusal(resolution, definition_idx)`: the `DefinitionGroupRefusal` for
    a sample that is not processed under exactly `definition_idx`, or None.
  - Pure core: `build_observations`, `match_assembly_tickets`, `resolve_sample`,
    `needed_definitions`.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, get_args

import asyncpg
from qiita_common.actions import context_schema_default
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
    canonical_json,
    is_definition_ref,
    load_groups,
    upstream_leaves,
)
from .repositories import require_transaction
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
    SOURCE_BLOCK_TICKET,
    SOURCE_GATE,
    SOURCE_SAMPLE_TICKET,
    AssemblyTicketCandidate,
    RunRow,
    fetch_assembly_ticket_candidates,
    fetch_run_rows,
)
from .runner._mask import MASK_IDX_BINDING
from .runner._processing import ASSEMBLER_BINDING, build_processing_params

__all__ = [
    "GroupResolution",
    "RunObservation",
    "build_observations",
    "cohort_refusal",
    "match_assembly_tickets",
    "needed_definitions",
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


# How an upstream's not-processed reason reads on the downstream sample.
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
    row) and its unfinished tickets as `(work_ticket_idx, state)`."""

    gate: str | None = None
    unfinished: tuple[tuple[int, WorkTicketState], ...] = ()


Observations = Mapping[DefinitionKind, Mapping[tuple[int, int], RunObservation]]


def build_observations(rows: Iterable[RunRow]) -> dict[tuple[int, int], RunObservation]:
    """One kind's observations, keyed `(definition_idx, prep_sample_idx)`, from its
    gate rows and producing tickets (cancelled ones already excluded). Of a
    sample's per-sample tickets only the newest counts; every block ticket counts."""
    gates: dict[tuple[int, int], str] = {}
    newest_sample_ticket: dict[tuple[int, int], tuple[int, WorkTicketState]] = {}
    block_tickets: dict[tuple[int, int], set[tuple[int, WorkTicketState]]] = {}
    for row in rows:
        key = (row.definition_idx, row.prep_sample_idx)
        if row.source == SOURCE_GATE:
            gates[key] = row.state
            continue
        if row.work_ticket_idx is None:
            raise ValueError(f"a {row.source} row carries no work_ticket_idx: {row}")
        ticket = (row.work_ticket_idx, WorkTicketState(row.state))
        if row.source == SOURCE_SAMPLE_TICKET:
            if key not in newest_sample_ticket or ticket[0] > newest_sample_ticket[key][0]:
                newest_sample_ticket[key] = ticket
        elif row.source == SOURCE_BLOCK_TICKET:
            block_tickets.setdefault(key, set()).add(ticket)
        else:
            raise ValueError(f"unknown run row source {row.source!r}")
    out: dict[tuple[int, int], RunObservation] = {}
    for key in gates.keys() | newest_sample_ticket.keys() | block_tickets.keys():
        tickets = set(block_tickets.get(key, ()))
        if key in newest_sample_ticket:
            tickets.add(newest_sample_ticket[key])
        out[key] = RunObservation(
            gate=gates.get(key),
            unfinished=tuple(
                sorted(
                    t
                    for t in tickets
                    if _TICKET_ROLE[t[1]] in (_TicketRole.RUNNING, _TicketRole.FAILED)
                )
            ),
        )
    return out


def match_assembly_tickets(
    candidates: Iterable[AssemblyTicketCandidate],
    processing_params: Mapping[int, Mapping[str, Any]],
) -> list[RunRow]:
    """The candidates whose minted params equal one of `processing_params`
    (`{processing_idx: params}`), as per-sample ticket rows of that processing."""
    by_params = {canonical_json(dict(params)): idx for idx, params in processing_params.items()}
    out: list[RunRow] = []
    for c in candidates:
        bound = {**c.action_context, MASK_IDX_BINDING: c.mask_idx}
        params = build_processing_params(
            c.action_id,
            c.action_version,
            bound,
            assembler_default=context_schema_default(c.context_schema, ASSEMBLER_BINDING),
        )
        idx = by_params.get(canonical_json(params))
        if idx is not None:
            out.append(
                RunRow(
                    source=SOURCE_SAMPLE_TICKET,
                    definition_idx=idx,
                    prep_sample_idx=c.prep_sample_idx,
                    state=c.state,
                    work_ticket_idx=c.work_ticket_idx,
                )
            )
    return out


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


def needed_definitions(
    groups: Groups, kind: DefinitionKind, definition_idx: int
) -> dict[DefinitionKind, set[int]]:
    """Every definition that resolving `kind` `definition_idx`'s group reads runs of:
    the group's members and, recursively, the groups of their upstreams."""
    out: dict[DefinitionKind, set[int]] = {}
    stack = [(kind, definition_idx)]
    while stack:
        k, idx = stack.pop()
        if idx in out.get(k, ()):
            continue
        members = groups.group_of(k, idx)
        out.setdefault(k, set()).update(members)
        for member in members:
            stack.extend(upstreams(groups, k, member))
    return out


def _has_run(kind: DefinitionKind, obs: RunObservation) -> bool:
    return bool(obs.unfinished) or _gate_role(kind, obs.gate) is not _GateRole.NONE


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
) -> SampleResolution:
    """`prep_sample_idx`'s resolution in `kind` `definition_idx`'s group, by the
    rules in the module docstring. `observations` must cover
    `needed_definitions(groups, kind, definition_idx)`; a missing entry reads as
    no gate row and no ticket."""
    kind_obs = observations.get(kind, {})
    empty = RunObservation()
    with_run = [
        m
        for m in groups.group_of(kind, definition_idx)
        if _has_run(kind, kind_obs.get((m, prep_sample_idx), empty))
    ]

    def result(state, **kw) -> SampleResolution:
        return _resolution(kind, prep_sample_idx, state, groups=groups, **kw)

    if not with_run:
        return result(SampleResolutionState.NOT_PROCESSED, reason=SampleResolutionReason.NO_RUN)
    member = max(with_run)
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
    if definition is not None and definition.deprecated:
        return result(
            SampleResolutionState.NOT_PROCESSED,
            definition_idx=member,
            reason=SampleResolutionReason.DEPRECATED,
        )
    ups = upstreams(groups, kind, member)
    for up_kind, up_idx in ups:
        up_definition = groups.definitions.get((up_kind, up_idx))
        if up_definition is not None and up_definition.deprecated:
            return result(
                SampleResolutionState.NOT_PROCESSED,
                definition_idx=member,
                reason=SampleResolutionReason.UPSTREAM_DEPRECATED,
                upstream=(up_kind, up_idx),
            )
    for up_kind, up_idx in ups:
        up = resolve_sample(groups, up_kind, up_idx, prep_sample_idx, observations)
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
    return result(SampleResolutionState.PROCESSED, definition_idx=member)


def cohort_refusal(
    resolution: SampleResolution, definition_idx: int
) -> DefinitionGroupRefusal | None:
    """None when `resolution` is `processed` under exactly `definition_idx`; else
    why a cohort naming `definition_idx` may not include the sample."""
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


@dataclass(frozen=True)
class GroupResolution:
    """The resolution of `prep_sample_idxs` in one group. `group` is the members,
    ascending; `samples` is keyed by prep_sample_idx, ascending."""

    kind: DefinitionKind
    definition_idx: int
    group: tuple[int, ...]
    samples: Mapping[int, SampleResolution]

    def not_resolved_to(self, definition_idx: int) -> list[SampleResolution]:
        """The samples not `processed` under exactly `definition_idx`, ascending."""
        return [r for r in self.samples.values() if cohort_refusal(r, definition_idx) is not None]


async def _load_observations(
    conn: asyncpg.Connection,
    groups: Groups,
    needed: Mapping[DefinitionKind, set[int]],
    prep_sample_idxs: Sequence[int],
) -> dict[DefinitionKind, dict[tuple[int, int], RunObservation]]:
    out: dict[DefinitionKind, dict[tuple[int, int], RunObservation]] = {}
    for kind, idxs in needed.items():
        rows = await fetch_run_rows(conn, kind, sorted(idxs), prep_sample_idxs)
        if kind is DefinitionKind.PROCESSING:
            candidates = await fetch_assembly_ticket_candidates(conn, prep_sample_idxs)
            rows.extend(
                match_assembly_tickets(
                    candidates, {idx: groups.params_of(kind, idx) for idx in idxs}
                )
            )
        out[kind] = build_observations(rows)
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
    DefinitionNotFound when `definition_idx` is not a `kind` definition."""
    require_transaction(conn)
    if groups is None:
        groups = await load_groups(conn)
    if (kind, definition_idx) not in groups.definitions:
        raise DefinitionNotFound(kind, definition_idx)
    samples = sorted(set(prep_sample_idxs))
    needed = needed_definitions(groups, kind, definition_idx)
    observations = await _load_observations(conn, groups, needed, samples)
    return GroupResolution(
        kind=kind,
        definition_idx=definition_idx,
        group=tuple(sorted(groups.group_of(kind, definition_idx))),
        samples={s: resolve_sample(groups, kind, definition_idx, s, observations) for s in samples},
    )
